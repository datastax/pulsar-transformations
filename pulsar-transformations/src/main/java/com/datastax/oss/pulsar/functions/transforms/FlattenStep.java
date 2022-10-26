/*
 * Copyright DataStax, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datastax.oss.pulsar.functions.transforms;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

import lombok.Builder;
import lombok.RequiredArgsConstructor;
import lombok.Value;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.common.schema.SchemaType;

@Builder
public class FlattenStep implements TransformStep {
  // TODO: Cache schema to flattened schema
  // TODO: Microbenchmark the flatten algorithm for performance optimization
  // TODO: Validate flatten delimiter
  // TODO: Add integration test

  public static final String AVRO_READ_OFFSET_PROP = "__AVRO_READ_OFFSET__";

  private static final String DEFAULT_DELIMITER = "_"; // '.' in not valid in AVRO field names

  @Builder.Default private final String delimiter = DEFAULT_DELIMITER;
  private final String part;
  private final Map<org.apache.avro.Schema, FlattenedSchema> keySchemaCache =
      new ConcurrentHashMap<>();
  private final Map<org.apache.avro.Schema, FlattenedSchema> valueSchemaCache =
      new ConcurrentHashMap<>();

  @Override
  public void process(TransformContext transformContext) throws Exception {
    if (part == null) {
      validateAvro(transformContext.getKeySchema());
      validateAvro(transformContext.getValueSchema());
      GenericRecord avroKeyRecord = (GenericRecord) transformContext.getKeyObject();
      GenericRecord avroValueRecord = (GenericRecord) transformContext.getValueObject();
      transformContext.setKeyObject(flattenGenericRecord(avroKeyRecord, keySchemaCache));
      transformContext.setValueObject(flattenGenericRecord(avroValueRecord, valueSchemaCache));
      transformContext.setKeyModified(true);
      transformContext.setValueModified(true);
    } else if ("key".equals(part)) {
      validateAvro(transformContext.getKeySchema());
      GenericRecord avroKeyRecord = (GenericRecord) transformContext.getKeyObject();
      transformContext.setKeyObject(flattenGenericRecord(avroKeyRecord, keySchemaCache));
      transformContext.setKeyModified(true);
    } else if ("value".equals(part)) {
      validateAvro(transformContext.getValueSchema());
      GenericRecord avroValueRecord = (GenericRecord) transformContext.getValueObject();
      transformContext.setValueObject(flattenGenericRecord(avroValueRecord, valueSchemaCache));
      transformContext.setValueModified(true);
    } else {
      throw new IllegalArgumentException("Unsupported part for Flatten: " + part);
    }
  }

  private void validateAvro(Schema<?> schema) {
    if (schema == null) {
      throw new IllegalStateException("Flatten requires non-null schemas!");
    }

    if (schema.getSchemaInfo().getType() != SchemaType.AVRO) {
      throw new IllegalStateException(
          "Unsupported schema type for Flatten: " + schema.getSchemaInfo().getType());
    }
  }

  private GenericRecord flattenGenericRecord(
      GenericRecord record, Map<org.apache.avro.Schema, FlattenedSchema> schemaCache) {
    FlattenedSchema modified =
        schemaCache.computeIfAbsent(
            record.getSchema(),
            schema -> {
              Map<org.apache.avro.Schema.Field, List<String>> flattenedFieldMap = new HashMap<>();
              buildFlattenedFields(record.getSchema(), flattenedFieldMap);
              List<org.apache.avro.Schema.Field> fields =
                  new ArrayList<>(flattenedFieldMap.keySet());
              return new FlattenedSchema(buildFlattenedSchema(record.getSchema(), fields), flattenedFieldMap);
            });

    GenericRecord newRecord = new GenericData.Record(modified.getSchema());
    modified.getFieldMap().forEach((k, v) -> newRecord.put(k.name(), getFlattenedFieldValue(v, record)));
    return newRecord;
  }

  private Object getFlattenedFieldValue(
      List<String> fieldNameParts, GenericRecord originalRecord) {
    GenericRecord currentLevel = originalRecord;
    for (int level = 0; level < fieldNameParts.size() - 1; level++) {
      Object candidateRecord = currentLevel.get(fieldNameParts.get(level));
      if (candidateRecord == null) {
        return null;
      }
      currentLevel = (GenericRecord) candidateRecord;
    }

    return currentLevel.get(fieldNameParts.get(fieldNameParts.size() - 1));
  }

  private void buildFlattenedFields(
      org.apache.avro.Schema originalSchema,
      Map<org.apache.avro.Schema.Field, List<String>> flattenedFieldMap) {
    for (org.apache.avro.Schema.Field field : originalSchema.getFields()) {
      flattenField(
          originalSchema,
          field,
          field.schema().isNullable(),
          new LinkedList<>(),
          flattenedFieldMap);
    }
  }

  private org.apache.avro.Schema buildFlattenedSchema(
      org.apache.avro.Schema originalSchema, List<org.apache.avro.Schema.Field> flattenedFields) {
    org.apache.avro.Schema flattenedSchema =
        org.apache.avro.Schema.createRecord(
            originalSchema.getName(),
            originalSchema.getDoc(),
            originalSchema.getNamespace(),
            false,
            flattenedFields);
    originalSchema
        .getObjectProps()
        .entrySet()
        .stream()
        .filter(e -> !AVRO_READ_OFFSET_PROP.equals(e.getKey()))
        .forEach(e -> flattenedSchema.addProp(e.getKey(), e.getValue()));
    return flattenedSchema;
  }

  /**
   * @param schema the schema of the record that contains the field to be flattened. Each recursive
   *     call will pass the schema one level deeper. At any level, the schema should exist.
   * @param field the field to be flattened
   * @param nullable true if the current field schema is nullable, this has to be passed all the way
   *     to the last nested level because anytime one of the ancestors is null, the flattened field
   *     schema could be null even if it is not nullable on the original schema
   * @param flattenedFieldNameParts the field name parts that is built incrementally with each
   *     recursive call.
   * @return flattened key/value pairs.
   */
  private void flattenField(
      org.apache.avro.Schema schema,
      org.apache.avro.Schema.Field field,
      boolean nullable,
      LinkedList<String> flattenedFieldNameParts,
      Map<org.apache.avro.Schema.Field, List<String>> flattenedFieldMap) {
    flattenedFieldNameParts.addLast(field.name());
    org.apache.avro.Schema recordSchema = getRecordSchema(schema, field.name());
    if (recordSchema == null) {
      org.apache.avro.Schema.Field flattenedField =
          createField(field, String.join(delimiter, flattenedFieldNameParts), nullable);
      flattenedFieldMap.put(flattenedField, new LinkedList<>(flattenedFieldNameParts));
      flattenedFieldNameParts.removeLast();
      return;
    }

    for (org.apache.avro.Schema.Field nestedField : recordSchema.getFields()) {
      flattenField(
          recordSchema,
          nestedField,
          nullable || nestedField.schema().isNullable(),
          flattenedFieldNameParts,
          flattenedFieldMap);
    }
  }

  private org.apache.avro.Schema getRecordSchema(org.apache.avro.Schema schema, String name) {
    if (schema.getType() != org.apache.avro.Schema.Type.RECORD) {
      return null;
    }

    org.apache.avro.Schema fieldSchema = schema.getField(name).schema();
    if (fieldSchema.isUnion()) {
      for (org.apache.avro.Schema s : fieldSchema.getTypes()) {
        if (s.getType() == org.apache.avro.Schema.Type.RECORD) {
          return s;
        }
      }
    }

    return fieldSchema.getType() == org.apache.avro.Schema.Type.RECORD ? fieldSchema : null;
  }

  /**
   * Creates a new Field instance with the same schema, doc, defaultValue, and order as field has
   * with changing the name to the specified one. It also copies all the props and aliases.
   */
  private org.apache.avro.Schema.Field createField(
      org.apache.avro.Schema.Field field, String name, boolean nullable) {
    org.apache.avro.Schema newSchema = field.schema();
    if (nullable && !newSchema.isNullable()) {
      newSchema = SchemaBuilder.unionOf().nullType().and().type(newSchema).endUnion();
    }
    org.apache.avro.Schema.Field newField =
        new org.apache.avro.Schema.Field(
            name, newSchema, field.doc(), field.defaultVal(), field.order());
    newField.putAll(field);
    field.aliases().forEach(newField::addAlias);

    return newField;
  }

  /**
   * A convenience class to facilitate dealing with flattened schema caches
   */
  @Value
  @RequiredArgsConstructor
  private static class FlattenedSchema {
    private final org.apache.avro.Schema schema;
    private final Map<org.apache.avro.Schema.Field, List<String>> fieldMap;
  }
}
