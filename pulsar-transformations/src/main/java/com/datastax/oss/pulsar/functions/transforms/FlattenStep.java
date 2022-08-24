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
import java.util.List;
import java.util.stream.Collectors;
import lombok.Builder;
import lombok.Value;
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

  private static final String DEFAULT_DELIMITER = "_"; // '.' in not valid in AVRO field names

  @Builder.Default private String delimiter = DEFAULT_DELIMITER;
  private String part;

  @Override
  public void process(TransformContext transformContext) throws Exception {
    if (part == null) {
      validateAvro(transformContext.getKeySchema());
      validateAvro(transformContext.getValueSchema());
      GenericRecord avroKeyRecord = (GenericRecord) transformContext.getKeyObject();
      GenericRecord avroValueRecord = (GenericRecord) transformContext.getValueObject();
      transformContext.setKeyObject(flattenGenericRecord(avroKeyRecord));
      transformContext.setValueObject(flattenGenericRecord(avroValueRecord));
      transformContext.setKeyModified(true);
      transformContext.setValueModified(true);
    } else if ("key".equals(part)) {
      validateAvro(transformContext.getKeySchema());
      GenericRecord avroKeyRecord = (GenericRecord) transformContext.getKeyObject();
      transformContext.setKeyObject(flattenGenericRecord(avroKeyRecord));
      transformContext.setKeyModified(true);
    } else if ("value".equals(part)) {
      validateAvro(transformContext.getValueSchema());
      GenericRecord avroValueRecord = (GenericRecord) transformContext.getValueObject();
      transformContext.setValueObject(flattenGenericRecord(avroValueRecord));
      transformContext.setValueModified(true);
    } else {
      throw new IllegalArgumentException("Unsupported part for Flatten: " + part);
    }
  }

  void validateAvro(Schema<?> schema) {
    if (schema == null) {
      throw new IllegalStateException("Flatten requires non-null schemas!");
    }

    if (schema.getSchemaInfo().getType() != SchemaType.AVRO) {
      throw new IllegalStateException(
          "Unsupported schema type for Flatten: " + schema.getSchemaInfo().getType());
    }
  }

  GenericRecord flattenGenericRecord(GenericRecord record) {
    List<FieldValuePair> fieldValuePairs = buildFlattenedFields(record);
    List<org.apache.avro.Schema.Field> fields =
        fieldValuePairs.stream().map(pair -> pair.getField()).collect(Collectors.toList());
    org.apache.avro.Schema modified = buildFlattenedSchema(record, fields);
    GenericRecord newRecord = new GenericData.Record(modified);
    fieldValuePairs.forEach(pair -> newRecord.put(pair.getField().name(), pair.getValue()));
    return newRecord;
  }

  private List<FieldValuePair> buildFlattenedFields(GenericRecord record) {
    List<FieldValuePair> flattenedFields = new ArrayList<>();
    org.apache.avro.Schema originalSchema = record.getSchema();
    for (org.apache.avro.Schema.Field field : originalSchema.getFields()) {
      flattenedFields.addAll(flattenField(record, field, ""));
    }

    return flattenedFields;
  }

  org.apache.avro.Schema buildFlattenedSchema(
      GenericRecord record, List<org.apache.avro.Schema.Field> flattenedFields) {
    org.apache.avro.Schema originalSchema = record.getSchema();

    org.apache.avro.Schema flattenedSchema =
        org.apache.avro.Schema.createRecord(
            originalSchema.getName(),
            originalSchema.getDoc(),
            originalSchema.getNamespace(),
            false,
            flattenedFields);

    return flattenedSchema;
  }

  List<FieldValuePair> flattenField(
      GenericRecord record, org.apache.avro.Schema.Field field, String flattenedFieldName) {
    List<FieldValuePair> flattenedFields = new ArrayList<>();
    // Because of UNION schemas, we cannot tell for sure if the current field is a record, so we
    // have to use reflection
    if (record.get(field.name()) instanceof GenericRecord) {
      GenericRecord genericRecord = (GenericRecord) record.get(field.name());
      for (org.apache.avro.Schema.Field nestedField : genericRecord.getSchema().getFields()) {
        flattenedFields.addAll(
            flattenField(
                genericRecord, nestedField, flattenedFieldName + field.name() + delimiter));
      }

      return flattenedFields;
    }

    org.apache.avro.Schema.Field flattenedField =
        createField(field, flattenedFieldName + field.name());
    FieldValuePair fieldValuePair = new FieldValuePair(flattenedField, record.get(field.name()));
    flattenedFields.add(fieldValuePair);

    return flattenedFields;
  }

  /**
   * Creates a new Field instance with the same schema, doc, defaultValue, and order as field has
   * with changing the name to the specified one. It also copies all the props and aliases.
   */
  org.apache.avro.Schema.Field createField(org.apache.avro.Schema.Field field, String name) {
    org.apache.avro.Schema.Field newField =
        new org.apache.avro.Schema.Field(
            name, field.schema(), field.doc(), field.defaultVal(), field.order());
    newField.putAll(field);
    field.aliases().forEach(alias -> field.addAlias(alias));

    return newField;
  }

  @Value
  private static class FieldValuePair {
    private org.apache.avro.Schema.Field field;
    private Object value;
  }
}
