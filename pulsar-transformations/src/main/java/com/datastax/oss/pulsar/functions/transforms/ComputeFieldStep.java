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

import static org.apache.pulsar.common.schema.SchemaType.AVRO;

import com.datastax.oss.pulsar.functions.transforms.model.ComputeField;
import com.datastax.oss.pulsar.functions.transforms.model.ComputeFieldType;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;
import lombok.Builder;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;

/** Computes a field dynamically based on JSTL expressions and adds it to the key or the value . */
@Builder
public class ComputeFieldStep implements TransformStep {

  @Builder.Default private final List<ComputeField> fields = new ArrayList<>();
  private final Map<org.apache.avro.Schema, org.apache.avro.Schema> keySchemaCache =
      new ConcurrentHashMap<>();
  private final Map<org.apache.avro.Schema, org.apache.avro.Schema> valueSchemaCache =
      new ConcurrentHashMap<>();
  private final Map<ComputeFieldType, org.apache.avro.Schema> fieldTypeToAvroSchemaCache =
      new ConcurrentHashMap<>();

  @Override
  public void process(TransformContext transformContext) {
    computeKeyFields(
        fields.stream().filter(f -> "key".equals(f.getPart())).collect(Collectors.toList()),
        transformContext);
    computeValueFields(
        fields.stream().filter(f -> "value".equals(f.getPart())).collect(Collectors.toList()),
        transformContext);
  }

  public void computeValueFields(List<ComputeField> fields, TransformContext context) {
    if (context.getValueSchema().getSchemaInfo().getType() == AVRO) {
      GenericRecord avroRecord = (GenericRecord) context.getValueObject();
      GenericRecord newRecord = computeFields(fields, avroRecord, valueSchemaCache, context);
      if (avroRecord != newRecord) {
        context.setValueModified(true);
      }
      context.setValueObject(newRecord);
    }
  }

  public void computeKeyFields(List<ComputeField> fields, TransformContext context) {
    if (context.getKeyObject() != null
        && context.getValueSchema().getSchemaInfo().getType() == AVRO) {
      GenericRecord avroRecord = (GenericRecord) context.getKeyObject();
      GenericRecord newRecord = computeFields(fields, avroRecord, keySchemaCache, context);
      if (avroRecord != newRecord) {
        context.setKeyModified(true);
      }
      context.setKeyObject(newRecord);
    }
  }

  private GenericRecord computeFields(
      List<ComputeField> fields,
      GenericRecord record,
      Map<org.apache.avro.Schema, org.apache.avro.Schema> schemaCache,
      TransformContext context) {
    org.apache.avro.Schema avroSchema = record.getSchema();

    List<Schema.Field> computedFields =
        fields
            .stream()
            .map(f -> new org.apache.avro.Schema.Field(f.getName(), getAvroSchema(f.getType())))
            .collect(Collectors.toList());

    Set<String> computedFieldNames =
        computedFields.stream().map(f -> f.name()).collect(Collectors.toSet());
    // New fields are the intersection between existing fields and computed fields. Computed fields
    // take precedence.
    List<Schema.Field> newFields =
        avroSchema
            .getFields()
            .stream()
            .filter(f -> !computedFieldNames.contains(f.name()))
            .map(
                f ->
                    new org.apache.avro.Schema.Field(
                        f.name(), f.schema(), f.doc(), f.defaultVal(), f.order()))
            .collect(Collectors.toList());
    newFields.addAll(computedFields);
    org.apache.avro.Schema newSchema =
        schemaCache.computeIfAbsent(
            avroSchema,
            schema ->
                org.apache.avro.Schema.createRecord(
                    avroSchema.getName(),
                    avroSchema.getDoc(),
                    avroSchema.getNamespace(),
                    avroSchema.isError(),
                    newFields));

    GenericRecord newRecord = new GenericData.Record(newSchema);
    // Add original fields
    for (org.apache.avro.Schema.Field field : avroSchema.getFields()) {
      newRecord.put(field.name(), record.get(field.name()));
    }
    // Add computed fields
    for (ComputeField field : fields) {
      newRecord.put(field.getName(), field.getEvaluator().evaluate(context));
    }
    return newRecord;
  }

  private Schema getAvroSchema(ComputeFieldType type) {
    switch (type) {
      case STRING:
        return fieldTypeToAvroSchemaCache.computeIfAbsent(
            type, (ignored) -> Schema.create(Schema.Type.STRING));
      case INT32:
        return fieldTypeToAvroSchemaCache.computeIfAbsent(
            type, (ignored) -> Schema.create(Schema.Type.INT));
      case INT64:
        return fieldTypeToAvroSchemaCache.computeIfAbsent(
            type, (ignored) -> Schema.create(Schema.Type.LONG));
      case FLOAT:
        return fieldTypeToAvroSchemaCache.computeIfAbsent(
            type, (ignored) -> Schema.create(Schema.Type.FLOAT));
      case DOUBLE:
        return fieldTypeToAvroSchemaCache.computeIfAbsent(
            type, (ignored) -> Schema.create(Schema.Type.DOUBLE));
      case BOOLEAN:
        return fieldTypeToAvroSchemaCache.computeIfAbsent(
            type, (ignored) -> Schema.create(Schema.Type.BOOLEAN));
      default:
        throw new UnsupportedOperationException("Unsupported compute field type: " + type);
    }
  }
}
