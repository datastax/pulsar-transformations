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

import static org.apache.pulsar.common.schema.SchemaType.*;
import static org.apache.pulsar.common.schema.SchemaType.STRING;

import com.datastax.oss.pulsar.functions.transforms.model.ComputeField;
import com.datastax.oss.pulsar.functions.transforms.predicate.jstl.JstlEvaluator;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;
import lombok.Builder;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.pulsar.common.schema.SchemaType;

/** Computes a field dynamically based on JSTL expressions and adds it to the key or the value . */
@Builder
public class ComputeFieldStep implements TransformStep {

  @Builder.Default private final List<ComputeField> keyFields = new ArrayList<>();
  @Builder.Default private final List<ComputeField> valueFields = new ArrayList<>();
  private final Map<org.apache.avro.Schema, org.apache.avro.Schema> keySchemaCache =
      new ConcurrentHashMap<>();
  private final Map<org.apache.avro.Schema, org.apache.avro.Schema> valueSchemaCache =
      new ConcurrentHashMap<>();

  @Override
  public void process(TransformContext transformContext) {
    computeKeyFields(keyFields, transformContext);
    computeValueFields(valueFields, transformContext);
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
            .map(f -> new org.apache.avro.Schema.Field(f.getName(), getSchema(f.getSchemaType())))
            .collect(Collectors.toList());
    List<Schema.Field> newFields =
        avroSchema
            .getFields()
            .stream()
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
      newRecord.put(
          field.getName(),
          new JstlEvaluator(field.getExpression(), getType(field.getSchemaType()))
              .evaluate(context));
    }
    return newRecord;
  }

  private Class<?> getType(SchemaType schemaType) {
    switch (schemaType) {
      case STRING:
        return String.class;
      case INT8:
      case INT16:
      case INT32:
        return Integer.class;
      case INT64:
        return Long.class;
      default:
        throw new UnsupportedOperationException("Unsupported schema type: " + schemaType);
    }
  }

  Schema getSchema(SchemaType schemaType) {
    switch (schemaType) {
      case STRING:
        return Schema.create(Schema.Type.STRING);
      case INT8:
      case INT16:
      case INT32:
      case INT64:
        return Schema.create(Schema.Type.INT);
      default:
        throw new UnsupportedOperationException("Unsupported schema type: " + schemaType);
    }
  }
}
