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
import java.nio.ByteBuffer;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;
import lombok.Builder;
import org.apache.avro.LogicalType;
import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;

/** Computes a field dynamically based on JSTL expressions and adds it to the key or the value . */
@Builder
public class ComputeStep implements TransformStep {

  @Builder.Default private final List<ComputeField> fields = new ArrayList<>();
  private final Map<org.apache.avro.Schema, org.apache.avro.Schema> keySchemaCache =
      new ConcurrentHashMap<>();
  private final Map<org.apache.avro.Schema, org.apache.avro.Schema> valueSchemaCache =
      new ConcurrentHashMap<>();
  private final Map<ComputeFieldType, org.apache.avro.Schema> fieldTypeToAvroSchemaCache =
      new ConcurrentHashMap<>();

  @Override
  public void process(TransformContext transformContext) {
    fields
        .stream()
        .filter(f -> "primitive".equals(f.getScope()))
        .findFirst()
        .ifPresent(f -> computePrimitiveField(f, transformContext));
    computeKeyFields(
        fields.stream().filter(f -> "key".equals(f.getScope())).collect(Collectors.toList()),
        transformContext);
    computeValueFields(
        fields.stream().filter(f -> "value".equals(f.getScope())).collect(Collectors.toList()),
        transformContext);
    computeHeaderFields(
        fields.stream().filter(f -> "header".equals(f.getScope())).collect(Collectors.toList()),
        transformContext);
    computeHeaderPropertiesFields(
        fields
            .stream()
            .filter(f -> "header.properties".equals(f.getScope()))
            .collect(Collectors.toList()),
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

  public void computePrimitiveField(ComputeField field, TransformContext context) {
    if (context.getValueSchema().getSchemaInfo().getType().isPrimitive()) {
      Object newValue = field.getEvaluator().evaluate(context);
      org.apache.pulsar.client.api.Schema newSchema = getPrimitiveSchema(field.getType());
      context.setValueObject(newValue);
      context.setValueSchema(newSchema);
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

  public void computeHeaderFields(List<ComputeField> fields, TransformContext context) {
    fields.forEach(
        field -> {
          switch (field.getName()) {
            case "destinationTopic":
              String topic = validateAndGetString(field, context);
              context.setOutputTopic(topic);
              break;
            case "messageKey":
              String key = validateAndGetString(field, context);
              context.setKey(key);
              break;
            default:
              throw new IllegalArgumentException("Invalid compute field name: " + field.getName());
          }
        });
  }

  public void computeHeaderPropertiesFields(List<ComputeField> fields, TransformContext context) {
    Map<String, String> properties =
        fields
            .stream()
            .map(field -> Map.entry(field.getName(), validateAndGetString(field, context)))
            .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
    context.setProperties(properties);
  }

  private String validateAndGetString(ComputeField field, TransformContext context) {
    Object value = field.getEvaluator().evaluate(context);
    if (value instanceof String) {
      return (String) value;
    }

    throw new IllegalArgumentException(
        String.format(
            "Invalid compute field type. " + "Name: %s, Type: %s, Expected Type: %s",
            field.getName(), value == null ? "null" : value.getClass().getSimpleName(), "String"));
  }

  private GenericRecord computeFields(
      List<ComputeField> fields,
      GenericRecord record,
      Map<org.apache.avro.Schema, org.apache.avro.Schema> schemaCache,
      TransformContext context) {
    org.apache.avro.Schema avroSchema = record.getSchema();

    List<Schema.Field> computedFields =
        fields.stream().map(this::createAvroField).collect(Collectors.toList());

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

    GenericRecordBuilder newRecordBuilder = new GenericRecordBuilder(newSchema);
    // Add original fields
    for (org.apache.avro.Schema.Field field : avroSchema.getFields()) {
      newRecordBuilder.set(field.name(), record.get(field.name()));
    }
    // Add computed fields
    for (ComputeField field : fields) {
      newRecordBuilder.set(
          field.getName(),
          getAvroValue(
              newSchema.getField(field.getName()).schema(),
              field.getEvaluator().evaluate(context)));
    }
    return newRecordBuilder.build();
  }

  private Object getAvroValue(Schema schema, Object value) {
    if (value == null) {
      return null;
    }

    if (value instanceof byte[]) {
      return ByteBuffer.wrap((byte[]) value);
    }

    LogicalType logicalType = getLogicalType(schema);
    if (logicalType == null) {
      return value;
    }

    // Avro logical type conversion: https://avro.apache.org/docs/1.8.2/spec.html#Logical+Types
    switch (logicalType.getName()) {
      case "date":
        validateLogicalType(value, schema.getLogicalType(), LocalDate.class);
        LocalDate localDate = (LocalDate) value;
        return (int) localDate.toEpochDay();
      case "time-millis":
        validateLogicalType(value, schema.getLogicalType(), LocalTime.class);
        LocalTime localTime = (LocalTime) value;
        return (int) (localTime.toNanoOfDay() / 1000000);
      case "timestamp-millis":
        validateLogicalType(value, schema.getLogicalType(), Instant.class);
        Instant instant = (Instant) value;
        return instant.toEpochMilli();
    }

    throw new IllegalArgumentException(
        String.format("Invalid logical type %s for value %s", schema.getLogicalType(), value));
  }

  private LogicalType getLogicalType(Schema schema) {
    if (!schema.isUnion()) {
      return schema.getLogicalType();
    }

    return schema
        .getTypes()
        .stream()
        .map(Schema::getLogicalType)
        .filter(Objects::nonNull)
        .findAny()
        .orElse(null);
  }

  void validateLogicalType(Object value, LogicalType logicalType, Class expectedClass) {
    if (!(value.getClass().equals(expectedClass))) {
      throw new IllegalArgumentException(
          String.format("Invalid value %s for logical type %s", value, logicalType));
    }
  }

  private Schema.Field createAvroField(ComputeField field) {
    Schema avroSchema = getAvroSchema(field.getType());
    Object defaultValue = null;
    if (field.isOptional()) {
      avroSchema = SchemaBuilder.unionOf().nullType().and().type(avroSchema).endUnion();
      defaultValue = Schema.Field.NULL_DEFAULT_VALUE;
    }
    return new Schema.Field(field.getName(), avroSchema, null, defaultValue);
  }

  private Schema getAvroSchema(ComputeFieldType type) {
    Schema.Type schemaType;
    switch (type) {
      case STRING:
        schemaType = Schema.Type.STRING;
        break;
      case INT32:
      case DATE:
      case TIME:
        schemaType = Schema.Type.INT;
        break;
      case INT64:
      case DATETIME:
        schemaType = Schema.Type.LONG;
        break;
      case FLOAT:
        schemaType = Schema.Type.FLOAT;
        break;
      case DOUBLE:
        schemaType = Schema.Type.DOUBLE;
        break;
      case BOOLEAN:
        schemaType = Schema.Type.BOOLEAN;
        break;
      case BYTES:
        schemaType = Schema.Type.BYTES;
        break;
      default:
        throw new UnsupportedOperationException("Unsupported compute field type: " + type);
    }

    return fieldTypeToAvroSchemaCache.computeIfAbsent(
        type,
        key -> {
          // Handle logical types: https://avro.apache.org/docs/1.10.2/spec.html#Logical+Types
          Schema schema = Schema.create(schemaType);
          switch (key) {
            case DATE:
              return LogicalTypes.date().addToSchema(schema);
            case TIME:
              return LogicalTypes.timeMillis().addToSchema(schema);
            case DATETIME:
              return LogicalTypes.timestampMillis().addToSchema(schema);
            default:
              return schema;
          }
        });
  }

  private org.apache.pulsar.client.api.Schema getPrimitiveSchema(ComputeFieldType type) {
    org.apache.pulsar.client.api.Schema schema;
    switch (type) {
      case STRING:
        schema = org.apache.pulsar.client.api.Schema.STRING;
        break;
      case INT32:
        schema = org.apache.pulsar.client.api.Schema.INT32;
        break;
      case INT64:
        schema = org.apache.pulsar.client.api.Schema.INT64;
        break;
      case FLOAT:
        schema = org.apache.pulsar.client.api.Schema.FLOAT;
        break;
      case DOUBLE:
        schema = org.apache.pulsar.client.api.Schema.DOUBLE;
        break;
      case BOOLEAN:
        schema = org.apache.pulsar.client.api.Schema.BOOL;
        break;
      case DATE:
        schema = org.apache.pulsar.client.api.Schema.LOCAL_DATE;
        break;
      case TIME:
        schema = org.apache.pulsar.client.api.Schema.LOCAL_TIME;
        break;
      case DATETIME:
        schema = org.apache.pulsar.client.api.Schema.INSTANT;
        break;
      case BYTES:
        schema = org.apache.pulsar.client.api.Schema.BYTES;
        break;
      default:
        throw new UnsupportedOperationException("Unsupported compute field type: " + type);
    }

    return schema;
  }
}
