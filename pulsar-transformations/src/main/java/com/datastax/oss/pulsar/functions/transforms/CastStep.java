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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.deser.std.DateDeserializers;
import java.nio.charset.StandardCharsets;
import java.sql.Time;
import java.sql.Timestamp;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.Date;
import java.util.function.Consumer;
import lombok.Builder;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.common.schema.SchemaType;

@Builder
public class CastStep implements TransformStep {

  private final SchemaType keySchemaType;
  private final SchemaType valueSchemaType;

  // TODO: make thread local
  private final SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss");

  @Override
  public void process(TransformContext transformContext) {
    if (transformContext.getKeySchema() != null) {
      Schema<?> schema = toSchema(keySchemaType);
      Object value = convertValue((Schema<Object>) transformContext.getKeySchema(), transformContext.getKeyObject(), schema);
      transformContext.setKeySchema(schema);
      transformContext.setKeyObject(value);
    }
    Schema<?> schema = toSchema(valueSchemaType);
    Object value = convertValue((Schema<Object>) transformContext.getValueSchema(), transformContext.getValueObject(), schema);
    transformContext.setValueSchema(schema);
    transformContext.setValueObject(value);
  }

  private Object convertValue(Schema<Object> originalSchema, Object originalValue, Schema<?> targetSchema) {
    if (originalSchema == targetSchema) {
      return originalValue;
    }
    if (originalValue instanceof byte[]) {
      return targetSchema.decode((byte[]) originalValue);
    }
    if (targetSchema == Schema.BYTES) {
      return originalSchema.encode(originalValue);
    }
    switch (targetSchema.getSchemaInfo().getType()) {
      case STRING:
        return ConverterUtil.toString(originalValue);
      case INT8:
        return ConverterUtil.toByte(originalValue);
      case INT16:
        return ConverterUtil.toShort(originalValue);
      case INT32:
        return ConverterUtil.toInteger(originalValue);
      case INT64:
        return ConverterUtil.toLong(originalValue);
      case FLOAT:
        return ConverterUtil.toFloat(originalValue);
      case DOUBLE:
        return ConverterUtil.toDouble(originalValue);
      case DATE:
        return ConverterUtil.toDate(originalValue);
      case TIME:
        return ConverterUtil.toTime(originalValue);
      case TIMESTAMP:
        return ConverterUtil.toTimestamp(originalValue);
      case INSTANT:
        return ConverterUtil.toInstant(originalValue);
      case LOCAL_DATE:
        return ConverterUtil.toLocalDate(originalValue);
      case LOCAL_TIME:
        return ConverterUtil.toLocalTime(originalValue);
      case LOCAL_DATE_TIME:
        return ConverterUtil.toLocalDateTime(originalValue);
      default:
        throw new IllegalStateException("Unexpected value: " + targetSchema.getSchemaInfo().getType());
    }
  }

  private Schema<?> toSchema(SchemaType schemaType) {
    switch (schemaType) {
      case STRING:
        return Schema.STRING;
      case INT8:
        return Schema.INT8;
      case INT16:
        return Schema.INT16;
      case INT32:
        return Schema.INT32;
      case INT64:
        return Schema.INT64;
      case FLOAT:
        return Schema.FLOAT;
      case DOUBLE:
        return Schema.DOUBLE;
      case DATE:
        return Schema.DATE;
      case TIME:
        return Schema.TIME;
      case TIMESTAMP:
        return Schema.TIMESTAMP;
      case INSTANT:
        return Schema.INSTANT;
      case LOCAL_DATE:
        return Schema.LOCAL_DATE;
      case LOCAL_TIME:
        return Schema.LOCAL_TIME;
      case LOCAL_DATE_TIME:
        return Schema.LOCAL_DATE_TIME;
      case BYTES:
        return Schema.BYTES;
      default:
        throw new IllegalStateException("Unexpected value: " + schemaType);
    }
  }

  public static class CastStepBuilder {
    private SchemaType keySchemaType;
    private SchemaType valueSchemaType;

    public CastStepBuilder keySchemaType(SchemaType keySchemaType) {
      if (keySchemaType != null && keySchemaType != SchemaType.STRING) {
        throw new IllegalArgumentException(
            "Unsupported key schema-type for Cast: " + keySchemaType);
      }
      this.keySchemaType = keySchemaType;
      return this;
    }

    public CastStepBuilder valueSchemaType(SchemaType valueSchemaType) {
      if (valueSchemaType != null && valueSchemaType != SchemaType.STRING) {
        throw new IllegalArgumentException(
            "Unsupported value schema-type for Cast: " + valueSchemaType);
      }
      this.valueSchemaType = valueSchemaType;
      return this;
    }
  }
}
