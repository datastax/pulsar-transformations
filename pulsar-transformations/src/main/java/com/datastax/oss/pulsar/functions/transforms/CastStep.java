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

import lombok.Builder;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.common.schema.SchemaType;

@Builder
public class CastStep implements TransformStep {
  private final SchemaType keySchemaType;
  private final SchemaType valueSchemaType;

  @Override
  public void process(TransformContext transformContext) {
    if (transformContext.getKeySchema() != null
        && keySchemaType != null
        && transformContext.getKeySchema().getSchemaInfo().getType() != keySchemaType) {
      Object value = convertValue(transformContext.getKeyObject(), keySchemaType);
      transformContext.setKeySchema(toSchema(keySchemaType));
      transformContext.setKeyObject(value);
    }
    if (valueSchemaType != null
        && transformContext.getValueSchema().getSchemaInfo().getType() != valueSchemaType) {
      Object value = convertValue(transformContext.getValueObject(), valueSchemaType);
      transformContext.setValueSchema(toSchema(valueSchemaType));
      transformContext.setValueObject(value);
    }
  }

  private Object convertValue(Object originalValue, SchemaType schemaType) {
    switch (schemaType) {
      case BYTES:
        return ConverterUtil.toBytes(originalValue);
      case STRING:
        return ConverterUtil.toString(originalValue);
      case BOOLEAN:
        return ConverterUtil.toBoolean(originalValue);
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
        throw new IllegalStateException("Unexpected value: " + schemaType);
    }
  }

  private Schema<?> toSchema(SchemaType schemaType) {
    switch (schemaType) {
      case STRING:
        return Schema.STRING;
      case BOOLEAN:
        return Schema.BOOL;
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
      if (keySchemaType != null
          && (!keySchemaType.isPrimitive() || keySchemaType == SchemaType.NONE)) {
        throw new IllegalArgumentException(
            "Unsupported key schema-type for Cast: " + keySchemaType);
      }
      this.keySchemaType = keySchemaType;
      return this;
    }

    public CastStepBuilder valueSchemaType(SchemaType valueSchemaType) {
      if (valueSchemaType != null
          && (!valueSchemaType.isPrimitive() || valueSchemaType == SchemaType.NONE)) {
        throw new IllegalArgumentException(
            "Unsupported value schema-type for Cast: " + valueSchemaType);
      }
      this.valueSchemaType = valueSchemaType;
      return this;
    }
  }
}
