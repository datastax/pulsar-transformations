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

import com.datastax.oss.pulsar.functions.transforms.util.AvroUtil;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.Conversions;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.EncoderFactory;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.schema.GenericObject;
import org.apache.pulsar.client.api.schema.KeyValueSchema;
import org.apache.pulsar.common.schema.KeyValue;
import org.apache.pulsar.common.schema.KeyValueEncodingType;
import org.apache.pulsar.common.schema.SchemaType;
import org.apache.pulsar.functions.api.Context;
import org.apache.pulsar.functions.api.Record;
import org.apache.pulsar.functions.api.utils.FunctionRecord;

@Slf4j
@Data
public class TransformContext {
  private final Context context;
  private Schema<?> keySchema;
  private Object keyObject;
  private boolean keyModified;
  private Schema<?> valueSchema;
  private Object valueObject;
  private boolean valueModified;
  private KeyValueEncodingType keyValueEncodingType;
  private String key;
  private Map<String, String> properties;
  private String outputTopic;
  private boolean dropCurrentRecord;

  public TransformContext(Context context, Object value) {
    Record<?> currentRecord = context.getCurrentRecord();
    this.context = context;
    this.outputTopic = context.getOutputTopic();
    Schema<?> schema = currentRecord.getSchema();
    if (schema instanceof KeyValueSchema && value instanceof KeyValue) {
      KeyValueSchema<?, ?> kvSchema = (KeyValueSchema<?, ?>) schema;
      KeyValue<?, ?> kv = (KeyValue<?, ?>) value;
      this.keySchema = kvSchema.getKeySchema();
      this.keyObject =
          this.keySchema.getSchemaInfo().getType() == SchemaType.AVRO
              ? ((GenericObject) kv.getKey()).getNativeObject()
              : kv.getKey();
      this.valueSchema = kvSchema.getValueSchema();
      this.valueObject =
          this.valueSchema.getSchemaInfo().getType() == SchemaType.AVRO
              ? ((GenericObject) kv.getValue()).getNativeObject()
              : kv.getValue();
      this.keyValueEncodingType = kvSchema.getKeyValueEncodingType();
    } else {
      this.valueSchema = schema;
      this.valueObject = value;
      this.key = currentRecord.getKey().orElse(null);
    }
  }

  public Record<GenericObject> send() throws IOException {
    if (dropCurrentRecord) {
      return null;
    }
    if (keyModified
        && keySchema != null
        && keySchema.getSchemaInfo().getType() == SchemaType.AVRO) {
      GenericRecord genericRecord = (GenericRecord) keyObject;
      keySchema = Schema.NATIVE_AVRO(genericRecord.getSchema());
      keyObject = serializeGenericRecord(genericRecord);
    }
    if (valueModified
        && valueSchema != null
        && valueSchema.getSchemaInfo().getType() == SchemaType.AVRO) {
      GenericRecord genericRecord = (GenericRecord) valueObject;
      valueSchema = Schema.NATIVE_AVRO(genericRecord.getSchema());
      valueObject = serializeGenericRecord(genericRecord);
    }

    Schema outputSchema;
    Object outputObject;
    GenericObject recordValue = (GenericObject) context.getCurrentRecord().getValue();
    if (keySchema != null) {
      outputSchema = Schema.KeyValue(keySchema, valueSchema, keyValueEncodingType);
      Object outputKeyObject =
          !keyModified && keySchema.getSchemaInfo().getType().isStruct()
              ? ((KeyValue<?, ?>) recordValue.getNativeObject()).getKey()
              : keyObject;
      Object outputValueObject =
          !valueModified && valueSchema.getSchemaInfo().getType().isStruct()
              ? ((KeyValue<?, ?>) recordValue.getNativeObject()).getValue()
              : valueObject;
      outputObject = new KeyValue<>(outputKeyObject, outputValueObject);
    } else {
      outputSchema = valueSchema;
      outputObject =
          !valueModified && valueSchema.getSchemaInfo().getType().isStruct()
              ? recordValue
              : valueObject;
    }

    if (log.isDebugEnabled()) {
      log.debug("output {} schema {}", outputObject, outputSchema);
    }

    FunctionRecord.FunctionRecordBuilder<GenericObject> recordBuilder =
        context
            .newOutputRecordBuilder(outputSchema)
            .destinationTopic(outputTopic)
            .value(outputObject)
            .properties(getOutputProperties());

    if (keySchema == null && key != null) {
      recordBuilder.key(key);
    }

    return recordBuilder.build();
  }

  public void addProperty(String key, String value) {
    if (this.properties == null) {
      this.properties = new HashMap<>();
    }
    this.properties.put(key, value);
  }

  public Map<String, String> getOutputProperties() {
    if (this.properties == null) {
      return context.getCurrentRecord().getProperties();
    }

    if (context.getCurrentRecord().getProperties() == null) {
      return this.properties;
    }

    Map<String, String> mergedProperties = new HashMap<>();
    mergedProperties.putAll(this.context.getCurrentRecord().getProperties());
    mergedProperties.putAll(
        this.properties); // Computed props will overwrite current record props if the keys match

    return mergedProperties;
  }

  public static byte[] serializeGenericRecord(GenericRecord record) throws IOException {
    GenericDatumWriter<GenericRecord> writer = new GenericDatumWriter<>(record.getSchema());
    // enable Decimal conversion, otherwise attempting to serialize java.math.BigDecimal will throw
    // ClassCastException
    writer.getData().addLogicalTypeConversion(new Conversions.DecimalConversion());
    ByteArrayOutputStream oo = new ByteArrayOutputStream();
    BinaryEncoder encoder = EncoderFactory.get().directBinaryEncoder(oo, null);
    writer.write(record, encoder);
    return oo.toByteArray();
  }

  public void addOrReplaceAvroValueFields(
      Map<org.apache.avro.Schema.Field, Object> newFields,
      Map<org.apache.avro.Schema, org.apache.avro.Schema> schemaCache) {
    if (valueSchema.getSchemaInfo().getType() == AVRO) {
      GenericRecord avroRecord = (GenericRecord) valueObject;
      GenericRecord newRecord = AvroUtil.addOrReplaceAvroFields(avroRecord, newFields, schemaCache);
      if (avroRecord != newRecord) {
        valueModified = true;
      }
      valueObject = newRecord;
    }
  }

  public void addOrReplaceAvroKeyFields(
      Map<org.apache.avro.Schema.Field, Object> newFields,
      Map<org.apache.avro.Schema, org.apache.avro.Schema> schemaCache) {
    if (keySchema.getSchemaInfo().getType() == AVRO) {
      GenericRecord avroRecord = (GenericRecord) keyObject;
      GenericRecord newRecord = AvroUtil.addOrReplaceAvroFields(avroRecord, newFields, schemaCache);
      if (avroRecord != newRecord) {
        keyModified = true;
      }
      keyObject = newRecord;
    }
  }
}
