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

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotSame;
import static org.testng.Assert.assertSame;

import org.apache.avro.generic.GenericData;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.schema.GenericObject;
import org.apache.pulsar.client.api.schema.KeyValueSchema;
import org.apache.pulsar.client.impl.schema.AutoConsumeSchema;
import org.apache.pulsar.common.schema.KeyValue;
import org.apache.pulsar.common.schema.KeyValueEncodingType;
import org.apache.pulsar.common.schema.SchemaType;
import org.apache.pulsar.functions.api.Record;
import org.testng.annotations.Test;

public class MergeKeyValueStepTest {

  @Test
  void testKeyValueAvro() throws Exception {
    Record<GenericObject> record = Utils.createTestAvroKeyValueRecord();
    Record<?> outputRecord = Utils.process(record, new MergeKeyValueStep());
    KeyValueSchema<?, ?> messageSchema = (KeyValueSchema<?, ?>) outputRecord.getSchema();
    KeyValue<?, ?> messageValue = (KeyValue<?, ?>) outputRecord.getValue();

    GenericData.Record read =
        Utils.getRecord(messageSchema.getValueSchema(), (byte[]) messageValue.getValue());
    assertEquals(
        read.toString(),
        "{\"keyField1\": \"key1\", \"keyField2\": \"key2\", \"keyField3\": \"key3\", "
            + "\"valueField1\": \"value1\", \"valueField2\": \"value2\", \"valueField3\": \"value3\"}");

    KeyValueSchema<?, ?> recordSchema = (KeyValueSchema) record.getSchema();
    KeyValue<?, ?> recordValue = (KeyValue<?, ?>) record.getValue().getNativeObject();
    assertSame(messageSchema.getKeySchema(), recordSchema.getKeySchema());
    assertSame(messageValue.getKey(), recordValue.getKey());
  }

  @Test
  void testPrimitive() throws Exception {
    Record<GenericObject> record =
        new Utils.TestRecord<>(
            Schema.STRING,
            AutoConsumeSchema.wrapPrimitiveObject("test-message", SchemaType.STRING, new byte[] {}),
            "test-key");
    Record<GenericObject> outputRecord = Utils.process(record, new MergeKeyValueStep());

    assertSame(outputRecord.getSchema(), record.getSchema());
    assertSame(outputRecord.getValue(), record.getValue().getNativeObject());
    assertEquals(outputRecord.getKey(), record.getKey());
  }

  @Test
  void testKeyValuePrimitives() throws Exception {
    Schema<KeyValue<String, Integer>> keyValueSchema =
        Schema.KeyValue(Schema.STRING, Schema.INT32, KeyValueEncodingType.SEPARATED);

    KeyValue<String, Integer> keyValue = new KeyValue<>("key", 42);

    Record<GenericObject> record =
        new Utils.TestRecord<>(
            keyValueSchema,
            AutoConsumeSchema.wrapPrimitiveObject(keyValue, SchemaType.KEY_VALUE, new byte[] {}),
            null);

    Record<?> outputRecord = Utils.process(record, new MergeKeyValueStep());
    KeyValueSchema<?, ?> messageSchema = (KeyValueSchema<?, ?>) outputRecord.getSchema();
    KeyValue<?, ?> messageValue = (KeyValue<?, ?>) outputRecord.getValue();

    KeyValueSchema<?, ?> recordSchema = (KeyValueSchema) record.getSchema();
    KeyValue<?, ?> recordValue = ((KeyValue<?, ?>) record.getValue().getNativeObject());
    assertSame(messageSchema.getKeySchema(), recordSchema.getKeySchema());
    assertSame(messageSchema.getValueSchema(), recordSchema.getValueSchema());
    assertSame(messageValue.getKey(), recordValue.getKey());
    assertSame(messageValue.getValue(), recordValue.getValue());
  }

  @Test
  void testKeyValueAvroCached() throws Exception {
    Record<GenericObject> record = Utils.createTestAvroKeyValueRecord();

    MergeKeyValueStep step = new MergeKeyValueStep();
    Record<?> outputRecord = Utils.process(record, step);
    KeyValueSchema<?, ?> messageSchema = (KeyValueSchema<?, ?>) outputRecord.getSchema();

    outputRecord = Utils.process(Utils.createTestAvroKeyValueRecord(), step);
    KeyValueSchema<?, ?> newMessageSchema = (KeyValueSchema<?, ?>) outputRecord.getSchema();

    // Schema was modified by process operation
    KeyValueSchema<?, ?> recordSchema = (KeyValueSchema) record.getSchema();
    assertNotSame(
        messageSchema.getValueSchema().getNativeSchema().orElseThrow(),
        recordSchema.getValueSchema().getNativeSchema().orElseThrow());

    // Multiple process output the same cached schema
    assertSame(
        messageSchema.getValueSchema().getNativeSchema().orElseThrow(),
        newMessageSchema.getValueSchema().getNativeSchema().orElseThrow());
  }
}
