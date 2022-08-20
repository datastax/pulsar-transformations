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
import static org.testng.Assert.assertThrows;

import com.fasterxml.jackson.databind.JsonNode;
import java.util.Optional;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericData;
import org.apache.avro.util.Utf8;
import org.apache.avro.util.internal.JacksonUtils;
import org.apache.pulsar.client.api.schema.GenericObject;
import org.apache.pulsar.client.api.schema.KeyValueSchema;
import org.apache.pulsar.client.impl.schema.generic.GenericAvroRecord;
import org.apache.pulsar.common.schema.KeyValue;
import org.apache.pulsar.common.schema.KeyValueEncodingType;
import org.apache.pulsar.functions.api.Record;
import org.testng.annotations.Test;

public class FlattenStepTest {

  @Test
  void testRegularKeyValueNotModified() throws Exception {
    // given
    Record<GenericObject> record = Utils.createTestAvroKeyValueRecord();

    // when
    Utils.TestTypedMessageBuilder<?> message =
        Utils.process(record, new FlattenStep(Optional.empty()));

    // then (key & value remain unchanged)
    KeyValueSchema messageSchema = (KeyValueSchema) message.getSchema();
    KeyValue messageValue = (KeyValue) message.getValue();

    GenericData.Record keyRecord =
        Utils.getRecord(messageSchema.getKeySchema(), (byte[]) messageValue.getKey());
    GenericData.Record valueRecord =
        Utils.getRecord(messageSchema.getValueSchema(), (byte[]) messageValue.getValue());

    assertEquals(keyRecord.getSchema().getFields().size(), 3);
    assertEquals(keyRecord.get("keyField1"), new Utf8("key1"));
    assertEquals(keyRecord.get("keyField2"), new Utf8("key2"));
    assertEquals(keyRecord.get("keyField3"), new Utf8("key3"));

    assertEquals(valueRecord.getSchema().getFields().size(), 3);
    assertEquals(valueRecord.get("valueField1"), new Utf8("value1"));
    assertEquals(valueRecord.get("valueField2"), new Utf8("value2"));
    assertEquals(valueRecord.get("valueField3"), new Utf8("value3"));

    assertEquals(messageSchema.getKeyValueEncodingType(), KeyValueEncodingType.SEPARATED);
  }

  @Test
  void testNestedKeyValueRegularKeyValueFlattened() throws Exception {
    // given
    Record<GenericObject> nestedKVRecord = Utils.createNestedAvroKeyValueRecord(4);

    // when
    Utils.TestTypedMessageBuilder<?> message =
        Utils.process(nestedKVRecord, new FlattenStep(Optional.empty()));

    // then
    KeyValueSchema messageSchema = (KeyValueSchema) message.getSchema();
    KeyValue messageValue = (KeyValue) message.getValue();

    GenericData.Record keyRecord =
        Utils.getRecord(messageSchema.getKeySchema(), (byte[]) messageValue.getKey());
    GenericData.Record valueRecord =
        Utils.getRecord(messageSchema.getValueSchema(), (byte[]) messageValue.getValue());

    assertEquals(keyRecord.getSchema().getFields().size(), 5);
    assertEquals(keyRecord.get("level1KeyField1"), new Utf8("level1_Key1"));
    assertEquals(keyRecord.get("level1KeyField2_level2KeyField1"), new Utf8("level2_Key1"));
    assertEquals(
        keyRecord.get("level1KeyField2_level2KeyField2_level3KeyField1"), new Utf8("level3_Key1"));
    assertEquals(
        keyRecord.get("level1KeyField2_level2KeyField2_level3KeyField2_level4KeyField1"),
        new Utf8("level4_Key1"));
    assertEquals(
        keyRecord.get("level1KeyField2_level2KeyField2_level3KeyField2_level4KeyField2"),
        new Utf8("level4_Key2"));

    assertEquals(valueRecord.getSchema().getFields().size(), 5);
    assertEquals(valueRecord.get("level1ValueField1"), new Utf8("level1_Value1"));
    assertEquals(valueRecord.get("level1ValueField2_level2ValueField1"), new Utf8("level2_Value1"));
    assertEquals(
        valueRecord.get("level1ValueField2_level2ValueField2_level3ValueField1"),
        new Utf8("level3_Value1"));
    assertEquals(
        valueRecord.get("level1ValueField2_level2ValueField2_level3ValueField2_level4ValueField1"),
        new Utf8("level4_Value1"));
    assertEquals(
        valueRecord.get("level1ValueField2_level2ValueField2_level3ValueField2_level4ValueField2"),
        new Utf8("level4_Value2"));

    assertEquals(messageSchema.getKeyValueEncodingType(), KeyValueEncodingType.SEPARATED);
  }

  @Test
  void testNestedKeyValueRegularKeyOnlyFlattened() throws Exception {
    // given
    Record<GenericObject> nestedKVRecord = Utils.createNestedAvroKeyValueRecord(4);

    // when
    Utils.TestTypedMessageBuilder<?> message =
        Utils.process(nestedKVRecord, new FlattenStep(Optional.of("key")));

    // then
    KeyValueSchema messageSchema = (KeyValueSchema) message.getSchema();
    KeyValue messageValue = (KeyValue) message.getValue();

    GenericData.Record keyRecord =
        Utils.getRecord(messageSchema.getKeySchema(), (byte[]) messageValue.getKey());
    GenericData.Record valueRecord =
        (GenericData.Record) ((GenericAvroRecord) messageValue.getValue()).getNativeObject();

    assertEquals(keyRecord.getSchema().getFields().size(), 5);
    assertEquals(keyRecord.get("level1KeyField1"), new Utf8("level1_Key1"));
    assertEquals(keyRecord.get("level1KeyField2_level2KeyField1"), new Utf8("level2_Key1"));
    assertEquals(
        keyRecord.get("level1KeyField2_level2KeyField2_level3KeyField1"), new Utf8("level3_Key1"));
    assertEquals(
        keyRecord.get("level1KeyField2_level2KeyField2_level3KeyField2_level4KeyField1"),
        new Utf8("level4_Key1"));
    assertEquals(
        keyRecord.get("level1KeyField2_level2KeyField2_level3KeyField2_level4KeyField2"),
        new Utf8("level4_Key2"));

    assertEquals(valueRecord.getSchema().getFields().size(), 2);
    assertEquals(valueRecord.get("level1ValueField1"), "level1_Value1");
    GenericData.Record level1Record = (GenericData.Record) valueRecord.get("level1ValueField2");
    assertEquals(level1Record.get("level2ValueField1"), "level2_Value1");
    GenericData.Record level2Record = (GenericData.Record) level1Record.get("level2ValueField2");
    assertEquals(level2Record.get("level3ValueField1"), "level3_Value1");
    GenericData.Record level3Record = (GenericData.Record) level2Record.get("level3ValueField2");
    assertEquals(level3Record.get("level4ValueField1"), "level4_Value1");
    assertEquals(level3Record.get("level4ValueField2"), "level4_Value2");
    assertEquals(messageSchema.getKeyValueEncodingType(), KeyValueEncodingType.SEPARATED);
  }

  @Test
  void testNestedKeyValueRegularValueOnlyFlattened() throws Exception {
    // given
    Record<GenericObject> nestedKVRecord = Utils.createNestedAvroKeyValueRecord(4);

    // when
    Utils.TestTypedMessageBuilder<?> message =
        Utils.process(nestedKVRecord, new FlattenStep(Optional.of("value")));

    // then
    KeyValueSchema messageSchema = (KeyValueSchema) message.getSchema();
    KeyValue messageValue = (KeyValue) message.getValue();

    GenericData.Record keyRecord =
        (GenericData.Record) ((GenericAvroRecord) messageValue.getKey()).getNativeObject();
    GenericData.Record valueRecord =
        Utils.getRecord(messageSchema.getValueSchema(), (byte[]) messageValue.getValue());

    assertEquals(keyRecord.getSchema().getFields().size(), 2);
    assertEquals(keyRecord.get("level1KeyField1"), "level1_Key1");
    GenericData.Record level1Record = (GenericData.Record) keyRecord.get("level1KeyField2");
    assertEquals(level1Record.get("level2KeyField1"), "level2_Key1");
    GenericData.Record level2Record = (GenericData.Record) level1Record.get("level2KeyField2");
    assertEquals(level2Record.get("level3KeyField1"), "level3_Key1");
    GenericData.Record level3Record = (GenericData.Record) level2Record.get("level3KeyField2");
    assertEquals(level3Record.get("level4KeyField1"), "level4_Key1");
    assertEquals(level3Record.get("level4KeyField2"), "level4_Key2");
    assertEquals(messageSchema.getKeyValueEncodingType(), KeyValueEncodingType.SEPARATED);

    assertEquals(valueRecord.getSchema().getFields().size(), 5);
    assertEquals(valueRecord.get("level1ValueField1"), new Utf8("level1_Value1"));
    assertEquals(valueRecord.get("level1ValueField2_level2ValueField1"), new Utf8("level2_Value1"));
    assertEquals(
        valueRecord.get("level1ValueField2_level2ValueField2_level3ValueField1"),
        new Utf8("level3_Value1"));
    assertEquals(
        valueRecord.get("level1ValueField2_level2ValueField2_level3ValueField2_level4ValueField1"),
        new Utf8("level4_Value1"));
    assertEquals(
        valueRecord.get("level1ValueField2_level2ValueField2_level3ValueField2_level4ValueField2"),
        new Utf8("level4_Value2"));
  }

  @Test
  void testNestedKeyValueRegularInvalidType() {
    // given
    Record<GenericObject> nestedKVRecord = Utils.createNestedAvroKeyValueRecord(4);

    // then
    assertThrows(
        IllegalArgumentException.class,
        () -> Utils.process(nestedKVRecord, new FlattenStep(Optional.of("invalid"))));
  }
}
