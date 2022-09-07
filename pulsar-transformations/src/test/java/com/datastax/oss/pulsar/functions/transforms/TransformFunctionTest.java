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
import static org.testng.AssertJUnit.assertNull;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import java.util.Map;
import org.apache.avro.generic.GenericData;
import org.apache.avro.util.Utf8;
import org.apache.pulsar.client.api.schema.GenericObject;
import org.apache.pulsar.client.api.schema.KeyValueSchema;
import org.apache.pulsar.common.schema.KeyValue;
import org.apache.pulsar.functions.api.Context;
import org.apache.pulsar.functions.api.Record;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

public class TransformFunctionTest {

  @DataProvider(name = "validConfigs")
  public static Object[][] validConfigs() {
    return new Object[][] {
      {"{'steps': [{'type': 'drop-fields', 'fields': 'some-field'}]}"},
      {"{'steps': [{'type': 'drop-fields', 'fields': 'some-field', 'part': 'key'}]}"},
      {"{'steps': [{'type': 'drop-fields', 'fields': 'some-field', 'part': 'value'}]}"},
      {"{'steps': [{'type': 'drop-fields', 'fields': 'some-field', 'when': 'key.k1==key1'}]}"},
      {"{'steps': [{'type': 'unwrap-key-value'}]}"},
      {"{'steps': [{'type': 'unwrap-key-value', 'unwrap-key': false}]}"},
      {"{'steps': [{'type': 'unwrap-key-value', 'unwrap-key': true}]}"},
      {"{'steps': [{'type': 'unwrap-key-value', 'unwrap-key': true, 'when': 'value.v1==val1'}]}"},
      {"{'steps': [{'type': 'cast', 'schema-type': 'STRING'}]}"},
      {"{'steps': [{'type': 'cast', 'schema-type': 'STRING', 'part': 'key'}]}"},
      {"{'steps': [{'type': 'cast', 'schema-type': 'STRING', 'part': 'value'}]}"},
      {"{'steps': [{'type': 'flatten'}]}"},
      {"{'steps': [{'type': 'flatten', 'part': 'key'}]}"},
      {"{'steps': [{'type': 'flatten', 'part': 'value'}]}"},
      {"{'steps': [{'type': 'flatten', 'delimiter': '_'}]}"},
      {"{'steps': [{'type': 'flatten', 'when': 'prop1==val1'}]}"},
    };
  }

  @Test(dataProvider = "validConfigs")
  void testValidConfig(String validConfig) {
    String userConfig = validConfig.replace("'", "\"");
    Map<String, Object> config =
        new Gson().fromJson(userConfig, new TypeToken<Map<String, Object>>() {}.getType());
    Context context = new Utils.TestContext(null, config);
    TransformFunction transformFunction = new TransformFunction();
    transformFunction.initialize(context);
  }

  @DataProvider(name = "invalidConfigs")
  public static Object[][] invalidConfigs() {
    return new Object[][] {
      {"{}"},
      {"{'steps': 'invalid'}"},
      {"{'steps': [{}]}"},
      {"{'steps': [{'type': 'invalid'}]}"},
      {"{'steps': [{'type': 'drop-fields'}]}"},
      {"{'steps': [{'type': 'drop-fields', 'fields': ''}]}"},
      {"{'steps': [{'type': 'drop-fields', 'fields': 'some-field', 'part': 'invalid'}]}"},
      {"{'steps': [{'type': 'drop-fields', 'fields': 'some-field', 'part': 42}]}"},
      {"{'steps': [{'type': 'drop-fields', 'fields': 'some-field', 'part': 42}]}"},
      {"{'steps': [{'type': 'drop-fields', 'fields': 'some-field', 'when': ''}]}"},
      {"{'steps': [{'type': 'unwrap-key-value', 'unwrap-key': 'invalid'}]}"},
      {"{'steps': [{'type': 'unwrap-key-value', 'when': ''}]}"},
      {"{'steps': [{'type': 'cast', 'schema-type': 42}]}"},
      {"{'steps': [{'type': 'cast', 'schema-type': 'INVALID'}]}"},
      {"{'steps': [{'type': 'cast', 'schema-type': 'STRING', 'part': 'invalid'}]}"},
      {"{'steps': [{'type': 'cast', 'schema-type': 'STRING', 'part': 42}]}"},
      {"{'steps': [{'type': 'cast', 'schema-type': 'STRING', 'part': 42}], 'when': ''}"},
      {"{'steps': [{'type': 'flatten', 'part': 'invalid-part'}]}"},
      {"{'steps': [{'type': 'flatten', 'when': ''}]}"},
    };
  }

  @Test(dataProvider = "invalidConfigs")
  void testInvalidConfig(String invalidConfig) {
    String userConfig = invalidConfig.replace("'", "\"");
    Map<String, Object> config =
        new Gson().fromJson(userConfig, new TypeToken<Map<String, Object>>() {}.getType());
    Context context = new Utils.TestContext(null, config);
    TransformFunction transformFunction = new TransformFunction();
    assertThrows(IllegalArgumentException.class, () -> transformFunction.initialize(context));
  }

  @Test
  void testDropFields() throws Exception {
    String userConfig =
        (""
                + "{'steps': ["
                + "    {'type': 'drop-fields', 'fields': 'keyField1'},"
                + "    {'type': 'drop-fields', 'fields': 'keyField2', 'part': 'key'},"
                + "    {'type': 'drop-fields', 'fields': 'keyField3', 'part': 'value'},"
                + "    {'type': 'drop-fields', 'fields': 'valueField1'},"
                + "    {'type': 'drop-fields', 'fields': 'valueField2', 'part': 'key'},"
                + "    {'type': 'drop-fields', 'fields': 'valueField3', 'part': 'value'}"
                + "]}")
            .replace("'", "\"");
    Map<String, Object> config =
        new Gson().fromJson(userConfig, new TypeToken<Map<String, Object>>() {}.getType());
    TransformFunction transformFunction = new TransformFunction();

    Record<GenericObject> record = Utils.createTestAvroKeyValueRecord();
    Utils.TestContext context = new Utils.TestContext(record, config);
    transformFunction.initialize(context);
    Record<GenericObject> outputRecord = transformFunction.process(record.getValue(), context);

    KeyValueSchema messageSchema = (KeyValueSchema) outputRecord.getSchema();
    KeyValue messageValue = (KeyValue) outputRecord.getValue();

    GenericData.Record keyAvroRecord =
        Utils.getRecord(messageSchema.getKeySchema(), (byte[]) messageValue.getKey());
    assertEquals(keyAvroRecord.get("keyField3"), new Utf8("key3"));
    assertNull(keyAvroRecord.getSchema().getField("keyField1"));
    assertNull(keyAvroRecord.getSchema().getField("keyField2"));

    GenericData.Record valueAvroRecord =
        Utils.getRecord(messageSchema.getValueSchema(), (byte[]) messageValue.getValue());
    assertEquals(valueAvroRecord.get("valueField2"), new Utf8("value2"));
    assertNull(valueAvroRecord.getSchema().getField("valueField1"));
    assertNull(valueAvroRecord.getSchema().getField("valueField3"));
  }

  @Test
  void testMatchingPredicate() throws Exception {
    String userConfig =
        (""
            + "{\"steps\": ["
            + "    {\"type\": \"drop-fields\", \"fields\": \"keyField1\", \"when\": \"key.keyField1 == 'key1'\"},"
            + "    {\"type\": \"drop-fields\", \"fields\": \"keyField2\", \"when\": \"key.keyField2 == 'key2'\"}"
            + "]}");
    Map<String, Object> config =
        new Gson().fromJson(userConfig, new TypeToken<Map<String, Object>>() {}.getType());
    TransformFunction transformFunction = new TransformFunction();

    Record<GenericObject> record = Utils.createTestAvroKeyValueRecord();
    Utils.TestContext context = new Utils.TestContext(record, config);
    transformFunction.initialize(context);
    Record<GenericObject> outputRecord = transformFunction.process(record.getValue(), context);

    KeyValueSchema messageSchema = (KeyValueSchema) outputRecord.getSchema();
    KeyValue messageValue = (KeyValue) outputRecord.getValue();

    GenericData.Record keyAvroRecord =
        Utils.getRecord(messageSchema.getKeySchema(), (byte[]) messageValue.getKey());
    assertEquals(keyAvroRecord.get("keyField3"), new Utf8("key3"));
    assertNull(keyAvroRecord.getSchema().getField("keyField1"));
    assertNull(keyAvroRecord.getSchema().getField("keyField2"));
  }

  @Test
  void testNonMatchingPredicate() throws Exception {
    String userConfig =
        (""
            + "{\"steps\": ["
            + "    {\"type\": \"drop-fields\", \"fields\": \"keyField1\", \"when\": \"key.keyField1 == 'key100'\"},"
            + "    {\"type\": \"drop-fields\", \"fields\": \"keyField2\", \"when\": \"key.keyField2 == 'key100'\"},"
            + "    {\"type\": \"drop-fields\", \"fields\": \"keyField3\"}"
            + "]}");
    Map<String, Object> config =
        new Gson().fromJson(userConfig, new TypeToken<Map<String, Object>>() {}.getType());
    TransformFunction transformFunction = new TransformFunction();

    Record<GenericObject> record = Utils.createTestAvroKeyValueRecord();
    Utils.TestContext context = new Utils.TestContext(record, config);
    transformFunction.initialize(context);
    transformFunction.process(record.getValue(), context);

    Record<GenericObject> outputRecord = transformFunction.process(record.getValue(), context);

    KeyValueSchema messageSchema = (KeyValueSchema) outputRecord.getSchema();
    KeyValue messageValue = (KeyValue) outputRecord.getValue();

    GenericData.Record keyAvroRecord =
        Utils.getRecord(messageSchema.getKeySchema(), (byte[]) messageValue.getKey());
    assertEquals(keyAvroRecord.get("keyField1"), new Utf8("key1"));
    assertEquals(keyAvroRecord.get("keyField2"), new Utf8("key2"));
    assertNull(keyAvroRecord.getSchema().getField("keyField3"));
  }

  @Test
  void testMixedPredicate() throws Exception {
    String userConfig =
        (""
            + "{\"steps\": ["
            + "    {\"type\": \"drop-fields\", \"fields\": \"keyField1\", \"when\": \"key.keyField1 == 'key1'\"},"
            + "    {\"type\": \"merge-key-value\", \"when\": \"key.keyField2 == 'key100'\"},"
            + "    {\"type\": \"unwrap-key-value\", \"when\": \"key.keyField3 == 'key100'\"},"
            + "    {\"type\": \"cast\", \"schema-type\": \"STRING\", \"when\": \"value.valueField1 == 'value1'\"}"
            + "]}");
    Map<String, Object> config =
        new Gson().fromJson(userConfig, new TypeToken<Map<String, Object>>() {}.getType());
    TransformFunction transformFunction = new TransformFunction();

    Record<GenericObject> record = Utils.createTestAvroKeyValueRecord();
    Utils.TestContext context = new Utils.TestContext(record, config);
    transformFunction.initialize(context);

    Record<GenericObject> outputRecord = transformFunction.process(record.getValue(), context);

    KeyValue<String, String> kv = (KeyValue<String, String>) outputRecord.getValue();
    assertEquals(kv.getKey(), "{\"keyField2\": \"key2\", \"keyField3\": \"key3\"}");
    assertEquals(
        kv.getValue(),
        "{\"valueField1\": \"value1\", \"valueField2\": \"value2\", \"valueField3\": \"value3\"}");
  }

  // TODO: just for demo. To be removed
  @Test
  void testRemoveMergeAndToString() throws Exception {
    String userConfig =
        (""
                + "{'steps': ["
                + "    {'type': 'drop-fields', 'fields': 'keyField1'},"
                + "    {'type': 'merge-key-value'},"
                + "    {'type': 'unwrap-key-value'},"
                + "    {'type': 'cast', 'schema-type': 'STRING'}"
                + "]}")
            .replace("'", "\"");
    Map<String, Object> config =
        new Gson().fromJson(userConfig, new TypeToken<Map<String, Object>>() {}.getType());
    TransformFunction transformFunction = new TransformFunction();

    Record<GenericObject> record = Utils.createTestAvroKeyValueRecord();
    Utils.TestContext context = new Utils.TestContext(record, config);
    transformFunction.initialize(context);
    Record<?> outputRecord = transformFunction.process(record.getValue(), context);

    assertEquals(
        outputRecord.getValue(),
        "{\"keyField2\": \"key2\", \"keyField3\": \"key3\", \"valueField1\": "
            + "\"value1\", \"valueField2\": \"value2\", \"valueField3\": \"value3\"}");
  }
}
