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
package com.datastax.oss.pulsar.functions.transforms.jstl;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

import com.datastax.oss.pulsar.functions.transforms.TransformContext;
import com.datastax.oss.pulsar.functions.transforms.Utils;
import java.util.HashMap;
import java.util.Map;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.schema.GenericObject;
import org.apache.pulsar.client.impl.schema.AutoConsumeSchema;
import org.apache.pulsar.common.schema.KeyValue;
import org.apache.pulsar.common.schema.KeyValueEncodingType;
import org.apache.pulsar.common.schema.SchemaType;
import org.apache.pulsar.functions.api.Record;
import org.testng.annotations.Test;

public class JstlTransformContextAdapterTest {
  @Test
  void testAdapterForKeyValueRecord() {
    // given
    Record<GenericObject> record = Utils.createTestAvroKeyValueRecord();
    /*
     Actual key: { "keyField1": "key1", "keyField2": "key2", "keyField3": "key3" }

     <p>Actual value: { "valueField1": "value1", "valueField2": "value2", "valueField3": "value3"
     }
    */
    Utils.TestContext context = new Utils.TestContext(record, new HashMap<>());
    TransformContext transformContext =
        new TransformContext(context, record.getValue().getNativeObject());

    // when
    JstlTransformContextAdapter adapter = new JstlTransformContextAdapter(transformContext);

    // then
    assertTrue(adapter.getKey() instanceof Map);
    Map<String, Object> keyMap = (Map<String, Object>) adapter.getKey();
    assertEquals(keyMap.get("keyField1"), "key1");
    assertEquals(keyMap.get("keyField2"), "key2");
    assertEquals(keyMap.get("keyField3"), "key3");
    assertNull(keyMap.get("keyField4"));

    assertTrue(adapter.getValue() instanceof Map);
    Map<String, Object> valueMap = (Map<String, Object>) adapter.getValue();
    assertEquals(valueMap.get("valueField1"), "value1");
    assertEquals(valueMap.get("valueField2"), "value2");
    assertEquals(valueMap.get("valueField3"), "value3");

    assertTrue(adapter.getKey() instanceof Map);
    assertNull(keyMap.get("valueField4"));
  }

  @Test
  void testAdapterForPrimitiveKeyValueRecord() {
    // given
    Schema<KeyValue<String, Integer>> keyValueSchema =
        Schema.KeyValue(Schema.STRING, Schema.INT32, KeyValueEncodingType.SEPARATED);
    KeyValue<String, Integer> keyValue = new KeyValue<>("key", 42);
    Record<GenericObject> primitiveKVRecord =
        new Utils.TestRecord<>(
            keyValueSchema,
            AutoConsumeSchema.wrapPrimitiveObject(keyValue, SchemaType.KEY_VALUE, new byte[] {}),
            "header-key");

    TransformContext transformContext =
        new TransformContext(
            new Utils.TestContext(primitiveKVRecord, new HashMap<>()),
            primitiveKVRecord.getValue().getNativeObject());

    // when
    JstlTransformContextAdapter adapter = new JstlTransformContextAdapter(transformContext);

    assertEquals(adapter.getKey(), "key");
    assertEquals(adapter.getValue(), 42);
    assertEquals(adapter.getHeader().get("messageKey"), "header-key");
  }

  @Test
  void testAdapterForPrimitiveRecord() {
    // given
    Record<GenericObject> record =
        new Utils.TestRecord<>(
            Schema.STRING,
            AutoConsumeSchema.wrapPrimitiveObject("test-message", SchemaType.STRING, new byte[] {}),
            "header-key");
    /* Actual key: "test-key" Actual value: "test-message" */
    Utils.TestContext context = new Utils.TestContext(record, new HashMap<>());
    TransformContext transformContext =
        new TransformContext(context, record.getValue().getNativeObject());

    // when
    JstlTransformContextAdapter adapter = new JstlTransformContextAdapter(transformContext);

    // then
    assertEquals(adapter.getHeader().get("messageKey"), "header-key");
    assertNull(adapter.getKey());

    assertEquals(adapter.getValue(), "test-message");
    assertEquals(adapter.getValue(), "test-message");
  }

  @Test
  void testAdapterForNestedValueRecord() {
    // given
    Record<GenericObject> record = Utils.createNestedAvroRecord(4, "header-key");
    /*
     Actual key: "header-key"

     <p>Actual value: "level1String": "level1_1", "level1Record": { "level2String": "level2_1",
     "level2Record": { "level3String": "level3_1", "level3Record": { "level4String": "level4_1",
     "level4Integer": 9, "level4Double": 8.8, "level4StringWithProps": "level4_WithProps",
     "level4Union": "level4_2" } } } }
    */
    Utils.TestContext context = new Utils.TestContext(record, new HashMap<>());
    TransformContext transformContext =
        new TransformContext(context, record.getValue().getNativeObject());

    // when
    JstlTransformContextAdapter adapter = new JstlTransformContextAdapter(transformContext);

    // then
    assertEquals(adapter.getHeader().get("messageKey"), "header-key");
    assertNull(adapter.getKey());
    assertTrue(adapter.getValue() instanceof Map);
    Map<String, Object> valueMap = (Map<String, Object>) adapter.getValue();
    assertNestedRecord(valueMap);
  }

  @Test
  void testAdapterForNestedKeyValueRecord() {
    // given
    Record<GenericObject> record = Utils.createNestedAvroKeyValueRecord(4);
    /*
     Actual key: { "level1String": "level1_1", "level1Record": { "level2String": "level2_1",
     "level2Record": { "level3String": "level3_1", "level3Record": { "level4String": "level4_1",
     "level4Integer": 9, "level4Double": 8.8, "level4StringWithProps": "level4_WithProps",
     "level4Union": "level4_2" } } } }

     <p>Actual value: "level1String": "level1_1", "level1Record": { "level2String": "level2_1",
     "level2Record": { "level3String": "level3_1", "level3Record": { "level4String": "level4_1",
     "level4Integer": 9, "level4Double": 8.8, "level4StringWithProps": "level4_WithProps",
     "level4Union": "level4_2" } } } }
    */
    Utils.TestContext context = new Utils.TestContext(record, new HashMap<>());
    TransformContext transformContext =
        new TransformContext(context, record.getValue().getNativeObject());

    // when
    JstlTransformContextAdapter adapter = new JstlTransformContextAdapter(transformContext);

    // then
    assertTrue(adapter.getKey() instanceof Map);
    assertNestedRecord((Map<String, Object>) adapter.getKey());

    assertTrue(adapter.getValue() instanceof Map);
    assertNestedRecord((Map<String, Object>) adapter.getValue());
  }

  @Test
  void testAdapterForRecordHeaders() {
    // given
    Map<String, String> props = new HashMap<>();
    props.put("p1", "v1");
    props.put("p2", "v2");
    Record<GenericObject> record =
        Utils.TestRecord.<GenericObject>builder()
            .schema(Schema.STRING)
            .value(
                AutoConsumeSchema.wrapPrimitiveObject(
                    "test-message", SchemaType.STRING, new byte[] {}))
            .key("test-key")
            .topicName("test-topic")
            .destinationTopic("test-dest-topic")
            .eventTime(1662493532L)
            .properties(props)
            .build();

    Utils.TestContext context = new Utils.TestContext(record, new HashMap<>());
    TransformContext transformContext =
        new TransformContext(context, record.getValue().getNativeObject());

    // when
    JstlTransformContextAdapter adapter = new JstlTransformContextAdapter(transformContext);

    // then
    assertEquals(adapter.getHeader().get("messageKey"), "test-key");
    assertEquals(adapter.getHeader().get("topicName"), "test-topic");
    assertEquals(adapter.getHeader().get("destinationTopic"), "test-dest-topic");
    assertEquals(adapter.getHeader().get("eventTime"), 1662493532L);
    assertTrue(adapter.getHeader().get("properties") instanceof Map);
    Map<String, Object> headerProps = (Map) adapter.getHeader().get("properties");
    assertEquals(headerProps.get("p1"), "v1");
    assertEquals(headerProps.get("p2"), "v2");
    assertNull(headerProps.get("p3"));
  }

  void assertNestedRecord(Map<String, Object> root) {
    assertTrue(root.get("level1Record") instanceof Map);
    Map<String, Object> l1Map = (Map) root.get("level1Record");
    assertEquals(l1Map.get("level2String"), "level2_1");

    assertTrue(l1Map.get("level2Record") instanceof Map);
    Map<String, Object> l2Map = (Map) l1Map.get("level2Record");
    assertEquals(l2Map.get("level3String"), "level3_1");

    assertTrue(l2Map.get("level3Record") instanceof Map);
    Map<String, Object> l3Map = (Map) l2Map.get("level3Record");
    assertEquals(l3Map.get("level4String"), "level4_1");
    assertEquals(l3Map.get("level4Integer"), 9);
    assertEquals(l3Map.get("level4Double"), 8.8D);

    assertNull(l3Map.get("level4Record"));
  }
}
