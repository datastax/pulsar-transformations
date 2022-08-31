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

import static org.testng.AssertJUnit.assertFalse;
import static org.testng.AssertJUnit.assertTrue;

import java.util.HashMap;
import org.apache.pulsar.client.api.schema.GenericObject;
import org.apache.pulsar.functions.api.Record;
import org.testng.annotations.Test;

public class JmsPredicateTest {

  @Test
  void testKeyScope() {
    JmsPredicate match = new JmsPredicate("key.keyField1='key1' AND key.keyField2='key2'");
    JmsPredicate matchLike = new JmsPredicate("key.keyField1 LIKE '%k%'");
    JmsPredicate dontMatch = new JmsPredicate("value.keyField1 ='key1'");
    JmsPredicate dontMatchLike = new JmsPredicate("key.keyField1 LIKE '%value%'");
    Record<GenericObject> record = Utils.createTestAvroKeyValueRecord();
    Utils.TestContext context = new Utils.TestContext(record, new HashMap<>());
    TransformContext transformContext =
        new TransformContext(context, record.getValue().getNativeObject());

    assertTrue(match.test(transformContext));
    assertTrue(matchLike.test(transformContext));
    assertFalse(dontMatch.test(transformContext));
    assertFalse(dontMatchLike.test(transformContext));
  }

  @Test
  void testValueScope() {
    JmsPredicate match =
        new JmsPredicate("value.valueField1='value1' AND value.valueField2='value2'");
    JmsPredicate matchLike = new JmsPredicate("value.valueField1 LIKE '%val%'");
    JmsPredicate dontMatch = new JmsPredicate("key.valueField1 ='value1'");
    JmsPredicate dontMatchLike = new JmsPredicate("value.valueField1 LIKE '%key%'");
    Record<GenericObject> record = Utils.createTestAvroKeyValueRecord();
    Utils.TestContext context = new Utils.TestContext(record, new HashMap<>());
    TransformContext transformContext =
        new TransformContext(context, record.getValue().getNativeObject());

    assertTrue(match.test(transformContext));
    assertTrue(matchLike.test(transformContext));
    assertFalse(dontMatch.test(transformContext));
    assertFalse(dontMatchLike.test(transformContext));
  }

  @Test
  void testMessageProps() {
    JmsPredicate match = new JmsPredicate("p1='value1' AND p2='value2'");
    JmsPredicate matchLike = new JmsPredicate("p1 LIKE '%val%'");
    JmsPredicate dontMatch = new JmsPredicate("p1='key1'");
    JmsPredicate dontMatchLike = new JmsPredicate("p1 LIKE '%key%'");
    Record<GenericObject> record = Utils.createTestAvroKeyValueRecord();
    Utils.TestContext context = new Utils.TestContext(record, new HashMap<>());
    TransformContext transformContext =
        new TransformContext(context, record.getValue().getNativeObject());

    assertTrue(match.test(transformContext));
    assertTrue(matchLike.test(transformContext));
    assertFalse(dontMatch.test(transformContext));
    assertFalse(dontMatchLike.test(transformContext));
  }

  @Test
  void testMessageKeyValueProps() {
    JmsPredicate match =
        new JmsPredicate(
            "key.keyField1='key1' AND key.keyField2='key2' "
                + "AND value.valueField1='value1' AND value.valueField2='value2' AND p1='value1' AND p2='value2'");
    JmsPredicate matchLike =
        new JmsPredicate(
            "key.keyField1 LIKE '%k%' AND value.valueField1 LIKE '%val%' AND p1 LIKE '%val%'");
    JmsPredicate dontMatch =
        new JmsPredicate(
            "key.keyField1='key1' AND key.keyField2='key2' "
                + "AND value.valueField1='value1' AND value.valueField2='value2' AND p1='value1' AND p2='value3'");
    JmsPredicate dontMatchLike =
        new JmsPredicate(
            "key.keyField1 LIKE '%k%' AND value.valueField1 LIKE '%val%' AND p1 LIKE '%random%'");
    Record<GenericObject> record = Utils.createTestAvroKeyValueRecord();
    Utils.TestContext context = new Utils.TestContext(record, new HashMap<>());
    TransformContext transformContext =
        new TransformContext(context, record.getValue().getNativeObject());

    assertTrue(match.test(transformContext));
    assertTrue(matchLike.test(transformContext));
    assertFalse(dontMatch.test(transformContext));
    assertFalse(dontMatchLike.test(transformContext));
  }
}
