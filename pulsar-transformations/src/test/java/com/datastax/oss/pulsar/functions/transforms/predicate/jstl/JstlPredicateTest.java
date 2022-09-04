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
package com.datastax.oss.pulsar.functions.transforms.predicate.jstl;

import static org.testng.AssertJUnit.assertTrue;

import com.datastax.oss.pulsar.functions.transforms.TransformContext;
import com.datastax.oss.pulsar.functions.transforms.Utils;
import java.util.HashMap;
import org.apache.pulsar.client.api.schema.GenericObject;
import org.apache.pulsar.functions.api.Record;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

public class JstlPredicateTest {

  @Test(dataProvider = "jstlPredicates")
  void testKeyScope(String when, boolean match) {
    JstlPredicate predicate = new JstlPredicate(when);

    Record<GenericObject> record = Utils.createNestedAvroKeyValueRecord(2);
    Utils.TestContext context = new Utils.TestContext(record, new HashMap<>());
    TransformContext transformContext =
        new TransformContext(context, record.getValue().getNativeObject());

    assertTrue(predicate.test(transformContext) == match);
  }

  /** @return {"expression", "record", "expectedMatch"} */
  @DataProvider(name = "jstlPredicates")
  public static Object[][] jstlPredicates() {
    return new Object[][] {
      // match
      {"${key.level1String == 'level1_1'}", true},
      {"${key.level1Record.level2String == 'level2_1'}", true},
      {"${key.level1Record.level2Integer == 9}", true},
      {"${key.level1Record.level2Double == 8.8}", true},
      {"${value.level1String.contains('level1')}", true},
      {"${value.level1Record.level2String.toUpperCase() == 'LEVEL2_1'}", true},
      {"${value.level1Record.level2Integer > 8}", true},
      {"${value.level1Record.level2Double < 8.9}", true},
      {"${header.key == 'key-1'}", true},
      {"${header.producerName == 'producer-1'}", true},
      {"${header.topicName == 'topic-1'}", true},
      {"${header.properties.p1 == 'v1'}", true},
      {"${header.properties.p2 == 'v2'}", true},
      // no match
      {"${key.level1String == 'leVel1_1'}", false},
      {"${key.level1Record.random == 'level2_1'}", false},
      {"${key.level1Record.level2Integer != 9}", false},
      {"${key.level1Record.level2Double < 8.8}", false},
      {"${value.level1String.contains('level2')}", false},
      {"${value.level1Record.level2String.toUpperCase() == 'LeVEL2_1'}", false},
      {"${value.level1Record.level2Integer > 10}", false},
      {"${value.level1Record.level2Double < 0}", false},
      {"${header.key == 'key1'}", false},
      {"${header.producerNames == 'producer-1'}", false},
      {"${header.topicName != 'topic-1'}", false},
      {"${header.properties.p1.substring(0,1) == 'v1'}", false},
      {"${header.properties.p2 == 'v3'}", false},
      // complex
      {
        "${key.level1String == 'level1_1' && key.level1Record.level2String == 'level2_1' && "
            + " key.level1Record.level2Integer == 9 && key.level1Record.level2Double == 8.8 && "
            + "value.level1String.contains('level1')}",
        true
      },
      {
        "${key.level1String == 'level1_1' || key.level1Record.level2String == 'random' || "
            + " key.level1Record.level2Integer == 5 || key.level1Record.level2Double != 8.8 || "
            + "value.level1String.contains('level1')}",
        true
      },
      {
        "${key.level1String == 'level1_1' && key.level1Record.level2String == 'level2_1' && "
            + " key.level1Record.level2Integer == 9} && key.level1Record.level2Double != 8.8} && "
            + "value.level1String.contains('level1')}",
        false
      },
    };
  }
}
