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

import com.datastax.oss.pulsar.jms.selectors.SelectorSupport;
import javax.jms.JMSException;
import org.testng.annotations.Test;

public class FiltersTest {

  @Test
  public void testFilter() throws JMSException {
    SelectorSupport ss = SelectorSupport.build("prop1 = 'my-prop1' AND prop2 = 'my-prop12'", true);

    boolean m =
        ss.matches(
            (a) -> {
              switch (a) {
                case "prop1":
                  return "my-prop1";
                case "prop2":
                  return "my-prop12";
                default:
                  return "";
              }
            });

    System.out.println(m);
  }
}
