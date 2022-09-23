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

import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertNull;
import static org.testng.AssertJUnit.assertTrue;

import org.testng.annotations.Test;

public class NullableTypeConverterTest {

  @Test
  void testNullConversion() {
    NullableTypeConverter converter = new NullableTypeConverter();
    assertNull(converter.convert(null, String.class));
    assertNull(converter.convert(null, int.class));
    assertNull(converter.convert(null, long.class));
    assertNull(converter.convert(null, float.class));
    assertNull(converter.convert(null, double.class));
    assertNull(converter.convert(null, boolean.class));
  }

  @Test
  void testNonNullConversion() {
    NullableTypeConverter converter = new NullableTypeConverter();
    assertEquals(converter.convert("test", String.class), "test");
    assertEquals((int) converter.convert(1, int.class), 1);
    assertEquals((long) converter.convert(1L, long.class), 1L);
    assertEquals(converter.convert(1.3F, float.class), 1.3F);
    assertEquals(converter.convert(1.4D, double.class), 1.4D);
    assertTrue(converter.convert(true, boolean.class));
  }
}
