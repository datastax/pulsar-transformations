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
import static org.testng.AssertJUnit.assertFalse;
import static org.testng.AssertJUnit.assertTrue;

import org.testng.annotations.Test;

public class JstlFunctionsTest {

  @Test
  void testUpperCase() {
    assertEquals("UPPERCASE", JstlFunctions.uppercase("uppercase"));
  }

  @Test
  void testUpperCaseInteger() {}

  @Test
  void testUpperCaseNull() {
    assertEquals(null, JstlFunctions.uppercase(null));
  }

  @Test
  void testLowerCase() {
    assertEquals("lowercase", JstlFunctions.lowercase("LOWERCASE"));
  }

  @Test
  void testLowerCaseInteger() {
    assertEquals("10", JstlFunctions.uppercase(10));
  }

  @Test
  void testLowerCaseNull() {
    assertEquals(null, JstlFunctions.uppercase(null));
  }

  @Test
  void testContains() {
    assertTrue(JstlFunctions.contains("full text", "l t"));
    assertTrue(JstlFunctions.contains("full text", "full"));
    assertTrue(JstlFunctions.contains("full text", "text"));

    assertFalse(JstlFunctions.contains("full text", "lt"));
    assertFalse(JstlFunctions.contains("full text", "fll"));
    assertFalse(JstlFunctions.contains("full text", "txt"));
  }

  @Test
  void testContainsInteger() {
    assertTrue(JstlFunctions.contains(123, "2"));
    assertTrue(JstlFunctions.contains("123", 3));
    assertTrue(JstlFunctions.contains(123, 3));
    assertTrue(JstlFunctions.contains("123", "3"));

    assertFalse(JstlFunctions.contains(123, "4"));
    assertFalse(JstlFunctions.contains("123", 4));
    assertFalse(JstlFunctions.contains(123, 4));
    assertFalse(JstlFunctions.contains("123", "4"));
  }

  @Test
  void testContainsNull() {
    assertFalse(JstlFunctions.contains("null", null));
    assertFalse(JstlFunctions.contains(null, "null"));
    assertFalse(JstlFunctions.contains(null, null));
  }

  @Test
  void testConcat() {
    assertEquals("full text", JstlFunctions.concat("full ", "text"));
  }

  void testConcatInteger() {
    assertEquals("12", JstlFunctions.concat(1, 2));
    assertEquals("12", JstlFunctions.concat("1", 2));
    assertEquals("12", JstlFunctions.concat(1, "2"));
  }

  void testConcatNull() {
    assertEquals("text", JstlFunctions.concat(null, "text"));
    assertEquals("full ", JstlFunctions.concat("full ", null));
    assertEquals("", JstlFunctions.concat(null, null));
  }
}
