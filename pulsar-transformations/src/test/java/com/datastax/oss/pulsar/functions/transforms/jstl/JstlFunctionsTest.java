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

import java.time.Instant;
import org.testng.annotations.DataProvider;
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

  @Test
  void testConcatInteger() {
    assertEquals("12", JstlFunctions.concat(1, 2));
    assertEquals("12", JstlFunctions.concat("1", 2));
    assertEquals("12", JstlFunctions.concat(1, "2"));
  }

  @Test
  void testConcatNull() {
    assertEquals("text", JstlFunctions.concat(null, "text"));
    assertEquals("full ", JstlFunctions.concat("full ", null));
    assertEquals("", JstlFunctions.concat(null, null));
  }

  @Test
  void testNow() {
    long minExpectedMillis = Instant.now().toEpochMilli();
    long actualNow = JstlFunctions.now();
    assertTrue(actualNow >= minExpectedMillis);
    assertTrue(actualNow < minExpectedMillis + 100L);
  }

  @Test(dataProvider = "millisDateAddProvider")
  void testAddDateMillis(long input, int delta, String unit, long expected) {
    assertEquals(expected, JstlFunctions.dateadd(input, delta, unit));
  }

  @Test(dataProvider = "isoDateAddProvider")
  void testAddDateIso(String input, int delta, String unit, long expected) {
    assertEquals(expected, JstlFunctions.dateadd(input, delta, unit));
  }

  @Test(
    expectedExceptions = IllegalArgumentException.class,
    expectedExceptionsMessageRegExp = "Invalid input type: Double\\..*"
  )
  void testInvalidAddDate() {
    JstlFunctions.dateadd(7D, 0, "days");
  }

  /**
   * @return {"input date (epoch millis or iso datetime string)", "delta", "unit", "expected value"}
   */
  @DataProvider(name = "millisDateAddProvider")
  public static Object[][] millisDateAddProvider() {
    Instant instant = Instant.parse("2022-10-02T01:02:03Z");
    long millis = instant.toEpochMilli();
    return new Object[][] {
      {millis, 0, "years", instant.toEpochMilli()},
      {millis, 5, "years", Instant.parse("2027-10-02T01:02:03Z").toEpochMilli()},
      {millis, -3, "years", Instant.parse("2019-10-02T01:02:03Z").toEpochMilli()},
      {millis, 0, "months", instant.toEpochMilli()},
      {millis, 5, "months", Instant.parse("2023-03-02T01:02:03Z").toEpochMilli()},
      {millis, -3, "months", Instant.parse("2022-07-02T01:02:03Z").toEpochMilli()},
      {millis, 0, "days", instant.toEpochMilli()},
      {millis, 5, "days", Instant.parse("2022-10-07T01:02:03Z").toEpochMilli()},
      {millis, -3, "days", Instant.parse("2022-09-29T01:02:03Z").toEpochMilli()},
      {millis, 0, "hours", instant.toEpochMilli()},
      {millis, 5, "hours", Instant.parse("2022-10-02T06:02:03Z").toEpochMilli()},
      {millis, -3, "hours", Instant.parse("2022-10-01T22:02:03Z").toEpochMilli()},
      {millis, 0, "minutes", instant.toEpochMilli()},
      {millis, 5, "minutes", Instant.parse("2022-10-02T01:07:03Z").toEpochMilli()},
      {millis, -3, "minutes", Instant.parse("2022-10-02T00:59:03Z").toEpochMilli()},
      {millis, 0, "seconds", instant.toEpochMilli()},
      {millis, 5, "seconds", Instant.parse("2022-10-02T01:02:08Z").toEpochMilli()},
      {millis, -3, "seconds", Instant.parse("2022-10-02T01:02:00Z").toEpochMilli()},
      {millis, 0, "millis", instant.toEpochMilli()},
      {millis, 5, "millis", Instant.parse("2022-10-02T01:02:03.005Z").toEpochMilli()},
      {millis, -3, "millis", Instant.parse("2022-10-02T01:02:02.997Z").toEpochMilli()},
    };
  }

  /**
   * @return {"input date (epoch millis or iso datetime string)", "delta", "unit", "expected value"}
   */
  @DataProvider(name = "isoDateAddProvider")
  public static Object[][] isoDateAddProvider() {
    String isoDateTime = "2022-10-02T01:02:03";
    Instant instant = Instant.parse("2022-10-02T01:02:03Z");
    return new Object[][] {
      {isoDateTime, 0, "years", instant.toEpochMilli()},
      {isoDateTime, 5, "years", Instant.parse("2027-10-02T01:02:03Z").toEpochMilli()},
      {isoDateTime, -3, "years", Instant.parse("2019-10-02T01:02:03Z").toEpochMilli()},
      {isoDateTime, 0, "months", instant.toEpochMilli()},
      {isoDateTime, 5, "months", Instant.parse("2023-03-02T01:02:03Z").toEpochMilli()},
      {isoDateTime, -3, "months", Instant.parse("2022-07-02T01:02:03Z").toEpochMilli()},
      {isoDateTime, 0, "days", instant.toEpochMilli()},
      {isoDateTime, 5, "days", Instant.parse("2022-10-07T01:02:03Z").toEpochMilli()},
      {isoDateTime, -3, "days", Instant.parse("2022-09-29T01:02:03Z").toEpochMilli()},
      {isoDateTime, 0, "hours", instant.toEpochMilli()},
      {isoDateTime, 5, "hours", Instant.parse("2022-10-02T06:02:03Z").toEpochMilli()},
      {isoDateTime, -3, "hours", Instant.parse("2022-10-01T22:02:03Z").toEpochMilli()},
      {isoDateTime, 0, "minutes", instant.toEpochMilli()},
      {isoDateTime, 5, "minutes", Instant.parse("2022-10-02T01:07:03Z").toEpochMilli()},
      {isoDateTime, -3, "minutes", Instant.parse("2022-10-02T00:59:03Z").toEpochMilli()},
      {isoDateTime, 0, "seconds", instant.toEpochMilli()},
      {isoDateTime, 5, "seconds", Instant.parse("2022-10-02T01:02:08Z").toEpochMilli()},
      {isoDateTime, -3, "seconds", Instant.parse("2022-10-02T01:02:00Z").toEpochMilli()},
      {isoDateTime, 0, "millis", instant.toEpochMilli()},
      {isoDateTime, 5, "millis", Instant.parse("2022-10-02T01:02:03.005Z").toEpochMilli()},
      {isoDateTime, -3, "millis", Instant.parse("2022-10-02T01:02:02.997Z").toEpochMilli()},
    };
  }

  @Test(
    expectedExceptions = IllegalArgumentException.class,
    expectedExceptionsMessageRegExp =
        "Invalid unit: lightyear. Should be one of \\[hours, seconds, months, minutes, days, millis, years\\]"
  )
  void testAddDateInvalidUnit() {
    JstlFunctions.dateadd(0L, 0, "lightyear");
  }
}
