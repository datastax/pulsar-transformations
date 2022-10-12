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

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
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
    long expectedMillis = 99L;
    Clock clock = Clock.fixed(Instant.ofEpochMilli(expectedMillis), ZoneOffset.UTC);
    JstlFunctions.setClock(clock);
    assertEquals(expectedMillis, JstlFunctions.now());
  }

  @Test(dataProvider = "millisDateAddProvider")
  void testAddDateMillis(long input, int delta, String unit, long expected) {
    assertEquals(expected, JstlFunctions.dateadd(input, delta, unit));
  }

  @Test(dataProvider = "utcDateAddProvider")
  void testAddDateUTC(String input, int delta, String unit, long expected) {
    assertEquals(expected, JstlFunctions.dateadd(input, delta, unit));
  }

  @Test(dataProvider = "nonUtcDateAddProvider")
  void testAddDateNonUTC(String input, int delta, String unit, long expected) {
    assertEquals(expected, JstlFunctions.dateadd(input, delta, unit));
  }

  @Test(
    expectedExceptions = IllegalArgumentException.class,
    expectedExceptionsMessageRegExp = "Invalid input type: Double\\..*"
  )
  void testInvalidAddDate() {
    JstlFunctions.dateadd(7D, 0, "days");
  }

  /** @return {"input date in epoch millis", "delta", "unit", "expected value (in epoch millis)"} */
  @DataProvider(name = "millisDateAddProvider")
  public static Object[][] millisDateAddProvider() {
    OffsetDateTime offsetDateTime = OffsetDateTime.parse("2022-10-02T01:02:03Z");
    long millis = offsetDateTime.toInstant().toEpochMilli();
    return new Object[][] {
      {millis, 0, "years", offsetDateTime.toInstant().toEpochMilli()},
      {millis, 5, "years", Instant.parse("2027-10-02T01:02:03Z").toEpochMilli()},
      {millis, -3, "years", Instant.parse("2019-10-02T01:02:03Z").toEpochMilli()},
      {millis, 0, "months", offsetDateTime.toInstant().toEpochMilli()},
      {millis, 5, "months", Instant.parse("2023-03-02T01:02:03Z").toEpochMilli()},
      {millis, -3, "months", Instant.parse("2022-07-02T01:02:03Z").toEpochMilli()},
      {millis, 0, "days", offsetDateTime.toInstant().toEpochMilli()},
      {millis, 5, "days", Instant.parse("2022-10-07T01:02:03Z").toEpochMilli()},
      {millis, -3, "days", Instant.parse("2022-09-29T01:02:03Z").toEpochMilli()},
      {millis, 0, "hours", offsetDateTime.toInstant().toEpochMilli()},
      {millis, 5, "hours", Instant.parse("2022-10-02T06:02:03Z").toEpochMilli()},
      {millis, -3, "hours", Instant.parse("2022-10-01T22:02:03Z").toEpochMilli()},
      {millis, 0, "minutes", offsetDateTime.toInstant().toEpochMilli()},
      {millis, 5, "minutes", Instant.parse("2022-10-02T01:07:03Z").toEpochMilli()},
      {millis, -3, "minutes", Instant.parse("2022-10-02T00:59:03Z").toEpochMilli()},
      {millis, 0, "seconds", offsetDateTime.toInstant().toEpochMilli()},
      {millis, 5, "seconds", Instant.parse("2022-10-02T01:02:08Z").toEpochMilli()},
      {millis, -3, "seconds", Instant.parse("2022-10-02T01:02:00Z").toEpochMilli()},
      {millis, 0, "millis", offsetDateTime.toInstant().toEpochMilli()},
      {millis, 5, "millis", Instant.parse("2022-10-02T01:02:03.005Z").toEpochMilli()},
      {millis, -3, "millis", Instant.parse("2022-10-02T01:02:02.997Z").toEpochMilli()},
    };
  }

  /**
   * @return {"input date in rfc 3339 format", "delta", "unit", "expected value (in epoch millis)"}
   */
  @DataProvider(name = "utcDateAddProvider")
  public static Object[][] utcDateAddProvider() {
    String utcDateTime = "2022-10-02T01:02:03Z";
    OffsetDateTime offsetDateTime = OffsetDateTime.parse("2022-10-02T01:02:03Z");
    return new Object[][] {
      {utcDateTime, 0, "years", offsetDateTime.toInstant().toEpochMilli()},
      {utcDateTime, 5, "years", Instant.parse("2027-10-02T01:02:03Z").toEpochMilli()},
      {utcDateTime, -3, "years", Instant.parse("2019-10-02T01:02:03Z").toEpochMilli()},
      {utcDateTime, 0, "months", offsetDateTime.toInstant().toEpochMilli()},
      {utcDateTime, 5, "months", Instant.parse("2023-03-02T01:02:03Z").toEpochMilli()},
      {utcDateTime, -3, "months", Instant.parse("2022-07-02T01:02:03Z").toEpochMilli()},
      {utcDateTime, 0, "days", offsetDateTime.toInstant().toEpochMilli()},
      {utcDateTime, 5, "days", Instant.parse("2022-10-07T01:02:03Z").toEpochMilli()},
      {utcDateTime, -3, "days", Instant.parse("2022-09-29T01:02:03Z").toEpochMilli()},
      {utcDateTime, 0, "hours", offsetDateTime.toInstant().toEpochMilli()},
      {utcDateTime, 5, "hours", Instant.parse("2022-10-02T06:02:03Z").toEpochMilli()},
      {utcDateTime, -3, "hours", Instant.parse("2022-10-01T22:02:03Z").toEpochMilli()},
      {utcDateTime, 0, "minutes", offsetDateTime.toInstant().toEpochMilli()},
      {utcDateTime, 5, "minutes", Instant.parse("2022-10-02T01:07:03Z").toEpochMilli()},
      {utcDateTime, -3, "minutes", Instant.parse("2022-10-02T00:59:03Z").toEpochMilli()},
      {utcDateTime, 0, "seconds", offsetDateTime.toInstant().toEpochMilli()},
      {utcDateTime, 5, "seconds", Instant.parse("2022-10-02T01:02:08Z").toEpochMilli()},
      {utcDateTime, -3, "seconds", Instant.parse("2022-10-02T01:02:00Z").toEpochMilli()},
      {utcDateTime, 0, "millis", offsetDateTime.toInstant().toEpochMilli()},
      {utcDateTime, 5, "millis", Instant.parse("2022-10-02T01:02:03.005Z").toEpochMilli()},
      {utcDateTime, -3, "millis", Instant.parse("2022-10-02T01:02:02.997Z").toEpochMilli()},
    };
  }

  /**
   * @return {"input date in rfc 3339 format)", "delta", "unit", "expected value (in epoch millis)"}
   */
  @DataProvider(name = "nonUtcDateAddProvider")
  public static Object[][] nonUtcDateAddProvider() {
    String nonUtcDateTime = "2022-10-02T01:02:03+02:00";
    long twoHoursMillis = Duration.ofHours(2).toMillis();
    OffsetDateTime offsetDateTime = OffsetDateTime.parse("2022-10-02T01:02:03Z");
    return new Object[][] {
      {nonUtcDateTime, 0, "years", offsetDateTime.toInstant().toEpochMilli() - twoHoursMillis},
      {
        nonUtcDateTime,
        5,
        "years",
        Instant.parse("2027-10-02T01:02:03Z").toEpochMilli() - twoHoursMillis
      },
      {
        nonUtcDateTime,
        -3,
        "years",
        Instant.parse("2019-10-02T01:02:03Z").toEpochMilli() - twoHoursMillis
      },
      {nonUtcDateTime, 0, "months", offsetDateTime.toInstant().toEpochMilli() - twoHoursMillis},
      {
        nonUtcDateTime,
        5,
        "months",
        Instant.parse("2023-03-02T01:02:03Z").toEpochMilli() - twoHoursMillis
      },
      {
        nonUtcDateTime,
        -3,
        "months",
        Instant.parse("2022-07-02T01:02:03Z").toEpochMilli() - twoHoursMillis
      },
      {nonUtcDateTime, 0, "days", offsetDateTime.toInstant().toEpochMilli() - twoHoursMillis},
      {
        nonUtcDateTime,
        5,
        "days",
        Instant.parse("2022-10-07T01:02:03Z").toEpochMilli() - twoHoursMillis
      },
      {
        nonUtcDateTime,
        -3,
        "days",
        Instant.parse("2022-09-29T01:02:03Z").toEpochMilli() - twoHoursMillis
      },
      {nonUtcDateTime, 0, "hours", offsetDateTime.toInstant().toEpochMilli() - twoHoursMillis},
      {
        nonUtcDateTime,
        5,
        "hours",
        Instant.parse("2022-10-02T06:02:03Z").toEpochMilli() - twoHoursMillis
      },
      {
        nonUtcDateTime,
        -3,
        "hours",
        Instant.parse("2022-10-01T22:02:03Z").toEpochMilli() - twoHoursMillis
      },
      {nonUtcDateTime, 0, "minutes", offsetDateTime.toInstant().toEpochMilli() - twoHoursMillis},
      {
        nonUtcDateTime,
        5,
        "minutes",
        Instant.parse("2022-10-02T01:07:03Z").toEpochMilli() - twoHoursMillis
      },
      {
        nonUtcDateTime,
        -3,
        "minutes",
        Instant.parse("2022-10-02T00:59:03Z").toEpochMilli() - twoHoursMillis
      },
      {nonUtcDateTime, 0, "seconds", offsetDateTime.toInstant().toEpochMilli() - twoHoursMillis},
      {
        nonUtcDateTime,
        5,
        "seconds",
        Instant.parse("2022-10-02T01:02:08Z").toEpochMilli() - twoHoursMillis
      },
      {
        nonUtcDateTime,
        -3,
        "seconds",
        Instant.parse("2022-10-02T01:02:00Z").toEpochMilli() - twoHoursMillis
      },
      {nonUtcDateTime, 0, "millis", offsetDateTime.toInstant().toEpochMilli() - twoHoursMillis},
      {
        nonUtcDateTime,
        5,
        "millis",
        Instant.parse("2022-10-02T01:02:03.005Z").toEpochMilli() - twoHoursMillis
      },
      {
        nonUtcDateTime,
        -3,
        "millis",
        Instant.parse("2022-10-02T01:02:02.997Z").toEpochMilli() - twoHoursMillis
      },
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
