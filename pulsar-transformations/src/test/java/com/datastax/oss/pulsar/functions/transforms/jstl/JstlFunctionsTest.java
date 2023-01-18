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
import static org.testng.AssertJUnit.assertNull;
import static org.testng.AssertJUnit.assertTrue;

import java.nio.charset.StandardCharsets;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalTime;
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
    assertNull(JstlFunctions.uppercase(null));
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
    assertNull(JstlFunctions.uppercase(null));
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
    Instant fixedInstant = Instant.now();
    Clock clock = Clock.fixed(fixedInstant, ZoneOffset.UTC);
    JstlFunctions.setClock(clock);
    assertEquals(fixedInstant, JstlFunctions.now());
  }

  @Test(dataProvider = "millisTimestampAddProvider")
  void testAddDateMillis(long input, int delta, String unit, Instant expected) {
    assertEquals(expected, JstlFunctions.timestampAdd(input, delta, unit));
  }

  @Test(dataProvider = "utcTimestampAddProvider")
  void testAddDateUTC(String input, int delta, String unit, Instant expected) {
    assertEquals(expected, JstlFunctions.timestampAdd(input, delta, unit));
  }

  @Test(dataProvider = "nonUtcTimestampAddProvider")
  void testAddDateNonUTC(String input, int delta, String unit, Instant expected) {
    assertEquals(expected, JstlFunctions.timestampAdd(input, delta, unit));
  }

  @Test
  void testAddDateDeltaConversion() {
    assertEquals(
        Instant.parse("2022-10-02T02:02:03Z"),
        JstlFunctions.timestampAdd("2022-10-02T01:02:03Z", LocalTime.of(1, 0, 0), "millis"));
  }

  @Test
  void testAddDateUnitConversion() {
    assertEquals(
        Instant.parse("2022-10-02T02:02:03Z"),
        JstlFunctions.timestampAdd(
            "2022-10-02T01:02:03Z", 1, "hours".getBytes(StandardCharsets.UTF_8)));
  }

  @Test(expectedExceptions = jakarta.el.ELException.class)
  void testInvalidAddDate() {
    JstlFunctions.dateadd((byte) 7, 0, "days");
  }

  /** @return {"input date in epoch millis", "delta", "unit", "expected value (in epoch millis)"} */
  @DataProvider(name = "millisTimestampAddProvider")
  public static Object[][] millisTimestampAddProvider() {
    Instant instant = Instant.parse("2022-10-02T01:02:03Z");
    long millis = instant.toEpochMilli();
    return new Object[][] {
      {millis, 0, "years", instant},
      {millis, 5, "years", Instant.parse("2027-10-02T01:02:03Z")},
      {millis, -3, "years", Instant.parse("2019-10-02T01:02:03Z")},
      {millis, 0, "months", instant},
      {millis, 5, "months", Instant.parse("2023-03-02T01:02:03Z")},
      {millis, -3, "months", Instant.parse("2022-07-02T01:02:03Z")},
      {millis, 0, "days", instant},
      {millis, 5, "days", Instant.parse("2022-10-07T01:02:03Z")},
      {millis, -3, "days", Instant.parse("2022-09-29T01:02:03Z")},
      {millis, 0, "hours", instant},
      {millis, 5, "hours", Instant.parse("2022-10-02T06:02:03Z")},
      {millis, -3, "hours", Instant.parse("2022-10-01T22:02:03Z")},
      {millis, 0, "minutes", instant},
      {millis, 5, "minutes", Instant.parse("2022-10-02T01:07:03Z")},
      {millis, -3, "minutes", Instant.parse("2022-10-02T00:59:03Z")},
      {millis, 0, "seconds", instant},
      {millis, 5, "seconds", Instant.parse("2022-10-02T01:02:08Z")},
      {millis, -3, "seconds", Instant.parse("2022-10-02T01:02:00Z")},
      {millis, 0, "millis", instant},
      {millis, 5, "millis", Instant.parse("2022-10-02T01:02:03.005Z")},
      {millis, -3, "millis", Instant.parse("2022-10-02T01:02:02.997Z")},
      {millis, 0, "nanos", instant},
      {millis, 5_000_000, "nanos", Instant.parse("2022-10-02T01:02:03.005Z")},
      {millis, -3_000_000, "nanos", Instant.parse("2022-10-02T01:02:02.997Z")},
    };
  }

  /** @return {"input date", "delta", "unit", "expected value"} */
  @DataProvider(name = "utcTimestampAddProvider")
  public static Object[][] utcTimestampAddProvider() {
    String utcDateTime = "2022-10-02T01:02:03Z";
    Instant instant = Instant.parse("2022-10-02T01:02:03Z");
    return new Object[][] {
      {utcDateTime, 0, "years", instant},
      {utcDateTime, 5, "years", Instant.parse("2027-10-02T01:02:03Z")},
      {utcDateTime, -3, "years", Instant.parse("2019-10-02T01:02:03Z")},
      {utcDateTime, 0, "months", instant},
      {utcDateTime, 5, "months", Instant.parse("2023-03-02T01:02:03Z")},
      {utcDateTime, -3, "months", Instant.parse("2022-07-02T01:02:03Z")},
      {utcDateTime, 0, "days", instant},
      {utcDateTime, 5, "days", Instant.parse("2022-10-07T01:02:03Z")},
      {utcDateTime, -3, "days", Instant.parse("2022-09-29T01:02:03Z")},
      {utcDateTime, 0, "hours", instant},
      {utcDateTime, 5, "hours", Instant.parse("2022-10-02T06:02:03Z")},
      {utcDateTime, -3, "hours", Instant.parse("2022-10-01T22:02:03Z")},
      {utcDateTime, 0, "minutes", instant},
      {utcDateTime, 5, "minutes", Instant.parse("2022-10-02T01:07:03Z")},
      {utcDateTime, -3, "minutes", Instant.parse("2022-10-02T00:59:03Z")},
      {utcDateTime, 0, "seconds", instant},
      {utcDateTime, 5, "seconds", Instant.parse("2022-10-02T01:02:08Z")},
      {utcDateTime, -3, "seconds", Instant.parse("2022-10-02T01:02:00Z")},
      {utcDateTime, 0, "millis", instant},
      {utcDateTime, 5, "millis", Instant.parse("2022-10-02T01:02:03.005Z")},
      {utcDateTime, -3, "millis", Instant.parse("2022-10-02T01:02:02.997Z")},
      {utcDateTime, 0, "nanos", instant},
      {utcDateTime, 5_000_000, "nanos", Instant.parse("2022-10-02T01:02:03.005Z")},
      {utcDateTime, -3_000_000, "nanos", Instant.parse("2022-10-02T01:02:02.997Z")},
    };
  }

  /** @return {"input date", "delta", "unit", "expected value (in epoch millis)"} */
  @DataProvider(name = "nonUtcTimestampAddProvider")
  public static Object[][] nonUtcTimestampAddProvider() {
    String nonUtcDateTime = "2022-10-02T01:02:03+02:00";
    long twoHoursMillis = Duration.ofHours(2).toMillis();
    Instant instant = Instant.parse("2022-10-02T01:02:03Z");
    return new Object[][] {
      {nonUtcDateTime, 0, "years", instant.minusMillis(twoHoursMillis)},
      {
        nonUtcDateTime,
        5,
        "years",
        Instant.parse("2027-10-02T01:02:03Z").minusMillis(twoHoursMillis)
      },
      {
        nonUtcDateTime,
        -3,
        "years",
        Instant.parse("2019-10-02T01:02:03Z").minusMillis(twoHoursMillis)
      },
      {nonUtcDateTime, 0, "months", instant.minusMillis(twoHoursMillis)},
      {
        nonUtcDateTime,
        5,
        "months",
        Instant.parse("2023-03-02T01:02:03Z").minusMillis(twoHoursMillis)
      },
      {
        nonUtcDateTime,
        -3,
        "months",
        Instant.parse("2022-07-02T01:02:03Z").minusMillis(twoHoursMillis)
      },
      {nonUtcDateTime, 0, "days", instant.minusMillis(twoHoursMillis)},
      {
        nonUtcDateTime, 5, "days", Instant.parse("2022-10-07T01:02:03Z").minusMillis(twoHoursMillis)
      },
      {
        nonUtcDateTime,
        -3,
        "days",
        Instant.parse("2022-09-29T01:02:03Z").minusMillis(twoHoursMillis)
      },
      {nonUtcDateTime, 0, "hours", instant.minusMillis(twoHoursMillis)},
      {
        nonUtcDateTime,
        5,
        "hours",
        Instant.parse("2022-10-02T06:02:03Z").minusMillis(twoHoursMillis)
      },
      {
        nonUtcDateTime,
        -3,
        "hours",
        Instant.parse("2022-10-01T22:02:03Z").minusMillis(twoHoursMillis)
      },
      {nonUtcDateTime, 0, "minutes", instant.minusMillis(twoHoursMillis)},
      {
        nonUtcDateTime,
        5,
        "minutes",
        Instant.parse("2022-10-02T01:07:03Z").minusMillis(twoHoursMillis)
      },
      {
        nonUtcDateTime,
        -3,
        "minutes",
        Instant.parse("2022-10-02T00:59:03Z").minusMillis(twoHoursMillis)
      },
      {nonUtcDateTime, 0, "seconds", instant.minusMillis(twoHoursMillis)},
      {
        nonUtcDateTime,
        5,
        "seconds",
        Instant.parse("2022-10-02T01:02:08Z").minusMillis(twoHoursMillis)
      },
      {
        nonUtcDateTime,
        -3,
        "seconds",
        Instant.parse("2022-10-02T01:02:00Z").minusMillis(twoHoursMillis)
      },
      {nonUtcDateTime, 0, "millis", instant.minusMillis(twoHoursMillis)},
      {
        nonUtcDateTime,
        5,
        "millis",
        Instant.parse("2022-10-02T01:02:03.005Z").minusMillis(twoHoursMillis)
      },
      {
        nonUtcDateTime,
        -3,
        "millis",
        Instant.parse("2022-10-02T01:02:02.997Z").minusMillis(twoHoursMillis)
      },
      {nonUtcDateTime, 0, "nanos", instant.minusMillis(twoHoursMillis)},
      {
        nonUtcDateTime,
        5_000_000,
        "nanos",
        Instant.parse("2022-10-02T01:02:03.005Z").minusMillis(twoHoursMillis)
      },
      {
        nonUtcDateTime,
        -3_000_000,
        "nanos",
        Instant.parse("2022-10-02T01:02:02.997Z").minusMillis(twoHoursMillis)
      },
    };
  }

  @Test(
    expectedExceptions = IllegalArgumentException.class,
    expectedExceptionsMessageRegExp =
        "Invalid unit: lightyear. Should be one of \\[years, months, days, hours, minutes, seconds, millis\\]"
  )
  void testAddDateInvalidUnit() {
    JstlFunctions.timestampAdd(0L, 0, "lightyear");
  }
}
