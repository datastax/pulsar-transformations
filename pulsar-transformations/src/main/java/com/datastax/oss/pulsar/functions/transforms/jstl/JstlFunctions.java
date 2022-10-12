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

import java.time.Clock;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.time.temporal.ChronoUnit;
import java.util.HashMap;
import java.util.Map;
import lombok.Setter;

/** Provides convenience methods to use in jstl expression. All functions should be static. */
public class JstlFunctions {
  @Setter private static Clock clock = Clock.systemUTC();

  public static String uppercase(Object input) {
    return input == null ? null : input.toString().toUpperCase();
  }

  public static String lowercase(Object input) {
    return input == null ? null : input.toString().toLowerCase();
  }

  public static boolean contains(Object input, Object value) {
    if (input == null || value == null) {
      return false;
    }
    return input.toString().contains(value.toString());
  }

  public static String trim(Object input) {
    return input == null ? null : input.toString().trim();
  }

  public static String concat(Object first, Object second) {
    return (first == null ? "" : first.toString()) + (second == null ? "" : second.toString());
  }

  public static Object coalesce(Object value, Object valueIfNull) {
    return value == null ? valueIfNull : value;
  }

  public static long now() {
    return clock.millis();
  }

  private static final Map<String, ChronoUnit> dateAddUnits = new HashMap<>();

  static {
    dateAddUnits.put("years", ChronoUnit.YEARS);
    dateAddUnits.put("months", ChronoUnit.MONTHS);
    dateAddUnits.put("days", ChronoUnit.DAYS);
    dateAddUnits.put("hours", ChronoUnit.HOURS);
    dateAddUnits.put("minutes", ChronoUnit.MINUTES);
    dateAddUnits.put("seconds", ChronoUnit.SECONDS);
    dateAddUnits.put("millis", ChronoUnit.MILLIS);
  }

  public static long dateadd(Object input, long delta, String unit) {
    if (input instanceof String) {
      return dateadd((String) input, delta, unit);
    } else if (input instanceof Long) {
      return dateadd((long) input, delta, unit);
    }

    throw new IllegalArgumentException(
        "Invalid input type: "
            + input.getClass().getSimpleName()
            + ". Should either be an RFC3339 datetime string like '2007-12-01T12:30:00Z' or epoch millis");
  }

  private static long dateadd(String rfc3339, long delta, String unit) {
    OffsetDateTime offsetDateTime = OffsetDateTime.parse(rfc3339);
    return dateadd(offsetDateTime, delta, unit);
  }

  private static long dateadd(long epochMillis, long delta, String unit) {
    Instant instant = Instant.ofEpochMilli(epochMillis);
    OffsetDateTime localDateTime = OffsetDateTime.ofInstant(instant, ZoneOffset.UTC);
    return dateadd(localDateTime, delta, unit);
  }

  private static long dateadd(OffsetDateTime offsetDateTime, long delta, String unit) {
    ChronoUnit chronoUnit = dateAddUnits.get(unit);
    if (chronoUnit == null) {
      throw new IllegalArgumentException(
          "Invalid unit: " + unit + ". Should be one of " + dateAddUnits.keySet());
    }

    return offsetDateTime.plus(delta, chronoUnit).toInstant().toEpochMilli();
  }
}
