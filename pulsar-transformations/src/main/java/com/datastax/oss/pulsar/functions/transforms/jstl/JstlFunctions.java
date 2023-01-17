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

import jakarta.el.ELException;
import java.time.Clock;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import lombok.Setter;
import org.apache.el.util.MessageFactory;

/** Provides convenience methods to use in jstl expression. All functions should be static. */
public class JstlFunctions {
  @Setter private static Clock clock = Clock.systemUTC();

  public static String uppercase(Object input) {
    return input == null ? null : toString(input).toUpperCase();
  }

  public static String lowercase(Object input) {
    return input == null ? null : toString(input).toLowerCase();
  }

  public static boolean contains(Object input, Object value) {
    if (input == null || value == null) {
      return false;
    }
    return toString(input).contains(toString(value));
  }

  public static String trim(Object input) {
    return input == null ? null : toString(input).trim();
  }

  public static String concat(Object first, Object second) {
    return toString(first) + toString(second);
  }

  public static Object coalesce(Object value, Object valueIfNull) {
    return value == null ? valueIfNull : value;
  }

  public static String replace(Object input, Object regex, Object replacement) {
    if (input == null) {
      return null;
    }
    if (regex == null || replacement == null) {
      return input.toString();
    }
    return toString(input).replaceAll(toString(regex), toString(replacement));
  }

  public static String toString(Object input) {
    return CustomTypeConverter.INSTANCE.coerceToString(input);
  }

  public static Instant now() {
    return Instant.now(clock);
  }

  public static Instant timestampAdd(Object input, Object delta, Object unit) {
    if (input == null || unit == null) {
      throw new ELException(MessageFactory.get("error.method.notypes"));
    }

    ChronoUnit chronoUnit;
    switch (CustomTypeConverter.INSTANCE.coerceToString(unit)) {
      case "years":
        chronoUnit = ChronoUnit.YEARS;
        break;
      case "months":
        chronoUnit = ChronoUnit.MONTHS;
        break;
      case "days":
        chronoUnit = ChronoUnit.DAYS;
        break;
      case "hours":
        chronoUnit = ChronoUnit.HOURS;
        break;
      case "minutes":
        chronoUnit = ChronoUnit.MINUTES;
        break;
      case "seconds":
        chronoUnit = ChronoUnit.SECONDS;
        break;
      case "millis":
        chronoUnit = ChronoUnit.MILLIS;
        break;
      case "nanos":
        chronoUnit = ChronoUnit.NANOS;
        break;
      default:
        throw new IllegalArgumentException(
            "Invalid unit: "
                + unit
                + ". Should be one of [years, months, days, hours, minutes, seconds, millis]");
    }
    if (chronoUnit == ChronoUnit.MONTHS || chronoUnit == ChronoUnit.YEARS) {
      return CustomTypeConverter.INSTANCE
          .coerceToOffsetDateTime(input)
          .plus(CustomTypeConverter.INSTANCE.coerceToLong(delta), chronoUnit)
          .toInstant();
    }
    return CustomTypeConverter.INSTANCE
        .coerceToInstant(input)
        .plus(CustomTypeConverter.INSTANCE.coerceToLong(delta), chronoUnit);
  }

  @Deprecated
  public static long dateadd(Object input, Object delta, Object unit) {
    return timestampAdd(input, delta, unit).toEpochMilli();
  }
}
