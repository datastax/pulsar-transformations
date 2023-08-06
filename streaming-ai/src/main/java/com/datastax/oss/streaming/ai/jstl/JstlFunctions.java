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
package com.datastax.oss.streaming.ai.jstl;

import com.datastax.oss.streaming.ai.TransformContext;
import com.datastax.oss.streaming.ai.jstl.predicate.JstlPredicate;
import jakarta.el.ELException;
import java.lang.reflect.Array;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.time.Clock;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.apache.el.util.MessageFactory;

/** Provides convenience methods to use in jstl expression. All functions should be static. */
@Slf4j
public class JstlFunctions {
  @Setter private static Clock clock = Clock.systemUTC();

  public static String uppercase(Object input) {
    return input == null ? null : toString(input).toUpperCase();
  }

  public static String lowercase(Object input) {
    return input == null ? null : toString(input).toLowerCase();
  }

  public static Object toDouble(Object input) {
    if (input == null) {
      return null;
    }
    return JstlTypeConverter.INSTANCE.coerceToDouble(input);
  }

  public static Object toInt(Object input) {
    if (input == null) {
      return null;
    }
    return JstlTypeConverter.INSTANCE.coerceToInteger(input);
  }

  public static List<Object> filter(Object input, String expression) {
    if (input == null) {
      return null;
    }
    List<Object> source;
    if (input instanceof List) {
      source = (List<Object>) input;
    } else if (input instanceof Collection) {
      source = new ArrayList<>((Collection<?>) input);
    } else if (input.getClass().isArray()) {
      source = new ArrayList<>(Array.getLength(input));
      for (int i = 0; i < Array.getLength(input); i++) {
        source.add(Array.get(input, i));
      }
    } else {
      throw new IllegalArgumentException(
          "fn:filter cannot filter object of type " + input.getClass().getName());
    }
    List<Object> result = new ArrayList<>();
    JstlPredicate predicate = new JstlPredicate(expression);
    for (Object o : source) {
      if (log.isDebugEnabled()) {
        log.info("Filtering object {}", o);
      }
      // nulls are always filtered out
      if (o != null) {
        TransformContext context = new TransformContext();
        context.setRecordObject(o);
        boolean evaluate = predicate.test(context);
        if (log.isDebugEnabled()) {
          log.debug("Result of evaluation is {}", evaluate);
        }
        if (evaluate) {
          result.add(o);
        }
      }
    }
    return result;
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
    return JstlTypeConverter.INSTANCE.coerceToString(input);
  }

  public static BigDecimal toBigDecimal(Object value, Object scale) {
    final BigInteger bigInteger = JstlTypeConverter.INSTANCE.coerceToBigInteger(value);
    final int scaleInteger = JstlTypeConverter.INSTANCE.coerceToInteger(scale);
    return new BigDecimal(bigInteger, scaleInteger);
  }

  public static BigDecimal toBigDecimal(Object value) {
    final double d = JstlTypeConverter.INSTANCE.coerceToDouble(value);
    return BigDecimal.valueOf(d);
  }

  public static Instant now() {
    return Instant.now(clock);
  }

  public static Instant timestampAdd(Object input, Object delta, Object unit) {
    if (input == null || unit == null) {
      throw new ELException(MessageFactory.get("error.method.notypes"));
    }

    ChronoUnit chronoUnit;
    switch (JstlTypeConverter.INSTANCE.coerceToString(unit)) {
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
      return JstlTypeConverter.INSTANCE
          .coerceToOffsetDateTime(input)
          .plus(JstlTypeConverter.INSTANCE.coerceToLong(delta), chronoUnit)
          .toInstant();
    }
    return JstlTypeConverter.INSTANCE
        .coerceToInstant(input)
        .plus(JstlTypeConverter.INSTANCE.coerceToLong(delta), chronoUnit);
  }

  @Deprecated
  public static long dateadd(Object input, Object delta, Object unit) {
    return timestampAdd(input, delta, unit).toEpochMilli();
  }
}
