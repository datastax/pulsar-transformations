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

import java.sql.Time;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.Date;
import java.util.Map;
import java.util.function.Function;
import org.apache.avro.util.Utf8;
import org.apache.pulsar.client.api.Schema;

public class ConverterUtil {

  public static final Map<Class<?>, Function<Object, ?>> CONVERTERS =
      Map.ofEntries(
          Map.entry(byte[].class, ConverterUtil::toBytes),
          Map.entry(String.class, ConverterUtil::toString),
          Map.entry(Boolean.class, ConverterUtil::toBoolean),
          Map.entry(Byte.class, ConverterUtil::toByte),
          Map.entry(Short.class, ConverterUtil::toShort),
          Map.entry(Integer.class, ConverterUtil::toInteger),
          Map.entry(Long.class, ConverterUtil::toLong),
          Map.entry(Float.class, ConverterUtil::toFloat),
          Map.entry(Double.class, ConverterUtil::toDouble),
          Map.entry(Date.class, ConverterUtil::toDate),
          Map.entry(Timestamp.class, ConverterUtil::toTimestamp),
          Map.entry(Time.class, ConverterUtil::toTime),
          Map.entry(LocalDate.class, ConverterUtil::toLocalDate),
          Map.entry(LocalTime.class, ConverterUtil::toLocalTime),
          Map.entry(LocalDateTime.class, ConverterUtil::toLocalDateTime),
          Map.entry(Instant.class, ConverterUtil::toInstant));

  private static final Map<Class<?>, Schema<?>> SCHEMAS =
      Map.ofEntries(
          Map.entry(String.class, Schema.STRING),
          Map.entry(Boolean.class, Schema.BOOL),
          Map.entry(Byte.class, Schema.INT8),
          Map.entry(Short.class, Schema.INT16),
          Map.entry(Integer.class, Schema.INT32),
          Map.entry(Long.class, Schema.INT64),
          Map.entry(Float.class, Schema.FLOAT),
          Map.entry(Double.class, Schema.DOUBLE),
          Map.entry(Date.class, Schema.DATE),
          Map.entry(Timestamp.class, Schema.TIMESTAMP),
          Map.entry(Time.class, Schema.TIME),
          Map.entry(LocalDate.class, Schema.LOCAL_DATE),
          Map.entry(LocalTime.class, Schema.LOCAL_TIME),
          Map.entry(LocalDateTime.class, Schema.LOCAL_DATE_TIME),
          Map.entry(Instant.class, Schema.INSTANT));

  public static byte[] toBytes(Object o) {
    if (SCHEMAS.containsKey(o.getClass())) {
      return ((Schema<Object>) SCHEMAS.get(o.getClass())).encode(o);
    }
    throw new IllegalArgumentException(
        "Cannot convert type " + o.getClass().getName() + " to byte[]");
  }

  public static String toString(Object o) {
    if (o instanceof Time) {
      return DateTimeFormatter.ISO_LOCAL_TIME.format(((Time) o).toLocalTime());
    }
    if (o instanceof Date) {
      return DateTimeFormatter.ISO_INSTANT.format(((Date) o).toInstant());
    }
    if (o instanceof byte[]) {
      return Schema.STRING.decode((byte[]) o);
    }
    return o.toString();
  }

  public static Boolean toBoolean(Object o) {
    if (o instanceof Boolean) {
      return (Boolean) o;
    }
    if (o instanceof String) {
      return Boolean.valueOf((String) o);
    }
    if (o instanceof byte[]) {
      return Schema.BOOL.decode((byte[]) o);
    }
    throw new IllegalArgumentException(
        "Cannot convert type " + o.getClass().getName() + " to Boolean");
  }

  public static Byte toByte(Object o) {
    if (o instanceof Number) {
      return ((Number) o).byteValue();
    }
    if (o instanceof String) {
      return Byte.parseByte((String) o);
    }
    if (o instanceof byte[]) {
      return Schema.INT8.decode((byte[]) o);
    }
    throw new IllegalArgumentException(
        "Cannot convert type " + o.getClass().getName() + " to Byte");
  }

  public static Short toShort(Object o) {
    if (o instanceof Number) {
      return ((Number) o).shortValue();
    }
    if (o instanceof String) {
      return Short.parseShort((String) o);
    }
    if (o instanceof byte[]) {
      return Schema.INT16.decode((byte[]) o);
    }
    throw new IllegalArgumentException(
        "Cannot convert type " + o.getClass().getName() + " to Short");
  }

  public static Integer toInteger(Object o) {
    if (o instanceof Number) {
      return ((Number) o).intValue();
    }
    if (o instanceof String) {
      return Integer.parseInt((String) o);
    }
    if (o instanceof byte[]) {
      return Schema.INT32.decode((byte[]) o);
    }
    throw new IllegalArgumentException("Cannot convert type " + o.getClass() + " to Integer");
  }

  public static Long toLong(Object o) {
    if (o instanceof Number) {
      return ((Number) o).longValue();
    }
    if (o instanceof String) {
      return Long.parseLong((String) o);
    }
    if (o instanceof LocalTime) {
      return ((LocalTime) o).toNanoOfDay() / 1_000_000;
    }
    if (o instanceof Time) {
      return ((Time) o).toLocalTime().toNanoOfDay() / 1_000_000;
    }
    if (o instanceof byte[]) {
      return Schema.INT64.decode((byte[]) o);
    }
    try {
      return toInstant(o).toEpochMilli();
    } catch (IllegalArgumentException e) {
      throw new IllegalArgumentException(
          "Cannot convert type " + o.getClass().getName() + " to Long", e);
    }
  }

  public static Float toFloat(Object o) {
    if (o instanceof Number) {
      return ((Number) o).floatValue();
    }
    if (o instanceof String) {
      return Float.parseFloat((String) o);
    }
    if (o instanceof byte[]) {
      return Schema.FLOAT.decode((byte[]) o);
    }
    throw new IllegalArgumentException(
        "Cannot convert type " + o.getClass().getName() + " to Float");
  }

  public static Double toDouble(Object o) {
    if (o instanceof Number) {
      return ((Number) o).doubleValue();
    }
    if (o instanceof String) {
      return Double.parseDouble((String) o);
    }
    if (o instanceof LocalTime) {
      return (double) ((LocalTime) o).toNanoOfDay() / 1_000_000;
    }
    if (o instanceof Time) {
      return (double) ((Time) o).toLocalTime().toNanoOfDay() / 1_000_000;
    }
    if (o instanceof byte[]) {
      return Schema.DOUBLE.decode((byte[]) o);
    }
    try {
      Instant instant = toInstant(o);
      return (double) instant.getEpochSecond() * 1000 + (double) instant.getNano() / 1_000_000;
    } catch (IllegalArgumentException e) {
      throw new IllegalArgumentException(
          "Cannot convert type " + o.getClass().getName() + " to Double", e);
    }
  }

  public static Date toDate(Object o) {
    if (o instanceof Timestamp) {
      return new Date(((Timestamp) o).getTime());
    }
    if (o instanceof Date && !(o instanceof Time)) {
      return (Date) o;
    }
    if (o instanceof Long || o instanceof Double) {
      return new Date(((Number) o).longValue());
    }
    if (o instanceof byte[]) {
      return Schema.DATE.decode((byte[]) o);
    }
    try {
      return Date.from(toInstant(o));
    } catch (IllegalArgumentException e) {
      throw new IllegalArgumentException(
          "Cannot convert type " + o.getClass().getName() + " to Date", e);
    }
  }

  public static Timestamp toTimestamp(Object o) {
    if (o instanceof Timestamp) {
      return (Timestamp) o;
    }
    if (o instanceof Long || o instanceof Double) {
      return new Timestamp(((Number) o).longValue());
    }
    if (o instanceof byte[]) {
      return Schema.TIMESTAMP.decode((byte[]) o);
    }
    try {
      return Timestamp.from(toInstant(o));
    } catch (IllegalArgumentException e) {
      throw new IllegalArgumentException(
          "Cannot convert type " + o.getClass() + " to Timestamp", e);
    }
  }

  public static Time toTime(Object o) {
    if (o instanceof Time) {
      return (Time) o;
    }
    if (o instanceof Long || o instanceof Double) {
      return new Time(((Number) o).longValue());
    }
    if (o instanceof LocalTime) {
      return Time.valueOf((LocalTime) o);
    }
    if (o instanceof byte[]) {
      return Schema.TIME.decode((byte[]) o);
    }
    if (o instanceof String) {
      return Time.valueOf(LocalTime.parse((CharSequence) o));
    }
    try {
      return new Time(toInstant(o).toEpochMilli());
    } catch (IllegalArgumentException e) {
      throw new IllegalArgumentException("Cannot convert type " + o.getClass() + " to Time", e);
    }
  }

  public static LocalTime toLocalTime(Object o) {
    if (o instanceof LocalTime) {
      return (LocalTime) o;
    }
    if (o instanceof Time) {
      return ((Time) o).toLocalTime();
    }
    if (o instanceof byte[]) {
      return Schema.LOCAL_TIME.decode((byte[]) o);
    }
    if (o instanceof CharSequence) {
      return LocalTime.parse((CharSequence) o);
    }
    try {
      return LocalTime.ofInstant(toInstant(o), ZoneOffset.UTC);
    } catch (IllegalArgumentException e) {
      throw new IllegalArgumentException(
          "Cannot convert type " + o.getClass() + " to LocalTime", e);
    }
  }

  public static LocalDate toLocalDate(Object o) {
    if (o instanceof LocalDate) {
      return (LocalDate) o;
    }
    if (o instanceof LocalDateTime) {
      return ((LocalDateTime) o).toLocalDate();
    }
    if (o instanceof byte[]) {
      return Schema.LOCAL_DATE.decode((byte[]) o);
    }
    if (o instanceof CharSequence) {
      return LocalDate.parse((CharSequence) o);
    }
    try {
      Instant instant = toInstant(o);
      return LocalDate.ofInstant(instant, ZoneOffset.UTC);
    } catch (IllegalArgumentException e) {
      throw new IllegalArgumentException(
          "Cannot convert type " + o.getClass() + " to LocalDate", e);
    }
  }

  public static LocalDateTime toLocalDateTime(Object o) {
    if (o instanceof LocalDateTime) {
      return (LocalDateTime) o;
    }
    if (o instanceof LocalDate) {
      return ((LocalDate) o).atStartOfDay();
    }
    if (o instanceof byte[]) {
      return Schema.LOCAL_DATE_TIME.decode((byte[]) o);
    }
    if (o instanceof CharSequence) {
      return LocalDateTime.parse((CharSequence) o);
    }
    try {
      Instant instant = toInstant(o);
      return LocalDateTime.ofInstant(instant, ZoneOffset.UTC);
    } catch (IllegalArgumentException e) {
      throw new IllegalArgumentException(
          "Cannot convert type " + o.getClass() + " to LocalDateTime", e);
    }
  }

  public static Instant toInstant(Object o) {
    if (o instanceof Instant) {
      return (Instant) o;
    }
    if (o instanceof LocalDate) {
      return ((LocalDate) o).atStartOfDay().toInstant(ZoneOffset.UTC);
    }
    if (o instanceof LocalDateTime) {
      return ((LocalDateTime) o).toInstant(ZoneOffset.UTC);
    }
    if (o instanceof OffsetDateTime) {
      return ((OffsetDateTime) o).toInstant();
    }
    if (o instanceof Date && !(o instanceof Time)) {
      return ((Date) o).toInstant();
    }
    if (o instanceof Long) {
      return Instant.ofEpochMilli(((Number) o).longValue());
    }
    if (o instanceof Double) {
      long seconds = (long) ((double) o / 1000);
      long nanos = Math.round(((double) o - seconds * 1000) * 1_000_000);
      return Instant.ofEpochSecond(seconds, nanos);
    }
    if (o instanceof CharSequence) {
      return DateTimeFormatter.ISO_DATE_TIME.parse((CharSequence) o, Instant::from);
    }
    if (o instanceof byte[]) {
      return Schema.INSTANT.decode((byte[]) o);
    }
    throw new IllegalArgumentException("Cannot convert type " + o.getClass() + " to Instant");
  }

  public static <T> T convert(Object o, Class<T> type) {
    if (o.getClass().equals(type)) {
      return type.cast(o);
    }

    if (o instanceof Utf8) {
      return convert(o.toString(), type);
    }
    if (CONVERTERS.containsKey(type)) {
      return (T) CONVERTERS.get(type).apply(o);
    }
    throw new IllegalArgumentException(
        "Cannot convert type " + o.getClass() + " to " + type.getName());
  }
}
