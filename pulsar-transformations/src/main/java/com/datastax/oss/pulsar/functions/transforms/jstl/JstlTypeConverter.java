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

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import jakarta.el.ELContext;
import jakarta.el.ELException;
import jakarta.el.TypeConverter;
import java.sql.Time;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.time.temporal.TemporalAccessor;
import java.util.Date;
import java.util.Map;
import org.apache.avro.util.Utf8;
import org.apache.el.util.MessageFactory;
import org.apache.pulsar.client.api.Schema;

/**
 * Overrides the default TypeConverter coerce to support null values & non-EL coercions (e.g.
 * date/time types) schemas.
 */
public class JstlTypeConverter extends TypeConverter {
  public static final JstlTypeConverter INSTANCE = new JstlTypeConverter();

  @SuppressFBWarnings("NP_BOOLEAN_RETURN_NULL")
  protected Boolean coerceToBoolean(Object value) {
    if (value instanceof byte[]) {
      return Schema.BOOL.decode((byte[]) value);
    }
    return null;
  }

  protected Double coerceToDouble(Object value) {
    if (value instanceof LocalTime) {
      return (double) ((LocalTime) value).toNanoOfDay() / 1_000_000;
    }
    if (value instanceof Time) {
      return (double) ((Time) value).toLocalTime().toNanoOfDay() / 1_000_000;
    }
    if (value instanceof Timestamp) {
      return (((Timestamp) value).getTime() / 1_000) * 1000
          + (double) ((Timestamp) value).getNanos() / 1_000_000_000;
    }
    if (value instanceof Date) {
      return (double) (((Date) value).getTime());
    }
    if (value instanceof byte[]) {
      return Schema.DOUBLE.decode((byte[]) value);
    }
    if (value instanceof LocalDate) {
      return (double) (((LocalDate) value).toEpochDay());
    }
    if (value instanceof TemporalAccessor) {
      Instant instant = coerceToInstant(value);
      return (double) instant.getEpochSecond() * 1000 + (double) instant.getNano() / 1_000_000;
    }
    return null;
  }

  protected Float coerceToFloat(Object value) {
    if (value instanceof byte[]) {
      return Schema.FLOAT.decode((byte[]) value);
    }
    if (value instanceof LocalDate) {
      return (float) (((LocalDate) value).toEpochDay());
    }
    return null;
  }

  protected Long coerceToLong(Object value) {
    if (value instanceof LocalTime) {
      return ((LocalTime) value).toNanoOfDay() / 1_000_000;
    }
    if (value instanceof Time) {
      return ((Time) value).toLocalTime().toNanoOfDay() / 1_000_000;
    }
    if (value instanceof Date) {
      return ((Date) value).getTime();
    }
    if (value instanceof byte[]) {
      return Schema.INT64.decode((byte[]) value);
    }
    if (value instanceof LocalDate) {
      return ((LocalDate) value).toEpochDay();
    }
    if (value instanceof TemporalAccessor) {
      return coerceToInstant(value).toEpochMilli();
    }
    return null;
  }

  protected Integer coerceToInteger(Object value) {
    if (value instanceof LocalTime) {
      return (int) (((LocalTime) value).toNanoOfDay() / 1_000_000);
    }
    if (value instanceof Time) {
      return (int) (((Time) value).toLocalTime().toNanoOfDay() / 1_000_000);
    }
    if (value instanceof byte[]) {
      return Schema.INT32.decode((byte[]) value);
    }
    if (value instanceof LocalDate) {
      return Math.toIntExact(((LocalDate) value).toEpochDay());
    }
    return null;
  }

  protected Short coerceToShort(Object value) {
    if (value instanceof byte[]) {
      return Schema.INT16.decode((byte[]) value);
    }
    return null;
  }

  protected Byte coerceToByte(Object value) {
    if (value instanceof byte[]) {
      return Schema.INT8.decode((byte[]) value);
    }
    return null;
  }

  protected String coerceToString(Object value) {
    if (value instanceof Time) {
      return DateTimeFormatter.ISO_LOCAL_TIME.format(((Time) value).toLocalTime());
    }
    if (value instanceof Date) {
      return DateTimeFormatter.ISO_INSTANT.format(((Date) value).toInstant());
    }
    if (value instanceof byte[]) {
      return Schema.STRING.decode((byte[]) value);
    }
    return null;
  }

  protected Object coerceToType(Object value, Class<?> type) {
    if (value == null) {
      return null;
    }
    if (value instanceof Utf8) {
      return coerceToType(value.toString(), type);
    }
    if (type == Boolean.class) {
      return coerceToBoolean(value);
    }
    if (type == Double.class) {
      return coerceToDouble(value);
    }
    if (type == Float.class) {
      return coerceToFloat(value);
    }
    if (type == Long.class) {
      return coerceToLong(value);
    }
    if (type == Short.class) {
      return coerceToShort(value);
    }
    if (type == Integer.class) {
      return coerceToInteger(value);
    }
    if (type == Byte.class) {
      return coerceToByte(value);
    }
    if (type == String.class) {
      return coerceToString(value);
    }
    if (type == byte[].class) {
      return coerceToBytes(value);
    }
    if (type == Timestamp.class) {
      return coerceToTimestamp(value);
    }
    if (type == Time.class) {
      return coerceToTime(value);
    }
    if (type == Date.class) {
      return coerceToDate(value);
    }
    if (type == LocalDateTime.class) {
      return coerceToLocalDateTime(value);
    }
    if (type == LocalDate.class) {
      return coerceToLocalDate(value);
    }
    if (type == LocalTime.class) {
      return coerceToLocalTime(value);
    }
    if (type == Instant.class) {
      return coerceToInstant(value);
    }
    if (type == OffsetDateTime.class) {
      return coerceToOffsetDateTime(value);
    }
    return null;
  }

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

  protected byte[] coerceToBytes(Object value) {
    if (value == null) {
      return null;
    }
    if (value instanceof byte[]) {
      return (byte[]) value;
    }
    if (value instanceof OffsetDateTime) {
      return coerceToBytes(((OffsetDateTime) value).toInstant());
    }
    if (SCHEMAS.containsKey(value.getClass())) {
      return ((Schema<Object>) SCHEMAS.get(value.getClass())).encode(value);
    }
    throw new IllegalArgumentException(
        "Cannot convert type " + value.getClass().getName() + " to byte[]");
  }

  protected Date coerceToDate(Object value) {
    if (value == null) {
      return null;
    }
    if (value instanceof Timestamp) {
      return new Date(((Timestamp) value).getTime());
    }
    if (value instanceof Date && !(value instanceof Time)) {
      return (Date) value;
    }
    if (value instanceof Long || value instanceof Double) {
      return new Date(((Number) value).longValue());
    }
    if (value instanceof byte[]) {
      return Schema.DATE.decode((byte[]) value);
    }
    if (value instanceof TemporalAccessor || value instanceof CharSequence) {
      return Date.from(coerceToInstant(value));
    }
    throw new ELException(MessageFactory.get("error.convert", value, value.getClass(), Date.class));
  }

  protected Timestamp coerceToTimestamp(Object value) {
    if (value == null) {
      return null;
    }
    if (value instanceof Timestamp) {
      return (Timestamp) value;
    }
    if (value instanceof Date && !(value instanceof Time)) {
      return new Timestamp(((Date) value).getTime());
    }
    if (value instanceof Long || value instanceof Double) {
      return new Timestamp(((Number) value).longValue());
    }
    if (value instanceof byte[]) {
      return Schema.TIMESTAMP.decode((byte[]) value);
    }
    if (value instanceof TemporalAccessor || value instanceof CharSequence) {
      return Timestamp.from(coerceToInstant(value));
    }
    throw new ELException(
        MessageFactory.get("error.convert", value, value.getClass(), Timestamp.class));
  }

  protected Time coerceToTime(Object value) {
    if (value == null) {
      return null;
    }
    if (value instanceof Time) {
      return (Time) value;
    }
    if (value instanceof Long || value instanceof Double) {
      return new Time(((Number) value).longValue());
    }
    if (value instanceof LocalTime) {
      return Time.valueOf((LocalTime) value);
    }
    if (value instanceof byte[]) {
      return Schema.TIME.decode((byte[]) value);
    }
    if (value instanceof CharSequence) {
      return Time.valueOf(LocalTime.parse((CharSequence) value));
    }
    if (value instanceof TemporalAccessor || value instanceof Date) {
      return new Time(coerceToInstant(value).toEpochMilli());
    }
    throw new ELException(MessageFactory.get("error.convert", value, value.getClass(), Time.class));
  }

  protected LocalTime coerceToLocalTime(Object value) {
    if (value == null) {
      return null;
    }
    if (value instanceof LocalTime) {
      return (LocalTime) value;
    }
    if (value instanceof Time) {
      return ((Time) value).toLocalTime();
    }
    if (value instanceof byte[]) {
      return Schema.LOCAL_TIME.decode((byte[]) value);
    }
    if (value instanceof CharSequence) {
      return LocalTime.parse((CharSequence) value);
    }
    if (value instanceof TemporalAccessor || value instanceof Number || value instanceof Date) {
      return LocalTime.ofInstant(coerceToInstant(value), ZoneOffset.UTC);
    }
    throw new ELException(
        MessageFactory.get("error.convert", value, value.getClass(), LocalTime.class));
  }

  protected LocalDate coerceToLocalDate(Object value) {
    if (value == null) {
      return null;
    }
    if (value instanceof LocalDate) {
      return (LocalDate) value;
    }
    if (value instanceof LocalDateTime) {
      return ((LocalDateTime) value).toLocalDate();
    }
    if (value instanceof byte[]) {
      return Schema.LOCAL_DATE.decode((byte[]) value);
    }
    if (value instanceof CharSequence) {
      return LocalDate.parse((CharSequence) value);
    }
    if (value instanceof Number) {
      return LocalDate.ofEpochDay(((Number) value).longValue());
    }
    if (value instanceof TemporalAccessor || value instanceof Date) {
      return LocalDate.ofInstant(coerceToInstant(value), ZoneOffset.UTC);
    }
    throw new ELException(
        MessageFactory.get("error.convert", value, value.getClass(), LocalDate.class));
  }

  protected LocalDateTime coerceToLocalDateTime(Object value) {
    if (value == null) {
      return null;
    }
    if (value instanceof LocalDateTime) {
      return (LocalDateTime) value;
    }
    if (value instanceof LocalDate) {
      return ((LocalDate) value).atStartOfDay();
    }
    if (value instanceof byte[]) {
      return Schema.LOCAL_DATE_TIME.decode((byte[]) value);
    }
    if (value instanceof CharSequence) {
      return LocalDateTime.parse((CharSequence) value);
    }
    if (value instanceof TemporalAccessor || value instanceof Number || value instanceof Date) {
      return LocalDateTime.ofInstant(coerceToInstant(value), ZoneOffset.UTC);
    }
    throw new ELException(
        MessageFactory.get("error.convert", value, value.getClass(), LocalDateTime.class));
  }

  protected Instant coerceToInstant(Object value) {
    if (value == null) {
      return null;
    }
    if (value instanceof Instant) {
      return (Instant) value;
    }
    if (value instanceof Date && !(value instanceof Time)) {
      return ((Date) value).toInstant();
    }
    if (value instanceof Long) {
      return Instant.ofEpochMilli(((Number) value).longValue());
    }
    if (value instanceof Double) {
      long seconds = (long) ((double) value / 1000);
      long nanos = Math.round(((double) value - seconds * 1000) * 1_000_000);
      return Instant.ofEpochSecond(seconds, nanos);
    }
    if (value instanceof byte[]) {
      return Schema.INSTANT.decode((byte[]) value);
    }
    if (value instanceof TemporalAccessor || value instanceof CharSequence) {
      return coerceToOffsetDateTime(value).toInstant();
    }
    throw new ELException(
        MessageFactory.get("error.convert", value, value.getClass(), Instant.class));
  }

  protected OffsetDateTime coerceToOffsetDateTime(Object value) {
    if (value == null) {
      return null;
    }
    if (value instanceof OffsetDateTime) {
      return (OffsetDateTime) value;
    }
    if (value instanceof LocalDate) {
      return ((LocalDate) value).atStartOfDay().atOffset(ZoneOffset.UTC);
    }
    if (value instanceof LocalDateTime) {
      return ((LocalDateTime) value).atOffset(ZoneOffset.UTC);
    }
    if (value instanceof Instant) {
      return ((Instant) value).atOffset(ZoneOffset.UTC);
    }
    if (value instanceof CharSequence) {
      if (((CharSequence) value).length() == 10) {
        LocalDate localDate =
            DateTimeFormatter.ISO_LOCAL_DATE.parse((CharSequence) value, LocalDate::from);
        return localDate.atStartOfDay(ZoneOffset.UTC).toOffsetDateTime();
      }
      return DateTimeFormatter.ISO_DATE_TIME.parse((CharSequence) value, OffsetDateTime::from);
    }
    if (value instanceof Number || value instanceof Date || value instanceof byte[]) {
      return coerceToInstant(value).atOffset(ZoneOffset.UTC);
    }
    throw new ELException(
        MessageFactory.get("error.convert", value, value.getClass(), OffsetDateTime.class));
  }

  @Override
  public <T> T convertToType(ELContext elContext, Object value, Class<T> type) {
    if (value == null) {
      elContext.setPropertyResolved(true);
      return null;
    }
    Object coercedValue = coerceToType(value, type);
    elContext.setPropertyResolved(coercedValue != null);
    return (T) coercedValue;
  }
}
