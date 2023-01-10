package com.datastax.oss.pulsar.functions.transforms;

import java.sql.Time;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.time.temporal.TemporalField;
import java.util.Date;
import java.util.Map;
import java.util.TimeZone;
import java.util.function.Function;
import org.apache.avro.util.Utf8;
import org.apache.pulsar.client.api.Schema;

public class ConverterUtil {

    private static final Map<Class<?>, Function<Object, ?>> converters = Map.ofEntries(
        Map.entry(String.class, ConverterUtil::toString),
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
        Map.entry(Instant.class, ConverterUtil::toInstant)
    );

    public static String toString(Object o) {
        if (o instanceof Time) {
            return DateTimeFormatter.ISO_LOCAL_TIME.format(((Time) o).toLocalTime());
        }
        if (o instanceof Date) {
            return DateTimeFormatter.ISO_INSTANT.format(((Date) o).toInstant());
        }
        return o.toString();
    }

    public static Byte toByte(Object o) {
        if (o instanceof Number) {
            return ((Number) o).byteValue();
        } else if (o instanceof String) {
            return Byte.parseByte((String) o);
        }
        throw new IllegalArgumentException("Cannot convert type " + o.getClass().getName() + " to Byte");
    }

    public static Short toShort(Object o) {
        if (o instanceof Number) {
            return ((Number) o).shortValue();
        } else if (o instanceof String) {
            return Short.parseShort((String) o);
        }
        throw new IllegalArgumentException("Cannot convert type " + o.getClass().getName() + " to Short");
    }

    public static Integer toInteger(Object o) {
        if (o instanceof Number) {
            return ((Number) o).intValue();
        }
        if (o instanceof String) {
            return Integer.parseInt((String) o);
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
        try {
            return toInstant(o).toEpochMilli();
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException("Cannot convert type " + o.getClass().getName() + " to Long", e);
        }
    }

    public static Float toFloat(Object o) {
        if (o instanceof Number) {
            return ((Number) o).floatValue();
        }
        if (o instanceof String) {
            return Float.parseFloat((String) o);
        }
        throw new IllegalArgumentException("Cannot convert type " + o.getClass().getName() + " to Float");
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
        try {
            Instant instant = toInstant(o);
            return (double) instant.getEpochSecond() * 1000 + (double) instant.getNano() / 1_000_000;
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException("Cannot convert type " + o.getClass().getName() + " to Double", e);
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
        try {
            return Date.from(toInstant(o));
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException("Cannot convert type " + o.getClass().getName() + " to Date", e);
        }
    }

    public static Timestamp toTimestamp(Object o) {
        if (o instanceof Timestamp) {
            return (Timestamp) o;
        }
        if (o instanceof Long || o instanceof Double) {
            return new Timestamp(((Number) o).longValue());
        }
        try {
            return Timestamp.from(toInstant(o));
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException("Cannot convert type " + o.getClass() + " to Timestamp", e);
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
        try {
            return LocalTime.ofInstant(toInstant(o), ZoneOffset.UTC);
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException("Cannot convert type " + o.getClass() + " to LocalTime", e);
        }
    }

    public static LocalDate toLocalDate(Object o) {
        if (o instanceof LocalDate) {
            return (LocalDate) o;
        }
        if (o instanceof CharSequence) {
            return LocalDate.parse((CharSequence) o);
        }
        if (o instanceof LocalDateTime) {
            return ((LocalDateTime) o).toLocalDate();
        }
        try {
            Instant instant = toInstant(o);
            return LocalDate.ofInstant(instant, ZoneOffset.UTC);
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException("Cannot convert type " + o.getClass() + " to LocalDate", e);
        }
    }

    public static LocalDateTime toLocalDateTime(Object o) {
        if (o instanceof LocalDateTime) {
            return (LocalDateTime) o;
        }
        if (o instanceof LocalDate) {
            return ((LocalDate) o).atStartOfDay();
        }
        try {
            Instant instant = toInstant(o);
            return LocalDateTime.ofInstant(instant, ZoneOffset.UTC);
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException("Cannot convert type " + o.getClass() + " to LocalDateTime", e);
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
            long nanos = Math.round(((double) o - seconds *1000) * 1_000_000);
            return Instant.ofEpochSecond(seconds, nanos);
        }
        if (o instanceof CharSequence) {
            return DateTimeFormatter.ISO_DATE_TIME.parse((CharSequence) o, Instant::from);
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
        if (converters.containsKey(type)) {
            return (T) converters.get(type).apply(o);
        }
        throw new IllegalArgumentException("Cannot convert type " + o.getClass() + " to " + type.getName());
    }





}
