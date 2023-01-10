package com.datastax.oss.pulsar.functions.transforms;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;
import java.sql.Time;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneOffset;
import java.time.temporal.ChronoUnit;
import java.time.temporal.TemporalUnit;
import java.util.Date;
import java.util.TimeZone;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

public class ConverterUtilTest {

    @DataProvider(name = "conversions")
    public static Object[][] conversions() {
        TimeZone.setDefault(TimeZone.getTimeZone(ZoneOffset.UTC));
        byte byteValue = (byte) 42;
        short shortValue = (short) 42;
        int intValue = 42;
        long longValue = 42L;
        float floatValue = 42.8F;
        double doubleValue = 42.8D;
        long dateTimeMillis = 1672700645000L;
        long midnightMillis = 1672617600000L;
        long timeMillis = 83045000L;
        double timeMillisWithNanos = 83045000.000006D;
        Date date = new Date(dateTimeMillis);
        Instant instant = Instant.ofEpochSecond(1672700645, 6);
        Timestamp timestamp = Timestamp.from(instant);
        LocalDateTime localDateTime = LocalDateTime.of(2023, 1, 2, 23, 4, 5, 6);
        LocalDate localDate = LocalDate.of(2023, 1, 2);
        LocalTime localTime = LocalTime.of(23, 4, 5, 6);
//        Time time = Time.valueOf(localTime);
        Time time = new Time(timeMillis);
        LocalDateTime smallLocalDateTime = LocalDateTime.ofEpochSecond(0, 42_000_000, ZoneOffset.UTC);
        LocalDateTime localDateTimeAtMillis = LocalDateTime.ofEpochSecond(dateTimeMillis / 1000, 0, ZoneOffset.UTC);
        return new Object[][]{
            // String
            {byteValue, String.class, "42"},
            {shortValue, String.class, "42"},
            {intValue, String.class, "42"},
            {longValue, String.class, "42"},
            {floatValue, String.class, "42.8"},
            {doubleValue, String.class, "42.8"},
            {date, String.class, "2023-01-02T23:04:05Z"},
            {timestamp, String.class, "2023-01-02T23:04:05.000000006Z"},
            {time, String.class, "23:04:05"},
            {localDateTime, String.class, "2023-01-02T23:04:05.000000006"},
            {instant, String.class, "2023-01-02T23:04:05.000000006Z"},
            {localDate, String.class, "2023-01-02"},
            {localTime, String.class, "23:04:05.000000006"},

            // Byte
            {byteValue, Byte.class, byteValue},
            {shortValue, Byte.class, byteValue},
            {intValue, Byte.class, byteValue},
            {longValue, Byte.class, byteValue},
            {floatValue, Byte.class, byteValue},
            {doubleValue, Byte.class, byteValue},

            // Short
            {byteValue, Short.class, shortValue},
            {shortValue, Short.class, shortValue},
            {intValue, Short.class, shortValue},
            {longValue, Short.class, shortValue},
            {floatValue, Short.class, shortValue},
            {doubleValue, Short.class, shortValue},

            // Integer
            {byteValue, Integer.class, intValue},
            {shortValue, Integer.class, intValue},
            {intValue, Integer.class, intValue},
            {longValue, Integer.class, intValue},
            {floatValue, Integer.class, intValue},
            {doubleValue, Integer.class, intValue},

            // Long
            {byteValue, Long.class, longValue},
            {shortValue, Long.class, longValue},
            {intValue, Long.class, longValue},
            {longValue, Long.class, longValue},
            {floatValue, Long.class, longValue},
            {doubleValue, Long.class, longValue},
            {date, Long.class, dateTimeMillis},
            {timestamp, Long.class, dateTimeMillis},
            {time, Long.class, timeMillis},
            {localDateTime, Long.class, dateTimeMillis},
            {instant, Long.class, dateTimeMillis},
            {localTime, Long.class, timeMillis},
            {localDate, Long.class, midnightMillis},

            // Float
            {byteValue, Float.class, 42F},
            {shortValue, Float.class, 42F},
            {intValue, Float.class, 42F},
            {longValue, Float.class, 42F},
            {floatValue, Float.class, floatValue},
            {doubleValue, Float.class, floatValue},

            // Double
            {byteValue, Double.class, 42D},
            {shortValue, Double.class, 42D},
            {intValue, Double.class, 42D},
            {longValue, Double.class, 42D},
            {floatValue, Double.class, (double) floatValue},
            {doubleValue, Double.class, doubleValue},
            {date, Double.class, (double) dateTimeMillis},
            {timestamp, Double.class, (double) dateTimeMillis},
            {time, Double.class, (double) timeMillis},
            {localDateTime, Double.class, (double) dateTimeMillis},
            {instant, Double.class, (double) dateTimeMillis},
            {localTime, Double.class, timeMillisWithNanos},
            {localDate, Double.class, (double) midnightMillis},

            // Date
            {dateTimeMillis, Date.class, date},
            {(double) dateTimeMillis, Date.class, date},
            {date, Date.class, date},
            {timestamp, Date.class, date},
            {localDateTime, Date.class, date},
            {instant, Date.class, date},
            {localDate, Date.class, new Date(midnightMillis)},

            // Timestamp
            {dateTimeMillis, Timestamp.class, new Timestamp(dateTimeMillis)},
            {(double) dateTimeMillis, Timestamp.class, new Timestamp(dateTimeMillis)},
            {date, Timestamp.class, new Timestamp(dateTimeMillis)},
            {timestamp, Timestamp.class, timestamp},
            {localDateTime, Timestamp.class, timestamp},
            {instant, Timestamp.class, timestamp},
            {localDate, Timestamp.class, new Timestamp(midnightMillis)},

            // Time
            {dateTimeMillis, Time.class, new Time(dateTimeMillis)},
            {(double) dateTimeMillis, Time.class, new Time(dateTimeMillis)},
            {date, Time.class, new Time(dateTimeMillis)},
            {timestamp, Time.class, time},
            {localDateTime, Time.class, time},
            {instant, Time.class, time},
            {localDate, Time.class, new Time(midnightMillis)},
            {time, Time.class, time},
            {localTime, Time.class, time},

            // LocalTime
            {dateTimeMillis, LocalTime.class, LocalTime.of(23, 4, 5)},
            {timeMillisWithNanos, LocalTime.class, localTime},
            {date, LocalTime.class, LocalTime.of(23, 4, 5)},
            {timestamp, LocalTime.class, localTime},
            {localDateTime, LocalTime.class, localTime},
            {instant, LocalTime.class, localTime},
            {localDate, LocalTime.class, LocalTime.ofSecondOfDay(0)},
            {time, LocalTime.class, LocalTime.of(23, 4, 5)},
            {localTime, LocalTime.class, localTime},

            // LocalDate
            {dateTimeMillis, LocalDate.class, localDate},
            {(double) dateTimeMillis, LocalDate.class, localDate},
            {date, LocalDate.class, localDate},
            {timestamp, LocalDate.class, localDate},
            {localDateTime, LocalDate.class, localDate},
            {instant, LocalDate.class, localDate},
            {localDate, LocalDate.class, localDate},

            // LocalDateTime
            {dateTimeMillis, LocalDateTime.class, localDateTimeAtMillis},
            {(double) dateTimeMillis, LocalDateTime.class, localDateTimeAtMillis},
            {date, LocalDateTime.class, localDateTimeAtMillis},
            {timestamp, LocalDateTime.class, localDateTime},
            {localDateTime, LocalDateTime.class, localDateTime},
            {instant, LocalDateTime.class, localDateTime},
            {localDate, LocalDateTime.class, LocalDateTime.of(localDate, LocalTime.MIDNIGHT)},

            // Instant
            {dateTimeMillis, Instant.class, Instant.ofEpochMilli(dateTimeMillis)},
            {(double) dateTimeMillis, Instant.class, Instant.ofEpochMilli(dateTimeMillis)},
            {date, Instant.class, Instant.ofEpochMilli(dateTimeMillis)},
            {timestamp, Instant.class, instant},
            {localDateTime, Instant.class, instant},
            {instant, Instant.class, instant},
            {localDate, Instant.class, instant.truncatedTo(ChronoUnit.DAYS)},
        };
    }

    @Test(dataProvider = "conversions")
    public void convert(Object o, Class<?> type, Object expected) {
        Object converted = ConverterUtil.convert(o, type);
        assertEquals(converted.getClass(), type);
        if (type.equals(Time.class)) {
            // j.s.Time equality is weird...
            assertEquals(((Time) converted).toLocalTime(), ((Time) expected).toLocalTime());
        } else {
            assertEquals(converted, expected);
        }
    }
}