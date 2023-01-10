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

import static org.testng.Assert.assertEquals;

import java.nio.charset.StandardCharsets;
import java.sql.Time;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneOffset;
import java.time.temporal.ChronoUnit;
import java.util.Date;
import java.util.TimeZone;
import org.apache.pulsar.client.api.Schema;
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
    Time time = new Time(timeMillis);
    LocalDateTime localDateTimeWithoutNanos =
        LocalDateTime.ofEpochSecond(dateTimeMillis / 1000, 0, ZoneOffset.UTC);
    return new Object[][] {
      // Bytes
      {new byte[] {1, 2, 3}, byte[].class, new byte[] {1, 2, 3}},
      {"test", byte[].class, "test".getBytes(StandardCharsets.UTF_8)},
      {true, byte[].class, new byte[] {1}},
      {byteValue, byte[].class, new byte[] {byteValue}},
      {shortValue, byte[].class, Schema.INT16.encode(shortValue)},
      {intValue, byte[].class, Schema.INT32.encode(intValue)},
      {longValue, byte[].class, Schema.INT64.encode(longValue)},
      {floatValue, byte[].class, Schema.FLOAT.encode(floatValue)},
      {doubleValue, byte[].class, Schema.DOUBLE.encode(doubleValue)},
      {date, byte[].class, Schema.DATE.encode(date)},
      {timestamp, byte[].class, Schema.TIMESTAMP.encode(timestamp)},
      {time, byte[].class, Schema.TIME.encode(time)},
      {localDateTime, byte[].class, Schema.LOCAL_DATE_TIME.encode(localDateTime)},
      {instant, byte[].class, Schema.INSTANT.encode(instant)},
      {localDate, byte[].class, Schema.LOCAL_DATE.encode(localDate)},
      {localTime, byte[].class, Schema.LOCAL_TIME.encode(localTime)},

      // String
      {"test".getBytes(StandardCharsets.UTF_8), String.class, "test"},
      {"test", String.class, "test"},
      {true, String.class, "true"},
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

      // Boolean
      {new byte[] {byteValue}, Boolean.class, true},
      {"true", Boolean.class, true},
      {true, Boolean.class, true},

      // Byte
      {new byte[] {byteValue}, Byte.class, byteValue},
      {"42", Byte.class, byteValue},
      {byteValue, Byte.class, byteValue},
      {shortValue, Byte.class, byteValue},
      {intValue, Byte.class, byteValue},
      {longValue, Byte.class, byteValue},
      {floatValue, Byte.class, byteValue},
      {doubleValue, Byte.class, byteValue},

      // Short
      {Schema.INT16.encode(shortValue), Short.class, shortValue},
      {"42", Short.class, shortValue},
      {byteValue, Short.class, shortValue},
      {shortValue, Short.class, shortValue},
      {intValue, Short.class, shortValue},
      {longValue, Short.class, shortValue},
      {floatValue, Short.class, shortValue},
      {doubleValue, Short.class, shortValue},

      // Integer
      {Schema.INT32.encode(intValue), Integer.class, intValue},
      {"42", Integer.class, intValue},
      {byteValue, Integer.class, intValue},
      {shortValue, Integer.class, intValue},
      {intValue, Integer.class, intValue},
      {longValue, Integer.class, intValue},
      {floatValue, Integer.class, intValue},
      {doubleValue, Integer.class, intValue},

      // Long
      {Schema.INT64.encode(longValue), Long.class, longValue},
      {"42", Long.class, longValue},
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
      {Schema.FLOAT.encode(floatValue), Float.class, floatValue},
      {"42.8", Float.class, floatValue},
      {byteValue, Float.class, 42F},
      {shortValue, Float.class, 42F},
      {intValue, Float.class, 42F},
      {longValue, Float.class, 42F},
      {floatValue, Float.class, floatValue},
      {doubleValue, Float.class, floatValue},

      // Double
      {Schema.DOUBLE.encode(doubleValue), Double.class, doubleValue},
      {"42.8", Double.class, doubleValue},
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
      {Schema.DATE.encode(date), Date.class, date},
      {"2023-01-02T23:04:05.000000006Z", Date.class, date},
      {dateTimeMillis, Date.class, date},
      {(double) dateTimeMillis, Date.class, date},
      {date, Date.class, date},
      {timestamp, Date.class, date},
      {localDateTime, Date.class, date},
      {instant, Date.class, date},
      {localDate, Date.class, new Date(midnightMillis)},

      // Timestamp
      {Schema.TIMESTAMP.encode(timestamp), Timestamp.class, new Timestamp(dateTimeMillis)},
      {"2023-01-02T23:04:05.000000006Z", Timestamp.class, timestamp},
      {dateTimeMillis, Timestamp.class, new Timestamp(dateTimeMillis)},
      {(double) dateTimeMillis, Timestamp.class, new Timestamp(dateTimeMillis)},
      {date, Timestamp.class, new Timestamp(dateTimeMillis)},
      {timestamp, Timestamp.class, timestamp},
      {localDateTime, Timestamp.class, timestamp},
      {instant, Timestamp.class, timestamp},
      {localDate, Timestamp.class, new Timestamp(midnightMillis)},

      // Time
      {Schema.TIME.encode(time), Time.class, time},
      {"23:04:05.000000006", Time.class, time},
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
      {Schema.LOCAL_TIME.encode(localTime), LocalTime.class, localTime},
      {"23:04:05.000000006", LocalTime.class, localTime},
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
      {Schema.LOCAL_DATE.encode(localDate), LocalDate.class, localDate},
      {"2023-01-02", LocalDate.class, localDate},
      {dateTimeMillis, LocalDate.class, localDate},
      {(double) dateTimeMillis, LocalDate.class, localDate},
      {date, LocalDate.class, localDate},
      {timestamp, LocalDate.class, localDate},
      {localDateTime, LocalDate.class, localDate},
      {instant, LocalDate.class, localDate},
      {localDate, LocalDate.class, localDate},

      // LocalDateTime
      {Schema.LOCAL_DATE_TIME.encode(localDateTime), LocalDateTime.class, localDateTime},
      {"2023-01-02T23:04:05.000000006", LocalDateTime.class, localDateTime},
      {(double) dateTimeMillis, LocalDateTime.class, localDateTimeWithoutNanos},
      {date, LocalDateTime.class, localDateTimeWithoutNanos},
      {timestamp, LocalDateTime.class, localDateTime},
      {localDateTime, LocalDateTime.class, localDateTime},
      {instant, LocalDateTime.class, localDateTime},
      {localDate, LocalDateTime.class, LocalDateTime.of(localDate, LocalTime.MIDNIGHT)},

      // Instant
      {Schema.INSTANT.encode(instant), Instant.class, instant},
      {"2023-01-02T23:04:05.000000006Z", Instant.class, instant},
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
