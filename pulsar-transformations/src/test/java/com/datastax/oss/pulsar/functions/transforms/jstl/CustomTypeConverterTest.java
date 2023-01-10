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
import static org.testng.AssertJUnit.assertNull;
import static org.testng.AssertJUnit.assertTrue;

import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import org.apache.avro.util.Utf8;
import org.testng.annotations.Test;

public class CustomTypeConverterTest {

  @Test
  void testNullConversion() {
    CustomTypeConverter converter = new CustomTypeConverter();
    assertNull(converter.convert(null, String.class));
    assertNull(converter.convert(null, int.class));
    assertNull(converter.convert(null, long.class));
    assertNull(converter.convert(null, float.class));
    assertNull(converter.convert(null, double.class));
    assertNull(converter.convert(null, boolean.class));
    assertNull(converter.convert(null, LocalDate.class));
    assertNull(converter.convert(null, LocalTime.class));
    assertNull(converter.convert(null, OffsetDateTime.class));
  }

  @Test
  void testNonNullConversion() {
    CustomTypeConverter converter = new CustomTypeConverter();
    assertEquals(converter.convert("test", String.class), "test");
    assertEquals((int) converter.convert("1", int.class), 1);
    assertEquals(converter.convert(new Utf8("test"), String.class), "test");
    assertEquals((int) converter.convert(new Utf8("1"), int.class), 1);
    assertEquals((int) converter.convert(1, int.class), 1);
    assertEquals((long) converter.convert(1L, long.class), 1L);
    assertEquals(converter.convert(1.3F, float.class), 1.3F);
    assertEquals(converter.convert(1.4D, double.class), 1.4D);
    assertTrue(converter.convert(true, boolean.class));
    LocalDate expectedDate = LocalDate.of(2022, 12, 2);
    assertEquals(expectedDate, converter.convert("2022-12-02", LocalDate.class));
    LocalTime expectedTime = LocalTime.of(10, 11, 12);
    assertEquals(expectedTime, converter.convert("10:11:12", LocalTime.class));
    assertEquals(
        Instant.parse("2022-12-02T10:11:12Z"),
        converter.convert("2022-12-02T10:11:12Z", OffsetDateTime.class));
    Long epoch = OffsetDateTime.parse("2022-12-02T10:11:12Z").toInstant().toEpochMilli();
    assertEquals(
        Instant.parse("2022-12-02T10:11:12Z"), converter.convert(epoch, OffsetDateTime.class));
  }
}
