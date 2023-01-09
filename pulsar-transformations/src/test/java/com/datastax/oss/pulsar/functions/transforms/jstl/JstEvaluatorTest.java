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

import com.datastax.oss.pulsar.functions.transforms.TransformContext;
import com.datastax.oss.pulsar.functions.transforms.Utils;
import de.odysseus.el.tree.TreeBuilderException;
import java.time.Clock;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.HashMap;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.schema.GenericObject;
import org.apache.pulsar.client.impl.schema.AutoConsumeSchema;
import org.apache.pulsar.common.schema.SchemaType;
import org.apache.pulsar.functions.api.Record;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

public class JstEvaluatorTest {

  @Test(
    dataProvider = "methodInvocationExpressionProvider",
    expectedExceptions = TreeBuilderException.class
  )
  void testMethodInvocationsDisabled(String expression, TransformContext context) {
    new JstlEvaluator<>(String.format("${%s}", expression), String.class).evaluate(context);
  }

  @Test(dataProvider = "functionExpressionProvider")
  void testFunctions(String expression, TransformContext context, Object expectedValue) {
    assertEquals(
        expectedValue,
        new JstlEvaluator<>(String.format("${%s}", expression), Object.class).evaluate(context));
  }

  @Test
  void testPrimitiveValue() {
    Record<GenericObject> primitiveStringRecord =
        new Utils.TestRecord<>(
            Schema.STRING,
            AutoConsumeSchema.wrapPrimitiveObject("test-message", SchemaType.STRING, new byte[] {}),
            "");
    TransformContext primitiveStringContext =
        new TransformContext(
            new Utils.TestContext(primitiveStringRecord, new HashMap<>()),
            primitiveStringRecord.getValue().getNativeObject());

    String value =
        new JstlEvaluator<String>("${value}", String.class).evaluate(primitiveStringContext);

    assertEquals("test-message", value);
  }

  @Test
  void testNowFunction() {
    Record<GenericObject> primitiveStringRecord =
        new Utils.TestRecord<>(
            Schema.STRING,
            AutoConsumeSchema.wrapPrimitiveObject("test-message", SchemaType.STRING, new byte[] {}),
            "header-key");
    TransformContext primitiveStringContext =
        new TransformContext(
            new Utils.TestContext(primitiveStringRecord, new HashMap<>()),
            primitiveStringRecord.getValue().getNativeObject());

    long expectedMillis = 123L;
    Clock clock = Clock.fixed(Instant.ofEpochMilli(expectedMillis), ZoneOffset.UTC);
    JstlFunctions.setClock(clock);

    long actualMillis =
        new JstlEvaluator<Long>("${fn:now()}", long.class).evaluate(primitiveStringContext);

    assertEquals(expectedMillis, actualMillis);
  }

  @Test
  void testDateAddFunctionsNow() {
    Record<GenericObject> primitiveStringRecord =
        new Utils.TestRecord<>(
            Schema.STRING,
            AutoConsumeSchema.wrapPrimitiveObject("test-message", SchemaType.STRING, new byte[] {}),
            "header-key");
    TransformContext primitiveStringContext =
        new TransformContext(
            new Utils.TestContext(primitiveStringRecord, new HashMap<>()),
            primitiveStringRecord.getValue().getNativeObject());

    long nowMillis = 5000L;
    long millisToAdd = -3333L * 1000L;
    Clock clock = Clock.fixed(Instant.ofEpochMilli(nowMillis), ZoneOffset.UTC);
    JstlFunctions.setClock(clock);
    long actualMillis =
        new JstlEvaluator<Long>("${fn:dateadd(fn:now(), -3333, 'seconds')}", long.class)
            .evaluate(primitiveStringContext);

    assertEquals(nowMillis + millisToAdd, actualMillis);
  }

  /** @return {"expression", "transform context"} */
  @DataProvider(name = "methodInvocationExpressionProvider")
  public static Object[][] methodInvocationExpressionProvider() {
    Record<GenericObject> primitiveStringRecord =
        new Utils.TestRecord<>(
            Schema.STRING,
            AutoConsumeSchema.wrapPrimitiveObject("test-message", SchemaType.STRING, new byte[] {}),
            "header-key");
    TransformContext primitiveStringContext =
        new TransformContext(
            new Utils.TestContext(primitiveStringRecord, new HashMap<>()),
            primitiveStringRecord.getValue().getNativeObject());

    return new Object[][] {
      {"value.contains('test')", primitiveStringContext},
      {"value.toUpperCase() == 'TEST-MESSAGE'", primitiveStringContext},
      {"value.toUpperCase().toLowerCase() == 'test-message'", primitiveStringContext},
      {"value.substring(0, 4) == 'test'", primitiveStringContext},
      {"value.contains('random')", primitiveStringContext},
    };
  }

  /** @return {"expression", "context", "expected value"} */
  @DataProvider(name = "functionExpressionProvider")
  public static Object[][] functionExpressionProvider() {
    Record<GenericObject> primitiveStringRecord =
        new Utils.TestRecord<>(
            Schema.STRING,
            AutoConsumeSchema.wrapPrimitiveObject("test-message", SchemaType.STRING, new byte[] {}),
            "header-key");
    TransformContext primitiveStringContext =
        new TransformContext(
            new Utils.TestContext(primitiveStringRecord, new HashMap<>()),
            primitiveStringRecord.getValue().getNativeObject());
    OffsetDateTime offsetDateTime = OffsetDateTime.parse("2017-01-02T00:01:02Z");
    long millis = offsetDateTime.toInstant().toEpochMilli();
    return new Object[][] {
      {"fn:uppercase('test')", primitiveStringContext, "TEST"},
      {"fn:uppercase(value) == 'TEST-MESSAGE'", primitiveStringContext, true},
      {"fn:uppercase(null)", primitiveStringContext, null},
      {"fn:lowercase('TEST')", primitiveStringContext, "test"},
      {"fn:lowercase(value) == 'test-message'", primitiveStringContext, true},
      {"fn:lowercase(fn:uppercase(value)) == 'test-message'", primitiveStringContext, true},
      {"fn:lowercase(fn:coalesce(null, 'another-value'))", primitiveStringContext, "another-value"},
      {"fn:lowercase(fn:coalesce('value', 'another-value'))", primitiveStringContext, "value"},
      {"fn:lowercase(null)", primitiveStringContext, null},
      {"fn:contains(value, 'test')", primitiveStringContext, true},
      {"fn:contains(value, 'random')", primitiveStringContext, false},
      {"fn:contains(null, 'random')", primitiveStringContext, false},
      {"fn:contains(value, null)", primitiveStringContext, false},
      {"fn:trim('    trimmed      ')", primitiveStringContext, "trimmed"},
      {"fn:trim(null)", primitiveStringContext, null},
      {"fn:concat(value, '-suffix')", primitiveStringContext, "test-message-suffix"},
      {"fn:concat(value, null)", primitiveStringContext, "test-message"},
      {"fn:concat(null, '-suffix')", primitiveStringContext, "-suffix"},
      {
        "fn:dateadd('2017-01-02T00:01:02Z', 1, 'years')",
        primitiveStringContext,
        OffsetDateTime.parse("2018-01-02T00:01:02Z").toInstant().toEpochMilli()
      },
      {
        "fn:dateadd('2017-01-02T00:01:02Z', -1, 'months')",
        primitiveStringContext,
        OffsetDateTime.parse("2016-12-02T00:01:02Z").toInstant().toEpochMilli()
      },
      {
        "fn:dateadd('2017-01-02T00:01:02Z', 1, 'days')",
        primitiveStringContext,
        OffsetDateTime.parse("2017-01-03T00:01:02Z").toInstant().toEpochMilli()
      },
      {
        "fn:dateadd('2017-01-02T00:01:02Z', -1, 'hours')",
        primitiveStringContext,
        OffsetDateTime.parse("2017-01-01T23:01:02Z").toInstant().toEpochMilli()
      },
      {
        "fn:dateadd('2017-01-02T00:01:02Z', 1, 'minutes')",
        primitiveStringContext,
        OffsetDateTime.parse("2017-01-02T00:02:02Z").toInstant().toEpochMilli()
      },
      {
        "fn:dateadd('2017-01-02T00:01:02Z', -1, 'seconds')",
        primitiveStringContext,
        OffsetDateTime.parse("2017-01-02T00:01:01Z").toInstant().toEpochMilli()
      },
      {
        "fn:dateadd('2017-01-02T00:01:02Z', 1, 'millis')",
        primitiveStringContext,
        OffsetDateTime.parse("2017-01-02T00:01:02.001Z").toInstant().toEpochMilli()
      },
      {
        "fn:dateadd(" + millis + ", 1, 'years')",
        primitiveStringContext,
        OffsetDateTime.parse("2018-01-02T00:01:02Z").toInstant().toEpochMilli()
      },
      {
        "fn:dateadd(" + millis + ", -1, 'months')",
        primitiveStringContext,
        OffsetDateTime.parse("2016-12-02T00:01:02Z").toInstant().toEpochMilli()
      },
      {
        "fn:dateadd(" + millis + ", 1, 'days')",
        primitiveStringContext,
        OffsetDateTime.parse("2017-01-03T00:01:02Z").toInstant().toEpochMilli()
      },
      {
        "fn:dateadd(" + millis + ", -1, 'hours')",
        primitiveStringContext,
        OffsetDateTime.parse("2017-01-01T23:01:02Z").toInstant().toEpochMilli()
      },
      {
        "fn:dateadd(" + millis + ", 1, 'minutes')",
        primitiveStringContext,
        OffsetDateTime.parse("2017-01-02T00:02:02Z").toInstant().toEpochMilli()
      },
      {
        "fn:dateadd(" + millis + ", -1, 'seconds')",
        primitiveStringContext,
        OffsetDateTime.parse("2017-01-02T00:01:01Z").toInstant().toEpochMilli()
      },
      {
        "fn:dateadd(" + millis + ", 1, 'millis')",
        primitiveStringContext,
        OffsetDateTime.parse("2017-01-02T00:01:02.001Z").toInstant().toEpochMilli()
      },
    };
  }
}
