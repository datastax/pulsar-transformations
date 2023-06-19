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

import com.azure.ai.openai.OpenAIClient;
import com.azure.ai.openai.OpenAIClientBuilder;
import com.azure.ai.openai.models.Embeddings;
import com.azure.ai.openai.models.EmbeddingsOptions;
import com.azure.core.credential.AzureKeyCredential;
import jakarta.el.ELException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.time.Clock;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;
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

  public static Object embed(Object input) {

    log.error("embed started: " + input);

    String azureOpenaiKey = "783fe7bc013149f2a197ce3a4ef54531";
    String endpoint = "https://datastax-openai-dev.openai.azure.com";
    String deploymentOrModelId = "text-embedding-ada-002";

    OpenAIClient client =
        new OpenAIClientBuilder()
            .endpoint(endpoint)
            .credential(new AzureKeyCredential(azureOpenaiKey))
            .buildClient();

    List<String> prompt = new ArrayList<>();
    String value = JstlTypeConverter.INSTANCE.coerceToString(input);
    prompt.add(value);
    EmbeddingsOptions options = new EmbeddingsOptions(prompt);

    Embeddings embeddings = client.getEmbeddings(deploymentOrModelId, options);
    embeddings.getData().forEach(System.out::println);

    ProcessBuilder builder = new ProcessBuilder();

    String cqlSh = "INSERT INTO online.embeddings (id, value, item_vector) VALUES (%s, '%s', %s);";
    cqlSh =
        String.format(
            cqlSh,
            ThreadLocalRandom.current().nextInt(),
            value,
            embeddings.getData().get(0).getEmbedding());
    builder.command(
        "/Users/ayman.khalil/Downloads/cqlsh-astra/bin/cqlsh",
        "-b",
        "/Users/ayman.khalil/Downloads/secure-connect-hackathon-db.zip",
        "-u",
        "UTFmPpZidZfkyZeNSKncFllQ",
        "-p",
        "E1DxA6H84oAy9dqRvOG4ZXh-v4ohBc2Y9ua2Jj2M5NxMgYwDlWKfXZb-4y890dpzdE-oGYLmsED38T4C0FbX+qk-Mc0OPndb80MMa+E+gNjLjS9n+LGZAx7btbeHX1TH",
        "-e",
        cqlSh);

    try {
      Process process = builder.start();
      int exitCode = process.waitFor();
      log.warn("\nExited with error code : " + exitCode);
    } catch (Exception e) {
      log.warn(e.getMessage());
    }

    return input;
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
