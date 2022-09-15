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

import com.datastax.oss.pulsar.functions.transforms.predicate.StepPredicatePair;
import com.datastax.oss.pulsar.functions.transforms.predicate.TransformPredicate;
import com.datastax.oss.pulsar.functions.transforms.predicate.jstl.JstlPredicate;
import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Predicate;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.client.api.schema.GenericObject;
import org.apache.pulsar.common.schema.SchemaType;
import org.apache.pulsar.functions.api.Context;
import org.apache.pulsar.functions.api.Function;
import org.apache.pulsar.functions.api.Record;

/**
 * <code>TransformFunction</code> is a {@link Function} that provides an easy way to apply a set of
 * usual basic transformations to the data.
 *
 * <p>It provides the following transformations:
 *
 * <ul>
 *   <li><code>cast</code>: modifies the key or value schema to a target compatible schema passed in
 *       the <code>schema-type</code> argument. This PR only enables <code>STRING</code>
 *       schema-type. The <code>part</code> argument allows to choose on which part to apply between
 *       <code>key</code> and <code>value</code>. If <code>part</code> is null or absent the
 *       transformations applies to both the key and value.
 *   <li><code>drop-fields</code>: drops fields given as a string list in parameter <code>fields
 *       </code>. The <code>part</code> argument allows to choose on which part to apply between
 *       <code>key</code> and <code>value</code>. If <code>part</code> is null or absent the
 *       transformations applies to both the key and value. Currently only AVRO is supported.
 *   <li><code>merge-key-value</code>: merges the fields of KeyValue records where both the key and
 *       value are structured types of the same schema type. Currently only AVRO is supported.
 *   <li><code>unwrap-key-value</code>: if the record is a KeyValue, extract the KeyValue's value
 *       and make it the record value. If parameter <code>unwrapKey</code> is present and set to
 *       <code>true</code>, extract the KeyValue's key instead.
 *   <li><code>flatten</code>: flattens a nested structure selected in the <code>part</code> by
 *       concatenating nested field names with a <code>delimiter</code> and populating them as top
 *       level fields. <code>
 *       delimiter</code> defaults to '_'. <code>part</code> could be any of <code>key</code> or
 *       <code>value</code>. If not specified, flatten will apply to key and value.
 * </ul>
 *
 * <p>The <code>TransformFunction</code> reads its configuration as Json from the {@link Context}
 * <code>userConfig</code> in the format:
 *
 * <pre><code class="lang-json">
 * {
 *   "steps": [
 *     {
 *       "type": "drop-fields", "fields": ["keyField1", "keyField2"], "part": "key"
 *     },
 *     {
 *       "type": "merge-key-value"
 *     },
 *     {
 *       "type": "unwrap-key-value"
 *     },
 *     {
 *       "type": "cast", "schema-type": "STRING"
 *     },
 *     {
 *       "type": "flatten", "delimiter" : "_" "part" : "value", "when": "value.field == 'value'"
 *     }
 *   ]
 * }
 * </code></pre>
 *
 * @see <a href="https://github.com/apache/pulsar/issues/15902">PIP-173 : Create a built-in Function
 *     implementing the most common basic transformations</a>
 */
@Slf4j
public class TransformFunction
    implements Function<GenericObject, Record<GenericObject>>, TransformStep {

  private final List<StepPredicatePair> steps = new ArrayList<>();
  private final Gson gson = new Gson();

  @Override
  public void initialize(Context context) {
    Object config =
        context
            .getUserConfigValue("steps")
            .orElseThrow(() -> new IllegalArgumentException("missing required 'steps' parameter"));
    LinkedList<Map<String, Object>> stepsConfig;
    try {
      TypeToken<LinkedList<Map<String, Object>>> typeToken = new TypeToken<>() {};
      stepsConfig = gson.fromJson((gson.toJson(config)), typeToken.getType());
    } catch (Exception e) {
      throw new IllegalArgumentException("could not parse configuration", e);
    }
    for (Map<String, Object> step : stepsConfig) {
      String type = getRequiredStringConfig(step, "type");
      Optional<String> when = getStringConfig(step, "when");
      TransformPredicate predicate = when.map(JstlPredicate::new).orElse(null);
      TransformStep transformStep;
      switch (type) {
        case "drop-fields":
          transformStep = newRemoveFieldFunction(step);
          break;
        case "cast":
          transformStep = newCastFunction(step);
          break;
        case "merge-key-value":
          transformStep = new MergeKeyValueStep();
          break;
        case "unwrap-key-value":
          transformStep = newUnwrapKeyValueFunction(step);
          break;
        case "flatten":
          transformStep = newFlattenFunction(step);
          break;
        default:
          throw new IllegalArgumentException("invalid step type: " + type);
      }
      steps.add(new StepPredicatePair(transformStep, predicate));
    }
  }

  @Override
  public Record<GenericObject> process(GenericObject input, Context context) throws Exception {
    Object nativeObject = input.getNativeObject();
    if (log.isDebugEnabled()) {
      Record<?> currentRecord = context.getCurrentRecord();
      log.debug("apply to {} {}", input, nativeObject);
      log.debug(
          "record with schema {} version {} {}",
          currentRecord.getSchema(),
          currentRecord.getMessage().orElseThrow().getSchemaVersion(),
          currentRecord);
    }

    TransformContext transformContext = new TransformContext(context, nativeObject);
    process(transformContext);
    return transformContext.send();
  }

  @Override
  public void process(TransformContext transformContext) throws Exception {
    for (StepPredicatePair pair : steps) {
      TransformStep step = pair.getTransformStep();
      Predicate<TransformContext> predicate = pair.getPredicate();
      if (predicate == null || predicate.test(transformContext)) {
        step.process(transformContext);
      }
    }
  }

  public static DropFieldStep newRemoveFieldFunction(Map<String, Object> step) {
    List<String> fieldList = new ArrayList<>();
    Object fields = step.get("fields");
    if (fields instanceof List) {
      for (Object field : (List<?>) fields) {
        if (field instanceof String && !((String) field).isEmpty()) {
          fieldList.add((String) field);
        }
      }
    }
    if (fieldList.size() == 0) {
      throw new IllegalArgumentException("Invalid 'fields' field in drop-fields step");
    }
    DropFieldStep.DropFieldStepBuilder builder = DropFieldStep.builder();
    return getStringConfig(step, "part")
        .map(
            part -> {
              if (part.equals("key")) {
                return builder.keyFields(fieldList);
              } else if (part.equals("value")) {
                return builder.valueFields(fieldList);
              } else {
                throw new IllegalArgumentException("invalid 'part' parameter: " + part);
              }
            })
        .orElseGet(() -> builder.keyFields(fieldList).valueFields(fieldList))
        .build();
  }

  public static CastStep newCastFunction(Map<String, Object> step) {
    String schemaTypeParam = getRequiredStringConfig(step, "schema-type");
    SchemaType schemaType = SchemaType.valueOf(schemaTypeParam);
    CastStep.CastStepBuilder builder = CastStep.builder();
    return getStringConfig(step, "part")
        .map(
            part -> {
              if (part.equals("key")) {
                return builder.keySchemaType(schemaType);
              } else if (part.equals("value")) {
                return builder.valueSchemaType(schemaType);
              } else {
                throw new IllegalArgumentException("invalid 'part' parameter: " + part);
              }
            })
        .orElseGet(() -> builder.keySchemaType(schemaType).valueSchemaType(schemaType))
        .build();
  }

  public static FlattenStep newFlattenFunction(Map<String, Object> step) {
    FlattenStep.FlattenStepBuilder builder = FlattenStep.builder();
    getStringConfig(step, "part")
        .ifPresent(
            part -> {
              if (!("key".equals(part) || "value".equals(part))) {
                throw new IllegalArgumentException("invalid 'part' parameter: " + part);
              }
              builder.part(part);
            });
    getStringConfig(step, "delimiter").ifPresent(builder::delimiter);
    return builder.build();
  }

  private static UnwrapKeyValueStep newUnwrapKeyValueFunction(Map<String, Object> step) {
    return new UnwrapKeyValueStep(getBooleanConfig(step, "unwrap-key").orElse(false));
  }

  private static Optional<String> getStringConfig(Map<String, Object> config, String fieldName) {
    Object fieldObject = config.get(fieldName);
    if (fieldObject == null) {
      return Optional.empty();
    }
    if (fieldObject instanceof String) {
      return Optional.of((String) fieldObject);
    }
    throw new IllegalArgumentException("field '" + fieldName + "' must be a string");
  }

  private static String getRequiredStringConfig(Map<String, Object> config, String fieldName) {
    return getStringConfig(config, fieldName)
        .filter(s -> !s.isEmpty())
        .orElseThrow(
            () -> new IllegalArgumentException("missing required '" + fieldName + "' parameter"));
  }

  private static Optional<Boolean> getBooleanConfig(Map<String, Object> config, String fieldName) {
    Object fieldObject = config.get(fieldName);
    if (fieldObject == null) {
      return Optional.empty();
    }
    if (fieldObject instanceof Boolean) {
      return Optional.of((Boolean) fieldObject);
    }
    throw new IllegalArgumentException("field '" + fieldName + "' must be a boolean");
  }
}
