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

import com.azure.ai.openai.OpenAIClient;
import com.azure.ai.openai.OpenAIClientBuilder;
import com.azure.ai.openai.models.NonAzureOpenAIKeyCredential;
import com.azure.core.credential.AzureKeyCredential;
import com.datastax.oss.pulsar.functions.transforms.embeddings.EmbeddingsService;
import com.datastax.oss.pulsar.functions.transforms.embeddings.OpenAIEmbeddingsService;
import com.datastax.oss.pulsar.functions.transforms.jstl.predicate.JstlPredicate;
import com.datastax.oss.pulsar.functions.transforms.jstl.predicate.StepPredicatePair;
import com.datastax.oss.pulsar.functions.transforms.model.ComputeField;
import com.datastax.oss.pulsar.functions.transforms.model.ComputeFieldType;
import com.datastax.oss.pulsar.functions.transforms.model.config.AddEmbeddingsConfig;
import com.datastax.oss.pulsar.functions.transforms.model.config.CastConfig;
import com.datastax.oss.pulsar.functions.transforms.model.config.ComputeConfig;
import com.datastax.oss.pulsar.functions.transforms.model.config.DropFieldsConfig;
import com.datastax.oss.pulsar.functions.transforms.model.config.FlattenConfig;
import com.datastax.oss.pulsar.functions.transforms.model.config.OpenAIConfig;
import com.datastax.oss.pulsar.functions.transforms.model.config.OpenAIProvider;
import com.datastax.oss.pulsar.functions.transforms.model.config.StepConfig;
import com.datastax.oss.pulsar.functions.transforms.model.config.TransformStepConfig;
import com.datastax.oss.pulsar.functions.transforms.model.config.UnwrapKeyValueConfig;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.networknt.schema.JsonSchema;
import com.networknt.schema.JsonSchemaFactory;
import com.networknt.schema.SpecVersion;
import com.networknt.schema.ValidationMessage;
import com.networknt.schema.urn.URNFactory;
import java.io.InputStream;
import java.net.URL;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Predicate;
import java.util.regex.Pattern;
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
 *   <li><code>drop</code>: drops the message from further processing. Use in conjunction with
 *       <code>when</code> to selectively drop messages.
 *   <li><code>compute</code>: dynamically calculates <code>fields</code> values in the key, value
 *       or header. Each field has a <code>name</code> to represents a new or existing field (in
 *       this case, it will be overwritten). The value of the fields is evaluated by the <code>
 *       expression</code> and respect the <code>type</code>. Supported types are [INT32, INT64,
 *       FLOAT, DOUBLE, BOOLEAN, DATE, TIME, DATETIME]. Each field is marked as nullable by default.
 *       To mark the field as non-nullable in the output schema, set <code>optional</code> to false.
 * </ul>
 *
 * <p>The <code>TransformFunction</code> reads its configuration as Json from the {@link Context}
 * <code>userConfig</code> in the format:
 *
 * <pre><code class="lang-json">
 * {
 *   "steps": [
 *     {
 *       "type": "cast", "schema-type": "STRING"
 *     },
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
 *       "type": "flatten", "delimiter" : "_" "part" : "value", "when": "value.field == 'value'"
 *     },
 *     {
 *       "type": "drop", "when": "value.field == 'value'"
 *     },
 *     {
 *       "type": "compute", "fields": [{"name": "value.new-field", "expression": "key.existing-field == 'value'", "type": "BOOLEAN"}]
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
  private OpenAIClient openAIClient;

  @Override
  public void initialize(Context context) {
    Map<String, Object> userConfigMap = context.getUserConfigMap();
    ObjectMapper mapper = new ObjectMapper(new YAMLFactory());
    JsonNode jsonNode = mapper.convertValue(userConfigMap, JsonNode.class);

    URNFactory urnFactory =
        urn -> {
          try {
            URL absoluteURL = Thread.currentThread().getContextClassLoader().getResource(urn);
            return absoluteURL.toURI();
          } catch (Exception ex) {
            return null;
          }
        };
    JsonSchemaFactory factory =
        JsonSchemaFactory.builder(JsonSchemaFactory.getInstance(SpecVersion.VersionFlag.V4))
            .objectMapper(mapper)
            .addUrnFactory(urnFactory)
            .build();
    InputStream is =
        Thread.currentThread().getContextClassLoader().getResourceAsStream("config-schema.yaml");

    JsonSchema schema = factory.getSchema(is);
    Set<ValidationMessage> errors = schema.validate(jsonNode);

    if (errors.size() != 0) {
      if (!jsonNode.hasNonNull("steps")) {
        throw new IllegalArgumentException("Missing config 'steps' field");
      }
      JsonNode steps = jsonNode.get("steps");
      if (!steps.isArray()) {
        throw new IllegalArgumentException("Config 'steps' field must be an array");
      }
      String errorMessage = null;
      try {
        for (JsonNode step : steps) {
          String type = step.get("type").asText();
          JsonSchema stepSchema =
              factory.getSchema(
                  String.format(
                      "{\"$ref\": \"config-schema.yaml#/components/schemas/%s\"}",
                      kebabToPascal(type)));

          errorMessage =
              stepSchema
                  .validate(step)
                  .stream()
                  .findFirst()
                  .map(v -> String.format("Invalid '%s' step config: %s", type, v))
                  .orElse(null);
          if (errorMessage != null) {
            break;
          }
        }
      } catch (Exception e) {
        log.debug("Exception during steps validation, ignoring", e);
      }

      if (errorMessage != null) {
        throw new IllegalArgumentException(errorMessage);
      }

      errors
          .stream()
          .findFirst()
          .ifPresent(
              validationMessage -> {
                throw new IllegalArgumentException(
                    "Configuration validation failed: " + validationMessage);
              });
    }

    TransformStep transformStep;

    TransformStepConfig config = mapper.convertValue(userConfigMap, TransformStepConfig.class);

    openAIClient = buildOpenAIClient(config.getOpenai());

    for (StepConfig step : config.getSteps()) {
      switch (step.getType()) {
        case "drop-fields":
          transformStep = newRemoveFieldFunction((DropFieldsConfig) step);
          break;
        case "cast":
          transformStep = newCastFunction((CastConfig) step);
          break;
        case "merge-key-value":
          transformStep = new MergeKeyValueStep();
          break;
        case "unwrap-key-value":
          transformStep = newUnwrapKeyValueFunction((UnwrapKeyValueConfig) step);
          break;
        case "flatten":
          transformStep = newFlattenFunction((FlattenConfig) step);
          break;
        case "drop":
          transformStep = new DropStep();
          break;
        case "compute":
          transformStep = newComputeFieldFunction((ComputeConfig) step);
          break;
        case "add-embeddings":
          transformStep = newAddEmbeddingsStep((AddEmbeddingsConfig) step);
          break;
        default:
          throw new IllegalArgumentException("Invalid step type: " + step.getType());
      }
      steps.add(
          new StepPredicatePair(
              transformStep, step.getWhen() == null ? null : new JstlPredicate(step.getWhen())));
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

  private static String kebabToPascal(String kebab) {
    return Pattern.compile("(?:^|-)(.)").matcher(kebab).replaceAll(mr -> mr.group(1).toUpperCase());
  }

  public static DropFieldStep newRemoveFieldFunction(DropFieldsConfig config) {
    DropFieldStep.DropFieldStepBuilder builder = DropFieldStep.builder();
    if (config.getPart() != null) {
      if (config.getPart().equals("key")) {
        builder.keyFields(config.getFields());
      } else {
        builder.valueFields(config.getFields());
      }
    } else {
      builder.keyFields(config.getFields()).valueFields(config.getFields());
    }
    return builder.build();
  }

  public static CastStep newCastFunction(CastConfig config) {
    String schemaTypeParam = config.getSchemaType();
    SchemaType schemaType = SchemaType.valueOf(schemaTypeParam);
    CastStep.CastStepBuilder builder = CastStep.builder();
    if (config.getPart() != null) {
      if (config.getPart().equals("key")) {
        builder.keySchemaType(schemaType);
      } else {
        builder.valueSchemaType(schemaType);
      }
    } else {
      builder.keySchemaType(schemaType).valueSchemaType(schemaType);
    }
    return builder.build();
  }

  public static FlattenStep newFlattenFunction(FlattenConfig config) {
    FlattenStep.FlattenStepBuilder builder = FlattenStep.builder();
    if (config.getPart() != null) {
      builder.part(config.getPart());
    }
    if (config.getDelimiter() != null) {
      builder.delimiter(config.getDelimiter());
    }
    return builder.build();
  }

  private static TransformStep newComputeFieldFunction(ComputeConfig config) {
    List<ComputeField> fieldList = new ArrayList<>();
    Set<String> seen = new HashSet<>();
    config
        .getFields()
        .forEach(
            field -> {
              if (seen.contains(field.getName())) {
                throw new IllegalArgumentException(
                    "Duplicate compute field name detected: " + field.getName());
              }
              if (field.getType() == ComputeFieldType.DATE
                  && ("value".equals(field.getName()) || "key".equals(field.getName()))) {
                throw new IllegalArgumentException(
                    "The compute operation cannot apply the type DATE to the message value or key. "
                        + "Please consider using the types TIMESTAMP or INSTANT instead and follow with a 'cast' "
                        + "to SchemaType.DATE operation.");
              }
              seen.add(field.getName());
              ComputeFieldType type =
                  "destinationTopic".equals(field.getName())
                          || "messageKey".equals(field.getName())
                          || field.getName().startsWith("properties.")
                      ? ComputeFieldType.STRING
                      : field.getType();
              fieldList.add(
                  ComputeField.builder()
                      .scopedName(field.getName())
                      .expression(field.getExpression())
                      .type(type)
                      .optional(field.isOptional())
                      .build());
            });
    return ComputeStep.builder().fields(fieldList).build();
  }

  private TransformStep newAddEmbeddingsStep(AddEmbeddingsConfig config) {
    return AddEmbeddingsStep.builder()
        .embeddingsService(
            new OpenAIEmbeddingsService(openAIClient, config.getModel())
        )
        .embeddingsFieldName(config.getEmbeddingsFieldName())
        .fields(config.getFields())
        .build();
  }

  private static UnwrapKeyValueStep newUnwrapKeyValueFunction(UnwrapKeyValueConfig config) {
    return new UnwrapKeyValueStep(config.isUnwrapKey());
  }

  private OpenAIClient buildOpenAIClient(OpenAIConfig openAIConfig) {
    if (openAIConfig == null) {
      return null;
    }
    OpenAIClientBuilder openAIClientBuilder = new OpenAIClientBuilder();
    if (openAIConfig.getProvider() == OpenAIProvider.AZURE) {
      openAIClientBuilder.credential(new AzureKeyCredential(openAIConfig.getAccessKey()));
    } else {
      openAIClientBuilder.credential(new NonAzureOpenAIKeyCredential(openAIConfig.getAccessKey()));
    }
    if (openAIConfig.getUrl() != null) {
      openAIClientBuilder.endpoint(openAIConfig.getUrl());
    }
    return openAIClientBuilder.buildClient();
  }
}
