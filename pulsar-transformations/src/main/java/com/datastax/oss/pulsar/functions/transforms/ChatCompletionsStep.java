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
import com.azure.ai.openai.models.ChatCompletions;
import com.azure.ai.openai.models.ChatCompletionsOptions;
import com.azure.ai.openai.models.ChatMessage;
import com.datastax.oss.pulsar.functions.transforms.model.JsonRecord;
import com.datastax.oss.pulsar.functions.transforms.model.config.ChatCompletionsConfig;
import com.datastax.oss.pulsar.functions.transforms.util.JsonConverter;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.samskivert.mustache.Mustache;
import com.samskivert.mustache.Template;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.functions.api.Record;

public class ChatCompletionsStep implements TransformStep {

  private final OpenAIClient client;
  private final ChatCompletionsConfig config;

  public ChatCompletionsStep(OpenAIClient client, ChatCompletionsConfig config) {
    this.client = client;
    this.config = config;
  }

  @Override
  public void process(TransformContext transformContext) throws Exception {
    JsonRecord jsonRecord = new JsonRecord();
    if (transformContext.getKeySchema() != null) {
      jsonRecord.setKey(
          toJsonSerializable(transformContext.getKeySchema(), transformContext.getKeyObject()));
    } else {
      jsonRecord.setKey(transformContext.getKey());
    }
    jsonRecord.setValue(
        toJsonSerializable(transformContext.getValueSchema(), transformContext.getValueObject()));
    jsonRecord.setDestinationTopic(transformContext.getOutputTopic());

    jsonRecord.setProperties(transformContext.getOutputProperties());
    Record<?> currentRecord = transformContext.getContext().getCurrentRecord();
    currentRecord.getEventTime().ifPresent(jsonRecord::setEventTime);
    currentRecord.getTopicName().ifPresent(jsonRecord::setTopicName);

    List<ChatMessage> messages =
        config
            .getMessages()
            .stream()
            .map(
                message ->
                    new ChatMessage(message.getRole())
                        .setContent(applyTemplate(message.getContent(), jsonRecord)))
            .collect(Collectors.toList());

    ChatCompletionsOptions chatCompletionsOptions =
        new ChatCompletionsOptions(messages)
            .setMaxTokens(config.getMaxTokens())
            .setTemperature(config.getTemperature())
            .setTopP(config.getTopP())
            .setLogitBias(config.getLogitBias())
            .setUser(config.getUser())
            .setStop(config.getStop())
            .setPresencePenalty(config.getPresencePenalty())
            .setFrequencyPenalty(config.getFrequencyPenalty());

    ChatCompletions chatCompletions =
        client.getChatCompletions(config.getModel(), chatCompletionsOptions);

    String content = chatCompletions.getChoices().get(0).getMessage().getContent();
    // TODO: At the moment, we only support outputing the value with STRING schema.

    String fieldName = config.getFieldName();
    if (fieldName == null || fieldName.equals("value")) {
      transformContext.setValueSchema(Schema.STRING);
      transformContext.setValueObject(content);
    } else if (fieldName.equals("key")) {
      transformContext.setKeySchema(Schema.STRING);
      transformContext.setKeyObject(content);
    } else if (fieldName.equals("destinationTopic")) {
      transformContext.setOutputTopic(content);
    } else if (fieldName.equals("messageKey")) {
      transformContext.setKey(content);
    } else if (fieldName.startsWith("properties.")) {
      String propertyKey = fieldName.substring("properties.".length());
      transformContext.addProperty(propertyKey, content);
    } else {
      throw new IllegalArgumentException(
          "Invalid fieldName: "
              + fieldName
              + ". fieldName must be one of [value, key, destinationTopic, messageKey, properties.*]");
    }
  }

  private static Object toJsonSerializable(Schema<?> schema, Object val) {
    if (schema == null || schema.getSchemaInfo().getType().isPrimitive()) {
      return val;
    }
    switch (schema.getSchemaInfo().getType()) {
      case AVRO:
        ObjectMapper mapper = new ObjectMapper();
        // TODO: do better than the double conversion AVRO -> JSON -> Map
        return mapper.convertValue(
            JsonConverter.toJson((org.apache.avro.generic.GenericRecord) val),
            new TypeReference<Map<String, Object>>() {});
      case JSON:
        return val;
      default:
        throw new UnsupportedOperationException(
            "Unsupported schemaType " + schema.getSchemaInfo().getType());
    }
  }

  private String applyTemplate(String template, JsonRecord record) {
    // TODO: pre-compile the templates at init time
    Template tmpl = Mustache.compiler().compile(template);
    return tmpl.execute(record);
  }
}
