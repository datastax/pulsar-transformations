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
import com.samskivert.mustache.Mustache;
import com.samskivert.mustache.Template;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;
import org.apache.avro.Schema;

public class ChatCompletionsStep implements TransformStep {

  private final OpenAIClient client;
  private final ChatCompletionsConfig config;

  private final Map<Schema, Schema> avroValueSchemaCache = new ConcurrentHashMap<>();

  private final Map<Schema, Schema> avroKeySchemaCache = new ConcurrentHashMap<>();

  private final Map<ChatMessage, Template> messageTemplates = new ConcurrentHashMap<>();

  public ChatCompletionsStep(OpenAIClient client, ChatCompletionsConfig config) {
    this.client = client;
    this.config = config;
    config
        .getMessages()
        .forEach(
            chatMessage ->
                messageTemplates.put(
                    chatMessage, Mustache.compiler().compile(chatMessage.getContent())));
  }

  @Override
  public void process(TransformContext transformContext) throws Exception {
    JsonRecord jsonRecord = transformContext.toJsonRecord();

    List<ChatMessage> messages =
        config
            .getMessages()
            .stream()
            .map(
                message ->
                    new ChatMessage(message.getRole())
                        .setContent(messageTemplates.get(message).execute(jsonRecord)))
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
    String fieldName = config.getFieldName();
    transformContext.setResultField(
        content,
        fieldName,
        org.apache.avro.Schema.create(org.apache.avro.Schema.Type.STRING),
        avroKeySchemaCache,
        avroValueSchemaCache);

    String logField = config.getLogField();
    if (logField != null && !logField.isEmpty()) {
      Map<String, Object> logMap = new HashMap<>();
      logMap.put("model", config.getModel());
      logMap.put("options", chatCompletionsOptions);
      transformContext.setResultField(
          TransformContext.toJson(logMap),
          logField,
          org.apache.avro.Schema.create(org.apache.avro.Schema.Type.STRING),
          avroKeySchemaCache,
          avroValueSchemaCache);
    }
  }
}
