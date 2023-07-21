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
package com.datastax.oss.streaming.ai.completions;

import static com.datastax.oss.streaming.ai.util.TransformFunctionUtil.convertFromMap;

import com.azure.ai.openai.OpenAIClient;
import com.azure.ai.openai.models.ChatCompletionsOptions;
import com.azure.ai.openai.models.ChatRole;
import com.datastax.oss.streaming.ai.model.config.ChatCompletionsConfig;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class OpenAICompletionService implements CompletionsService {

  private OpenAIClient client;

  public OpenAICompletionService(OpenAIClient client) {
    this.client = client;
  }

  @Override
  public ChatCompletions getChatCompletions(
      List<ChatMessage> messages, Map<String, Object> options) {
    ChatCompletionsConfig config = convertFromMap(options, ChatCompletionsConfig.class);
    ChatCompletionsOptions chatCompletionsOptions =
        new ChatCompletionsOptions(
                messages
                    .stream()
                    .map(
                        message ->
                            new com.azure.ai.openai.models.ChatMessage(
                                    ChatRole.fromString(message.getRole()))
                                .setContent(message.getContent()))
                    .collect(Collectors.toList()))
            .setMaxTokens(config.getMaxTokens())
            .setTemperature(config.getTemperature())
            .setTopP(config.getTopP())
            .setLogitBias(config.getLogitBias())
            .setUser(config.getUser())
            .setStop(config.getStop())
            .setPresencePenalty(config.getPresencePenalty())
            .setFrequencyPenalty(config.getFrequencyPenalty());
    com.azure.ai.openai.models.ChatCompletions chatCompletions =
        client.getChatCompletions(config.getModel(), chatCompletionsOptions);
    ChatCompletions result = new ChatCompletions();
    result.setChoices(
        chatCompletions
            .getChoices()
            .stream()
            .map(
                c ->
                    new ChatChoice(
                        new ChatMessage(
                            c.getMessage().getRole().toString(), c.getMessage().getContent())))
            .collect(Collectors.toList()));
    return result;
  }
}
