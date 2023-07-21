package com.datastax.oss.streaming.ai.completions;

import com.azure.ai.openai.OpenAIClient;
import com.azure.ai.openai.models.ChatCompletions;
import com.azure.ai.openai.models.ChatCompletionsOptions;

public class OpenAICompletionService implements CompletionsService {

    private OpenAIClient client;
    public OpenAICompletionService(OpenAIClient client) {
        this.client = client;
    }

    @Override
    public ChatCompletions getChatCompletions(String model, ChatCompletionsOptions chatCompletionsOptions) {
        return client.getChatCompletions(model, chatCompletionsOptions);
    }
}
