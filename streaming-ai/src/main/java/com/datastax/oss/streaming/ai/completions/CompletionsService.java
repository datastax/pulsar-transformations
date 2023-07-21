package com.datastax.oss.streaming.ai.completions;

import com.azure.ai.openai.models.ChatCompletions;
import com.azure.ai.openai.models.ChatCompletionsOptions;

public interface CompletionsService {
    ChatCompletions getChatCompletions(String model, ChatCompletionsOptions chatCompletionsOptions);

    public static class NoopCompletionService implements CompletionsService {
        @Override
        public ChatCompletions getChatCompletions(String model, ChatCompletionsOptions chatCompletionsOptions) {
            throw new UnsupportedOperationException();
        }
    }
}
