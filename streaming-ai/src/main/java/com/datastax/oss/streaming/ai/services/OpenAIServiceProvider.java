package com.datastax.oss.streaming.ai.services;

import com.azure.ai.openai.OpenAIClient;
import com.datastax.oss.streaming.ai.completions.CompletionsService;
import com.datastax.oss.streaming.ai.completions.OpenAICompletionService;
import com.datastax.oss.streaming.ai.embeddings.EmbeddingsService;
import com.datastax.oss.streaming.ai.embeddings.OpenAIEmbeddingsService;
import com.datastax.oss.streaming.ai.model.config.TransformStepConfig;
import com.datastax.oss.streaming.ai.services.ServiceProvider;
import com.datastax.oss.streaming.ai.util.TransformFunctionUtil;

import java.util.Map;

public class OpenAIServiceProvider implements ServiceProvider {

    private final OpenAIClient client;
    public OpenAIServiceProvider(TransformStepConfig config) {
        client = TransformFunctionUtil.buildOpenAIClient(config.getOpenai());
    }

    public OpenAIServiceProvider(OpenAIClient client) {
        this.client = client;
    }

    @Override
    public CompletionsService getCompletionsService(Map<String, Object> additionalConfiguration) {
        return new OpenAICompletionService(client);
    }

    @Override
    public EmbeddingsService getEmbeddingsService(Map<String, Object> additionalConfiguration) {
        String model = (String) additionalConfiguration.get("model");
        return new OpenAIEmbeddingsService(client, model);
    }
}
