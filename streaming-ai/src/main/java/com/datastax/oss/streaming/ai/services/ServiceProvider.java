package com.datastax.oss.streaming.ai.services;

import com.datastax.oss.streaming.ai.completions.CompletionsService;
import com.datastax.oss.streaming.ai.embeddings.EmbeddingsService;

import java.util.Map;

public interface ServiceProvider {
    CompletionsService getCompletionsService(Map<String, Object> additionalConfiguration) throws Exception;

    EmbeddingsService getEmbeddingsService(Map<String, Object> additionalConfiguration) throws Exception;

    public static class NoopServiceProvider implements ServiceProvider {
        @Override
        public CompletionsService getCompletionsService(Map<String, Object> additionalConfiguration) {
            throw new IllegalArgumentException("There is no provider configured for completion service");
        }

        @Override
        public EmbeddingsService getEmbeddingsService(Map<String, Object> additionalConfiguration) {
            throw new IllegalArgumentException("There is no provider configured for embeddings service");
        }
    }
}
