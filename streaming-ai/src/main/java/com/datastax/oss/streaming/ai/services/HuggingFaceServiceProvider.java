package com.datastax.oss.streaming.ai.services;

import com.datastax.oss.driver.shaded.guava.common.base.Strings;
import com.datastax.oss.streaming.ai.completions.CompletionsService;
import com.datastax.oss.streaming.ai.embeddings.AbstractHuggingFaceEmbeddingService;
import com.datastax.oss.streaming.ai.embeddings.EmbeddingsService;
import com.datastax.oss.streaming.ai.embeddings.HuggingFaceEmbeddingService;
import com.datastax.oss.streaming.ai.embeddings.HuggingFaceRestEmbeddingService;
import com.datastax.oss.streaming.ai.model.config.ComputeProvider;
import com.datastax.oss.streaming.ai.model.config.HuggingFaceConfig;
import com.datastax.oss.streaming.ai.model.config.TransformStepConfig;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;

import java.util.Map;
import java.util.Objects;

import static com.datastax.oss.streaming.ai.embeddings.AbstractHuggingFaceEmbeddingService.DLJ_BASE_URL;

@Slf4j
public class HuggingFaceServiceProvider implements ServiceProvider {

    private final Map<String, Object> providerConfiguration;

    public HuggingFaceServiceProvider(Map<String, Object> providerConfiguration) {
        this.providerConfiguration = providerConfiguration;
    }

    public HuggingFaceServiceProvider(TransformStepConfig tranformConfiguration) {
        this.providerConfiguration =
                new ObjectMapper().convertValue(tranformConfiguration.getHuggingface(), Map.class);
    }

    @Override
    public CompletionsService getCompletionsService(Map<String, Object> additionalConfiguration) {
        throw new IllegalArgumentException("Completions is still not available for HuggingFace");
    }

    @Override
    public EmbeddingsService getEmbeddingsService(Map<String, Object> additionalConfiguration) throws Exception {
        String provider = additionalConfiguration.getOrDefault("provider", ComputeProvider.API.name()).toString().toUpperCase();
        String modelUrl = (String) additionalConfiguration.get("modelUrl");
        String model = (String) additionalConfiguration.get("model");
        Map<String, String> options = (Map<String, String>) additionalConfiguration.get("options");
        Map<String, String> arguments = (Map<String, String>) additionalConfiguration.get("arguments");
        switch (provider) {
            case "LOCAL":
                AbstractHuggingFaceEmbeddingService.HuggingFaceConfig.HuggingFaceConfigBuilder builder =
                        AbstractHuggingFaceEmbeddingService.HuggingFaceConfig.builder()
                                .options(options)
                                .arguments(arguments);
                if (!Strings.isNullOrEmpty(model)) {
                    builder.modelName(model);

                    // automatically build the model URL if not provided
                    if (Strings.isNullOrEmpty(modelUrl)) {
                        modelUrl = DLJ_BASE_URL + model;
                        log.info("Automatically computed model URL {}", modelUrl);
                    }
                }
                builder.modelUrl(modelUrl);

                return new HuggingFaceEmbeddingService(builder.build());
            case "API":
                Objects.requireNonNull(model, "model name is required");
                HuggingFaceRestEmbeddingService.HuggingFaceApiConfig.HuggingFaceApiConfigBuilder
                        apiBuilder =
                        HuggingFaceRestEmbeddingService.HuggingFaceApiConfig.builder()
                                .accessKey((String) providerConfiguration.get("access-key"))
                                .model(model);

                String apiUurl = (String) providerConfiguration.get("api-url");
                if (!Strings.isNullOrEmpty(apiUurl)) {
                    apiBuilder.hfUrl(apiUurl);
                }
                if (options != null && !options.isEmpty()) {
                    apiBuilder.options(options);
                } else {
                    apiBuilder.options(Map.of("wait_for_model", "true"));
                }

                return new HuggingFaceRestEmbeddingService(apiBuilder.build());
            default:
                throw new IllegalArgumentException(
                        "Unsupported HuggingFace service type: " + provider);
        }
    }
}
