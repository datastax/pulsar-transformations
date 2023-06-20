package com.datastax.oss.pulsar.functions.transforms.model.config;

import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.List;
import lombok.Getter;

@Getter
public class ComputeAIEmbeddingsConfig extends StepConfig {
  @JsonProperty(value = "model", required = true)
  private String model;

  @JsonProperty(value = "fields", required = true)
  private List<String> fields;

  @JsonProperty(value = "embeddings-field", required = true)
  private String embeddingsFieldName;
}
