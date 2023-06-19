package com.datastax.oss.pulsar.functions.transforms.model.config;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Getter;

@Getter
public class AddEmbeddingsConfig extends StepConfig {
  @JsonProperty(value = "model", required = true)
  private String model;

  @JsonProperty(value = "field", required = true)
  private String field;

  @JsonProperty(value = "embeddings-field", required = true)
  private String embeddingsFieldName;
}
