package com.datastax.oss.pulsar.functions.transforms.model.config;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Getter;

import java.util.List;

@Getter
public class AddEmbeddingsConfig extends StepConfig {
  @JsonProperty(value = "model", required = true)
  private String model;

  @JsonProperty(value = "fields", required = true)
  private List<String> fields;

  @JsonProperty(value = "embeddings-field", required = true)
  private String embeddingsFieldName;
}
