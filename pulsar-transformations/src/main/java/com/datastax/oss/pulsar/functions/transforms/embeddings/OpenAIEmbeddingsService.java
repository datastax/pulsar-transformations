package com.datastax.oss.pulsar.functions.transforms.embeddings;

import com.azure.ai.openai.OpenAIClient;
import com.azure.ai.openai.models.Embeddings;
import com.azure.ai.openai.models.EmbeddingsOptions;
import java.util.List;
import java.util.stream.Collectors;

public class OpenAIEmbeddingsService implements EmbeddingsService {

  private final OpenAIClient openAIClient;
  private final String model;

  public OpenAIEmbeddingsService(OpenAIClient openAIClient, String model) {
    this.openAIClient = openAIClient;
    this.model = model;
  }

  @Override
  public List<List<Double>> computeEmbeddings(List<String> texts) {
    EmbeddingsOptions embeddingsOptions = new EmbeddingsOptions(texts);
    Embeddings embeddings = openAIClient.getEmbeddings(model, embeddingsOptions);
    return embeddings
        .getData()
        .stream()
        .map(embedding -> embedding.getEmbedding())
        .collect(Collectors.toList());
  }
}
