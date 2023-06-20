package com.datastax.oss.pulsar.functions.transforms.embeddings;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class MockEmbeddingsService implements EmbeddingsService {

  private final Map<String, List<Double>> embeddingsMapping = new HashMap<>();

  public void setEmbeddingsForText(String text, List<Double> embeddings) {
    embeddingsMapping.put(text, embeddings);
  }

  @Override
  public List<List<Double>> computeEmbeddings(List<String> texts) {
    System.out.println(
        "MockEmbeddingsService.calculateEmbeddings" + texts + " " + embeddingsMapping);
    return texts
        .stream()
        .map(text -> embeddingsMapping.get(text))
        .collect(java.util.stream.Collectors.toList());
  }
}
