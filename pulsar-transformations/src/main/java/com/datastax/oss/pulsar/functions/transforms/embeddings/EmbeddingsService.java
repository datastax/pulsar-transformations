package com.datastax.oss.pulsar.functions.transforms.embeddings;

import java.util.List;

public interface EmbeddingsService {

  List<List<Double>> calculateEmbeddings(List<String> texts);
}
