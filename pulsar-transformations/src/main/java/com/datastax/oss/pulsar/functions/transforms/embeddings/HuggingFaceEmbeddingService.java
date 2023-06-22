/*
 * Copyright DataStax, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datastax.oss.pulsar.functions.transforms.embeddings;

import ai.djl.MalformedModelException;
import ai.djl.repository.zoo.ModelNotFoundException;
import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class HuggingFaceEmbeddingService
    extends AbstractHuggingFaceEmbeddingService<String, float[]> {
  public HuggingFaceEmbeddingService(HuggingConfig conf)
      throws IOException, ModelNotFoundException, MalformedModelException {
    super(conf);
  }

  @Override
  List<String> convertInput(List<String> texts) {
    return texts;
  }

  @Override
  List<List<Double>> convertOutput(List<float[]> result) {
    List<List<Double>> out = new ArrayList<>(result.size());
    for (float[] floats : result) {
      List<Double> l = new ArrayList<>(floats.length);
      for (float aFloat : floats) {
        l.add((double) aFloat);
      }
      out.add(l);
    }
    return out;
  }

  public static void main(String[] args) throws Exception {
    /*
    brew install git-lfs
    git lfs install
    git clone https://huggingface.co/bert-base-uncased
    */
    HuggingConfig conf =
        HuggingConfig.builder()
            .engine("PyTorch")
            .modelDir(Path.of("/Users/andreyyegorov/src/bert-base-uncased"))
            // .modelUrl("https://api-inference.huggingface.co/models/")
            .arguments(Map.of("tokenizer", "bert-base-uncased"))
            .modelName("pytorch_model.bin")
            .shape(List.of(1L, 4L, 384L))
            .build();
    try (EmbeddingsService service = new HuggingFaceEmbeddingService(conf)) {
      List<List<Double>> result = service.computeEmbeddings(List.of("Hello World"));
      System.out.println(result);
    }
  }
}
