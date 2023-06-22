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
import ai.djl.huggingface.translator.TextEmbeddingTranslatorFactory;
import ai.djl.inference.Predictor;
import ai.djl.ndarray.NDArray;
import ai.djl.ndarray.NDList;
import ai.djl.ndarray.NDManager;
import ai.djl.ndarray.types.Shape;
import ai.djl.nn.Block;
import ai.djl.nn.LambdaBlock;
import ai.djl.repository.zoo.Criteria;
import ai.djl.repository.zoo.ModelNotFoundException;
import ai.djl.repository.zoo.ZooModel;
import ai.djl.translate.TranslateException;
import java.io.IOException;
import java.lang.reflect.ParameterizedType;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import lombok.Builder;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public abstract class AbstractHuggingFaceEmbeddingService<IN, OUT>
    implements EmbeddingsService, AutoCloseable {

  @Override
  public void close() throws Exception {
    if (predictor != null) {
      predictor.close();
    }
    if (model != null) {
      model.close();
    }
  }

  @Data
  @Builder
  public static class HuggingConfig {
    String engine;

    @Builder.Default Map<String, String> options = Map.of("hasParameter", "false");

    @Builder.Default Map<String, String> arguments = Map.of();

    Path modelDir;
    String modelUrl;

    String modelName;

    List<Long> shape;
  }

  ZooModel<IN, OUT> model;
  Predictor<IN, OUT> predictor;

  public AbstractHuggingFaceEmbeddingService(HuggingConfig conf)
      throws IOException, ModelNotFoundException, MalformedModelException {
    Objects.requireNonNull(conf);
    Objects.requireNonNull(conf.shape);

    // https://stackoverflow.com/a/1901275/2237794
    // https://github.com/deepjavalibrary/djl/blob/master/extensions/tokenizers/src/test/java/ai/djl/huggingface/tokenizers/TextEmbeddingTranslatorTest.java
    Class<IN> inClass =
        (Class<IN>)
            ((ParameterizedType) getClass().getGenericSuperclass()).getActualTypeArguments()[0];
    Class<OUT> outClass =
        (Class<OUT>)
            ((ParameterizedType) getClass().getGenericSuperclass()).getActualTypeArguments()[1];

    Criteria.Builder<IN, OUT> builder = Criteria.builder().setTypes(inClass, outClass);
    if (conf.modelUrl != null) {
      builder.optModelUrls(conf.modelUrl);
      log.info("Loading model from {}", conf.modelUrl);
    } else if (conf.modelDir != null) {
      Files.createDirectories(conf.modelDir.toAbsolutePath());
      builder.optModelPath(conf.modelDir.toAbsolutePath());
      log.info("Loading model from {}", conf.modelDir.toAbsolutePath());
    } else {
      throw new IllegalArgumentException("Either modelUrl or modelDir must be set");
    }

    if (conf.modelName != null) {
      builder.optModelName(conf.modelName);
    }

    if (conf.engine != null) {
      builder.optEngine(conf.engine);
    }
    if (conf.options != null && !conf.options.isEmpty()) {
      conf.options.forEach(builder::optOption);
    }
    if (conf.arguments != null && !conf.arguments.isEmpty()) {
      conf.arguments.forEach(builder::optArgument);
    }

    Block block =
        new LambdaBlock(
            a -> {
              NDManager manager = a.getManager();
              NDArray arr = manager.ones(new Shape(conf.shape));
              arr.setName("last_hidden_state");
              return new NDList(arr);
            },
            "model");

    builder.optBlock(block);
    builder.optTranslatorFactory(new TextEmbeddingTranslatorFactory());

    Criteria<IN, OUT> criteria = builder.build();

    model = criteria.loadModel();
    predictor = model.newPredictor();
  }

  public List<OUT> compute(List<IN> texts) throws TranslateException {
    return predictor.batchPredict(texts);
  }

  abstract List<IN> convertInput(List<String> texts);

  abstract List<List<Double>> convertOutput(List<OUT> result);

  @Override
  public List<List<Double>> computeEmbeddings(List<String> texts) {
    try {
      List<OUT> results = compute(convertInput(texts));
      return convertOutput(results);
    } catch (TranslateException e) {
      log.error("failed to run compute", e);
      throw new RuntimeException("failed to run compute", e);
    }
  }
}
