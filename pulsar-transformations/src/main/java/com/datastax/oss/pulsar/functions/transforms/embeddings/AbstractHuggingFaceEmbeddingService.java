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
import ai.djl.repository.zoo.Criteria;
import ai.djl.repository.zoo.ModelNotFoundException;
import ai.djl.repository.zoo.ZooModel;
import ai.djl.translate.TranslateException;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.lang.reflect.ParameterizedType;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentLinkedQueue;
import lombok.Builder;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public abstract class AbstractHuggingFaceEmbeddingService<IN, OUT>
    implements EmbeddingsService, AutoCloseable {

  private static final ObjectMapper om = EmbeddingsService.createObjectMapper();

  @Override
  public void close() throws Exception {
    while (!predictorList.isEmpty()) {
      Predictor<?, ?> p = predictorList.poll();
      if (p != null) {
        p.close();
      }
    }

    if (model != null) {
      model.close();
    }
  }

  @Data
  @Builder
  public static class HuggingConfig {

    public static HuggingConfig fromJsonString(String jsonStr) throws JsonProcessingException {
      return om.readValue(jsonStr, HuggingConfig.class);
    }

    String engine;

    @Builder.Default Map<String, String> options = Map.of();

    @Builder.Default Map<String, String> arguments = Map.of();

    String modelUrl;

    String modelName;
  }

  // thread safety:
  // http://djl.ai/docs/development/inference_performance_optimization.html#multithreading-support
  ZooModel<IN, OUT> model;

  private static final ThreadLocal<Predictor<?, ?>> predictorThreadLocal = new ThreadLocal<>();
  private static final ConcurrentLinkedQueue<Predictor<?, ?>> predictorList =
      new ConcurrentLinkedQueue<>();

  public AbstractHuggingFaceEmbeddingService(HuggingConfig conf)
      throws IOException, ModelNotFoundException, MalformedModelException {
    Objects.requireNonNull(conf);
    Objects.requireNonNull(conf.modelUrl);

    // https://stackoverflow.com/a/1901275/2237794
    // https://github.com/deepjavalibrary/djl/blob/master/extensions/tokenizers/src/test/java/ai/djl/huggingface/tokenizers/TextEmbeddingTranslatorTest.java
    Class<IN> inClass =
        (Class<IN>)
            ((ParameterizedType) getClass().getGenericSuperclass()).getActualTypeArguments()[0];
    Class<OUT> outClass =
        (Class<OUT>)
            ((ParameterizedType) getClass().getGenericSuperclass()).getActualTypeArguments()[1];

    Criteria.Builder<IN, OUT> builder = Criteria.builder().setTypes(inClass, outClass);

    builder.optModelUrls(conf.modelUrl);
    log.info("Loading model from {}", conf.modelUrl);

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

    // for getting embeddings
    builder.optTranslatorFactory(new TextEmbeddingTranslatorFactory());

    Criteria<IN, OUT> criteria = builder.build();

    model = criteria.loadModel();
  }

  public List<OUT> compute(List<IN> texts) throws TranslateException {
    Predictor<IN, OUT> predictor = (Predictor<IN, OUT>) predictorThreadLocal.get();
    if (predictor == null) {
      predictor = model.newPredictor();
      predictorThreadLocal.set(predictor);
      predictorList.add(predictor);
    }

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
