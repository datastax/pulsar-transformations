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

import com.fasterxml.jackson.annotation.JsonAlias;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import lombok.Builder;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;

/**
 * EmbeddingsService implementation using HuggingFace REST API.
 *
 * <p>The model requested there should be trained for "sentence similarity" task.
 */
@Slf4j
public class HuggingFaceRestEmbeddingService implements EmbeddingsService {

  // https://huggingface.co/docs/api-inference/detailed_parameters#feature-extraction-task
  @Data
  @Builder
  public static class HuggingRestConfig {
    @Builder.Default public String hfUrl = HF_URL;

    @Builder.Default Map<String, Object> options = Map.of("wait_for_model", true);
  }

  private static final String HF_URL =
      "https://api-inference.huggingface.co/pipeline/feature-extraction/";
  private static final ObjectMapper om = EmbeddingsService.createObjectMapper();

  private final HuggingRestConfig conf;
  private final String model;
  private final String token;

  private final URL modelUrl;

  @Data
  @Builder
  public static class HuggingPojo {
    @JsonAlias("inputs")
    public List<String> inputs;

    @JsonAlias("options")
    public Map<String, Object> options;
  }

  public HuggingFaceRestEmbeddingService(String token, String model) throws MalformedURLException {
    this(HuggingRestConfig.builder().build(), token, model);
  }

  public HuggingFaceRestEmbeddingService(HuggingRestConfig conf, String token, String model)
      throws MalformedURLException {
    this.conf = conf;
    this.model = model;
    this.token = token;
    this.modelUrl = new URL(conf.hfUrl + model);

    // TODO: try checking if model is valid https://huggingface.co/docs/datasets-server/valid
    // TODO: check if model is suitable for "sentence similarity" task
  }

  @Override
  public List<List<Double>> computeEmbeddings(List<String> texts) {
    HuggingPojo pojo = HuggingPojo.builder().inputs(texts).options(conf.options).build();

    try {
      String jsonContent = om.writeValueAsString(pojo);

      String body = query(jsonContent);

      Object result = om.readValue(body, Object.class);
      return (List<List<Double>>) result;
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  // TODO: use some HTTP client library with better performance, if needed
  private String query(String jsonPayload) throws Exception {
    HttpURLConnection connection = (HttpURLConnection) modelUrl.openConnection();
    connection.setRequestMethod("POST");
    connection.setRequestProperty("Authorization", "Bearer " + token);
    connection.setDoOutput(true);

    java.io.OutputStream outputStream = connection.getOutputStream();
    outputStream.write(jsonPayload.getBytes("UTF-8"));
    outputStream.close();

    BufferedReader reader =
        new BufferedReader(
            new InputStreamReader(connection.getInputStream(), StandardCharsets.UTF_8));
    StringBuilder response = new StringBuilder();
    String line;
    while ((line = reader.readLine()) != null) {
      response.append(line);
    }
    reader.close();

    return response.toString();
  }

  public static void main(String[] args) throws Exception {
    try (EmbeddingsService service =
        new HuggingFaceRestEmbeddingService(args[0], "sentence-transformers/all-MiniLM-L6-v2")) {
      List<List<Double>> result =
          service.computeEmbeddings(List.of("hello world", "stranger things"));
      result.forEach(System.out::println);
    }
  }
}
