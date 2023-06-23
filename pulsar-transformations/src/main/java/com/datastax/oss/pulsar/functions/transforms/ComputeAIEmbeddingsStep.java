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
package com.datastax.oss.pulsar.functions.transforms;

import com.datastax.oss.pulsar.functions.transforms.embeddings.EmbeddingsService;
import com.datastax.oss.pulsar.functions.transforms.model.JsonRecord;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.samskivert.mustache.Mustache;
import com.samskivert.mustache.Template;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.pulsar.common.schema.SchemaType;

/**
 * Compute AI Embeddings from a template filled with the received message fields and metadata and
 * put the value into a new or existing field.
 */
public class ComputeAIEmbeddingsStep implements TransformStep {

  private final Template template;
  private final String embeddingsFieldName;
  private final EmbeddingsService embeddingsService;
  private final Map<org.apache.avro.Schema, org.apache.avro.Schema> avroValueSchemaCache =
      new ConcurrentHashMap<>();

  public ComputeAIEmbeddingsStep(
      String text, String embeddingsFieldName, EmbeddingsService embeddingsService) {
    this.template = Mustache.compiler().compile(text);
    this.embeddingsFieldName = embeddingsFieldName;
    this.embeddingsService = embeddingsService;
  }

  @Override
  public void process(TransformContext transformContext) {
    JsonRecord jsonRecord = transformContext.toJsonRecord();
    String text = template.execute(jsonRecord);

    final List<Double> embeddings = embeddingsService.computeEmbeddings(List.of(text)).get(0);
    applyEmbeddingsToRecord(transformContext, embeddings);
  }

  private void applyEmbeddingsToRecord(TransformContext transformContext, List<Double> embeddings) {
    final SchemaType type = transformContext.getValueSchema().getSchemaInfo().getType();
    switch (type) {
      case AVRO:
        applyEmbeddingsToAvroRecord(transformContext, embeddings);
        break;
      case JSON:
        applyEmbeddingsToJsonRecord(transformContext, embeddings);
        break;
      default:
        return;
    }
  }

  private void applyEmbeddingsToAvroRecord(
      TransformContext transformContext, List<Double> embeddings) {
    Schema.Field embeddingSchema =
        new Schema.Field(
            embeddingsFieldName,
            Schema.createArray(Schema.create(Schema.Type.DOUBLE)),
            "embeddings",
            List.of());
    transformContext.addOrReplaceAvroValueFields(
        Map.of(embeddingSchema, embeddings), avroValueSchemaCache);
  }

  private void applyEmbeddingsToJsonRecord(
      TransformContext transformContext, List<Double> embeddings) {
    ObjectNode jsonNode = (ObjectNode) transformContext.getValueObject();
    final ArrayNode arrayNode =
        jsonNode
            .arrayNode()
            .addAll(
                embeddings.stream().map(v -> jsonNode.numberNode(v)).collect(Collectors.toList()));
    jsonNode.set(embeddingsFieldName, arrayNode);
    transformContext.setValueModified(true);
    transformContext.setValueObject(jsonNode);
  }

  private boolean isSchemaCompatible(TransformContext transformContext) {
    if (transformContext.getValueObject() == null) {
      return false;
    }
    final SchemaType type = transformContext.getValueSchema().getSchemaInfo().getType();
    switch (type) {
      case AVRO:
      case JSON:
        return true;
      default:
        transformContext
            .getContext()
            .getLogger()
            .debug("Skipping message, schema is not a generic record");
        return false;
    }
  }

  private void collectFieldValuesFromKey(
      List<String> fields, TransformContext record, Map<String, String> collect) {
    collectFieldsFromRecord(fields, record.getKeySchema(), record.getKeyObject(), collect);
  }

  private void collectFieldValuesFromValue(
      List<String> fields, TransformContext record, Map<String, String> collect) {
    collectFieldsFromRecord(fields, record.getValueSchema(), record.getValueObject(), collect);
  }

  private void collectFieldsFromRecord(
      List<String> fields,
      org.apache.pulsar.client.api.Schema schema,
      Object object,
      Map<String, String> collect) {
    if (object == null) {
      return;
    }

    final SchemaType type = schema.getSchemaInfo().getType();
    switch (type) {
      case AVRO:
        GenericRecord avroRecord = (GenericRecord) object;
        collectFieldsFromAvro(fields, collect, avroRecord);
        break;
      case JSON:
        final JsonNode json = (JsonNode) object;
        collectFieldsFromJson(fields, collect, json);
        break;
      default:
        return;
    }
  }

  private void collectFieldsFromAvro(
      List<String> fields, Map<String, String> collect, GenericRecord avroRecord) {
    for (String field : fields) {
      if (!avroRecord.hasField(field)) {
        continue;
      }
      final Object rawValue = avroRecord.get(field);
      if (rawValue != null) {
        collect.put(field, rawValue.toString());
      }
    }
  }

  private void collectFieldsFromJson(
      List<String> fields, Map<String, String> collect, JsonNode json) {
    for (String field : fields) {
      if (!json.has(field)) {
        continue;
      }
      final JsonNode node = json.get(field);
      if (node != null) {
        collect.put(field, node.asText());
      }
    }
  }
}
