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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import lombok.Builder;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.pulsar.common.schema.SchemaType;

/** Compute AI Embeddings for one or more records fields and put the value into a new or existing field. */
@Builder
public class ComputeAIEmbeddingsStep implements TransformStep {

  @Builder.Default private final List<String> fields = new ArrayList<>();
  @Builder.Default private final String embeddingsFieldName;
  @Builder.Default private final EmbeddingsService embeddingsService;
  private final Map<org.apache.avro.Schema, org.apache.avro.Schema> avroValueSchemaCache =
      new ConcurrentHashMap<>();

  @Override
  public void process(TransformContext transformContext) {
    if (!isSchemaCompatible(transformContext)) {
      return;
    }

    Map<String, String> values = new HashMap<>();
    collectFieldValuesFromKey(fields, transformContext, values);
    collectFieldValuesFromValue(fields, transformContext, values);
    if (values.isEmpty()) {
      transformContext
          .getContext()
          .getLogger()
          .debug("Skipping message, empty values for configured fields");
      return;
    }
    final String text =
        fields
            .stream()
            .map(f -> values.get(f))
            .filter(Objects::nonNull)
            .collect(Collectors.joining(" "));

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

  private void applyEmbeddingsToAvroRecord(TransformContext transformContext, List<Double> embeddings) {
    GenericRecord avroRecord = (GenericRecord) transformContext.getValueObject();
    Schema avroSchema = avroRecord.getSchema();
    Schema modified =
        avroValueSchemaCache.computeIfAbsent(
            avroSchema,
            schema -> {
              AtomicBoolean fieldExists = new AtomicBoolean(false);
              final List<Schema.Field> newFields =
                  avroSchema
                      .getFields()
                      .stream()
                      .peek(
                          f -> {
                            if (f.name().equals(embeddingsFieldName)) {
                              fieldExists.set(true);
                            }
                          })
                      .map(
                          f ->
                              new Schema.Field(
                                  f.name(), f.schema(), f.doc(), f.defaultVal(), f.order()))
                      .collect(Collectors.toList());
              if (!fieldExists.get()) {
                newFields.add(
                    new Schema.Field(
                        embeddingsFieldName,
                        Schema.createArray(Schema.create(Schema.Type.DOUBLE)),
                        "embeddings",
                        List.of()));
              }
              return Schema.createRecord(
                  avroSchema.getName(),
                  avroSchema.getDoc(),
                  avroSchema.getNamespace(),
                  avroSchema.isError(),
                  newFields);
            });

    GenericRecord newRecord = new GenericData.Record(modified);
    for (Schema.Field field : modified.getFields()) {
      if (field.name().equals(embeddingsFieldName)) {
        newRecord.put(field.name(), embeddings);
      } else {
        newRecord.put(field.name(), avroRecord.get(field.name()));
      }
    }
    if (avroRecord != newRecord) {
      transformContext.setValueModified(true);
    }
    transformContext.setValueObject(newRecord);
  }


  private void applyEmbeddingsToJsonRecord(TransformContext transformContext, List<Double> embeddings) {
    ObjectNode jsonNode = (ObjectNode) transformContext.getValueObject();
    final ArrayNode arrayNode = jsonNode.arrayNode().addAll(embeddings.stream().map(v -> jsonNode.numberNode(v)).collect(Collectors.toList()));
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

  private void collectFieldsFromRecord(List<String> fields,
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
