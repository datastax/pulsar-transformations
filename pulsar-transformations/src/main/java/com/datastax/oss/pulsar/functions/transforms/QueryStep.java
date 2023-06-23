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

import com.datastax.oss.pulsar.functions.transforms.datasource.QueryStepDataSource;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;
import lombok.Builder;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericArray;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.pulsar.common.schema.SchemaType;
import org.apache.pulsar.functions.api.Record;

/**
 * Compute AI Embeddings for one or more records fields and put the value into a new or existing
 * field.
 */
@Builder
@Slf4j
public class QueryStep implements TransformStep {

  @Builder.Default private final List<String> fields = new ArrayList<>();
  private final String outputFieldName;
  private final String query;
  private final QueryStepDataSource dataSource;
  private final Map<Schema, Schema> avroValueSchemaCache = new ConcurrentHashMap<>();

  @Override
  public void process(TransformContext transformContext) {
    Record<?> currentRecord = transformContext.getContext().getCurrentRecord();
    List<Object> params = new ArrayList<>();
    fields.forEach(
        field -> {
          if (field.equals("value")) {
            params.add(transformContext.getValueObject());
          } else if (field.equals("key")) {
            params.add(transformContext.getKeyObject());
          } else if (field.equals("messageKey")) {
            params.add(transformContext.getKey());
          } else if (field.startsWith("properties.")) {
            String propName = field.substring("properties.".length());
            params.add(transformContext.getOutputProperties().get(propName));
          } else if (field.equals("destinationTopic")) {
            params.add(transformContext.getOutputTopic());
          } else if (field.equals("topicName")) {
            params.add(currentRecord.getTopicName().orElse(null));
          } else if (field.equals("eventTime")) {
            params.add(currentRecord.getEventTime().orElse(null));
          } else if (field.startsWith("value.")) {
            params.add(
                getField(
                    "value",
                    field,
                    transformContext.getValueSchema(),
                    transformContext.getValueObject()));
          } else if (field.startsWith("key.")) {
            params.add(
                getField(
                    "key",
                    field,
                    transformContext.getKeySchema(),
                    transformContext.getKeyObject()));
          }
        });
    final List<Map<String, String>> results = dataSource.fetchData(query, params);
    applyResultsToRecord(transformContext, results);
  }

  private Object getField(
      String key,
      String field,
      org.apache.pulsar.client.api.Schema<?> keySchema,
      Object keyObject) {
    String fieldName = field.substring((key.length() + 1));
    SchemaType schemaType = keySchema.getSchemaInfo().getType();
    switch (schemaType) {
      case AVRO:
        GenericRecord avroRecord = (GenericRecord) keyObject;
        return getAvroField(fieldName, avroRecord);
      case JSON:
        JsonNode json = (JsonNode) keyObject;
        return getJsonField(fieldName, json);
      default:
        throw new TransformFunctionException(
            String.format("%s.* can only be used in query step with AVRO or JSON schema", key));
    }
  }

  private static Object getJsonField(String fieldName, JsonNode json) {
    Object rawValue;
    JsonNode node = json.get(fieldName);
    if (node != null && !node.isNull()) {
      if (node.isNumber()) {
        rawValue = node.asDouble();
      } else {
        rawValue = node.asText();
      }
    } else {
      throw new TransformFunctionException(
          String.format("Field %s is null in JSON record", fieldName));
    }
    return rawValue;
  }

  private static Object getAvroField(String fieldName, GenericRecord avroRecord) {
    Object rawValue = avroRecord.get(fieldName);
    if (rawValue != null) {
      // TODO: handle numbers...
      if (rawValue instanceof CharSequence) {
        // AVRO utf8...
        rawValue = rawValue.toString();
      }
    } else {
      throw new TransformFunctionException(
          String.format("Field %s is null in AVRO record", fieldName));
    }
    return rawValue;
  }

  private void applyResultsToRecord(
      TransformContext transformContext, List<Map<String, String>> results) {
    final SchemaType type = transformContext.getValueSchema().getSchemaInfo().getType();
    switch (type) {
      case AVRO:
        applyResultsToAvroRecord(transformContext, results);
        break;
      case JSON:
        applyResultsToJsonRecord(transformContext, results);
        break;
      default:
    }
  }

  private void applyResultsToAvroRecord(
      TransformContext transformContext, List<Map<String, String>> results) {
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
                            if (f.name().equals(outputFieldName)) {
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
                        outputFieldName,
                        Schema.createArray(Schema.createMap(Schema.create(Schema.Type.STRING))),
                        "query results",
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
      if (field.name().equals(outputFieldName)) {
        GenericArray array = new GenericData.Array<>(results.size(), field.schema());
        results.forEach(array::add);
        newRecord.put(field.name(), array);
      } else {
        newRecord.put(field.name(), avroRecord.get(field.name()));
      }
    }
    if (avroRecord != newRecord) {
      transformContext.setValueModified(true);
    }
    transformContext.setValueObject(newRecord);
  }

  private void applyResultsToJsonRecord(
      TransformContext transformContext, List<Map<String, String>> results) {
    ObjectNode jsonNode = (ObjectNode) transformContext.getValueObject();
    final ArrayNode arrayNode =
        jsonNode
            .arrayNode()
            .addAll(
                results
                    .stream()
                    .map(
                        v -> {
                          ObjectNode node = jsonNode.objectNode();
                          v.forEach(node::put);
                          return node;
                        })
                    .collect(Collectors.toList()));
    jsonNode.set(outputFieldName, arrayNode);
    transformContext.setValueModified(true);
    transformContext.setValueObject(jsonNode);
  }
}
