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
import java.util.HashMap;
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

/**
 * Compute AI Embeddings for one or more records fields and put the value into a new or existing
 * field.
 */
@Builder
@Slf4j
public class QueryStep implements TransformStep {

  @Builder.Default private final List<String> fields = new ArrayList<>();
  @Builder.Default private final String outputFieldName;
  @Builder.Default private final String query;
  @Builder.Default private final QueryStepDataSource dataSource;
  private final Map<Schema, Schema> avroValueSchemaCache = new ConcurrentHashMap<>();

  @Override
  public void process(TransformContext transformContext) {
    log.info(
        "process {} {} with {}",
        transformContext.getKeyObject(),
        transformContext.getValueObject(),
        dataSource);
    if (!isSchemaCompatible(transformContext)) {
      return;
    }

    Map<String, Object> values = new HashMap<>();
    collectFieldValuesFromKey(fields, transformContext, values);
    collectFieldValuesFromValue(fields, transformContext, values);
    List<Object> params = fields.stream().map(values::get).collect(Collectors.toList());
    final List<Map<String, String>> results = dataSource.fetchData(query, params);
    log.info("Results {}", results);
    applyResultsToRecord(transformContext, results);
  }

  private void applyResultsToRecord(
      TransformContext transformContext, List<Map<String, String>> results) {
    final SchemaType type = transformContext.getValueSchema().getSchemaInfo().getType();
    log.info("applyResultsToRecord {} {}", type, results);
    switch (type) {
      case AVRO:
        applyResultsToAvroRecord(transformContext, results);
        break;
      case JSON:
        applyResultsToJsonRecord(transformContext, results);
        break;
      default:
        return;
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
        log.info("Putting {} in field {}", results, field.name());
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

  private boolean isSchemaCompatible(TransformContext transformContext) {
    if (transformContext.getValueObject() == null) {
      return false;
    }
    final SchemaType type = transformContext.getValueSchema().getSchemaInfo().getType();
    log.info("schema type {}", type);
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
      List<String> fields, TransformContext record, Map<String, Object> collect) {
    collectFieldsFromRecord(fields, record.getKeySchema(), record.getKeyObject(), collect);
  }

  private void collectFieldValuesFromValue(
      List<String> fields, TransformContext record, Map<String, Object> collect) {
    collectFieldsFromRecord(fields, record.getValueSchema(), record.getValueObject(), collect);
  }

  private void collectFieldsFromRecord(
      List<String> fields,
      org.apache.pulsar.client.api.Schema schema,
      Object object,
      Map<String, Object> collect) {
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
      List<String> fields, Map<String, Object> collect, GenericRecord avroRecord) {
    for (String field : fields) {
      if (!avroRecord.hasField(field)) {
        continue;
      }
      final Object rawValue = avroRecord.get(field);
      if (rawValue != null) {
        // TODO: handle numbers...
        if (rawValue instanceof CharSequence) {
          // AVRO utf8...
          collect.put(field, rawValue.toString());
        } else {
          collect.put(field, rawValue);
        }
      }
    }
  }

  private void collectFieldsFromJson(
      List<String> fields, Map<String, Object> collect, JsonNode json) {
    for (String field : fields) {
      if (!json.has(field)) {
        continue;
      }
      final JsonNode node = json.get(field);
      if (node != null && !node.isNull()) {
        if (node.isNumber()) {
          collect.put(field, node.asDouble());
        } else {
          collect.put(field, node.asText());
        }
      }
    }
  }
}
