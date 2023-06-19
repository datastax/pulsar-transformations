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
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import lombok.Builder;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.pulsar.common.schema.SchemaType;

/** Computes a field dynamically based on JSTL expressions and adds it to the key or the value . */
@Builder
public class AddEmbeddingsStep implements TransformStep {

  public static final long MILLIS_PER_DAY = TimeUnit.DAYS.toMillis(1);
  @Builder.Default private final List<String> fields = new ArrayList<>();
  @Builder.Default private final String embeddingsFieldName;
  @Builder.Default private final EmbeddingsService embeddingsService;
  private final Map<org.apache.avro.Schema, org.apache.avro.Schema> valueSchemaCache =
      new ConcurrentHashMap<>();

  @Override
  public void process(TransformContext transformContext) {
    if (!isEmbeddingsAppliableToTheSchema(transformContext)) {
      return;
    }
    String value = getFieldFromKey(fields, transformContext);
    if (value == null) {
      value = getFieldFromValue(fields, transformContext);
    }
    if (value == null) {
      return;
    }
    final List<Double> embeddings = embeddingsService.calculateEmbeddings(List.of(value)).get(0);
    applyEmbeddingsToRecord(transformContext, embeddings);
  }

  private void applyEmbeddingsToRecord(TransformContext transformContext, List<Double> embeddings) {
    GenericRecord avroRecord = (GenericRecord) transformContext.getValueObject();
    Schema avroSchema = avroRecord.getSchema();
    Schema modified =
        valueSchemaCache.computeIfAbsent(
            avroSchema,
            schema -> {
              final List<Schema.Field> newFields =
                  avroSchema
                      .getFields()
                      .stream()
                      .map(
                          f ->
                              new Schema.Field(
                                  f.name(), f.schema(), f.doc(), f.defaultVal(), f.order()))
                      .collect(Collectors.toList());
              newFields.add(
                  new Schema.Field(
                          embeddingsFieldName,
                      Schema.createArray(Schema.create(Schema.Type.DOUBLE)),
                      "embeddings",
                      List.of()));
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

  private boolean isEmbeddingsAppliableToTheSchema(TransformContext transformContext) {
    if (transformContext.getValueObject() != null
        && transformContext.getValueSchema().getSchemaInfo().getType() == SchemaType.AVRO) {
      if (transformContext.getValueObject() instanceof GenericRecord) {
        return true;
      }
    }
    return false;
  }

  public String getFieldFromKey(List<String> fields, TransformContext record) {
    if (record.getKeyObject() != null
        && record.getValueSchema().getSchemaInfo().getType() == SchemaType.AVRO) {
      if (record.getKeyObject() instanceof GenericRecord) {
        GenericRecord avroRecord = (GenericRecord) record.getKeyObject();
        final Object rawValue = avroRecord.get(fields.get(0));
        if (rawValue == null) {
          return null;
        }
        return rawValue.toString();
      }
    }
    return null;
  }

  public String getFieldFromValue(List<String> fields, TransformContext record) {
    if (record.getValueObject() != null
        && record.getValueSchema().getSchemaInfo().getType() == SchemaType.AVRO) {
      if (record.getValueObject() instanceof GenericRecord) {
        GenericRecord avroRecord = (GenericRecord) record.getValueObject();
        final Object rawValue = avroRecord.get(fields.get(0));
        if (rawValue == null) {
          return null;
        }
        return rawValue.toString();
      }
    }
    return null;
  }
}
