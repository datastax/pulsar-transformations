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

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;
import lombok.Builder;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.pulsar.common.schema.SchemaType;

/** This function removes a "field" from a message. */
@Builder
public class DropFieldStep implements TransformStep {

  @Builder.Default private final List<String> keyFields = new ArrayList<>();
  @Builder.Default private final List<String> valueFields = new ArrayList<>();

  private final Map<org.apache.avro.Schema, org.apache.avro.Schema> keySchemaCache =
      new ConcurrentHashMap<>();
  private final Map<org.apache.avro.Schema, org.apache.avro.Schema> valueSchemaCache =
      new ConcurrentHashMap<>();

  @Override
  public void process(TransformContext transformContext) {
    dropKeyFields(keyFields, transformContext);
    dropValueFields(valueFields, transformContext);
  }

  public void dropValueFields(List<String> fields, TransformContext context) {
    if (context.getValueSchema().getSchemaInfo().getType() == SchemaType.AVRO) {
      GenericRecord avroRecord = (GenericRecord) context.getValueObject();
      GenericRecord newRecord = dropFields(fields, avroRecord, valueSchemaCache);
      if (avroRecord != newRecord) {
        context.setValueModified(true);
      }
      context.setValueObject(newRecord);
    }
  }

  public void dropKeyFields(List<String> fields, TransformContext context) {
    if (context.getKeyObject() != null
        && context.getValueSchema().getSchemaInfo().getType() == SchemaType.AVRO) {
      GenericRecord avroRecord = (GenericRecord) context.getKeyObject();
      GenericRecord newRecord = dropFields(fields, avroRecord, keySchemaCache);
      if (avroRecord != newRecord) {
        context.setKeyModified(true);
      }
      context.setKeyObject(newRecord);
    }
  }

  private GenericRecord dropFields(
      List<String> fields,
      GenericRecord record,
      Map<org.apache.avro.Schema, org.apache.avro.Schema> schemaCache) {
    org.apache.avro.Schema avroSchema = record.getSchema();
    if (schemaCache.get(avroSchema) != null
        || fields.stream().anyMatch(field -> avroSchema.getField(field) != null)) {
      org.apache.avro.Schema modified =
          schemaCache.computeIfAbsent(
              avroSchema,
              schema ->
                  org.apache.avro.Schema.createRecord(
                      avroSchema.getName(),
                      avroSchema.getDoc(),
                      avroSchema.getNamespace(),
                      avroSchema.isError(),
                      avroSchema
                          .getFields()
                          .stream()
                          .filter(f -> !fields.contains(f.name()))
                          .map(
                              f ->
                                  new org.apache.avro.Schema.Field(
                                      f.name(), f.schema(), f.doc(), f.defaultVal(), f.order()))
                          .collect(Collectors.toList())));

      GenericRecord newRecord = new GenericData.Record(modified);
      for (org.apache.avro.Schema.Field field : modified.getFields()) {
        newRecord.put(field.name(), record.get(field.name()));
      }
      return newRecord;
    }
    return record;
  }
}
