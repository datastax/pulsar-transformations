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
package com.datastax.oss.pulsar.functions.transforms.util;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.avro.LogicalType;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericArray;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;

public class AvroUtil {

  /**
   * Returns the logical type of the schema. If the schema is a union, it will return the logical
   * type of any of the union types.
   */
  public static LogicalType getLogicalType(Schema schema) {
    if (!schema.isUnion()) {
      return schema.getLogicalType();
    }

    return schema
        .getTypes()
        .stream()
        .map(Schema::getLogicalType)
        .filter(Objects::nonNull)
        .findAny()
        .orElse(null);
  }

  public static GenericData.Record addOrReplaceAvroFields(
      GenericRecord record, Map<Schema.Field, Object> newFields, Map<Schema, Schema> schemaCache) {
    Schema avroSchema = record.getSchema();
    Set<String> computedFieldNames =
        newFields.keySet().stream().map(Schema.Field::name).collect(Collectors.toSet());
    // original fields - overwritten fields
    List<Schema.Field> nonOverwrittenFields =
        avroSchema
            .getFields()
            .stream()
            .filter(f -> !computedFieldNames.contains(f.name()))
            .map(f -> new Schema.Field(f.name(), f.schema(), f.doc(), f.defaultVal(), f.order()))
            .collect(Collectors.toList());
    // allFields is the intersection between existing fields and computed fields. Computed fields
    // take precedence.
    List<Schema.Field> allFields = new ArrayList<>();
    allFields.addAll(nonOverwrittenFields);
    allFields.addAll(new ArrayList<>(newFields.keySet()));
    Schema newSchema =
        schemaCache.computeIfAbsent(
            avroSchema,
            schema ->
                Schema.createRecord(
                    avroSchema.getName(),
                    avroSchema.getDoc(),
                    avroSchema.getNamespace(),
                    avroSchema.isError(),
                    allFields));

    GenericRecordBuilder newRecordBuilder = new GenericRecordBuilder(newSchema);
    // Add original fields
    for (Schema.Field field : nonOverwrittenFields) {
      newRecordBuilder.set(field.name(), record.get(field.name()));
    }
    // Add computed fields
    for (Map.Entry<Schema.Field, Object> entry : newFields.entrySet()) {
      // set the field by name to preserve field position
      Object value = entry.getValue();
      if ((value instanceof Collection) && !(value instanceof GenericArray)) {
        value = new GenericData.Array<>(entry.getKey().schema(), (Collection<Object>) value);
      }
      newRecordBuilder.set(entry.getKey().name(), value);
    }
    return newRecordBuilder.build();
  }
}
