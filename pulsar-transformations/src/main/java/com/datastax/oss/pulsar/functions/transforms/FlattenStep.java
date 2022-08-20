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
import java.util.Optional;
import lombok.RequiredArgsConstructor;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.common.schema.SchemaType;

@RequiredArgsConstructor
public class FlattenStep implements TransformStep {
  // TODO: Remove dependency on Avro's Field default value. It will not work with Union schemas "org.apache.avro.AvroTypeException: Invalid default for field a: "cloth" not a ["null","string"]"
  // TODO: Cache schema to flattened schema
  // TODO: Make flatten delimiter configurable
  // TODO: Validate flatten delimiter if possible
  // TODO: Microbenchmark the flatten algorithm to optimize if needed
  // TODO: Add unit tests for schemas other than string for the unnested schema
  // TODO: Add unit test for non-KeyValue schemas
  // TODO: Add unit tests for Union schemas
  // TODO: Add integration test

  private static final String FLATTEN_DELIMITER = "_"; // '.' in not valid in AVRO field names
  private final Optional<String> part;

  @Override
  public void process(TransformContext transformContext) throws Exception {
    Schema<?> keySchema = transformContext.getKeySchema();
    if (keySchema == null) {
      return;
    }
    if (keySchema.getSchemaInfo().getType() == SchemaType.AVRO
        && transformContext.getValueSchema().getSchemaInfo().getType() == SchemaType.AVRO) {

      if (!part.isPresent()) {
        validateAvro(transformContext.getKeySchema().getSchemaInfo().getType());
        validateAvro(transformContext.getValueSchema().getSchemaInfo().getType());
        GenericRecord avroKeyRecord = (GenericRecord) transformContext.getKeyObject();
        GenericRecord avroValueRecord = (GenericRecord) transformContext.getValueObject();
        transformContext.setKeyObject(flattenGenericRecord(avroKeyRecord));
        transformContext.setValueObject(flattenGenericRecord(avroValueRecord));
        transformContext.setKeyModified(true);
        transformContext.setValueModified(true);
      } else if (part.isPresent() && "key".equals(part.get())) {
        validateAvro(transformContext.getKeySchema().getSchemaInfo().getType());
        GenericRecord avroKeyRecord = (GenericRecord) transformContext.getKeyObject();
        transformContext.setKeyObject(flattenGenericRecord(avroKeyRecord));
        transformContext.setKeyModified(true);
      } else if (part.isPresent() && "value".equals(part.get())) {
        validateAvro(transformContext.getValueSchema().getSchemaInfo().getType());
        GenericRecord avroValueRecord = (GenericRecord) transformContext.getValueObject();
        transformContext.setValueObject(flattenGenericRecord(avroValueRecord));
        transformContext.setValueModified(true);
      } else {
        throw new IllegalArgumentException("Unsupported part for Flatten: " + part.get());
      }
    }
  }

  void validateAvro(SchemaType schemaType) {
    if (schemaType == SchemaType.AVRO) {
      return;
    }

    throw new IllegalStateException("Unsupported schema type for Flatten: " + schemaType);
  }

  GenericRecord flattenGenericRecord(GenericRecord record) {
    org.apache.avro.Schema modified = buildFlattenedSchema(record);
    GenericRecord newRecord = new GenericData.Record(modified);
    for (org.apache.avro.Schema.Field field : modified.getFields()) {
      newRecord.put(field.name(), field.defaultVal());
    }
    return newRecord;
  }

  org.apache.avro.Schema buildFlattenedSchema(GenericRecord record) {
    org.apache.avro.Schema originalSchema = record.getSchema();
    List<org.apache.avro.Schema.Field> flattenedFields = new ArrayList<>();
    for (org.apache.avro.Schema.Field field : originalSchema.getFields()) {
      flattenedFields.addAll(flattenField(record, field, ""));
    }
    org.apache.avro.Schema flattenedSchema =
        org.apache.avro.Schema.createRecord(
            originalSchema.getName(),
            originalSchema.getDoc(),
            originalSchema.getNamespace(),
            false,
            flattenedFields);

    return flattenedSchema;
  }

  List<org.apache.avro.Schema.Field> flattenField(
      GenericRecord record, org.apache.avro.Schema.Field field, String flattenedFieldName) {
    List<org.apache.avro.Schema.Field> flattenedFields = new ArrayList<>();
    if (field.schema().getType() == org.apache.avro.Schema.Type.RECORD) {
      for (org.apache.avro.Schema.Field nestedField : field.schema().getFields()) {
        flattenedFields.addAll(
            flattenField(
                (GenericRecord) record.get(field.name()),
                nestedField,
                flattenedFieldName + field.name() + FLATTEN_DELIMITER));
      }

      return flattenedFields;
    }

    org.apache.avro.Schema.Field flattenedField =
        new org.apache.avro.Schema.Field(
            flattenedFieldName + field.name(),
            field.schema(),
            field.doc(),
            record.get(field.name()),
            field.order());

    flattenedFields.add(flattenedField);

    return flattenedFields;
  }
}
