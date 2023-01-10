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

import static com.datastax.oss.pulsar.functions.transforms.Utils.assertOptionalField;
import static org.apache.avro.Schema.Type.BOOLEAN;
import static org.apache.avro.Schema.Type.BYTES;
import static org.apache.avro.Schema.Type.DOUBLE;
import static org.apache.avro.Schema.Type.FLOAT;
import static org.apache.avro.Schema.Type.INT;
import static org.apache.avro.Schema.Type.LONG;
import static org.apache.avro.Schema.Type.STRING;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertSame;
import static org.testng.Assert.assertTrue;

import com.datastax.oss.pulsar.functions.transforms.model.ComputeField;
import com.datastax.oss.pulsar.functions.transforms.model.ComputeFieldType;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.apache.avro.AvroRuntimeException;
import org.apache.avro.LogicalTypes;
import org.apache.avro.generic.GenericData;
import org.apache.avro.util.Utf8;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.schema.GenericObject;
import org.apache.pulsar.client.api.schema.GenericRecord;
import org.apache.pulsar.client.api.schema.GenericSchema;
import org.apache.pulsar.client.api.schema.KeyValueSchema;
import org.apache.pulsar.client.api.schema.RecordSchemaBuilder;
import org.apache.pulsar.client.api.schema.SchemaBuilder;
import org.apache.pulsar.client.impl.schema.AutoConsumeSchema;
import org.apache.pulsar.common.schema.KeyValue;
import org.apache.pulsar.common.schema.KeyValueEncodingType;
import org.apache.pulsar.common.schema.SchemaInfo;
import org.apache.pulsar.common.schema.SchemaType;
import org.apache.pulsar.functions.api.Record;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

public class ComputeStepTest {

  private static final org.apache.avro.Schema STRING_SCHEMA = org.apache.avro.Schema.create(STRING);
  private static final org.apache.avro.Schema INT_SCHEMA = org.apache.avro.Schema.create(INT);
  private static final org.apache.avro.Schema LONG_SCHEMA = org.apache.avro.Schema.create(LONG);
  private static final org.apache.avro.Schema FLOAT_SCHEMA = org.apache.avro.Schema.create(FLOAT);
  private static final org.apache.avro.Schema DOUBLE_SCHEMA = org.apache.avro.Schema.create(DOUBLE);
  private static final org.apache.avro.Schema BOOLEAN_SCHEMA =
      org.apache.avro.Schema.create(BOOLEAN);
  private static final org.apache.avro.Schema BYTES_SCHEMA = org.apache.avro.Schema.create(BYTES);

  private static final org.apache.avro.Schema DATE_SCHEMA =
      LogicalTypes.date().addToSchema(org.apache.avro.Schema.create(INT));

  private static final org.apache.avro.Schema TIME_SCHEMA =
      LogicalTypes.timeMillis().addToSchema(org.apache.avro.Schema.create(INT));
  private static final org.apache.avro.Schema TIMESTAMP_SCHEMA =
      LogicalTypes.timestampMillis().addToSchema(org.apache.avro.Schema.create(LONG));

  @Test
  void testAvro() throws Exception {
    RecordSchemaBuilder recordSchemaBuilder = SchemaBuilder.record("record");
    recordSchemaBuilder.field("firstName").type(SchemaType.STRING);
    recordSchemaBuilder.field("lastName").type(SchemaType.STRING);
    recordSchemaBuilder.field("age").type(SchemaType.INT32);

    SchemaInfo schemaInfo = recordSchemaBuilder.build(SchemaType.AVRO);
    GenericSchema<GenericRecord> genericSchema = Schema.generic(schemaInfo);

    GenericRecord genericRecord =
        genericSchema
            .newRecordBuilder()
            .set("firstName", "Jane")
            .set("lastName", "Doe")
            .set("age", 42)
            .build();

    Record<GenericObject> record = new Utils.TestRecord<>(genericSchema, genericRecord, "test-key");

    List<ComputeField> fields = buildComputeFields("value", false, false);
    fields.add(
        ComputeField.builder()
            .scopedName("value.age")
            .expression("value.age + 1")
            .type(ComputeFieldType.STRING)
            .build());
    ComputeStep step = ComputeStep.builder().fields(fields).build();
    Record<?> outputRecord = Utils.process(record, step);
    assertEquals(outputRecord.getKey().orElse(null), "test-key");

    GenericData.Record read =
        Utils.getRecord(outputRecord.getSchema(), (byte[]) outputRecord.getValue());
    assertEquals(read.get("firstName"), new Utf8("Jane"));

    assertTrue(read.hasField("newStringField"));
    assertEquals(read.getSchema().getField("newStringField").schema(), STRING_SCHEMA);
    assertEquals(read.get("newStringField"), new Utf8("Hotaru"));

    assertTrue(read.hasField("newInt32Field"));
    assertEquals(read.getSchema().getField("newInt32Field").schema(), INT_SCHEMA);
    assertEquals(read.get("newInt32Field"), 2147483647);

    assertTrue(read.hasField("newInt64Field"));
    assertEquals(read.getSchema().getField("newInt64Field").schema(), LONG_SCHEMA);
    assertEquals(read.get("newInt64Field"), 9223372036854775807L);

    assertTrue(read.hasField("newFloatField"));
    assertEquals(read.getSchema().getField("newFloatField").schema(), FLOAT_SCHEMA);
    assertEquals(read.get("newFloatField"), 340282346638528859999999999999999999999.999999F);

    assertTrue(read.hasField("newDoubleField"));
    assertEquals(read.getSchema().getField("newDoubleField").schema(), DOUBLE_SCHEMA);
    assertEquals(read.get("newDoubleField"), 1.79769313486231570e+308D);

    assertTrue(read.hasField("newBooleanField"));
    assertEquals(read.getSchema().getField("newBooleanField").schema(), BOOLEAN_SCHEMA);
    assertTrue((Boolean) read.get("newBooleanField"));

    assertTrue(read.hasField("newBytesField"));
    assertEquals(read.getSchema().getField("newBytesField").schema(), BYTES_SCHEMA);
    assertEquals(
        read.get("newBytesField"), ByteBuffer.wrap("Hotaru".getBytes(StandardCharsets.UTF_8)));

    assertTrue(read.hasField("newDateField"));
    assertEquals(read.getSchema().getField("newDateField").schema(), DATE_SCHEMA);
    assertEquals(read.get("newDateField"), 13850); // 13850 days since 1970-01-01

    assertTrue(read.hasField("newTimeField"));
    assertEquals(read.getSchema().getField("newTimeField").schema(), TIME_SCHEMA);
    assertEquals(read.get("newTimeField"), 36930000); // 36930000 ms since 00:00:00

    assertTrue(read.hasField("newDateTimeField"));
    assertEquals(read.getSchema().getField("newDateTimeField").schema(), TIMESTAMP_SCHEMA);
    assertEquals(
        read.get("newDateTimeField"),
        OffsetDateTime.parse("2007-12-03T10:15:30.00Z").toInstant().toEpochMilli());

    assertEquals(read.getSchema().getField("age").schema(), STRING_SCHEMA);
    assertEquals(read.get("age"), new Utf8("43"));
  }

  @Test(expectedExceptions = AvroRuntimeException.class)
  void testAvroNullsNotAllowed() throws Exception {
    RecordSchemaBuilder recordSchemaBuilder = SchemaBuilder.record("record");
    recordSchemaBuilder.field("firstName").type(SchemaType.STRING);

    SchemaInfo schemaInfo = recordSchemaBuilder.build(SchemaType.AVRO);
    GenericSchema<GenericRecord> genericSchema = Schema.generic(schemaInfo);

    GenericRecord genericRecord = genericSchema.newRecordBuilder().set("firstName", "Jane").build();

    Record<GenericObject> record = new Utils.TestRecord<>(genericSchema, genericRecord, "test-key");

    ComputeStep step =
        ComputeStep.builder()
            .fields(
                Arrays.asList(
                    ComputeField.builder()
                        .scopedName("value.newLongField")
                        .expression("null")
                        .optional(false)
                        .type(ComputeFieldType.INT64)
                        .build()))
            .build();
    Utils.process(record, step);
  }

  @Test
  void testAvroWithNonNullOptionalFields() throws Exception {
    RecordSchemaBuilder recordSchemaBuilder = SchemaBuilder.record("record");
    recordSchemaBuilder.field("firstName").type(SchemaType.STRING);

    SchemaInfo schemaInfo = recordSchemaBuilder.build(SchemaType.AVRO);
    GenericSchema<GenericRecord> genericSchema = Schema.generic(schemaInfo);

    GenericRecord genericRecord = genericSchema.newRecordBuilder().set("firstName", "Jane").build();

    Record<GenericObject> record = new Utils.TestRecord<>(genericSchema, genericRecord, "test-key");

    ComputeStep step =
        ComputeStep.builder().fields(buildComputeFields("value", true, false)).build();
    Record<?> outputRecord = Utils.process(record, step);
    assertEquals(outputRecord.getKey().orElse(null), "test-key");

    GenericData.Record read =
        Utils.getRecord(outputRecord.getSchema(), (byte[]) outputRecord.getValue());
    assertEquals(read.get("firstName"), new Utf8("Jane"));
    assertOptionalField(read, "newStringField", STRING, new Utf8("Hotaru"));
    assertOptionalField(read, "newInt32Field", INT, 2147483647);
    assertOptionalField(read, "newInt64Field", LONG, 9223372036854775807L);
    assertOptionalField(
        read, "newFloatField", FLOAT, 340282346638528859999999999999999999999.999999F);
    assertOptionalField(read, "newDoubleField", DOUBLE, 1.79769313486231570e+308D);
    assertOptionalField(read, "newBooleanField", BOOLEAN, true);
    assertOptionalField(
        read, "newBytesField", BYTES, ByteBuffer.wrap("Hotaru".getBytes(StandardCharsets.UTF_8)));
  }

  @Test
  void testAvroWithNullOptionalFields() throws Exception {
    RecordSchemaBuilder recordSchemaBuilder = SchemaBuilder.record("record");
    recordSchemaBuilder.field("firstName").type(SchemaType.STRING);

    SchemaInfo schemaInfo = recordSchemaBuilder.build(SchemaType.AVRO);
    GenericSchema<GenericRecord> genericSchema = Schema.generic(schemaInfo);

    GenericRecord genericRecord = genericSchema.newRecordBuilder().set("firstName", "Jane").build();

    Record<GenericObject> record = new Utils.TestRecord<>(genericSchema, genericRecord, "test-key");

    ComputeStep step =
        ComputeStep.builder().fields(buildComputeFields("value", true, true)).build();
    Record<?> outputRecord = Utils.process(record, step);
    assertEquals(outputRecord.getKey().orElse(null), "test-key");

    GenericData.Record read =
        Utils.getRecord(outputRecord.getSchema(), (byte[]) outputRecord.getValue());
    assertEquals(read.get("firstName"), new Utf8("Jane"));
    assertOptionalFieldNull(read, "newStringField", STRING);
    assertOptionalFieldNull(read, "newInt32Field", INT);
    assertOptionalFieldNull(read, "newInt64Field", LONG);
    assertOptionalFieldNull(read, "newFloatField", FLOAT);
    assertOptionalFieldNull(read, "newDoubleField", DOUBLE);
    assertOptionalFieldNull(read, "newBooleanField", BOOLEAN);
    assertOptionalFieldNull(read, "newBytesField", BYTES);
  }

  @Test
  void testKeyValueAvro() throws Exception {
    ComputeStep step =
        ComputeStep.builder()
            .fields(
                Arrays.asList(
                    ComputeField.builder()
                        .scopedName("value.newValueStringField")
                        .expression("'Hotaru'")
                        .type(ComputeFieldType.STRING)
                        .build(),
                    ComputeField.builder()
                        .scopedName("key.newKeyStringField")
                        .expression("'Hotaru'")
                        .type(ComputeFieldType.STRING)
                        .build()))
            .build();

    Record<?> outputRecord = Utils.process(Utils.createTestAvroKeyValueRecord(), step);
    KeyValueSchema<?, ?> messageSchema = (KeyValueSchema<?, ?>) outputRecord.getSchema();
    KeyValue<?, ?> messageValue = (KeyValue<?, ?>) outputRecord.getValue();

    GenericData.Record keyAvroRecord =
        Utils.getRecord(messageSchema.getKeySchema(), (byte[]) messageValue.getKey());
    assertEquals(keyAvroRecord.getSchema().getFields().size(), 4);
    assertEquals(keyAvroRecord.get("keyField1"), new Utf8("key1"));
    assertEquals(keyAvroRecord.get("keyField2"), new Utf8("key2"));
    assertEquals(keyAvroRecord.get("keyField3"), new Utf8("key3"));

    assertTrue(keyAvroRecord.hasField("newKeyStringField"));
    assertEquals(
        keyAvroRecord.getSchema().getField("newKeyStringField").schema().getType(), STRING);
    assertEquals(keyAvroRecord.get("newKeyStringField"), new Utf8("Hotaru"));

    GenericData.Record valueAvroRecord =
        Utils.getRecord(messageSchema.getValueSchema(), (byte[]) messageValue.getValue());
    assertEquals(valueAvroRecord.getSchema().getFields().size(), 4);
    assertEquals(valueAvroRecord.get("valueField1"), new Utf8("value1"));
    assertEquals(valueAvroRecord.get("valueField2"), new Utf8("value2"));
    assertEquals(valueAvroRecord.get("valueField3"), new Utf8("value3"));

    assertTrue(valueAvroRecord.hasField("newValueStringField"));
    assertEquals(
        valueAvroRecord.getSchema().getField("newValueStringField").schema().getType(), STRING);
    assertEquals(valueAvroRecord.get("newValueStringField"), new Utf8("Hotaru"));

    assertEquals(messageSchema.getKeyValueEncodingType(), KeyValueEncodingType.SEPARATED);
  }

  @Test(dataProvider = "primitiveTypeComputeProvider")
  void testPrimitiveSchemaTypes(
      Object oldValue,
      Schema<?> oldSchema,
      String expression,
      ComputeFieldType newSchema,
      Object expectedValue,
      Schema<?> expectedSchema)
      throws Exception {
    Record<GenericObject> record =
        new Utils.TestRecord<>(
            oldSchema,
            AutoConsumeSchema.wrapPrimitiveObject(
                oldValue, oldSchema.getSchemaInfo().getType(), new byte[] {}),
            "test-key");

    ComputeStep step =
        ComputeStep.builder()
            .fields(
                Collections.singletonList(
                    ComputeField.builder()
                        .scopedName("value")
                        .expression(expression)
                        .type(newSchema)
                        .build()))
            .build();

    Record<GenericObject> outputRecord = Utils.process(record, step);

    assertEquals(outputRecord.getSchema(), expectedSchema);
    assertEquals(outputRecord.getValue(), expectedValue);
  }

  @Test
  void testPrimitivesNotModified() throws Exception {
    Record<GenericObject> record =
        new Utils.TestRecord<>(
            Schema.STRING,
            AutoConsumeSchema.wrapPrimitiveObject("value", SchemaType.STRING, new byte[] {}),
            "test-key");

    ComputeStep step =
        ComputeStep.builder()
            .fields(
                Arrays.asList(
                    ComputeField.builder()
                        .scopedName("value.newField")
                        .expression("newValue")
                        .type(ComputeFieldType.STRING)
                        .build()))
            .build();

    Record<GenericObject> outputRecord = Utils.process(record, step);

    assertSame(outputRecord.getSchema(), record.getSchema());
    assertSame(outputRecord.getValue(), record.getValue().getNativeObject());
  }

  @Test
  void testKeyValuePrimitivesNotModified() throws Exception {
    Schema<KeyValue<String, Integer>> keyValueSchema =
        Schema.KeyValue(Schema.STRING, Schema.INT32, KeyValueEncodingType.SEPARATED);

    KeyValue<String, Integer> keyValue = new KeyValue<>("key", 42);

    Record<GenericObject> record =
        new Utils.TestRecord<>(
            keyValueSchema,
            AutoConsumeSchema.wrapPrimitiveObject(keyValue, SchemaType.KEY_VALUE, new byte[] {}),
            null);

    ComputeStep step =
        ComputeStep.builder()
            .fields(
                Arrays.asList(
                    ComputeField.builder()
                        .scopedName("value.newField")
                        .expression("newValue")
                        .type(ComputeFieldType.STRING)
                        .build(),
                    ComputeField.builder()
                        .scopedName("key.newField")
                        .expression("newValue")
                        .type(ComputeFieldType.STRING)
                        .build()))
            .build();

    Record<?> outputRecord = Utils.process(record, step);
    KeyValueSchema<?, ?> messageSchema = (KeyValueSchema<?, ?>) outputRecord.getSchema();
    KeyValue<?, ?> messageValue = (KeyValue<?, ?>) outputRecord.getValue();

    KeyValueSchema<?, ?> recordSchema = (KeyValueSchema) record.getSchema();
    KeyValue<?, ?> recordValue = ((KeyValue<?, ?>) record.getValue().getNativeObject());
    assertSame(messageSchema.getKeySchema(), recordSchema.getKeySchema());
    assertSame(messageSchema.getValueSchema(), recordSchema.getValueSchema());
    assertSame(messageValue.getKey(), recordValue.getKey());
    assertSame(messageValue.getValue(), recordValue.getValue());
  }

  @Test(dataProvider = "destinationTopicProvider")
  void testAvroComputeDestinationTopic(String topic) throws Exception {
    RecordSchemaBuilder recordSchemaBuilder = SchemaBuilder.record("record");
    recordSchemaBuilder.field("firstName").type(SchemaType.STRING);

    SchemaInfo schemaInfo = recordSchemaBuilder.build(SchemaType.AVRO);
    GenericSchema<GenericRecord> genericSchema = Schema.generic(schemaInfo);

    GenericRecord genericRecord = genericSchema.newRecordBuilder().set("firstName", "Jane").build();

    Record<GenericObject> record =
        Utils.TestRecord.<GenericObject>builder()
            .schema(genericSchema)
            .value(genericRecord)
            .destinationTopic(topic)
            .build();

    List<ComputeField> fields = buildComputeFields("value", false, false);
    fields.add(
        ComputeField.builder()
            .scopedName("destinationTopic")
            .expression("destinationTopic == 'targetTopic' ? 'route' : 'dont-route'")
            .type(ComputeFieldType.STRING)
            .build());
    ComputeStep step = ComputeStep.builder().fields(fields).build();
    Record<?> outputRecord = Utils.process(record, step);
    GenericData.Record read =
        Utils.getRecord(outputRecord.getSchema(), (byte[]) outputRecord.getValue());

    assertEquals(
        outputRecord.getDestinationTopic().get(),
        topic.equals("targetTopic") ? "route" : "dont-route");
    assertEquals(read.get("firstName"), new Utf8("Jane"));
  }

  @Test
  void testAvroComputeMessageKey() throws Exception {
    RecordSchemaBuilder recordSchemaBuilder = SchemaBuilder.record("record");
    recordSchemaBuilder.field("firstName").type(SchemaType.STRING);

    SchemaInfo schemaInfo = recordSchemaBuilder.build(SchemaType.AVRO);
    GenericSchema<GenericRecord> genericSchema = Schema.generic(schemaInfo);

    GenericRecord genericRecord = genericSchema.newRecordBuilder().set("firstName", "Jane").build();

    Record<GenericObject> record =
        Utils.TestRecord.<GenericObject>builder()
            .schema(genericSchema)
            .value(genericRecord)
            .key("old")
            .build();

    List<ComputeField> fields = buildComputeFields("value", false, false);
    fields.add(
        ComputeField.builder()
            .scopedName("messageKey")
            .expression("'new'")
            .type(ComputeFieldType.STRING)
            .build());
    ComputeStep step = ComputeStep.builder().fields(fields).build();
    Record<?> outputRecord = Utils.process(record, step);
    GenericData.Record read =
        Utils.getRecord(outputRecord.getSchema(), (byte[]) outputRecord.getValue());

    assertTrue(outputRecord.getKey().isPresent());
    assertEquals(outputRecord.getKey().get(), "new");
    assertEquals(read.get("firstName"), new Utf8("Jane"));
  }

  @Test
  void testAvroComputeHeaderProperties() throws Exception {
    RecordSchemaBuilder recordSchemaBuilder = SchemaBuilder.record("record");
    recordSchemaBuilder.field("firstName").type(SchemaType.STRING);

    SchemaInfo schemaInfo = recordSchemaBuilder.build(SchemaType.AVRO);
    GenericSchema<GenericRecord> genericSchema = Schema.generic(schemaInfo);

    GenericRecord genericRecord = genericSchema.newRecordBuilder().set("firstName", "Jane").build();

    Record<GenericObject> record =
        Utils.TestRecord.<GenericObject>builder()
            .schema(genericSchema)
            .value(genericRecord)
            .properties(Map.of("existingKey", "v1", "nonExistingKey", "v2"))
            .build();

    List<ComputeField> fields = new ArrayList<>();
    fields.add(
        ComputeField.builder()
            .scopedName("properties.existingKey")
            .expression("'c1'")
            .type(ComputeFieldType.STRING)
            .build());
    fields.add(
        ComputeField.builder()
            .scopedName("properties.newKey")
            .expression("'c2'")
            .type(ComputeFieldType.STRING)
            .build());
    ComputeStep step = ComputeStep.builder().fields(fields).build();
    Record<?> outputRecord = Utils.process(record, step);

    assertEquals(outputRecord.getProperties().size(), 3);
    assertTrue(outputRecord.getProperties().containsKey("existingKey"));
    assertTrue(outputRecord.getProperties().containsKey("nonExistingKey"));
    assertTrue(outputRecord.getProperties().containsKey("newKey"));
    assertEquals(outputRecord.getProperties().get("existingKey"), "c1");
    assertEquals(outputRecord.getProperties().get("nonExistingKey"), "v2");
    assertEquals(outputRecord.getProperties().get("newKey"), "c2");
  }

  @DataProvider(name = "destinationTopicProvider")
  public static Object[] destinationTopicProvider() {
    return new Object[] {"targetTopic", "randomTopic"};
  }

  /**
   * @return {"old value", "old schema", "expression", "new schema", "expected value",
   *     "expectedSchema"}
   */
  @DataProvider(name = "primitiveTypeComputeProvider")
  public static Object[][] primitiveTypeComputeProvider() {
    return new Object[][] {
      {"oldValue", Schema.STRING, "'newValue'", ComputeFieldType.STRING, "newValue", Schema.STRING},
      {
        "oldValue",
        Schema.STRING,
        "fn:concat(value, '!')",
        ComputeFieldType.STRING,
        "oldValue!",
        Schema.STRING
      },
      {"1.3", Schema.STRING, "2.6", ComputeFieldType.DOUBLE, 2.6D, Schema.DOUBLE},
      {"3", Schema.INT32, "4", ComputeFieldType.INT32, 4, Schema.INT32},
      {"3", Schema.INT32, "value + 2", ComputeFieldType.INT32, 5, Schema.INT32},
      {"3", Schema.INT32, "'newValue'", ComputeFieldType.STRING, "newValue", Schema.STRING},
      {"3", Schema.INT64, "4", ComputeFieldType.INT64, 4L, Schema.INT64},
      {"3", Schema.INT64, "value + 2", ComputeFieldType.INT64, 5L, Schema.INT64},
      {"3", Schema.INT64, "'newValue'", ComputeFieldType.STRING, "newValue", Schema.STRING},
      {"3.2", Schema.FLOAT, "3.3", ComputeFieldType.FLOAT, 3.3F, Schema.FLOAT},
      {"3.2", Schema.FLOAT, "value + 1", ComputeFieldType.FLOAT, 4.2F, Schema.FLOAT},
      {"3.2", Schema.FLOAT, "'newValue'", ComputeFieldType.STRING, "newValue", Schema.STRING},
      {"3.2", Schema.DOUBLE, "3.3", ComputeFieldType.DOUBLE, 3.3D, Schema.DOUBLE},
      {"3.2", Schema.DOUBLE, "value + 1", ComputeFieldType.DOUBLE, 4.2D, Schema.DOUBLE},
      {"3.2", Schema.DOUBLE, "'newValue'", ComputeFieldType.STRING, "newValue", Schema.STRING},
      {"false", Schema.BOOL, "true", ComputeFieldType.BOOLEAN, true, Schema.BOOL},
      {"false", Schema.BOOL, "value || true", ComputeFieldType.BOOLEAN, true, Schema.BOOL},
      {"true", Schema.BOOL, "1", ComputeFieldType.DOUBLE, 1D, Schema.DOUBLE},
      {
        "2007-01-02",
        Schema.LOCAL_DATE,
        "'2008-02-07'",
        ComputeFieldType.DATE,
        LocalDate.parse("2008-02-07"),
        Schema.LOCAL_DATE
      },
      {
        "2007-01-02",
        Schema.LOCAL_DATE,
        "'2008-02-07'",
        ComputeFieldType.STRING,
        "2008-02-07",
        Schema.STRING
      },
      {
        "01:02:03",
        Schema.LOCAL_TIME,
        "'03:04:05'",
        ComputeFieldType.TIME,
        LocalTime.parse("03:04:05"),
        Schema.LOCAL_TIME
      },
      {"01:02:03", Schema.LOCAL_TIME, "'03:04:05'", ComputeFieldType.STRING, "03:04:05", Schema.STRING},
      {
        "2007-01-02T01:02:03Z",
        Schema.INSTANT,
        "'2022-01-02T02:03:04Z'",
        ComputeFieldType.DATETIME,
        Instant.parse("2022-01-02T02:03:04Z"),
        Schema.INSTANT
      },
      {
        "2007-01-02T01:02:03Z",
        Schema.INSTANT,
        "fn:dateadd(value, 2, 'years')",
        ComputeFieldType.DATETIME,
        Instant.parse("2009-01-02T01:02:03Z"),
        Schema.INSTANT
      },
      {
        "2007-01-02T01:02:03Z",
        Schema.LOCAL_DATE_TIME,
        "'2010-01-02T01:02:03Z'",
        ComputeFieldType.STRING,
        "2010-01-02T01:02:03Z",
        Schema.STRING
      },
      {
        "'oldValue'",
        Schema.BYTES,
        "'newValue'.bytes",
        ComputeFieldType.BYTES,
        "newValue".getBytes(StandardCharsets.UTF_8),
        Schema.BYTES
      },
      {
        "'oldValue'", Schema.BYTES, "'newValue'", ComputeFieldType.STRING, "newValue", Schema.STRING
      },
    };
  }

  private void assertOptionalFieldNull(
      GenericData.Record record, String fieldName, org.apache.avro.Schema.Type expectedType) {
    assertOptionalField(record, fieldName, expectedType, null);
  }

  private List<ComputeField> buildComputeFields(String scope, boolean optional, boolean nullify) {
    List<ComputeField> fields = new ArrayList<>();
    fields.add(
        ComputeField.builder()
            .scopedName(scope + "." + "newStringField")
            .expression(nullify ? "null" : "'Hotaru'")
            .optional(optional)
            .type(ComputeFieldType.STRING)
            .build());
    fields.add(
        ComputeField.builder()
            .scopedName(scope + "." + "newInt32Field")
            .expression(nullify ? "null" : "2147483647")
            .optional(optional)
            .type(ComputeFieldType.INT32)
            .build());
    fields.add(
        ComputeField.builder()
            .scopedName(scope + "." + "newInt64Field")
            .expression(nullify ? "null" : "9223372036854775807")
            .optional(optional)
            .type(ComputeFieldType.INT64)
            .build());
    fields.add(
        ComputeField.builder()
            .scopedName(scope + "." + "newFloatField")
            .expression(nullify ? "null" : "340282346638528859999999999999999999999.999999")
            .optional(optional)
            .type(ComputeFieldType.FLOAT)
            .build());
    fields.add(
        ComputeField.builder()
            .scopedName(scope + "." + "newDoubleField")
            .expression(nullify ? "null" : "1.79769313486231570e+308")
            .optional(optional)
            .type(ComputeFieldType.DOUBLE)
            .build());
    fields.add(
        ComputeField.builder()
            .scopedName(scope + "." + "newBooleanField")
            .expression(nullify ? "null" : "1 == 1")
            .optional(optional)
            .type(ComputeFieldType.BOOLEAN)
            .build());
    fields.add(
        ComputeField.builder()
            .scopedName(scope + "." + "newDateField")
            .expression(nullify ? "null" : "'2007-12-03'")
            .optional(optional)
            .type(ComputeFieldType.DATE)
            .build());
    fields.add(
        ComputeField.builder()
            .scopedName(scope + "." + "newTimeField")
            .expression(nullify ? "null" : "'10:15:30'")
            .optional(optional)
            .type(ComputeFieldType.TIME)
            .build());
    fields.add(
        ComputeField.builder()
            .scopedName(scope + "." + "newDateTimeField")
            .expression(nullify ? "null" : "'2007-12-03T10:15:30+00:00'")
            .optional(optional)
            .type(ComputeFieldType.DATETIME)
            .build());
    fields.add(
        ComputeField.builder()
            .scopedName(scope + "." + "newBytesField")
            .expression(nullify ? "null" : "'Hotaru'.bytes")
            .optional(optional)
            .type(ComputeFieldType.BYTES)
            .build());

    return fields;
  }
}
