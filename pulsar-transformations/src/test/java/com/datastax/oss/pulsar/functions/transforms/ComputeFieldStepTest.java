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

import static com.datastax.oss.pulsar.functions.transforms.ComputeFieldStep.DURATION_TYPE;
import static com.datastax.oss.pulsar.functions.transforms.Utils.assertOptionalField;
import static org.apache.avro.Schema.Type.BOOLEAN;
import static org.apache.avro.Schema.Type.DOUBLE;
import static org.apache.avro.Schema.Type.FIXED;
import static org.apache.avro.Schema.Type.FLOAT;
import static org.apache.avro.Schema.Type.INT;
import static org.apache.avro.Schema.Type.LONG;
import static org.apache.avro.Schema.Type.STRING;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertSame;
import static org.testng.Assert.assertTrue;

import com.datastax.oss.pulsar.functions.transforms.model.ComputeField;
import com.datastax.oss.pulsar.functions.transforms.model.ComputeFieldType;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
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

public class ComputeFieldStepTest {

  private static final org.apache.avro.Schema STRING_SCHEMA = org.apache.avro.Schema.create(STRING);
  private static final org.apache.avro.Schema INT_SCHEMA = org.apache.avro.Schema.create(INT);
  private static final org.apache.avro.Schema LONG_SCHEMA = org.apache.avro.Schema.create(LONG);
  private static final org.apache.avro.Schema FLOAT_SCHEMA = org.apache.avro.Schema.create(FLOAT);
  private static final org.apache.avro.Schema DOUBLE_SCHEMA = org.apache.avro.Schema.create(DOUBLE);
  private static final org.apache.avro.Schema BOOLEAN_SCHEMA =
      org.apache.avro.Schema.create(BOOLEAN);

  private static final org.apache.avro.Schema DATE_SCHEMA =
      LogicalTypes.date().addToSchema(org.apache.avro.Schema.create(INT));

  private static final org.apache.avro.Schema TIME_SCHEMA =
      LogicalTypes.timeMillis().addToSchema(org.apache.avro.Schema.create(INT));
  private static final org.apache.avro.Schema TIMESTAMP_SCHEMA =
      LogicalTypes.timestampMillis().addToSchema(org.apache.avro.Schema.create(LONG));
  private static final org.apache.avro.Schema DURATION_SCHEMA =
      DURATION_TYPE.addToSchema(org.apache.avro.Schema.createFixed("duration", "","", 3));

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
    ComputeFieldStep step = ComputeFieldStep.builder().fields(fields).build();
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

    assertTrue(read.hasField("newDateField"));
    assertEquals(read.getSchema().getField("newDateField").schema(), DATE_SCHEMA);
    assertEquals(read.get("newDateField"), 13850); // 13850 days since 1970-01-01

    assertTrue(read.hasField("newTimeField"));
    assertEquals(read.getSchema().getField("newTimeField").schema(), TIME_SCHEMA);
    assertEquals(read.get("newTimeField"), 36930000); // 36930000 ms since 00:00:00

    assertTrue(read.hasField("newDateTimeField"));
    assertEquals(read.getSchema().getField("newDateTimeField").schema(), TIMESTAMP_SCHEMA);
    assertEquals(
        read.get("newDateTimeField"), Instant.parse("2007-12-03T10:15:30.00Z").toEpochMilli());

    assertTrue(read.hasField("newDurationField"));
    assertEquals(read.getSchema().getField("newDurationField").schema(), DURATION_SCHEMA);
    assertEquals(read.get("newDateTimeField"), Duration.parse("P1Y2M3W4DT5H6M7.987654321S"));

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

    ComputeFieldStep step =
        ComputeFieldStep.builder()
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

    ComputeFieldStep step =
        ComputeFieldStep.builder().fields(buildComputeFields("value", true, false)).build();
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
  }

  @Test
  void testAvroWithNullOptionalFields() throws Exception {
    RecordSchemaBuilder recordSchemaBuilder = SchemaBuilder.record("record");
    recordSchemaBuilder.field("firstName").type(SchemaType.STRING);

    SchemaInfo schemaInfo = recordSchemaBuilder.build(SchemaType.AVRO);
    GenericSchema<GenericRecord> genericSchema = Schema.generic(schemaInfo);

    GenericRecord genericRecord = genericSchema.newRecordBuilder().set("firstName", "Jane").build();

    Record<GenericObject> record = new Utils.TestRecord<>(genericSchema, genericRecord, "test-key");

    ComputeFieldStep step =
        ComputeFieldStep.builder().fields(buildComputeFields("value", true, true)).build();
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
  }

  @Test
  void testKeyValueAvro() throws Exception {
    ComputeFieldStep step =
        ComputeFieldStep.builder()
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

  @Test
  void testPrimitivesNotModified() throws Exception {
    Record<GenericObject> record =
        new Utils.TestRecord<>(
            Schema.STRING,
            AutoConsumeSchema.wrapPrimitiveObject("value", SchemaType.STRING, new byte[] {}),
            "test-key");

    ComputeFieldStep step =
        ComputeFieldStep.builder()
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

    ComputeFieldStep step =
        ComputeFieldStep.builder()
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
  void testAvroDestinationTopicChange(String topic) throws Exception {
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
    ComputeFieldStep step = ComputeFieldStep.builder().fields(fields).build();
    Record<?> outputRecord = Utils.process(record, step);
    GenericData.Record read =
        Utils.getRecord(outputRecord.getSchema(), (byte[]) outputRecord.getValue());

    assertEquals(
        outputRecord.getDestinationTopic().get(),
        topic.equals("targetTopic") ? "route" : "dont-route");
    assertEquals(read.get("firstName"), new Utf8("Jane"));
  }

  @DataProvider(name = "destinationTopicProvider")
  public static Object[] destinationTopicProvider() {
    return new Object[] {"targetTopic", "randomTopic"};
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
            .expression(nullify ? "null" : "'2007-12-03T10:15:30'")
            .optional(optional)
            .type(ComputeFieldType.DATETIME)
            .build());
    fields.add(
        ComputeField.builder()
            .scopedName(scope + "." + "newDurationField")
            .expression(nullify ? "null" : "'P2DT3H4M20.345S'")
            .optional(optional)
            .type(ComputeFieldType.DURATION)
            .build());

    return fields;
  }
}
