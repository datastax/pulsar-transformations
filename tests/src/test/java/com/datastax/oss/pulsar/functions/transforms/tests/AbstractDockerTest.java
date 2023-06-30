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
package com.datastax.oss.pulsar.functions.transforms.tests;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.fail;
import static org.testng.AssertJUnit.assertNull;

import com.datastax.oss.pulsar.functions.transforms.tests.util.CqlLogicalTypes;
import com.datastax.oss.pulsar.functions.transforms.tests.util.NativeSchemaWrapper;
import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import java.math.BigDecimal;
import java.time.LocalDate;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import lombok.Value;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.Conversions;
import org.apache.avro.LogicalTypes;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericData;
import org.apache.avro.reflect.ReflectData;
import org.apache.commons.lang3.StringUtils;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.TypedMessageBuilder;
import org.apache.pulsar.client.api.schema.GenericObject;
import org.apache.pulsar.client.api.schema.GenericRecord;
import org.apache.pulsar.common.functions.FunctionConfig;
import org.apache.pulsar.common.policies.data.FunctionStatus;
import org.apache.pulsar.common.schema.KeyValue;
import org.apache.pulsar.common.schema.KeyValueEncodingType;
import org.apache.pulsar.common.schema.SchemaType;
import org.testcontainers.containers.Network;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

@Slf4j
public abstract class AbstractDockerTest {
  private static final GenericData[] GD_INSTANCES = {
    ReflectData.AllowNull.get(), ReflectData.get(), GenericData.get()
  };

  static {
    // A workaround to register decimal conversion on the generic data objected associated with the
    // consumer used to
    // read the output topic inspired by https://github.com/apache/pulsar/issues/15899
    for (GenericData gd : GD_INSTANCES) {
      gd.addLogicalTypeConversion(new Conversions.DecimalConversion());
    }
  }

  private final String image;
  private Network network;
  private PulsarContainer pulsarContainer;
  private PulsarAdmin admin;
  private PulsarClient client;
  private static final org.apache.avro.Schema dateType =
      LogicalTypes.date()
          .addToSchema(org.apache.avro.Schema.create(org.apache.avro.Schema.Type.INT));

  AbstractDockerTest(String image) {
    this.image = image;
  }

  @BeforeClass
  public void setup() throws Exception {
    network = Network.newNetwork();
    pulsarContainer = new PulsarContainer(network, image);
    // start Pulsar and wait for it to be ready to accept requests
    pulsarContainer.start();
    admin =
        PulsarAdmin.builder()
            .serviceHttpUrl(
                "http://localhost:" + pulsarContainer.getPulsarContainer().getMappedPort(8080))
            .build();
    client =
        PulsarClient.builder()
            .serviceUrl(
                "pulsar://localhost:" + pulsarContainer.getPulsarContainer().getMappedPort(6650))
            .build();
  }

  @AfterClass(alwaysRun = true)
  public void teardown() {
    if (client != null) {
      client.closeAsync();
    }
    if (admin != null) {
      admin.close();
    }
    if (pulsarContainer != null) {
      pulsarContainer.close();
    }
    if (network != null) {
      network.close();
    }
  }

  @Test
  public void testPrimitive() throws Exception {
    String userConfig =
        ("{'steps': [{'type': 'cast', 'schema-type': 'STRING'}]}").replace("'", "\"");

    GenericRecord value = testTransformFunction(userConfig, Schema.INT32, 42, "test-key");
    assertEquals(value.getSchemaType(), SchemaType.STRING);
    assertEquals(value.getNativeObject(), "42");
  }

  @Test
  public void testKVPrimitiveInline() throws Exception {
    String userConfig =
        ("{'steps': [{'type': 'cast', 'schema-type': 'STRING'}]}").replace("'", "\"");

    GenericRecord value =
        testTransformFunction(
            userConfig,
            Schema.KeyValue(Schema.STRING, Schema.STRING, KeyValueEncodingType.INLINE),
            new KeyValue<>("a", "b"),
            "test-key");
    assertEquals(value.getSchemaType(), SchemaType.KEY_VALUE);
    KeyValue<?, ?> keyValue = (KeyValue<?, ?>) value.getNativeObject();
    assertEquals(keyValue.getKey(), "a");
    assertEquals(keyValue.getValue(), "b");
  }

  @Test
  public void testKVPrimitiveSeparated() throws Exception {
    String userConfig =
        ("{'steps': [{'type': 'cast', 'schema-type': 'STRING'}]}").replace("'", "\"");

    GenericRecord value =
        testTransformFunction(
            userConfig,
            Schema.KeyValue(Schema.STRING, Schema.STRING, KeyValueEncodingType.SEPARATED),
            new KeyValue<>("a", "b"));
    assertEquals(value.getSchemaType(), SchemaType.KEY_VALUE);
    KeyValue<?, ?> keyValue = (KeyValue<?, ?>) value.getNativeObject();
    assertEquals(keyValue.getKey(), "a");
    assertEquals(keyValue.getValue(), "b");
  }

  @Test
  public void testAvro() throws Exception {
    String expression = "'c'";
    String userConfig =
        String.format(
            (""
                    + "{'steps': ["
                    + "    {'type': 'unwrap-key-value'},"
                    + "    {'type': 'drop-fields', 'fields': ['a']},"
                    + "    {'type': 'compute', 'fields': [{'name': 'value.c', 'expression' :'%s', 'type' : 'STRING'}]}"
                    + "]}")
                .replace("'", "\""),
            expression);

    GenericRecord value =
        testTransformFunction(userConfig, Schema.AVRO(Pojo1.class), new Pojo1("a", "b"));
    assertEquals(value.getSchemaType(), SchemaType.AVRO);
    org.apache.avro.generic.GenericRecord genericRecord =
        (org.apache.avro.generic.GenericRecord) value.getNativeObject();
    assertEquals(genericRecord.toString(), "{\"b\": \"b\", \"c\": \"c\"}");
  }

  @Test
  public void testJson() throws Exception {
    String expression = "'c'";
    String userConfig =
        String.format(
            (""
                    + "{'steps': ["
                    + "    {'type': 'compute', 'fields': [{'name': 'value.c', 'expression' :'%s', 'type' : 'STRING'}]}"
                    + "]}")
                .replace("'", "\""),
            expression);

    GenericRecord value =
        testTransformFunction(userConfig, Schema.JSON(Pojo1.class), new Pojo1("a", "b"));
    assertEquals(value.getSchemaType(), SchemaType.JSON);
    assertEquals(value.getNativeObject().toString(), "{\"a\":\"a\",\"b\":\"b\",\"c\":\"c\"}");
  }

  @Test
  public void testKVAvro() throws Exception {
    String keyExpression = "key.b";
    String valueExpression = "value.d";
    String userConfig =
        String.format(
            (""
                    + "{'steps': ["
                    + "    {'type': 'drop-fields', 'fields': ['a','c']},"
                    + "    {'type': 'compute', 'fields': ["
                    + "        {'name': 'key.k', 'expression' :'%s', 'type' : 'STRING'},"
                    + "        {'name': 'value.v', 'expression' :'%s', 'type' : 'STRING'}]}"
                    + "]}")
                .replace("'", "\""),
            keyExpression,
            valueExpression);

    GenericRecord value =
        testTransformFunction(
            userConfig,
            Schema.KeyValue(Schema.AVRO(Pojo1.class), Schema.AVRO(Pojo2.class)),
            new KeyValue<>(new Pojo1("a", "b"), new Pojo2("c", "d")));
    assertEquals(value.getSchemaType(), SchemaType.KEY_VALUE);
    KeyValue<GenericObject, GenericObject> keyValue =
        (KeyValue<GenericObject, GenericObject>) value.getNativeObject();

    assertEquals(keyValue.getKey().getSchemaType(), SchemaType.AVRO);
    assertEquals(keyValue.getKey().getNativeObject().toString(), "{\"b\": \"b\", \"k\": \"b\"}");
    assertEquals(keyValue.getValue().getSchemaType(), SchemaType.AVRO);
    assertEquals(keyValue.getValue().getNativeObject().toString(), "{\"d\": \"d\", \"v\": \"d\"}");
  }

  @Test
  public void testKVJson() throws Exception {
    String keyExpression = "key.a";
    String valueExpression = "value.c";
    String userConfig =
        String.format(
            (""
                    + "{'steps': ["
                    + "    {'type': 'compute', 'fields': ["
                    + "        {'name': 'key.k', 'expression' :'%s', 'type' : 'STRING'},"
                    + "        {'name': 'value.v', 'expression' :'%s', 'type' : 'STRING'}]}"
                    + "]}")
                .replace("'", "\""),
            keyExpression,
            valueExpression);

    GenericRecord value =
        testTransformFunction(
            userConfig,
            Schema.KeyValue(Schema.JSON(Pojo1.class), Schema.JSON(Pojo2.class)),
            new KeyValue<>(new Pojo1("a", "b"), new Pojo2("c", "d")));
    assertEquals(value.getSchemaType(), SchemaType.KEY_VALUE);
    KeyValue<GenericObject, GenericObject> keyValue =
        (KeyValue<GenericObject, GenericObject>) value.getNativeObject();

    assertEquals(keyValue.getKey().getSchemaType(), SchemaType.JSON);
    assertEquals(
        keyValue.getKey().getNativeObject().toString(), "{\"a\":\"a\",\"b\":\"b\",\"k\":\"a\"}");
    assertEquals(keyValue.getValue().getSchemaType(), SchemaType.JSON);
    assertEquals(
        keyValue.getValue().getNativeObject().toString(), "{\"c\":\"c\",\"d\":\"d\",\"v\":\"c\"}");
  }

  @Test
  public void testAvroNotModified() throws Exception {
    String userConfig = ("{'steps': []}").replace("'", "\"");

    GenericRecord value =
        testTransformFunction(userConfig, Schema.AVRO(Pojo1.class), new Pojo1("a", "b"));
    assertEquals(value.getSchemaType(), SchemaType.AVRO);
    org.apache.avro.generic.GenericRecord genericRecord =
        (org.apache.avro.generic.GenericRecord) value.getNativeObject();
    assertEquals(genericRecord.toString(), "{\"a\": \"a\", \"b\": \"b\"}");
  }

  @Test
  public void testKVAvroNotModified() throws Exception {
    String userConfig = ("{'steps': []}").replace("'", "\"");

    GenericRecord value =
        testTransformFunction(
            userConfig,
            Schema.KeyValue(Schema.AVRO(Pojo1.class), Schema.AVRO(Pojo2.class)),
            new KeyValue<>(new Pojo1("a", "b"), new Pojo2("c", "d")));
    assertEquals(value.getSchemaType(), SchemaType.KEY_VALUE);
    KeyValue<GenericObject, GenericObject> keyValue =
        (KeyValue<GenericObject, GenericObject>) value.getNativeObject();

    assertEquals(keyValue.getKey().getSchemaType(), SchemaType.AVRO);
    assertEquals(keyValue.getKey().getNativeObject().toString(), "{\"a\": \"a\", \"b\": \"b\"}");
    assertEquals(keyValue.getValue().getSchemaType(), SchemaType.AVRO);
    assertEquals(keyValue.getValue().getNativeObject().toString(), "{\"c\": \"c\", \"d\": \"d\"}");
  }

  @Test
  public void testKVAvroWhenPredicate() throws Exception {
    String userConfig =
        (""
            + "{\"steps\": ["
            + "    {\"type\": \"drop-fields\", \"fields\": [\"a\"], \"when\": \"key.a=='a'\"},"
            + "    {\"type\": \"drop-fields\", \"fields\": [\"b\"], \"when\": \"key.b!='b'\"},"
            + "    {\"type\": \"drop-fields\", \"fields\": [\"c\"], \"when\": \"value.c=='c'\"},"
            + "    {\"type\": \"drop-fields\", \"fields\": [\"d\"], \"when\": \"value.d!='d'\"}"
            + "]}");
    GenericRecord value =
        testTransformFunction(
            userConfig,
            Schema.KeyValue(Schema.AVRO(Pojo1.class), Schema.AVRO(Pojo2.class)),
            new KeyValue<>(new Pojo1("a", "b"), new Pojo2("c", "d")));

    assertEquals(value.getSchemaType(), SchemaType.KEY_VALUE);
    KeyValue<GenericObject, GenericObject> keyValue =
        (KeyValue<GenericObject, GenericObject>) value.getNativeObject();

    assertEquals(keyValue.getKey().getSchemaType(), SchemaType.AVRO);
    assertEquals(keyValue.getKey().getNativeObject().toString(), "{\"b\": \"b\"}");
    assertEquals(keyValue.getValue().getSchemaType(), SchemaType.AVRO);
    assertEquals(keyValue.getValue().getNativeObject().toString(), "{\"d\": \"d\"}");
  }

  @Test
  void testDropMessageOnPredicateMatch() throws Exception {
    String userConfig = "{\"steps\": [{\"type\": \"drop\", \"when\": \"value.a=='a'\"}]}";
    GenericRecord value =
        testTransformFunction(
            userConfig, Schema.AVRO(Pojo1.class), new Pojo1("a", "b"), null, true);
    assertNull(value);
  }

  @Test
  void testDropMessageOnPredicateMissMatch() throws Exception {
    String userConfig = "{\"steps\": [{\"type\": \"drop\", \"when\": \"value.a=='a'\"}]}";
    GenericRecord value =
        testTransformFunction(userConfig, Schema.AVRO(Pojo1.class), new Pojo1("c", "d"));
    assertEquals(value.getSchemaType(), SchemaType.AVRO);
    org.apache.avro.generic.GenericRecord genericRecord =
        (org.apache.avro.generic.GenericRecord) value.getNativeObject();
    assertEquals(genericRecord.toString(), "{\"a\": \"c\", \"b\": \"d\"}");
  }

  @Test
  void testComputeTopicRouting()
      throws PulsarClientException, PulsarAdminException, InterruptedException {
    String functionId = UUID.randomUUID().toString();
    String inputTopic = "input-" + functionId;
    String outputTopic = "output-" + functionId;
    String routingTopic = "routing-" + functionId;
    String functionName = "function-" + functionId;

    String expression = String.format("'%s'", routingTopic);
    String when = "value.a ne 'a'";
    String userConfig =
        (""
            + "{'steps': ["
            + "   {'type': 'compute', 'fields': [{'name': 'destinationTopic', 'expression': '%s', 'type': 'STRING'}], 'when': '%s'}"
            + "]}");
    userConfig = String.format(userConfig.replace("'", "\""), expression, when);
    deployFunction(userConfig, functionName, inputTopic, outputTopic);
    try (Producer<Pojo1> producer =
        client.newProducer(Schema.AVRO(Pojo1.class)).topic(inputTopic).create()) {

      // Register consumers on output and routing topics
      try (Consumer<GenericRecord> outputTopicConsumer =
          client
              .newConsumer(Schema.AUTO_CONSUME())
              .topic(outputTopic)
              .subscriptionName(UUID.randomUUID().toString())
              .subscribe()) {

        try (Consumer<GenericRecord> routingTopicConsumer =
            client
                .newConsumer(Schema.AUTO_CONSUME())
                .topic(routingTopic)
                .subscriptionName(UUID.randomUUID().toString())
                .subscribe()) {

          // send first message, should go to outputTopic
          producer.newMessage().value(new Pojo1("a", "b")).send();

          // send second message, should go to routingTopic
          producer.newMessage().value(new Pojo1("c", "d")).send();

          Message<GenericRecord> pojo1 = outputTopicConsumer.receive(30, TimeUnit.SECONDS);
          Message<GenericRecord> pojo2 = routingTopicConsumer.receive(30, TimeUnit.SECONDS);

          assertNotNull(pojo1);
          assertNotNull(pojo2);

          assertEquals(
              pojo1.getValue().getNativeObject().toString(), "{\"a\": \"a\", \"b\": \"b\"}");
          assertEquals(
              pojo2.getValue().getNativeObject().toString(), "{\"a\": \"c\", \"b\": \"d\"}");
        }
      }
    }
  }

  @Test
  public void testComputePrimitive() throws Exception {
    String userConfig =
        ("{\"steps\": [{\"type\": \"compute\", \"fields\": [{\"name\": \"value\", \"expression\": \"fn:concat(value, '!')\", \"type\": \"STRING\"}]}]}");

    GenericRecord value = testTransformFunction(userConfig, Schema.STRING, "hello", "test-key");
    assertEquals(value.getSchemaType(), SchemaType.STRING);
    assertEquals(value.getNativeObject(), "hello!");
  }

  @Test
  public void testComputeUtf8ToInteger() throws Exception {
    String userConfig =
        ("{\"steps\": [{\"type\": \"compute\", \"fields\": [{\"name\": \"value.b\", \"expression\": \"value.a\", \"type\": \"INT32\"}]}]}");

    GenericRecord value =
        testTransformFunction(userConfig, Schema.AVRO(Pojo1.class), new Pojo1("13360", "13360"));

    assertEquals(value.getSchemaType(), SchemaType.AVRO);
    org.apache.avro.generic.GenericRecord genericRecord =
        (org.apache.avro.generic.GenericRecord) value.getNativeObject();
    assertEquals(genericRecord.toString(), "{\"a\": \"13360\", \"b\": 13360}");
  }

  @Test
  public void testComputeAvroDateToString() throws Exception {
    String userConfig =
        ("{\"steps\": [{\"type\": \"compute\", \"fields\": ["
            + "{\"name\": \"value.dateField\", \"expression\": \"value.dateField\", \"type\": \"STRING\"},"
            + "{\"name\": \"value.optionalDateField\", \"expression\": \"value.optionalDateField\", \"type\": \"STRING\"}"
            + "]}]}");

    List<org.apache.avro.Schema.Field> fields =
        List.of(createDateField("dateField", false), createDateField("optionalDateField", true));
    org.apache.avro.Schema avroSchema =
        org.apache.avro.Schema.createRecord("avro_date", "", "ns", false, fields);
    org.apache.avro.generic.GenericRecord record = new GenericData.Record(avroSchema);

    LocalDate date = LocalDate.parse("2023-04-01");
    LocalDate optionalDate = LocalDate.parse("2023-04-02");
    record.put("dateField", (int) date.toEpochDay());
    record.put("optionalDateField", (int) optionalDate.toEpochDay());

    Schema pulsarSchema = new NativeSchemaWrapper(avroSchema, SchemaType.AVRO);
    GenericRecord value = testTransformFunction(userConfig, pulsarSchema, record);

    assertEquals(value.getSchemaType(), SchemaType.AVRO);
    org.apache.avro.generic.GenericRecord genericRecord =
        (org.apache.avro.generic.GenericRecord) value.getNativeObject();
    assertEquals(
        genericRecord.toString(),
        "{\"dateField\": \"2023-04-01\", \"optionalDateField\": \"2023-04-02\"}");
  }

  @Test
  public void testComputeCqlDecimalToAvroDecimal() throws Exception {
    String userConfig =
        ("{\"steps\": [{\"type\": \"compute\", \"fields\": ["
            + "{\"name\": \"key.cqlDecimal\", \"expression\":\"fn:decimalFromUnscaled(key.cqlDecimal.bigint, key.cqlDecimal.scale)\", \"type\":\"DECIMAL\"},"
            + "{\"name\": \"value.cqlDecimalOptional\", \"expression\": \"fn:decimalFromUnscaled(value.cqlDecimalOptional.bigint, value.cqlDecimalOptional.scale)\"}," // Inferred
            + "{\"name\": \"key.cqlDecimalAsString\", \"expression\":\"fn:decimalFromUnscaled(key.cqlDecimal.bigint, key.cqlDecimal.scale)\", \"type\":\"STRING\"},"
            + "{\"name\": \"value.cqlDecimalOptionalAsString\", \"expression\": \"fn:str(fn:decimalFromUnscaled(value.cqlDecimalOptional.bigint, value.cqlDecimalOptional.scale))\"}" // Inferred
            + "]}]}");

    org.apache.avro.Schema avroKeySchema =
        org.apache.avro.Schema.createRecord(
            "key",
            "",
            "ns",
            false,
            List.of(CqlLogicalTypes.createDecimalField("cqlDecimal", false)));
    org.apache.avro.Schema avroValueSchema =
        org.apache.avro.Schema.createRecord(
            "value",
            "",
            "ns",
            false,
            List.of(CqlLogicalTypes.createDecimalField("cqlDecimalOptional", true)));
    org.apache.avro.generic.GenericRecord keyRecord = new GenericData.Record(avroKeySchema);
    org.apache.avro.generic.GenericRecord valueRecord = new GenericData.Record(avroValueSchema);

    BigDecimal decimal = new BigDecimal("123456789012345678901234567890.123456789");
    BigDecimal optionalDecimal = new BigDecimal("567890123456789012345678901234.456789");
    keyRecord.put("cqlDecimal", CqlLogicalTypes.createDecimalRecord(decimal));
    valueRecord.put("cqlDecimalOptional", CqlLogicalTypes.createDecimalRecord(optionalDecimal));

    Schema keySchema = new NativeSchemaWrapper(avroKeySchema, SchemaType.AVRO);
    Schema valueSchema = new NativeSchemaWrapper(avroValueSchema, SchemaType.AVRO);
    GenericRecord value =
        testTransformFunction(
            userConfig,
            Schema.KeyValue(keySchema, valueSchema),
            new KeyValue<>(keyRecord, valueRecord));

    assertEquals(value.getSchemaType(), SchemaType.KEY_VALUE);
    KeyValue<GenericRecord, GenericRecord> keyValue =
        (KeyValue<GenericRecord, GenericRecord>) value.getNativeObject();
    assertEquals(keyValue.getKey().getField("cqlDecimal"), decimal);
    assertEquals(keyValue.getKey().getField("cqlDecimalAsString"), decimal.toString());
    assertEquals(keyValue.getValue().getField("cqlDecimalOptional"), optionalDecimal);
    assertEquals(
        keyValue.getValue().getField("cqlDecimalOptionalAsString"), optionalDecimal.toString());
  }

  private org.apache.avro.Schema.Field createDateField(String name, boolean optional) {
    org.apache.avro.Schema.Field dateField = new org.apache.avro.Schema.Field(name, dateType);
    if (optional) {
      dateField =
          new org.apache.avro.Schema.Field(
              name,
              SchemaBuilder.unionOf().nullType().and().type(dateField.schema()).endUnion(),
              null,
              org.apache.avro.Schema.Field.NULL_DEFAULT_VALUE);
    }
    return dateField;
  }

  private <T> GenericRecord testTransformFunction(String userConfig, Schema<T> schema, T value)
      throws PulsarAdminException, InterruptedException, PulsarClientException {
    return testTransformFunction(userConfig, schema, value, null);
  }

  private <T> GenericRecord testTransformFunction(
      String userConfig, Schema<T> schema, T value, String key)
      throws PulsarAdminException, InterruptedException, PulsarClientException {
    return testTransformFunction(userConfig, schema, value, key, false);
  }

  private <T> GenericRecord testTransformFunction(
      String userConfig, Schema<T> schema, T value, String key, boolean allowNullMessage)
      throws PulsarAdminException, InterruptedException, PulsarClientException {
    String functionId = UUID.randomUUID().toString();
    String inputTopic = "input-" + functionId;
    String outputTopic = "output-" + functionId;
    String functionName = "function-" + functionId;
    deployFunction(userConfig, functionName, inputTopic, outputTopic);

    try (Consumer<GenericRecord> consumer =
        client
            .newConsumer(Schema.AUTO_CONSUME())
            .topic(outputTopic)
            .subscriptionName(UUID.randomUUID().toString())
            .subscribe()) {

      try (Producer<T> producer = client.newProducer(schema).topic(inputTopic).create()) {
        TypedMessageBuilder<T> producerMessage =
            producer.newMessage().value(value).property("prop-key", "prop-value");
        if (key != null) {
          producerMessage.key(key);
        }
        producerMessage.send();
      }
      Message<GenericRecord> message = consumer.receive(30, TimeUnit.SECONDS);

      if (allowNullMessage && message == null) {
        return null;
      }
      assertNotNull(message);
      if (key != null) {
        assertEquals(message.getKey(), key);
      }
      assertEquals(message.getProperty("prop-key"), "prop-value");

      GenericRecord messageValue = message.getValue();
      assertNotNull(messageValue);
      return messageValue;
    }
  }

  private void deployFunction(
      String userConfig, String functionName, String inputTopic, String outputTopic)
      throws PulsarAdminException, InterruptedException {
    admin
        .topics()
        .createSubscription(
            inputTopic, String.format("public/default/%s", functionName), MessageId.latest);

    FunctionConfig functionConfig =
        FunctionConfig.builder()
            .tenant("public")
            .namespace("default")
            .name(functionName)
            .inputs(Collections.singletonList(inputTopic))
            .output(outputTopic)
            .jar("builtin://transforms")
            .runtime(FunctionConfig.Runtime.JAVA)
            .userConfig(
                new Gson().fromJson(userConfig, new TypeToken<Map<String, Object>>() {}.getType()))
            .build();

    admin.functions().createFunction(functionConfig, null);

    FunctionStatus functionStatus = null;
    for (int i = 0; i < 300; i++) {
      functionStatus = admin.functions().getFunctionStatus("public", "default", functionName);
      if (functionStatus.getNumRunning() == 1) {
        break;
      }
      log.info("Function status: {}", functionStatus);
      functionStatus
          .getInstances()
          .forEach(
              f -> {
                log.info("Function instance status: {}", f);
                if (!StringUtils.isEmpty(f.getStatus().getError())) {
                  log.error("Function errored out " + f);
                }
              });
      Thread.sleep(100);
    }

    if (functionStatus.getNumRunning() != 1) {
      fail("Function didn't start in time");
    }
  }

  @Value
  private static class Pojo1 {
    String a;
    String b;
  }

  @Value
  private static class Pojo2 {
    String c;
    String d;
  }
}
