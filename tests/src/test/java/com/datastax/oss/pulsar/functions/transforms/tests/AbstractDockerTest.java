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
/**
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 *
 * <p>http://www.apache.org/licenses/LICENSE-2.0
 *
 * <p>Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datastax.oss.pulsar.functions.transforms.tests;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.fail;
import static org.testng.AssertJUnit.assertNull;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import java.util.Collections;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import lombok.Value;
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

public abstract class AbstractDockerTest {

  private final String image;
  private Network network;
  private PulsarContainer pulsarContainer;
  private PulsarAdmin admin;
  private PulsarClient client;

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

  @AfterClass
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
    KeyValue<String, String> keyValue = (KeyValue<String, String>) value.getNativeObject();
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
    KeyValue<String, String> keyValue = (KeyValue<String, String>) value.getNativeObject();
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
                    + "    {'type': 'compute-fields', 'fields': [{'name': 'value.c', 'expression' :'%s', 'type' : 'STRING'}]}"
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
  public void testKVAvro() throws Exception {
    String keyExpression = "'k'";
    String valueExpression = "'v'";
    String userConfig =
        String.format(
            (""
                    + "{'steps': ["
                    + "    {'type': 'drop-fields', 'fields': ['a','c']},"
                    + "    {'type': 'compute-fields', 'fields': ["
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
    assertEquals(keyValue.getKey().getNativeObject().toString(), "{\"b\": \"b\", \"k\": \"k\"}");
    assertEquals(keyValue.getValue().getSchemaType(), SchemaType.AVRO);
    assertEquals(keyValue.getValue().getNativeObject().toString(), "{\"d\": \"d\", \"v\": \"v\"}");
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
  void testComputeFieldsTopicRouting()
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
            + "   {'type': 'compute-fields', 'fields': [{'name': 'destinationTopic', 'expression': '%s', 'type': 'STRING'}], 'when': '%s'}"
            + "]}");
    userConfig = String.format(userConfig.replace("'", "\""), expression, when);
    deployFunction(userConfig, functionName, inputTopic, outputTopic);
    Producer producer = client.newProducer(Schema.AVRO(Pojo1.class)).topic(inputTopic).create();

    // Register consumers on output and routing topics
    Consumer<GenericRecord> outputTopicConsumer =
        client
            .newConsumer(Schema.AUTO_CONSUME())
            .topic(outputTopic)
            .subscriptionName(UUID.randomUUID().toString())
            .subscribe();

    Consumer<GenericRecord> routingTopicConsumer =
        client
            .newConsumer(Schema.AUTO_CONSUME())
            .topic(routingTopic)
            .subscriptionName(UUID.randomUUID().toString())
            .subscribe();

    // send first message, should go to outputTopic
    producer.newMessage().value(new Pojo1("a", "b")).send();

    // send second message, should go to routingTopic
    producer.newMessage().value(new Pojo1("c", "d")).send();

    Message<GenericRecord> pojo1 = outputTopicConsumer.receive(30, TimeUnit.SECONDS);
    Message<GenericRecord> pojo2 = routingTopicConsumer.receive(30, TimeUnit.SECONDS);

    assertNotNull(pojo1);
    assertNotNull(pojo2);

    assertEquals(pojo1.getValue().getNativeObject().toString(), "{\"a\": \"a\", \"b\": \"b\"}");
    assertEquals(pojo2.getValue().getNativeObject().toString(), "{\"a\": \"c\", \"b\": \"d\"}");
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

    Consumer<GenericRecord> consumer =
        client
            .newConsumer(Schema.AUTO_CONSUME())
            .topic(outputTopic)
            .subscriptionName(UUID.randomUUID().toString())
            .subscribe();

    Producer producer = client.newProducer(schema).topic(inputTopic).create();

    TypedMessageBuilder producerMessage =
        producer.newMessage().value(value).property("prop-key", "prop-value");
    if (key != null) {
      producerMessage.key(key);
    }
    producerMessage.send();

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
