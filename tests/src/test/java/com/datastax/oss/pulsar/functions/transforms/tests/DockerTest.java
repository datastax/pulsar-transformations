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

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import java.util.Collections;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.schema.GenericRecord;
import org.apache.pulsar.common.functions.FunctionConfig;
import org.apache.pulsar.common.policies.data.FunctionStatus;
import org.apache.pulsar.common.schema.KeyValue;
import org.apache.pulsar.common.schema.SchemaType;
import org.testcontainers.containers.Network;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

public class DockerTest {

  private static final String IMAGE_LUNASTREAMING210 = "datastax/lunastreaming:2.10_1.5";
  private static final String IMAGE_PULSAR211 = "apachepulsar/pulsar:2.11.0";

  @DataProvider(name = "images")
  public static Object[][] validConfigs() {
    return new Object[][] {
      {IMAGE_LUNASTREAMING210},
    };
  }

  @Test(dataProvider = "images")
  public void test(String image) throws Exception {
    // create a docker network
    try (Network network = Network.newNetwork();
        PulsarContainer pulsarContainer = new PulsarContainer(network, image)) {
      // start Pulsar and wait for it to be ready to accept requests
      pulsarContainer.start();

      String userConfig =
          (""
                  + "{'steps': ["
                  + "    {'type': 'drop-fields', 'fields': 'a'},"
                  + "    {'type': 'merge-key-value'},"
                  + "    {'type': 'unwrap-key-value'},"
                  + "    {'type': 'cast', 'schema-type': 'STRING'}"
                  + "]}")
              .replace("'", "\"");

      PulsarAdmin admin =
          PulsarAdmin.builder()
              .serviceHttpUrl(
                  "http://localhost:" + pulsarContainer.getPulsarContainer().getMappedPort(8080))
              .build();

      String inputTopic = UUID.randomUUID().toString();
      String outputTopic = UUID.randomUUID().toString();
      String functionName = UUID.randomUUID().toString();

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
                  new Gson()
                      .fromJson(userConfig, new TypeToken<Map<String, Object>>() {}.getType()))
              .build();

      admin.functions().createFunction(functionConfig, null);

      FunctionStatus functionStatus = null;
      for (int i = 0; i < 100; i++) {
        functionStatus = admin.functions().getFunctionStatus("public", "default", functionName);
        if (functionStatus.getNumRunning() == 1) {
          break;
        }
        Thread.sleep(100);
      }

      if (functionStatus.getNumRunning() != 1) {
        fail("Function didn't start in time");
      }

      PulsarClient client =
          PulsarClient.builder()
              .serviceUrl(
                  "pulsar://localhost:" + pulsarContainer.getPulsarContainer().getMappedPort(6650))
              .build();

      Consumer<GenericRecord> consumer =
          client
              .newConsumer(Schema.AUTO_CONSUME())
              .topic(outputTopic)
              .subscriptionName(UUID.randomUUID().toString())
              .subscribe();

      Schema<KeyValue<Pojo1, Pojo2>> keyValueSchema =
          Schema.KeyValue(Schema.AVRO(Pojo1.class), Schema.AVRO(Pojo2.class));
      Producer<KeyValue<Pojo1, Pojo2>> producer =
          client.newProducer(keyValueSchema).topic(inputTopic).create();

      KeyValue<Pojo1, Pojo2> kv = new KeyValue<>(new Pojo1("a", "b"), new Pojo2("c", "d"));
      producer.newMessage().value(kv).send();

      Message<GenericRecord> message = consumer.receive(5, TimeUnit.SECONDS);
      GenericRecord value = message.getValue();
      assertNotNull(value);
      assertEquals(value.getSchemaType(), SchemaType.STRING);
      assertEquals(value.getNativeObject(), "{\"b\": \"b\", \"c\": \"c\", \"d\": \"d\"}");
    }
  }

  private static class Pojo1 {
    private final String a;
    private final String b;

    private Pojo1(String a, String b) {
      this.a = a;
      this.b = b;
    }
  }

  private static class Pojo2 {
    private final String c;
    private final String d;

    private Pojo2(String c, String d) {
      this.c = c;
      this.d = d;
    }
  }
}
