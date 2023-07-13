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

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.RequiredArgsConstructor;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.api.ConsumerBuilder;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.TypedMessageBuilder;
import org.apache.pulsar.client.api.schema.GenericObject;
import org.apache.pulsar.client.api.schema.GenericRecord;
import org.apache.pulsar.client.api.schema.GenericSchema;
import org.apache.pulsar.client.api.schema.RecordSchemaBuilder;
import org.apache.pulsar.common.schema.KeyValue;
import org.apache.pulsar.common.schema.KeyValueEncodingType;
import org.apache.pulsar.common.schema.SchemaType;
import org.apache.pulsar.functions.api.Context;
import org.apache.pulsar.functions.api.Record;
import org.apache.pulsar.functions.api.utils.FunctionRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Utils {

  public static GenericData.Record getRecord(Schema<?> schema, byte[] value) throws IOException {
    DatumReader<GenericData.Record> reader =
        new GenericDatumReader<>((org.apache.avro.Schema) schema.getNativeSchema().orElseThrow());
    Decoder decoder = DecoderFactory.get().binaryDecoder(value, null);
    return reader.read(null, decoder);
  }

  public static Record<GenericObject> createTestAvroKeyValueRecord() {
    RecordSchemaBuilder keySchemaBuilder =
        org.apache.pulsar.client.api.schema.SchemaBuilder.record("record");
    keySchemaBuilder.field("keyField1").type(SchemaType.STRING);
    keySchemaBuilder.field("keyField2").type(SchemaType.STRING);
    keySchemaBuilder.field("keyField3").type(SchemaType.STRING);
    GenericSchema<GenericRecord> keySchema =
        Schema.generic(keySchemaBuilder.build(SchemaType.AVRO));

    RecordSchemaBuilder valueSchemaBuilder =
        org.apache.pulsar.client.api.schema.SchemaBuilder.record("record");
    valueSchemaBuilder.field("valueField1").type(SchemaType.STRING);
    valueSchemaBuilder.field("valueField2").type(SchemaType.STRING);
    valueSchemaBuilder.field("valueField3").type(SchemaType.STRING);
    GenericSchema<GenericRecord> valueSchema =
        Schema.generic(valueSchemaBuilder.build(SchemaType.AVRO));

    GenericRecord keyRecord =
        keySchema
            .newRecordBuilder()
            .set("keyField1", "key1")
            .set("keyField2", "key2")
            .set("keyField3", "key3")
            .build();

    GenericRecord valueRecord =
        valueSchema
            .newRecordBuilder()
            .set("valueField1", "value1")
            .set("valueField2", "value2")
            .set("valueField3", "value3")
            .build();

    Schema<KeyValue<GenericRecord, GenericRecord>> keyValueSchema =
        Schema.KeyValue(keySchema, valueSchema, KeyValueEncodingType.SEPARATED);

    KeyValue<GenericRecord, GenericRecord> keyValue = new KeyValue<>(keyRecord, valueRecord);

    GenericObject genericObject =
        new GenericObject() {
          @Override
          public SchemaType getSchemaType() {
            return SchemaType.KEY_VALUE;
          }

          @Override
          public Object getNativeObject() {
            return keyValue;
          }
        };

    return new TestRecord<>(keyValueSchema, genericObject, null);
  }

  @Builder
  @RequiredArgsConstructor
  @AllArgsConstructor
  public static class TestRecord<T> implements Record<T> {
    private final Schema<?> schema;
    private final T value;
    private final String key;
    private String topicName;
    private String destinationTopic;
    private Long eventTime;
    Map<String, String> properties;

    @Override
    public Optional<String> getKey() {
      return Optional.ofNullable(key);
    }

    @Override
    public Schema<T> getSchema() {
      return (Schema<T>) schema;
    }

    @Override
    public T getValue() {
      return value;
    }

    @Override
    public Optional<String> getTopicName() {
      return Optional.ofNullable(topicName);
    }

    @Override
    public Optional<String> getDestinationTopic() {
      return Optional.ofNullable(destinationTopic);
    }

    @Override
    public Optional<Long> getEventTime() {
      return Optional.ofNullable(eventTime);
    }

    @Override
    public Map<String, String> getProperties() {
      return properties;
    }
  }

  public static class TestContext implements Context {
    private final Record<?> currentRecord;
    private final Map<String, Object> userConfig;

    public TestContext(Record<?> currentRecord, Map<String, Object> userConfig) {
      this.currentRecord = currentRecord;
      this.userConfig = userConfig;
    }

    @Override
    public Collection<String> getInputTopics() {
      return null;
    }

    @Override
    public String getOutputTopic() {
      return "test-context-topic";
    }

    @Override
    public Record<?> getCurrentRecord() {
      return currentRecord;
    }

    @Override
    public String getOutputSchemaType() {
      return null;
    }

    @Override
    public String getFunctionName() {
      return null;
    }

    @Override
    public String getFunctionId() {
      return null;
    }

    @Override
    public String getFunctionVersion() {
      return null;
    }

    @Override
    public Map<String, Object> getUserConfigMap() {
      return userConfig;
    }

    @Override
    public Optional<Object> getUserConfigValue(String key) {
      return Optional.ofNullable(userConfig.get(key));
    }

    @Override
    public Object getUserConfigValueOrDefault(String key, Object defaultValue) {
      return null;
    }

    @Override
    public PulsarAdmin getPulsarAdmin() {
      return null;
    }

    @Override
    public <X> CompletableFuture<Void> publish(
        String topicName, X object, String schemaOrSerdeClassName) {
      return null;
    }

    @Override
    public <X> CompletableFuture<Void> publish(String topicName, X object) {
      return null;
    }

    @Override
    public <X> TypedMessageBuilder<X> newOutputMessage(String topicName, Schema<X> schema) {
      return null;
    }

    @Override
    public <X> ConsumerBuilder<X> newConsumerBuilder(Schema<X> schema) {
      return null;
    }

    @Override
    public <X> FunctionRecord.FunctionRecordBuilder<X> newOutputRecordBuilder(Schema<X> schema) {
      return FunctionRecord.from(
          new Context() {
            @Override
            public Collection<String> getInputTopics() {
              return null;
            }

            @Override
            public String getOutputTopic() {
              return "test-context-topic";
            }

            @Override
            public Record<?> getCurrentRecord() {
              return currentRecord;
            }

            @Override
            public String getOutputSchemaType() {
              return null;
            }

            @Override
            public String getFunctionName() {
              return null;
            }

            @Override
            public String getFunctionId() {
              return null;
            }

            @Override
            public String getFunctionVersion() {
              return null;
            }

            @Override
            public Map<String, Object> getUserConfigMap() {
              return null;
            }

            @Override
            public Optional<Object> getUserConfigValue(String key) {
              return Optional.empty();
            }

            @Override
            public Object getUserConfigValueOrDefault(String key, Object defaultValue) {
              return null;
            }

            @Override
            public PulsarAdmin getPulsarAdmin() {
              return null;
            }

            @Override
            public <O> CompletableFuture<Void> publish(
                String topicName, O object, String schemaOrSerdeClassName) {
              return null;
            }

            @Override
            public <O> CompletableFuture<Void> publish(String topicName, O object) {
              return null;
            }

            @Override
            public <O> TypedMessageBuilder<O> newOutputMessage(String topicName, Schema<O> schema) {
              return null;
            }

            @Override
            public <O> ConsumerBuilder<O> newConsumerBuilder(Schema<O> schema) {
              return null;
            }

            @Override
            public <O> FunctionRecord.FunctionRecordBuilder<O> newOutputRecordBuilder(
                Schema<O> schema) {
              return null;
            }

            @Override
            public String getTenant() {
              return null;
            }

            @Override
            public String getNamespace() {
              return null;
            }

            @Override
            public int getInstanceId() {
              return 0;
            }

            @Override
            public int getNumInstances() {
              return 0;
            }

            @Override
            public Logger getLogger() {
              return LoggerFactory.getILoggerFactory().getLogger("Context");
            }

            @Override
            public String getSecret(String secretName) {
              return null;
            }

            @Override
            public void putState(String key, ByteBuffer value) {}

            @Override
            public CompletableFuture<Void> putStateAsync(String key, ByteBuffer value) {
              return null;
            }

            @Override
            public ByteBuffer getState(String key) {
              return null;
            }

            @Override
            public CompletableFuture<ByteBuffer> getStateAsync(String key) {
              return null;
            }

            @Override
            public void deleteState(String key) {}

            @Override
            public CompletableFuture<Void> deleteStateAsync(String key) {
              return null;
            }

            @Override
            public void incrCounter(String key, long amount) {}

            @Override
            public CompletableFuture<Void> incrCounterAsync(String key, long amount) {
              return null;
            }

            @Override
            public long getCounter(String key) {
              return 0;
            }

            @Override
            public CompletableFuture<Long> getCounterAsync(String key) {
              return null;
            }

            @Override
            public void recordMetric(String metricName, double value) {}
          },
          schema);
    }

    @Override
    public String getTenant() {
      return null;
    }

    @Override
    public String getNamespace() {
      return null;
    }

    @Override
    public int getInstanceId() {
      return 0;
    }

    @Override
    public int getNumInstances() {
      return 0;
    }

    @Override
    public Logger getLogger() {
      return LoggerFactory.getILoggerFactory().getLogger("Context");
    }

    @Override
    public String getSecret(String secretName) {
      return null;
    }

    @Override
    public void putState(String key, ByteBuffer value) {}

    @Override
    public CompletableFuture<Void> putStateAsync(String key, ByteBuffer value) {
      return null;
    }

    @Override
    public ByteBuffer getState(String key) {
      return null;
    }

    @Override
    public CompletableFuture<ByteBuffer> getStateAsync(String key) {
      return null;
    }

    @Override
    public void deleteState(String key) {}

    @Override
    public CompletableFuture<Void> deleteStateAsync(String key) {
      return null;
    }

    @Override
    public void incrCounter(String key, long amount) {}

    @Override
    public CompletableFuture<Void> incrCounterAsync(String key, long amount) {
      return null;
    }

    @Override
    public long getCounter(String key) {
      return 0;
    }

    @Override
    public CompletableFuture<Long> getCounterAsync(String key) {
      return null;
    }

    @Override
    public void recordMetric(String metricName, double value) {}
  }
}
