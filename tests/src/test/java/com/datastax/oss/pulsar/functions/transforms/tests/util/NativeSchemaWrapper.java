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
package com.datastax.oss.pulsar.functions.transforms.tests.util;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Optional;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.pulsar.client.api.schema.SchemaInfoProvider;
import org.apache.pulsar.client.impl.schema.SchemaInfoImpl;
import org.apache.pulsar.common.schema.SchemaInfo;
import org.apache.pulsar.common.schema.SchemaType;

public class NativeSchemaWrapper
    implements org.apache.pulsar.client.api.Schema<org.apache.avro.generic.GenericRecord> {
  private final SchemaInfo pulsarSchemaInfo;
  private final org.apache.avro.Schema nativeSchema;

  private final SchemaType pulsarSchemaType;

  private final SpecificDatumWriter datumWriter;

  public NativeSchemaWrapper(org.apache.avro.Schema nativeSchema, SchemaType pulsarSchemaType) {
    this.nativeSchema = nativeSchema;
    this.pulsarSchemaType = pulsarSchemaType;
    this.pulsarSchemaInfo =
        SchemaInfoImpl.builder()
            .schema(nativeSchema.toString(false).getBytes(StandardCharsets.UTF_8))
            .properties(new HashMap<>())
            .type(pulsarSchemaType)
            .name(nativeSchema.getName())
            .build();
    this.datumWriter = new SpecificDatumWriter<>(this.nativeSchema);
  }

  @Override
  public byte[] encode(org.apache.avro.generic.GenericRecord record) {
    try {
      SpecificDatumWriter<org.apache.avro.generic.GenericRecord> datumWriter =
          new SpecificDatumWriter<>(nativeSchema);
      ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
      BinaryEncoder binaryEncoder = new EncoderFactory().binaryEncoder(byteArrayOutputStream, null);
      datumWriter.write(record, binaryEncoder);
      binaryEncoder.flush();
      return byteArrayOutputStream.toByteArray();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public SchemaInfo getSchemaInfo() {
    return pulsarSchemaInfo;
  }

  @Override
  public NativeSchemaWrapper clone() {
    return new NativeSchemaWrapper(nativeSchema, pulsarSchemaType);
  }

  @Override
  public void validate(byte[] message) {
    // nothing to do
  }

  @Override
  public boolean supportSchemaVersioning() {
    return true;
  }

  @Override
  public void setSchemaInfoProvider(SchemaInfoProvider schemaInfoProvider) {}

  @Override
  public GenericRecord decode(byte[] bytes) {
    throw new UnsupportedOperationException("Not implemented");
  }

  @Override
  public boolean requireFetchingSchemaInfo() {
    return true;
  }

  @Override
  public void configureSchemaInfo(String topic, String componentName, SchemaInfo schemaInfo) {}

  @Override
  public Optional<Object> getNativeSchema() {
    return Optional.of(nativeSchema);
  }
}
