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

import lombok.Builder;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.common.schema.SchemaType;

@Builder
public class CastStep implements TransformStep {

  private final SchemaType keySchemaType;
  private final SchemaType valueSchemaType;

  @Override
  public void process(TransformContext transformContext) {
    if (transformContext.getKeySchema() != null) {
      Object outputKeyObject = transformContext.getKeyObject();
      Schema<?> outputSchema = transformContext.getKeySchema();
      if (keySchemaType == SchemaType.STRING) {
        outputSchema = Schema.STRING;
        outputKeyObject = outputKeyObject.toString();
      }
      transformContext.setKeySchema(outputSchema);
      transformContext.setKeyObject(outputKeyObject);
    }
    if (valueSchemaType == SchemaType.STRING) {
      transformContext.setValueSchema(Schema.STRING);
      transformContext.setValueObject(transformContext.getValueObject().toString());
    }
  }

  public static class CastStepBuilder {
    private SchemaType keySchemaType;
    private SchemaType valueSchemaType;

    public CastStepBuilder keySchemaType(SchemaType keySchemaType) {
      if (keySchemaType != null && keySchemaType != SchemaType.STRING) {
        throw new IllegalArgumentException(
            "Unsupported key schema-type for Cast: " + keySchemaType);
      }
      this.keySchemaType = keySchemaType;
      return this;
    }

    public CastStepBuilder valueSchemaType(SchemaType valueSchemaType) {
      if (valueSchemaType != null && valueSchemaType != SchemaType.STRING) {
        throw new IllegalArgumentException(
            "Unsupported value schema-type for Cast: " + valueSchemaType);
      }
      this.valueSchemaType = valueSchemaType;
      return this;
    }
  }
}
