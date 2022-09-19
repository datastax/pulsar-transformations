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
package com.datastax.oss.pulsar.functions.transforms.jstl;

import com.datastax.oss.pulsar.functions.transforms.TransformContext;
import java.util.HashMap;
import java.util.Map;
import org.apache.avro.generic.GenericRecord;
import org.apache.commons.collections4.Transformer;
import org.apache.commons.collections4.map.LazyMap;
import org.apache.pulsar.functions.api.Record;

/**
 * A java bean that adapts the underlying {@link TransformContext} to be ready for jstl expression
 * language binding.
 */
public class JstlTransformContextAdapter {
  private TransformContext transformContext;

  /**
   * A key transformer backing the lazy key map. It transforms top level key fields to either a
   * value or another lazy map for nested evaluation.
   */
  private Transformer<String, Object> keyTransformer =
      (fieldName) -> {
        if (this.transformContext.getKeyObject() instanceof GenericRecord) {
          GenericRecord genericRecord = (GenericRecord) this.transformContext.getKeyObject();
          GenericRecordTransformer transformer = new GenericRecordTransformer(genericRecord);
          return transformer.transform(fieldName);
        }
        return null;
      };

  private final Map<String, Object> lazyKey = LazyMap.lazyMap(new HashMap<>(), keyTransformer);

  /**
   * A value transformer backing the lazy value map. It transforms top level value fields to either
   * a value or another lazy map for nested evaluation.
   */
  private Transformer<String, Object> valueTransformer =
      (fieldName) -> {
        if (this.transformContext.getValueObject() instanceof GenericRecord) {
          GenericRecord genericRecord = (GenericRecord) this.transformContext.getValueObject();
          GenericRecordTransformer transformer = new GenericRecordTransformer(genericRecord);
          return transformer.transform(fieldName);
        }
        return null;
      };

  private final Map<String, Object> lazyValue = LazyMap.lazyMap(new HashMap<>(), valueTransformer);

  /** A header transformer to return message headers the user is allowed to filter on. */
  private Transformer<String, Object> headerTransformer =
      (fieldName) -> {
        Record<?> currentRecord = transformContext.getContext().getCurrentRecord();
        // Allow list message headers in the expression
        switch (fieldName) {
          case "messageKey":
            return currentRecord.getKey().orElse(null);
          case "topicName":
            return currentRecord.getTopicName().orElse(null);
          case "destinationTopic":
            return currentRecord.getDestinationTopic().orElse(null);
          case "eventTime":
            return currentRecord.getEventTime().orElse(null);
          case "properties":
            return currentRecord.getProperties();
          default:
            return null;
        }
      };

  private final Map<String, Object> lazyHeader =
      LazyMap.lazyMap(new HashMap<>(), headerTransformer);

  public JstlTransformContextAdapter(TransformContext transformContext) {
    this.transformContext = transformContext;
  }

  /**
   * @return either a lazily evaluated map to access top-level and nested fields on a generic
   *     object, or the primitive type itself.
   */
  public Object getKey() {
    return this.transformContext.getKeyObject() instanceof GenericRecord
        ? lazyKey
        : this.transformContext.getKeyObject();
  }

  /**
   * @return either a lazily evaluated map to access top-level and nested fields on a generic
   *     object, or the primitive type itself.
   */
  public Object getValue() {
    return this.transformContext.getValueObject() instanceof GenericRecord
        ? lazyValue
        : this.transformContext.getValueObject();
  }

  public Map<String, Object> getHeader() {
    return lazyHeader;
  }

  /** Enables {@link LazyMap} lookup on {@link GenericRecord}. */
  static class GenericRecordTransformer implements Transformer<String, Object> {

    GenericRecord genericRecord;

    public GenericRecordTransformer(GenericRecord genericRecord) {
      this.genericRecord = genericRecord;
    }

    @Override
    public Object transform(String key) {
      Object value = null;
      if (genericRecord.hasField(key)) {
        value = genericRecord.get(key);
        if (value instanceof GenericRecord) {
          value =
              LazyMap.lazyMap(new HashMap<>(), new GenericRecordTransformer((GenericRecord) value));
        }
      }

      return value;
    }
  }
}
