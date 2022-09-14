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
package com.datastax.oss.pulsar.functions.transforms.predicate.jstl;

import com.datastax.oss.pulsar.functions.transforms.TransformContext;
import com.datastax.oss.pulsar.functions.transforms.predicate.TransformPredicate;
import de.odysseus.el.util.SimpleContext;
import java.util.HashMap;
import java.util.Map;
import javax.el.ELContext;
import javax.el.ExpressionFactory;
import javax.el.PropertyNotFoundException;
import javax.el.ValueExpression;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.generic.GenericRecord;
import org.apache.commons.collections4.Transformer;
import org.apache.commons.collections4.map.LazyMap;

/** A {@link TransformPredicate} implementation based on the Uniform Transform Language. */
@Slf4j
public class JstlPredicate implements TransformPredicate {
  private static final ExpressionFactory FACTORY = new de.odysseus.el.ExpressionFactoryImpl();
  private final ValueExpression valueExpression;
  private final ELContext expressionContext;

  public JstlPredicate(String when) {
    this.expressionContext = new SimpleContext();
    try {
      final String expression = String.format("${%s}", when);
      this.valueExpression =
          FACTORY.createValueExpression(expressionContext, expression, boolean.class);
    } catch (RuntimeException ex) {
      throw new IllegalArgumentException("invalid when: " + when, ex);
    }
  }

  @Override
  public boolean test(TransformContext transformContext) {
    JstlTransformContextAdapter adapter = new JstlTransformContextAdapter(transformContext);
    FACTORY
        .createValueExpression(expressionContext, "${key}", Object.class)
        .setValue(expressionContext, adapter.getKey());
    FACTORY
        .createValueExpression(expressionContext, "${value}", Object.class)
        .setValue(expressionContext, adapter.getValue());

    // Register message headers as top level fields
    FACTORY
        .createValueExpression(expressionContext, "${messageKey}", String.class)
        .setValue(expressionContext, adapter.getHeader().get("messageKey"));
    FACTORY
        .createValueExpression(expressionContext, "${topicName}", String.class)
        .setValue(expressionContext, adapter.getHeader().get("topicName"));
    FACTORY
        .createValueExpression(expressionContext, "${destinationTopic}", String.class)
        .setValue(expressionContext, adapter.getHeader().get("destinationTopic"));
    FACTORY
        .createValueExpression(expressionContext, "${eventTime}", Long.class)
        .setValue(expressionContext, adapter.getHeader().get("eventTime"));
    FACTORY
        .createValueExpression(expressionContext, "${properties}", Map.class)
        .setValue(expressionContext, adapter.getHeader().get("properties"));
    try {
      return (boolean) this.valueExpression.getValue(expressionContext);
    } catch (PropertyNotFoundException ex) {
      log.warn("a property in the when expression was not found in the message", ex);
      return false;
    }
  }

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
