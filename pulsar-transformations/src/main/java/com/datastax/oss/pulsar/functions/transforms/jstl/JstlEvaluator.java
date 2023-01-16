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
import de.odysseus.el.ExpressionFactoryImpl;
import de.odysseus.el.util.SimpleContext;
import java.util.Map;
import java.util.Properties;
import javax.el.ExpressionFactory;
import javax.el.ValueExpression;
import lombok.SneakyThrows;

public class JstlEvaluator<T> {

  private static final ExpressionFactory FACTORY =
      new ExpressionFactoryImpl(buildDefaultProperties(), CustomTypeConverter.INSTANCE);

  private final ValueExpression valueExpression;
  private final SimpleContext expressionContext;

  public JstlEvaluator(String expression, Class<? extends T> type) {
    this.expressionContext = new SimpleContext();
    registerFunctions();
    this.valueExpression = FACTORY.createValueExpression(expressionContext, expression, type);
  }

  @SneakyThrows
  private void registerFunctions() {
    this.expressionContext.setFunction(
        "fn", "uppercase", JstlFunctions.class.getMethod("uppercase", Object.class));
    this.expressionContext.setFunction(
        "fn", "lowercase", JstlFunctions.class.getMethod("lowercase", Object.class));
    this.expressionContext.setFunction(
        "fn", "contains", JstlFunctions.class.getMethod("contains", Object.class, Object.class));
    this.expressionContext.setFunction(
        "fn", "trim", JstlFunctions.class.getMethod("trim", Object.class));
    this.expressionContext.setFunction(
        "fn", "concat", JstlFunctions.class.getMethod("concat", Object.class, Object.class));
    this.expressionContext.setFunction(
        "fn", "coalesce", JstlFunctions.class.getMethod("coalesce", Object.class, Object.class));
    this.expressionContext.setFunction(
        "fn", "str", JstlFunctions.class.getMethod("toString", Object.class));
    this.expressionContext.setFunction(
        "fn",
        "replace",
        JstlFunctions.class.getMethod("replace", Object.class, Object.class, Object.class));
    this.expressionContext.setFunction("fn", "now", JstlFunctions.class.getMethod("now"));
    this.expressionContext.setFunction(
        "fn",
        "dateadd",
        JstlFunctions.class.getMethod("dateadd", Object.class, long.class, String.class));
  }

  public T evaluate(TransformContext transformContext) {
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
    return (T) this.valueExpression.getValue(expressionContext);
  }

  private static Properties buildDefaultProperties() {
    Properties properties = new Properties();
    properties.setProperty(ExpressionFactoryImpl.PROP_METHOD_INVOCATIONS, "false");
    return properties;
  }
}
