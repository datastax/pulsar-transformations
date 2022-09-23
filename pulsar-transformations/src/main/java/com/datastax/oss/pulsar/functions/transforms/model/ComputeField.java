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
package com.datastax.oss.pulsar.functions.transforms.model;

import com.datastax.oss.pulsar.functions.transforms.jstl.JstlEvaluator;
import lombok.Builder;
import lombok.Data;

@Data
public class ComputeField {
  private final String name;
  private final ComputeFieldType type;
  private final String part;
  private final JstlEvaluator evaluator;

  private final boolean optional;

  @Builder
  private ComputeField(
      String name, JstlEvaluator evaluator, ComputeFieldType type, String part, boolean optional) {
    this.name = name;
    this.evaluator = evaluator;
    this.type = type;
    this.part = part;
    this.optional = optional;
  }

  public static class ComputeFieldBuilder {
    private String expression;
    private JstlEvaluator<Object> evaluator;

    public ComputeFieldBuilder expression(String expression) {
      this.expression = expression;
      return this;
    }

    public ComputeField build() {
      // Compile the jstl evaluator to validate the expression syntax early on.
      try {
        this.evaluator =
            new JstlEvaluator<>(String.format("${%s}", this.expression), getJavaType());
      } catch (RuntimeException ex) {
        throw new IllegalArgumentException("invalid expression: " + "expression", ex);
      }
      return new ComputeField(name, evaluator, type, part == null ? "value" : part, optional);
    }

    private Class<?> getJavaType() {
      switch (this.type) {
        case STRING:
          return String.class;
        case INT32:
          return int.class;
        case INT64:
          return long.class;
        case FLOAT:
          return float.class;
        case DOUBLE:
          return double.class;
        case BOOLEAN:
          return boolean.class;
        default:
          throw new UnsupportedOperationException("Unsupported compute field type: " + type);
      }
    }
  }
}
