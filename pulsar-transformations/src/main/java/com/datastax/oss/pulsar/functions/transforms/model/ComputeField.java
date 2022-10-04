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
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.Set;
import javax.el.ELException;
import lombok.AccessLevel;
import lombok.Builder;
import lombok.Getter;

@Getter
public class ComputeField {
  /**
   * Full field name in the following format: "value.fieldName", "key.fieldName" or "headerName".
   */
  @Getter(AccessLevel.NONE)
  private final String scopedName;

  private final ComputeFieldType type;
  /** The name of the field without the prefix. */
  private String name;
  /**
   * The scope of the message where the computed field will be applied. Could be "key", "value" or
   * "header"
   */
  private String scope;

  private JstlEvaluator evaluator;
  private final boolean optional;
  private static final Set<String> validComputeHeaders = Set.of("destinationTopic");

  @Builder
  private ComputeField(String scopedName, ComputeFieldType type, boolean optional) {
    this.scopedName = scopedName;
    this.type = type;
    this.optional = optional;
  }

  private ComputeField(
      String name, JstlEvaluator evaluator, ComputeFieldType type, String scope, boolean optional) {
    this(name, type, optional);
    this.evaluator = evaluator;
    this.scope = scope;
    this.name = name;
  }

  public static class ComputeFieldBuilder {
    private String expression;
    private JstlEvaluator<Object> evaluator;
    private String scope;
    private String name;

    public ComputeFieldBuilder expression(String expression) {
      this.expression = expression;
      return this;
    }

    public ComputeField build() {
      // Compile the jstl evaluator to validate the expression syntax early on.
      try {
        this.validateAndParseScopedName();
        this.evaluator =
            new JstlEvaluator<>(String.format("${%s}", this.expression), getJavaType());
      } catch (ELException ex) {
        throw new IllegalArgumentException("invalid expression: " + "expression", ex);
      }
      return new ComputeField(name, evaluator, type, scope, optional);
    }

    private void validateAndParseScopedName() {
      // If the name is in the [key|value].fieldName format, split the name prefix from the part
      if (this.scopedName.startsWith("key.") || this.scopedName.startsWith("value.")) {
        String[] nameParts = this.scopedName.split("\\.", 2);
        this.scope = nameParts[0];
        this.name = nameParts[1];
      } else if (validComputeHeaders.contains(this.scopedName)) {
        this.scope = "header";
        this.name = this.scopedName;
      } else {
        throw new IllegalArgumentException(
            String.format(
                "Invalid compute field name: %s. "
                    + "It should be prefixed with 'key.' or 'value.' or be one of %s",
                this.scopedName, validComputeHeaders));
      }
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
        case DATE:
          return LocalDate.class;
        case TIME:
          return LocalTime.class;
        case DATETIME:
          return LocalDateTime.class;
        default:
          throw new UnsupportedOperationException("Unsupported compute field type: " + type);
      }
    }
  }
}
