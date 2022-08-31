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

import com.datastax.oss.pulsar.jms.selectors.SelectorSupport;
import java.util.function.Predicate;
import javax.jms.JMSException;
import org.apache.avro.generic.GenericRecord;
import org.apache.pulsar.client.api.Message;

public class JmsPredicate implements Predicate<TransformContext> {

  private final SelectorSupport selector;

  public JmsPredicate(String filter) {
    try {
      // A hacky way in to init the predicate as JMS filters doesn't accept . in field names,
      // Parsing and passing a positional list of ket/value is tricky as we need to understand the
      // SQL syntax
      // TODO: Find a better way to handle the scope of the field
      String sanitizedFilter =
          filter.replaceAll("key\\.", "key\\$").replaceAll("value\\.", "value\\$");
      selector = SelectorSupport.build(sanitizedFilter, true);
    } catch (JMSException ex) {
      throw new IllegalArgumentException("invalid filter: " + filter, ex);
    }
  }

  @Override
  public boolean test(TransformContext transformContext) {
    try {
      return selector.matches((key) -> fieldAccessor(key, transformContext));
    } catch (JMSException ex) {
      throw new RuntimeException("failed to test predicate", ex);
    }
  }

  private Object fieldAccessor(String field, TransformContext context) {
    String scope = null;
    String key = field;
    if (field.startsWith("key$") || field.startsWith("value$")) {
      String[] filterParts = field.split("\\$", 2);
      scope = filterParts[0];
      key = filterParts[1];
    }

    if (scope == null) {
      Message message = context.getContext().getCurrentRecord().getMessage().orElse(null);
      return message == null ? null : message.getProperties().get(key);
    } else if ("key".equals(scope) && context.getKeyObject() instanceof GenericRecord) {
      GenericRecord record = (GenericRecord) context.getKeyObject();
      return record.hasField(key) ? record.get(key) : null;
    } else if ("value".equals(scope) && context.getValueObject() instanceof GenericRecord) {
      GenericRecord record = (GenericRecord) context.getValueObject();
      return record.hasField(key) ? record.get(key) : null;
    }
    return null; // should never happen
  }
}
