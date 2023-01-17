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

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import jakarta.el.ELException;
import org.apache.el.lang.ELSupport;
import org.apache.pulsar.client.api.Schema;

/**
 * Overrides the default TypeConverter coerce to support null values & non-EL coercions (e.g.
 * date/time types) schemas.
 */
public class CustomTypeConverter extends JstlTypeConverter {

  public static final CustomTypeConverter INSTANCE = new CustomTypeConverter();

  private CustomTypeConverter() {
    super();
  }

  @SuppressFBWarnings("NP_BOOLEAN_RETURN_NULL")
  protected Boolean coerceToBoolean(Object value) {
    Boolean result = super.coerceToBoolean(value);
    if (result != null) {
      return result;
    }
    return ELSupport.coerceToBoolean(null, value, false);
  }

  @Override
  protected Double coerceToDouble(Object value) {
    Double result = super.coerceToDouble(value);
    if (result != null) {
      return result;
    }
    return ELSupport.coerceToNumber(null, value, Double.class).doubleValue();
  }

  @Override
  protected Float coerceToFloat(Object value) {
    Float result = super.coerceToFloat(value);
    if (result != null) {
      return result;
    }
    return ELSupport.coerceToNumber(null, value, Float.class).floatValue();
  }

  @Override
  protected Long coerceToLong(Object value) {
    Long result = super.coerceToLong(value);
    if (result != null) {
      return result;
    }
    return ELSupport.coerceToNumber(null, value, Long.class).longValue();
  }

  @Override
  protected Integer coerceToInteger(Object value) {
    Integer result = super.coerceToInteger(value);
    if (result != null) {
      return result;
    }
    return ELSupport.coerceToNumber(null, value, Long.class).intValue();
  }

  @Override
  protected Short coerceToShort(Object value) {
    if (value instanceof byte[]) {
      return Schema.INT16.decode((byte[]) value);
    }
    return ELSupport.coerceToNumber(null, value, Long.class).shortValue();
  }

  @Override
  protected Byte coerceToByte(Object value) {
    if (value instanceof byte[]) {
      return Schema.INT8.decode((byte[]) value);
    }
    return ELSupport.coerceToType(null, value, Byte.class);
  }

  @Override
  protected String coerceToString(Object value) {
    String result = super.coerceToString(value);
    if (result != null) {
      return result;
    }
    return ELSupport.coerceToString(null, value);
  }

  public <T> T convert(Object value, Class<T> type) throws ELException {
    return (T) super.coerceToType(value, type);
  }
}
