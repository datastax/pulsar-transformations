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

import de.odysseus.el.misc.TypeConverter;
import de.odysseus.el.misc.TypeConverterImpl;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import javax.el.ELException;

/**
 * Overrides the default TypeConverter coerce to support null values & non-EL coercions (e.g.
 * date/time types) schemas.
 */
public class CustomTypeConverter implements TypeConverter {
  private static TypeConverterImpl typeConverter = new TypeConverterImpl();

  @Override
  public <T> T convert(Object o, Class<T> aClass) throws ELException {
    if (o == null) {
      return null;
    } else if (aClass == LocalDate.class) {
      return (T) LocalDate.parse(o.toString());
    } else if (aClass == LocalTime.class) {
      return (T) LocalTime.parse(o.toString());
    } else if (aClass == LocalDateTime.class) {
      return (T) LocalDateTime.parse(o.toString());
    }
    return typeConverter.convert(o, aClass);
  }
}
