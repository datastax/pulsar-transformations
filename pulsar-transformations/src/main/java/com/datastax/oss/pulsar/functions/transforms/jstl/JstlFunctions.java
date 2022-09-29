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

/** Provides convenience methods to use in jstl expression. All functions should be static. */
public class JstlFunctions {

  public static String uppercase(String input) {
    return input.toUpperCase();
  }

  public static String lowercase(String input) {
    return input.toLowerCase();
  }

  public static boolean contains(String input, String value) {
    return input.contains(value);
  }

  public static Object coalesce(Object value, Object valueIfNull) {
    return value == null ? valueIfNull : value;
  }
}
