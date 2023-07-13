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
package com.datastax.oss.pulsar.functions.aitools;

import com.datastax.oss.pulsar.functions.transforms.TransformFunction;

/**
 * This is a dummy class to allow having a different classname for the "ai-tools" function.
 * In this module you can find a different config-schema.yaml file that unlocks the "ai-tools" function.
 */
public class GenAIToolkit extends TransformFunction {
}
