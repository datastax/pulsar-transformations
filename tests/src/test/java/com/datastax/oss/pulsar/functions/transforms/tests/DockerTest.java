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
package com.datastax.oss.pulsar.functions.transforms.tests;

public class DockerTest {

  private static final String IMAGE_LUNASTREAMING210 = "datastax/lunastreaming:2.10_4.7";
  private static final String IMAGE_PULSAR211 = "apachepulsar/pulsar:2.11.1";
  private static final String IMAGE_PULSAR30 = "apachepulsar/pulsar:3.0.0";

  public static class LunaStreaming210Test extends AbstractDockerTest {
    LunaStreaming210Test() {
      super(IMAGE_LUNASTREAMING210);
    }
  }

  public static class Pulsar211Test extends AbstractDockerTest {
    Pulsar211Test() {
      super(IMAGE_PULSAR211);
    }
  }

  public static class Pulsar30Test extends AbstractDockerTest {
    Pulsar30Test() {
      super(IMAGE_PULSAR30);
    }
  }
}
