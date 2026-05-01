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

  private static final String IMAGE_LUNASTREAMING31 = "datastax/lunastreaming:3.1.4_26";
  private static final String IMAGE_LUNASTREAMING40 = "datastax/lunastreaming:4.0.7_7";
  private static final String IMAGE_PULSAR30 = "apachepulsar/pulsar:3.0.17";
  private static final String IMAGE_PULSAR40 = "apachepulsar/pulsar:4.0.10";

  public static class LunaStreaming31Test extends AbstractDockerTest {
    LunaStreaming31Test() {
      super(IMAGE_LUNASTREAMING31);
    }
  }

  public static class LunaStreaming40Test extends AbstractDockerTest {
    LunaStreaming40Test() {
      super(IMAGE_LUNASTREAMING40);
    }
  }

  public static class Pulsar30Test extends AbstractDockerTest {
    Pulsar30Test() {
      super(IMAGE_PULSAR30);
    }
  }

  public static class Pulsar40Test extends AbstractDockerTest {
    Pulsar40Test() {
      super(IMAGE_PULSAR40);
    }
  }
}
