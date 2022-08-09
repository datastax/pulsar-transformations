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
/**
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 *
 * <p>http://www.apache.org/licenses/LICENSE-2.0
 *
 * <p>Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datastax.oss.pulsar.functions.transforms.tests;

import static org.testng.AssertJUnit.assertTrue;

import java.net.URL;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.utility.MountableFile;

public class PulsarContainer implements AutoCloseable {

  private static final Logger log = LoggerFactory.getLogger(PulsarContainer.class);

  protected static final String PULSAR_TRANSFORMATIONS_NAR = "/pulsar-transformations.nar";

  private GenericContainer<?> pulsarContainer;
  private final Network network;
  private final String image;

  public PulsarContainer(Network network, String image) {
    this.network = network;
    this.image = image;
  }

  public void start() throws Exception {
    CountDownLatch pulsarReady = new CountDownLatch(1);
    pulsarContainer =
        new GenericContainer<>(image)
            .withNetwork(network)
            .withNetworkAliases("pulsar")
            .withExposedPorts(8080, 6650) // ensure that the ports are listening
            .withCopyFileToContainer(
                MountableFile.forHostPath(getProtocolHandlerPath()),
                "/pulsar/functions/pulsar-transformations.nar")
            .withCopyFileToContainer(
                MountableFile.forClasspathResource("standalone_with_transforms.conf"),
                "/pulsar/conf/standalone.conf")
            .withCommand("bin/pulsar", "standalone", "-nss")
            .withLogConsumer(
                (f) -> {
                  String text = f.getUtf8String().trim();
                  if (text.contains(
                      "Successfully updated the policies on namespace public/default")) {
                    pulsarReady.countDown();
                  }
                  log.info(text);
                });
    pulsarContainer.start();
    assertTrue(pulsarReady.await(1, TimeUnit.MINUTES));
  }

  @Override
  public void close() {
    if (pulsarContainer != null) {
      pulsarContainer.stop();
    }
  }

  protected Path getProtocolHandlerPath() {
    URL testHandlerUrl = this.getClass().getResource(PULSAR_TRANSFORMATIONS_NAR);
    Path handlerPath;
    try {
      if (testHandlerUrl == null) {
        throw new RuntimeException("Cannot find " + PULSAR_TRANSFORMATIONS_NAR);
      }
      handlerPath = Paths.get(testHandlerUrl.toURI());
    } catch (Exception e) {
      log.error("failed to get handler Path, handlerUrl: {}. Exception: ", testHandlerUrl, e);
      throw new RuntimeException(e);
    }
    Path res = handlerPath.toFile().toPath();
    log.info("Loading NAR file from {}", res.toAbsolutePath());
    return res;
  }

  public GenericContainer<?> getPulsarContainer() {
    return pulsarContainer;
  }
}
