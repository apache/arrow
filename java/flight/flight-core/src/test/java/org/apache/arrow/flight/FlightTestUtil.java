/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.arrow.flight;

import java.io.File;
import java.lang.reflect.InvocationTargetException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;

import org.apache.arrow.vector.test.util.ArrowTestDataUtil;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.function.Executable;

/**
 * Utility methods and constants for testing flight servers.
 */
public class FlightTestUtil {

  public static final String LOCALHOST = "localhost";

  static Path getFlightTestDataRoot() {
    return ArrowTestDataUtil.getTestDataRoot().resolve("flight");
  }

  static Path exampleTlsRootCert() {
    return getFlightTestDataRoot().resolve("root-ca.pem");
  }

  static List<CertKeyPair> exampleTlsCerts() {
    final Path root = getFlightTestDataRoot();
    final Path cert0Pem = root.resolve("cert0.pem");
    if (!Files.exists(cert0Pem)) {
      throw new RuntimeException(cert0Pem + " doesn't exist. Make sure submodules are initialized (see https://arrow.apache.org/docs/dev/developers/java/building.html#building)");
    }
    return Arrays.asList(new CertKeyPair(cert0Pem.toFile(), root.resolve("cert0.pkcs1").toFile()),
        new CertKeyPair(root.resolve("cert1.pem").toFile(), root.resolve("cert1.pkcs1").toFile()));
  }

  static boolean isEpollAvailable() {
    try {
      Class<?> epoll = Class.forName("io.netty.channel.epoll.Epoll");
      return (Boolean) epoll.getMethod("isAvailable").invoke(null);
    } catch (ClassNotFoundException | NoSuchMethodException | IllegalAccessException | InvocationTargetException e) {
      return false;
    }
  }

  static boolean isKqueueAvailable() {
    try {
      Class<?> kqueue = Class.forName("io.netty.channel.kqueue.KQueue");
      return (Boolean) kqueue.getMethod("isAvailable").invoke(null);
    } catch (ClassNotFoundException | NoSuchMethodException | IllegalAccessException | InvocationTargetException e) {
      return false;
    }
  }

  static boolean isNativeTransportAvailable() {
    return isEpollAvailable() || isKqueueAvailable();
  }

  /**
   * Assert that the given runnable fails with a Flight exception of the given code.
   * @param code The expected Flight status code.
   * @param r The code to run.
   * @return The thrown status.
   */
  public static CallStatus assertCode(FlightStatusCode code, Executable r) {
    final FlightRuntimeException ex = Assertions.assertThrows(FlightRuntimeException.class, r);
    Assertions.assertEquals(code, ex.status().code());
    return ex.status();
  }

  public static class CertKeyPair {

    public final File cert;
    public final File key;

    public CertKeyPair(File cert, File key) {
      this.cert = cert;
      this.key = key;
    }
  }

  private FlightTestUtil() {
  }
}
