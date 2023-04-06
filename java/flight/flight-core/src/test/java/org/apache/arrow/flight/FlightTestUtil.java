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
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.Random;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.function.Executable;

/**
 * Utility methods and constants for testing flight servers.
 */
public class FlightTestUtil {

  private static final Random RANDOM = new Random();

  public static final String LOCALHOST = "localhost";
  public static final String TEST_DATA_ENV_VAR = "ARROW_TEST_DATA";
  public static final String TEST_DATA_PROPERTY = "arrow.test.dataRoot";

  static Path getTestDataRoot() {
    String path = System.getenv(TEST_DATA_ENV_VAR);
    if (path == null) {
      path = System.getProperty(TEST_DATA_PROPERTY);
    }
    return Paths.get(Objects.requireNonNull(path,
        String.format("Could not find test data path. Set the environment variable %s or the JVM property %s.",
            TEST_DATA_ENV_VAR, TEST_DATA_PROPERTY)));
  }

  static Path getFlightTestDataRoot() {
    return getTestDataRoot().resolve("flight");
  }

  static Path exampleTlsRootCert() {
    return getFlightTestDataRoot().resolve("root-ca.pem");
  }

  static List<CertKeyPair> exampleTlsCerts() {
    final Path root = getFlightTestDataRoot();
    return Arrays.asList(new CertKeyPair(root.resolve("cert0.pem").toFile(), root.resolve("cert0.pkcs1").toFile()),
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
