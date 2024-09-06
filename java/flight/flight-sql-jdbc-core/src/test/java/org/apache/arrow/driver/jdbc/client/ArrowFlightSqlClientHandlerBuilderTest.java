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
package org.apache.arrow.driver.jdbc.client;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Optional;
import org.apache.arrow.driver.jdbc.FlightServerTestExtension;
import org.apache.arrow.driver.jdbc.utils.CoreMockedSqlProducers;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

/** Test the behavior of ArrowFlightSqlClientHandler.Builder. */
public class ArrowFlightSqlClientHandlerBuilderTest {

  @RegisterExtension
  public static final FlightServerTestExtension FLIGHT_SERVER_TEST_EXTENSION =
      FlightServerTestExtension.createStandardTestExtension(
          CoreMockedSqlProducers.getLegacyProducer());

  private static BufferAllocator allocator;

  @BeforeAll
  public static void setup() {
    allocator = new RootAllocator(Long.MAX_VALUE);
  }

  @AfterAll
  public static void tearDown() {
    allocator.close();
  }

  @Test
  public void testRetainCookiesOnAuthOff() throws Exception {
    // Arrange
    final ArrowFlightSqlClientHandler.Builder rootBuilder =
        new ArrowFlightSqlClientHandler.Builder()
            .withHost(FLIGHT_SERVER_TEST_EXTENSION.getHost())
            .withPort(FLIGHT_SERVER_TEST_EXTENSION.getPort())
            .withBufferAllocator(allocator)
            .withUsername(FlightServerTestExtension.DEFAULT_USER)
            .withPassword(FlightServerTestExtension.DEFAULT_PASSWORD)
            .withEncryption(false)
            .withRetainCookies(true)
            .withRetainAuth(false);

    try (ArrowFlightSqlClientHandler rootHandler = rootBuilder.build()) {
      // Act
      final ArrowFlightSqlClientHandler.Builder testBuilder =
          new ArrowFlightSqlClientHandler.Builder(rootBuilder);

      // Assert
      assertSame(rootBuilder.cookieFactory, testBuilder.cookieFactory);
      assertNotSame(rootBuilder.authFactory, testBuilder.authFactory);
    }
  }

  @Test
  public void testRetainCookiesOffAuthOff() throws Exception {
    // Arrange
    final ArrowFlightSqlClientHandler.Builder rootBuilder =
        new ArrowFlightSqlClientHandler.Builder()
            .withHost(FLIGHT_SERVER_TEST_EXTENSION.getHost())
            .withPort(FLIGHT_SERVER_TEST_EXTENSION.getPort())
            .withBufferAllocator(allocator)
            .withUsername(FlightServerTestExtension.DEFAULT_USER)
            .withPassword(FlightServerTestExtension.DEFAULT_PASSWORD)
            .withEncryption(false)
            .withRetainCookies(false)
            .withRetainAuth(false);

    try (ArrowFlightSqlClientHandler rootHandler = rootBuilder.build()) {
      // Act
      final ArrowFlightSqlClientHandler.Builder testBuilder =
          new ArrowFlightSqlClientHandler.Builder(rootBuilder);

      // Assert
      assertNotSame(rootBuilder.cookieFactory, testBuilder.cookieFactory);
      assertNotSame(rootBuilder.authFactory, testBuilder.authFactory);
    }
  }

  @Test
  public void testRetainCookiesOnAuthOn() throws Exception {
    // Arrange
    final ArrowFlightSqlClientHandler.Builder rootBuilder =
        new ArrowFlightSqlClientHandler.Builder()
            .withHost(FLIGHT_SERVER_TEST_EXTENSION.getHost())
            .withPort(FLIGHT_SERVER_TEST_EXTENSION.getPort())
            .withBufferAllocator(allocator)
            .withUsername(FlightServerTestExtension.DEFAULT_USER)
            .withPassword(FlightServerTestExtension.DEFAULT_PASSWORD)
            .withEncryption(false)
            .withRetainCookies(true)
            .withRetainAuth(true);

    try (ArrowFlightSqlClientHandler rootHandler = rootBuilder.build()) {
      // Act
      final ArrowFlightSqlClientHandler.Builder testBuilder =
          new ArrowFlightSqlClientHandler.Builder(rootBuilder);

      // Assert
      assertSame(rootBuilder.cookieFactory, testBuilder.cookieFactory);
      assertSame(rootBuilder.authFactory, testBuilder.authFactory);
    }
  }

  @Test
  public void testDefaults() {
    final ArrowFlightSqlClientHandler.Builder builder = new ArrowFlightSqlClientHandler.Builder();

    // Validate all non-mandatory fields against defaults in ArrowFlightConnectionProperty.
    assertNull(builder.username);
    assertNull(builder.password);
    assertTrue(builder.useEncryption);
    assertFalse(builder.disableCertificateVerification);
    assertNull(builder.trustStorePath);
    assertNull(builder.trustStorePassword);
    assertTrue(builder.useSystemTrustStore);
    assertNull(builder.token);
    assertTrue(builder.retainAuth);
    assertTrue(builder.retainCookies);
    assertNull(builder.tlsRootCertificatesPath);
    assertNull(builder.clientCertificatePath);
    assertNull(builder.clientKeyPath);
    assertEquals(Optional.empty(), builder.catalog);
  }

  @Test
  public void testCatalog() {
    ArrowFlightSqlClientHandler.Builder rootBuilder = new ArrowFlightSqlClientHandler.Builder();

    rootBuilder.withCatalog(null);
    assertFalse(rootBuilder.catalog.isPresent());

    rootBuilder.withCatalog("");
    assertTrue(rootBuilder.catalog.isPresent());

    rootBuilder.withCatalog("   ");
    assertTrue(rootBuilder.catalog.isPresent());

    final String noSpaces = "noSpaces";
    rootBuilder.withCatalog(noSpaces);
    assertTrue(rootBuilder.catalog.isPresent());
    assertEquals(noSpaces, rootBuilder.catalog.get());

    final String nameWithSpaces = "  spaces ";
    rootBuilder.withCatalog(nameWithSpaces);
    assertTrue(rootBuilder.catalog.isPresent());
    assertEquals(nameWithSpaces, rootBuilder.catalog.get());
  }
}
