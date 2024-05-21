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

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

import org.apache.arrow.driver.jdbc.FlightServerTestRule;
import org.apache.arrow.driver.jdbc.utils.CoreMockedSqlProducers;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;

/**
 * Test the behavior of ArrowFlightSqlClientHandler.Builder
 */
public class ArrowFlightSqlClientHandlerBuilderTest {
  @ClassRule
  public static final FlightServerTestRule FLIGHT_SERVER_TEST_RULE = FlightServerTestRule
      .createStandardTestRule(CoreMockedSqlProducers.getLegacyProducer());

  private static BufferAllocator allocator;

  @BeforeClass
  public static void setup() {
    allocator = new RootAllocator(Long.MAX_VALUE);
  }

  @AfterClass
  public static void tearDown() {
    allocator.close();
  }

  @Test
  public void testRetainCookiesOnAuthOff() throws Exception {
    // Arrange
    final ArrowFlightSqlClientHandler.Builder rootBuilder = new ArrowFlightSqlClientHandler.Builder()
        .withHost(FLIGHT_SERVER_TEST_RULE.getHost())
        .withPort(FLIGHT_SERVER_TEST_RULE.getPort())
        .withBufferAllocator(allocator)
        .withUsername(FlightServerTestRule.DEFAULT_USER)
        .withPassword(FlightServerTestRule.DEFAULT_PASSWORD)
        .withEncryption(false)
        .withRetainCookies(true)
        .withRetainAuth(false);

    try (ArrowFlightSqlClientHandler rootHandler = rootBuilder.build()) {
      // Act
      final ArrowFlightSqlClientHandler.Builder testBuilder = new ArrowFlightSqlClientHandler.Builder(rootBuilder);

      // Assert
      assertSame(rootBuilder.cookieFactory, testBuilder.cookieFactory);
      assertNotSame(rootBuilder.authFactory, testBuilder.authFactory);
    }
  }

  @Test
  public void testRetainCookiesOffAuthOff() throws Exception {
    // Arrange
    final ArrowFlightSqlClientHandler.Builder rootBuilder = new ArrowFlightSqlClientHandler.Builder()
        .withHost(FLIGHT_SERVER_TEST_RULE.getHost())
        .withPort(FLIGHT_SERVER_TEST_RULE.getPort())
        .withBufferAllocator(allocator)
        .withUsername(FlightServerTestRule.DEFAULT_USER)
        .withPassword(FlightServerTestRule.DEFAULT_PASSWORD)
        .withEncryption(false)
        .withRetainCookies(false)
        .withRetainAuth(false);

    try (ArrowFlightSqlClientHandler rootHandler = rootBuilder.build()) {
      // Act
      final ArrowFlightSqlClientHandler.Builder testBuilder = new ArrowFlightSqlClientHandler.Builder(rootBuilder);

      // Assert
      assertNotSame(rootBuilder.cookieFactory, testBuilder.cookieFactory);
      assertNotSame(rootBuilder.authFactory, testBuilder.authFactory);
    }
  }

  @Test
  public void testRetainCookiesOnAuthOn() throws Exception {
    // Arrange
    final ArrowFlightSqlClientHandler.Builder rootBuilder = new ArrowFlightSqlClientHandler.Builder()
        .withHost(FLIGHT_SERVER_TEST_RULE.getHost())
        .withPort(FLIGHT_SERVER_TEST_RULE.getPort())
        .withBufferAllocator(allocator)
        .withUsername(FlightServerTestRule.DEFAULT_USER)
        .withPassword(FlightServerTestRule.DEFAULT_PASSWORD)
        .withEncryption(false)
        .withRetainCookies(true)
        .withRetainAuth(true);

    try (ArrowFlightSqlClientHandler rootHandler = rootBuilder.build()) {
      // Act
      final ArrowFlightSqlClientHandler.Builder testBuilder = new ArrowFlightSqlClientHandler.Builder(rootBuilder);

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
  }
}
