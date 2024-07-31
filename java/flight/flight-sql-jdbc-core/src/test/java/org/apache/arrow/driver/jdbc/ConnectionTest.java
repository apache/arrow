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
package org.apache.arrow.driver.jdbc;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.net.URISyntaxException;
import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;
import org.apache.arrow.driver.jdbc.authentication.UserPasswordAuthentication;
import org.apache.arrow.driver.jdbc.client.ArrowFlightSqlClientHandler;
import org.apache.arrow.driver.jdbc.utils.ArrowFlightConnectionConfigImpl.ArrowFlightConnectionProperty;
import org.apache.arrow.driver.jdbc.utils.MockFlightSqlProducer;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.util.AutoCloseables;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

/** Tests for {@link Connection}. */
public class ConnectionTest {

  @RegisterExtension public static final FlightServerTestExtension FLIGHT_SERVER_TEST_EXTENSION;
  private static final MockFlightSqlProducer PRODUCER = new MockFlightSqlProducer();
  private static final String userTest = "user1";
  private static final String passTest = "pass1";

  static {
    UserPasswordAuthentication authentication =
        new UserPasswordAuthentication.Builder().user(userTest, passTest).build();

    FLIGHT_SERVER_TEST_EXTENSION =
        new FlightServerTestExtension.Builder()
            .authentication(authentication)
            .producer(PRODUCER)
            .build();
  }

  private BufferAllocator allocator;

  @BeforeEach
  public void setUp() throws Exception {
    allocator = new RootAllocator(Long.MAX_VALUE);
  }

  @AfterEach
  public void tearDown() throws Exception {
    allocator.getChildAllocators().forEach(BufferAllocator::close);
    AutoCloseables.close(allocator);
  }

  /**
   * Checks if an unencrypted connection can be established successfully when the provided valid
   * credentials.
   *
   * @throws SQLException on error.
   */
  @Test
  public void testUnencryptedConnectionShouldOpenSuccessfullyWhenProvidedValidCredentials()
      throws Exception {
    final Properties properties = new Properties();

    properties.put(ArrowFlightConnectionProperty.HOST.camelName(), "localhost");
    properties.put(
        ArrowFlightConnectionProperty.PORT.camelName(), FLIGHT_SERVER_TEST_EXTENSION.getPort());
    properties.put(ArrowFlightConnectionProperty.USER.camelName(), userTest);
    properties.put(ArrowFlightConnectionProperty.PASSWORD.camelName(), passTest);
    properties.put("useEncryption", false);

    try (Connection connection =
        DriverManager.getConnection(
            "jdbc:arrow-flight-sql://"
                + FLIGHT_SERVER_TEST_EXTENSION.getHost()
                + ":"
                + FLIGHT_SERVER_TEST_EXTENSION.getPort(),
            properties)) {
      assertTrue(connection.isValid(300));
    }
  }

  /**
   * Checks if a token is provided it takes precedence over username/pass. In this case, the
   * connection should fail if a token is passed in.
   */
  @Test
  public void testTokenOverridesUsernameAndPasswordAuth() {
    final Properties properties = new Properties();

    properties.put(ArrowFlightConnectionProperty.HOST.camelName(), "localhost");
    properties.put(
        ArrowFlightConnectionProperty.PORT.camelName(), FLIGHT_SERVER_TEST_EXTENSION.getPort());
    properties.put(ArrowFlightConnectionProperty.USER.camelName(), userTest);
    properties.put(ArrowFlightConnectionProperty.PASSWORD.camelName(), passTest);
    properties.put(ArrowFlightConnectionProperty.TOKEN.camelName(), "token");
    properties.put("useEncryption", false);

    SQLException e =
        assertThrows(
            SQLException.class,
            () -> {
              try (Connection conn =
                  DriverManager.getConnection(
                      "jdbc:arrow-flight-sql://"
                          + FLIGHT_SERVER_TEST_EXTENSION.getHost()
                          + ":"
                          + FLIGHT_SERVER_TEST_EXTENSION.getPort(),
                      properties)) {
                fail();
              }
            });
    assertTrue(e.getMessage().contains("UNAUTHENTICATED"));
  }

  /**
   * Checks if the exception SQLException is thrown when trying to establish a connection without a
   * host.
   *
   * @throws SQLException on error.
   */
  @Test
  public void testUnencryptedConnectionWithEmptyHost() throws Exception {
    final Properties properties = new Properties();

    properties.put("user", userTest);
    properties.put("password", passTest);
    final String invalidUrl = "jdbc:arrow-flight-sql://";

    assertThrows(
        SQLException.class,
        () -> {
          try (Connection conn = DriverManager.getConnection(invalidUrl, properties)) {
            fail("Expected SQLException.");
          }
        });
  }

  /**
   * Try to instantiate a basic FlightClient.
   *
   * @throws URISyntaxException on error.
   */
  @Test
  public void testGetBasicClientAuthenticatedShouldOpenConnection() throws Exception {

    try (ArrowFlightSqlClientHandler client =
        new ArrowFlightSqlClientHandler.Builder()
            .withHost(FLIGHT_SERVER_TEST_EXTENSION.getHost())
            .withPort(FLIGHT_SERVER_TEST_EXTENSION.getPort())
            .withEncryption(false)
            .withUsername(userTest)
            .withPassword(passTest)
            .withBufferAllocator(allocator)
            .build()) {

      assertNotNull(client);
    }
  }

  /**
   * Checks if the exception IllegalArgumentException is thrown when trying to establish an
   * unencrypted connection providing with an invalid port.
   *
   * @throws SQLException on error.
   */
  @Test
  public void testUnencryptedConnectionProvidingInvalidPort() throws Exception {
    final Properties properties = new Properties();

    properties.put(ArrowFlightConnectionProperty.HOST.camelName(), "localhost");
    properties.put(ArrowFlightConnectionProperty.USER.camelName(), userTest);
    properties.put(ArrowFlightConnectionProperty.PASSWORD.camelName(), passTest);
    properties.put(ArrowFlightConnectionProperty.USE_ENCRYPTION.camelName(), false);
    final String invalidUrl =
        "jdbc:arrow-flight-sql://" + FLIGHT_SERVER_TEST_EXTENSION.getHost() + ":" + 65537;

    assertThrows(
        SQLException.class,
        () -> {
          try (Connection conn = DriverManager.getConnection(invalidUrl, properties)) {
            fail("Expected SQLException");
          }
        });
  }

  /**
   * Try to instantiate a basic FlightClient.
   *
   * @throws URISyntaxException on error.
   */
  @Test
  public void testGetBasicClientNoAuthShouldOpenConnection() throws Exception {

    try (ArrowFlightSqlClientHandler client =
        new ArrowFlightSqlClientHandler.Builder()
            .withHost(FLIGHT_SERVER_TEST_EXTENSION.getHost())
            .withBufferAllocator(allocator)
            .withEncryption(false)
            .build()) {
      assertNotNull(client);
    }
  }

  /**
   * Checks if an unencrypted connection can be established successfully when not providing
   * credentials.
   *
   * @throws SQLException on error.
   */
  @Test
  public void testUnencryptedConnectionShouldOpenSuccessfullyWithoutAuthentication()
      throws Exception {
    final Properties properties = new Properties();
    properties.put(ArrowFlightConnectionProperty.HOST.camelName(), "localhost");
    properties.put(
        ArrowFlightConnectionProperty.PORT.camelName(), FLIGHT_SERVER_TEST_EXTENSION.getPort());
    properties.put(ArrowFlightConnectionProperty.USE_ENCRYPTION.camelName(), false);
    try (Connection connection =
        DriverManager.getConnection("jdbc:arrow-flight-sql://localhost:32010", properties)) {
      assertTrue(connection.isValid(300));
    }
  }

  /**
   * Check if an unencrypted connection throws an exception when provided with invalid credentials.
   *
   * @throws SQLException The exception expected to be thrown.
   */
  @Test
  public void testUnencryptedConnectionShouldThrowExceptionWhenProvidedWithInvalidCredentials()
      throws Exception {

    final Properties properties = new Properties();

    properties.put(ArrowFlightConnectionProperty.HOST.camelName(), "localhost");
    properties.put(ArrowFlightConnectionProperty.USER.camelName(), "invalidUser");
    properties.put(
        ArrowFlightConnectionProperty.PORT.camelName(), FLIGHT_SERVER_TEST_EXTENSION.getPort());
    properties.put(ArrowFlightConnectionProperty.USE_ENCRYPTION.camelName(), false);
    properties.put(ArrowFlightConnectionProperty.PASSWORD.camelName(), "invalidPassword");

    assertThrows(
        SQLException.class,
        () -> {
          try (Connection ignored =
              DriverManager.getConnection("jdbc:arrow-flight-sql://localhost:32010", properties)) {
            fail();
          }
        });
  }

  /**
   * Check if an non-encrypted connection can be established successfully when connecting through
   * the DriverManager using just a connection url.
   *
   * @throws Exception on error.
   */
  @Test
  public void testTLSConnectionPropertyFalseCorrectCastUrlWithDriverManager() throws Exception {
    final Driver driver = new ArrowFlightJdbcDriver();
    DriverManager.registerDriver(driver);

    try (Connection connection =
        DriverManager.getConnection(
            String.format(
                "jdbc:arrow-flight-sql://localhost:%s?user=%s&password=%s&useEncryption=false",
                FLIGHT_SERVER_TEST_EXTENSION.getPort(), userTest, passTest))) {
      assertTrue(connection.isValid(0));
    }
  }

  /**
   * Check if an non-encrypted connection can be established successfully when connecting through
   * the DriverManager using a connection url and properties with String K-V pairs.
   *
   * @throws Exception on error.
   */
  @Test
  public void
      testTLSConnectionPropertyFalseCorrectCastUrlAndPropertiesUsingSetPropertyWithDriverManager()
          throws Exception {
    final Driver driver = new ArrowFlightJdbcDriver();
    DriverManager.registerDriver(driver);

    Properties properties = new Properties();

    properties.setProperty(ArrowFlightConnectionProperty.USER.camelName(), userTest);
    properties.setProperty(ArrowFlightConnectionProperty.PASSWORD.camelName(), passTest);
    properties.setProperty(ArrowFlightConnectionProperty.USE_ENCRYPTION.camelName(), "false");

    try (Connection connection =
        DriverManager.getConnection(
            String.format(
                "jdbc:arrow-flight-sql://localhost:%s", FLIGHT_SERVER_TEST_EXTENSION.getPort()),
            properties)) {
      assertTrue(connection.isValid(0));
    }
  }

  /**
   * Check if an non-encrypted connection can be established successfully when connecting through
   * the DriverManager using a connection url and properties with Object K-V pairs.
   *
   * @throws Exception on error.
   */
  @Test
  public void testTLSConnectionPropertyFalseCorrectCastUrlAndPropertiesUsingPutWithDriverManager()
      throws Exception {
    final Driver driver = new ArrowFlightJdbcDriver();
    DriverManager.registerDriver(driver);

    Properties properties = new Properties();
    properties.put(ArrowFlightConnectionProperty.USER.camelName(), userTest);
    properties.put(ArrowFlightConnectionProperty.PASSWORD.camelName(), passTest);
    properties.put(ArrowFlightConnectionProperty.USE_ENCRYPTION.camelName(), false);

    try (Connection connection =
        DriverManager.getConnection(
            String.format(
                "jdbc:arrow-flight-sql://localhost:%s", FLIGHT_SERVER_TEST_EXTENSION.getPort()),
            properties)) {
      assertTrue(connection.isValid(0));
    }
  }

  /**
   * Check if an non-encrypted connection can be established successfully when connecting through
   * the DriverManager using just a connection url and using 0 and 1 as ssl values.
   *
   * @throws Exception on error.
   */
  @Test
  public void testTLSConnectionPropertyFalseIntegerCorrectCastUrlWithDriverManager()
      throws Exception {
    final Driver driver = new ArrowFlightJdbcDriver();
    DriverManager.registerDriver(driver);

    try (Connection connection =
        DriverManager.getConnection(
            String.format(
                "jdbc:arrow-flight-sql://localhost:%s?user=%s&password=%s&useEncryption=0",
                FLIGHT_SERVER_TEST_EXTENSION.getPort(), userTest, passTest))) {
      assertTrue(connection.isValid(0));
    }
  }

  /**
   * Check if an non-encrypted connection can be established successfully when connecting through
   * the DriverManager using a connection url and properties with String K-V pairs and using 0 and 1
   * as ssl values.
   *
   * @throws Exception on error.
   */
  @Test
  public void
      testTLSConnectionPropertyFalseIntegerCorrectCastUrlAndPropertiesUsingSetPropertyWithDriverManager()
          throws Exception {
    final Driver driver = new ArrowFlightJdbcDriver();
    DriverManager.registerDriver(driver);
    Properties properties = new Properties();

    properties.setProperty(ArrowFlightConnectionProperty.USER.camelName(), userTest);
    properties.setProperty(ArrowFlightConnectionProperty.PASSWORD.camelName(), passTest);
    properties.setProperty(ArrowFlightConnectionProperty.USE_ENCRYPTION.camelName(), "0");

    try (Connection connection =
        DriverManager.getConnection(
            String.format(
                "jdbc:arrow-flight-sql://localhost:%s", FLIGHT_SERVER_TEST_EXTENSION.getPort()),
            properties)) {
      assertTrue(connection.isValid(0));
    }
  }

  /**
   * Check if an non-encrypted connection can be established successfully when connecting through
   * the DriverManager using a connection url and properties with Object K-V pairs and using 0 and 1
   * as ssl values.
   *
   * @throws Exception on error.
   */
  @Test
  public void
      testTLSConnectionPropertyFalseIntegerCorrectCastUrlAndPropertiesUsingPutWithDriverManager()
          throws Exception {
    final Driver driver = new ArrowFlightJdbcDriver();
    DriverManager.registerDriver(driver);

    Properties properties = new Properties();
    properties.put(ArrowFlightConnectionProperty.USER.camelName(), userTest);
    properties.put(ArrowFlightConnectionProperty.PASSWORD.camelName(), passTest);
    properties.put(ArrowFlightConnectionProperty.USE_ENCRYPTION.camelName(), 0);

    try (Connection connection =
        DriverManager.getConnection(
            String.format(
                "jdbc:arrow-flight-sql://localhost:%s", FLIGHT_SERVER_TEST_EXTENSION.getPort()),
            properties)) {
      assertTrue(connection.isValid(0));
    }
  }

  /**
   * Check if an non-encrypted connection can be established successfully when connecting through
   * the DriverManager using just a connection url.
   *
   * @throws Exception on error.
   */
  @Test
  public void testThreadPoolSizeConnectionPropertyCorrectCastUrlWithDriverManager()
      throws Exception {
    final Driver driver = new ArrowFlightJdbcDriver();
    DriverManager.registerDriver(driver);

    try (Connection connection =
        DriverManager.getConnection(
            String.format(
                "jdbc:arrow-flight-sql://localhost:%s?user=%s&password=%s&threadPoolSize=1&useEncryption=%s",
                FLIGHT_SERVER_TEST_EXTENSION.getPort(), userTest, passTest, false))) {
      assertTrue(connection.isValid(0));
    }
  }

  /**
   * Check if an non-encrypted connection can be established successfully when connecting through
   * the DriverManager using a connection url and properties with String K-V pairs and using 0 and 1
   * as ssl values.
   *
   * @throws Exception on error.
   */
  @Test
  public void
      testThreadPoolSizeConnectionPropertyCorrectCastUrlAndPropertiesUsingSetPropertyWithDriverManager()
          throws Exception {
    final Driver driver = new ArrowFlightJdbcDriver();
    DriverManager.registerDriver(driver);
    Properties properties = new Properties();

    properties.setProperty(ArrowFlightConnectionProperty.USER.camelName(), userTest);
    properties.setProperty(ArrowFlightConnectionProperty.PASSWORD.camelName(), passTest);
    properties.setProperty(ArrowFlightConnectionProperty.THREAD_POOL_SIZE.camelName(), "1");
    properties.put("useEncryption", false);

    try (Connection connection =
        DriverManager.getConnection(
            String.format(
                "jdbc:arrow-flight-sql://localhost:%s", FLIGHT_SERVER_TEST_EXTENSION.getPort()),
            properties)) {
      assertTrue(connection.isValid(0));
    }
  }

  /**
   * Check if an non-encrypted connection can be established successfully when connecting through
   * the DriverManager using a connection url and properties with Object K-V pairs and using 0 and 1
   * as ssl values.
   *
   * @throws Exception on error.
   */
  @Test
  public void
      testThreadPoolSizeConnectionPropertyCorrectCastUrlAndPropertiesUsingPutWithDriverManager()
          throws Exception {
    final Driver driver = new ArrowFlightJdbcDriver();
    DriverManager.registerDriver(driver);

    Properties properties = new Properties();
    properties.put(ArrowFlightConnectionProperty.USER.camelName(), userTest);
    properties.put(ArrowFlightConnectionProperty.PASSWORD.camelName(), passTest);
    properties.put(ArrowFlightConnectionProperty.THREAD_POOL_SIZE.camelName(), 1);
    properties.put("useEncryption", false);

    try (Connection connection =
        DriverManager.getConnection(
            String.format(
                "jdbc:arrow-flight-sql://localhost:%s", FLIGHT_SERVER_TEST_EXTENSION.getPort()),
            properties)) {
      assertTrue(connection.isValid(0));
    }
  }

  /**
   * Check if an non-encrypted connection can be established successfully when connecting through
   * the DriverManager using just a connection url.
   *
   * @throws Exception on error.
   */
  @Test
  public void testPasswordConnectionPropertyIntegerCorrectCastUrlWithDriverManager()
      throws Exception {
    final Driver driver = new ArrowFlightJdbcDriver();
    DriverManager.registerDriver(driver);

    try (Connection connection =
        DriverManager.getConnection(
            String.format(
                "jdbc:arrow-flight-sql://localhost:%s?user=%s&password=%s&useEncryption=%s",
                FLIGHT_SERVER_TEST_EXTENSION.getPort(), userTest, passTest, false))) {
      assertTrue(connection.isValid(0));
    }
  }

  /**
   * Check if an non-encrypted connection can be established successfully when connecting through
   * the DriverManager using a connection url and properties with String K-V pairs and using 0 and 1
   * as ssl values.
   *
   * @throws Exception on error.
   */
  @Test
  public void
      testPasswordConnectionPropertyIntegerCorrectCastUrlAndPropertiesUsingSetPropertyWithDriverManager()
          throws Exception {
    final Driver driver = new ArrowFlightJdbcDriver();
    DriverManager.registerDriver(driver);
    Properties properties = new Properties();

    properties.setProperty(ArrowFlightConnectionProperty.USER.camelName(), userTest);
    properties.setProperty(ArrowFlightConnectionProperty.PASSWORD.camelName(), passTest);
    properties.put("useEncryption", false);

    try (Connection connection =
        DriverManager.getConnection(
            String.format(
                "jdbc:arrow-flight-sql://localhost:%s", FLIGHT_SERVER_TEST_EXTENSION.getPort()),
            properties)) {
      assertTrue(connection.isValid(0));
    }
  }

  /**
   * Check if an non-encrypted connection can be established successfully when connecting through
   * the DriverManager using a connection url and properties with Object K-V pairs and using 0 and 1
   * as ssl values.
   *
   * @throws Exception on error.
   */
  @Test
  public void
      testPasswordConnectionPropertyIntegerCorrectCastUrlAndPropertiesUsingPutWithDriverManager()
          throws Exception {
    final Driver driver = new ArrowFlightJdbcDriver();
    DriverManager.registerDriver(driver);

    Properties properties = new Properties();
    properties.put(ArrowFlightConnectionProperty.USER.camelName(), userTest);
    properties.put(ArrowFlightConnectionProperty.PASSWORD.camelName(), passTest);
    properties.put("useEncryption", false);

    try (Connection connection =
        DriverManager.getConnection(
            String.format(
                "jdbc:arrow-flight-sql://localhost:%s", FLIGHT_SERVER_TEST_EXTENSION.getPort()),
            properties)) {
      assertTrue(connection.isValid(0));
    }
  }
}
