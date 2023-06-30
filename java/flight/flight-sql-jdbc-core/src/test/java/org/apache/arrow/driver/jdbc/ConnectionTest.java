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

import static org.junit.Assert.assertNotNull;

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
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;

/**
 * Tests for {@link Connection}.
 */
public class ConnectionTest {

  @ClassRule
  public static final FlightServerTestRule FLIGHT_SERVER_TEST_RULE;
  private static final MockFlightSqlProducer PRODUCER = new MockFlightSqlProducer();
  private static final String userTest = "user1";
  private static final String passTest = "pass1";

  static {
    UserPasswordAuthentication authentication =
        new UserPasswordAuthentication.Builder()
            .user(userTest, passTest)
            .build();

    FLIGHT_SERVER_TEST_RULE = new FlightServerTestRule.Builder()
        .authentication(authentication)
        .producer(PRODUCER)
        .build();
  }

  private BufferAllocator allocator;

  @Before
  public void setUp() throws Exception {
    allocator = new RootAllocator(Long.MAX_VALUE);
  }

  @After
  public void tearDown() throws Exception {
    allocator.getChildAllocators().forEach(BufferAllocator::close);
    AutoCloseables.close(allocator);
  }

  /**
   * Checks if an unencrypted connection can be established successfully when
   * the provided valid credentials.
   *
   * @throws SQLException on error.
   */
  @Test
  public void testUnencryptedConnectionShouldOpenSuccessfullyWhenProvidedValidCredentials()
      throws Exception {
    final Properties properties = new Properties();

    properties.put(ArrowFlightConnectionProperty.HOST.camelName(), "localhost");
    properties.put(ArrowFlightConnectionProperty.PORT.camelName(),
        FLIGHT_SERVER_TEST_RULE.getPort());
    properties.put(ArrowFlightConnectionProperty.USER.camelName(),
        userTest);
    properties.put(ArrowFlightConnectionProperty.PASSWORD.camelName(),
        passTest);
    properties.put("useEncryption", false);

    try (Connection connection = DriverManager.getConnection(
        "jdbc:arrow-flight-sql://" + FLIGHT_SERVER_TEST_RULE.getHost() + ":" +
            FLIGHT_SERVER_TEST_RULE.getPort(), properties)) {
      assert connection.isValid(300);
    }
  }

  /**
   * Checks if the exception SQLException is thrown when trying to establish a connection without a host.
   *
   * @throws SQLException on error.
   */
  @Test(expected = SQLException.class)
  public void testUnencryptedConnectionWithEmptyHost()
      throws Exception {
    final Properties properties = new Properties();

    properties.put("user", userTest);
    properties.put("password", passTest);
    final String invalidUrl = "jdbc:arrow-flight-sql://";

    DriverManager.getConnection(invalidUrl, properties);
  }

  /**
   * Try to instantiate a basic FlightClient.
   *
   * @throws URISyntaxException on error.
   */
  @Test
  public void testGetBasicClientAuthenticatedShouldOpenConnection()
      throws Exception {

    try (ArrowFlightSqlClientHandler client =
             new ArrowFlightSqlClientHandler.Builder()
                 .withHost(FLIGHT_SERVER_TEST_RULE.getHost())
                 .withPort(FLIGHT_SERVER_TEST_RULE.getPort())
                 .withUsername(userTest)
                 .withPassword(passTest)
                 .withBufferAllocator(allocator)
                 .build()) {
      assertNotNull(client);
    }
  }

  /**
   * Checks if the exception IllegalArgumentException is thrown when trying to establish an  unencrypted
   * connection providing with an invalid port.
   *
   * @throws SQLException on error.
   */
  @Test(expected = SQLException.class)
  public void testUnencryptedConnectionProvidingInvalidPort()
      throws Exception {
    final Properties properties = new Properties();

    properties.put(ArrowFlightConnectionProperty.HOST.camelName(), "localhost");
    properties.put(ArrowFlightConnectionProperty.USER.camelName(),
        userTest);
    properties.put(ArrowFlightConnectionProperty.PASSWORD.camelName(),
        passTest);
    properties.put(ArrowFlightConnectionProperty.USE_ENCRYPTION.camelName(),
        false);
    final String invalidUrl = "jdbc:arrow-flight-sql://" + FLIGHT_SERVER_TEST_RULE.getHost() +
        ":" + 65537;

    DriverManager.getConnection(invalidUrl, properties);
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
                 .withHost(FLIGHT_SERVER_TEST_RULE.getHost())
                 .withBufferAllocator(allocator)
                 .build()) {
      assertNotNull(client);
    }
  }

  /**
   * Checks if an unencrypted connection can be established successfully when
   * not providing credentials.
   *
   * @throws SQLException on error.
   */
  @Test
  public void testUnencryptedConnectionShouldOpenSuccessfullyWithoutAuthentication()
      throws Exception {
    final Properties properties = new Properties();
    properties.put(ArrowFlightConnectionProperty.HOST.camelName(), "localhost");
    properties.put(ArrowFlightConnectionProperty.PORT.camelName(),
        FLIGHT_SERVER_TEST_RULE.getPort());
    properties.put(ArrowFlightConnectionProperty.USE_ENCRYPTION.camelName(),
        false);
    try (Connection connection = DriverManager
        .getConnection("jdbc:arrow-flight-sql://localhost:32010", properties)) {
      assert connection.isValid(300);
    }
  }

  /**
   * Check if an unencrypted connection throws an exception when provided with
   * invalid credentials.
   *
   * @throws SQLException The exception expected to be thrown.
   */
  @Test(expected = SQLException.class)
  public void testUnencryptedConnectionShouldThrowExceptionWhenProvidedWithInvalidCredentials()
      throws Exception {

    final Properties properties = new Properties();

    properties.put(ArrowFlightConnectionProperty.HOST.camelName(), "localhost");
    properties.put(ArrowFlightConnectionProperty.USER.camelName(),
        "invalidUser");
    properties.put(ArrowFlightConnectionProperty.PORT.camelName(),
        FLIGHT_SERVER_TEST_RULE.getPort());
    properties.put(ArrowFlightConnectionProperty.USE_ENCRYPTION.camelName(),
        false);
    properties.put(ArrowFlightConnectionProperty.PASSWORD.camelName(),
        "invalidPassword");

    try (Connection ignored = DriverManager.getConnection("jdbc:arrow-flight-sql://localhost:32010",
        properties)) {
      Assert.fail();
    }
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

    Connection connection = DriverManager.getConnection(
        String.format(
            "jdbc:arrow-flight-sql://localhost:%s?user=%s&password=%s&useEncryption=false",
            FLIGHT_SERVER_TEST_RULE.getPort(),
            userTest,
            passTest));
    Assert.assertTrue(connection.isValid(0));
    connection.close();
  }

  /**
   * Check if an non-encrypted connection can be established successfully when connecting through
   * the DriverManager using a connection url and properties with String K-V pairs.
   *
   * @throws Exception on error.
   */
  @Test
  public void testTLSConnectionPropertyFalseCorrectCastUrlAndPropertiesUsingSetPropertyWithDriverManager()
      throws Exception {
    final Driver driver = new ArrowFlightJdbcDriver();
    DriverManager.registerDriver(driver);

    Properties properties = new Properties();

    properties.setProperty(ArrowFlightConnectionProperty.USER.camelName(),
        userTest);
    properties.setProperty(ArrowFlightConnectionProperty.PASSWORD.camelName(),
        passTest);
    properties.setProperty(ArrowFlightConnectionProperty.USE_ENCRYPTION.camelName(), "false");

    Connection connection = DriverManager.getConnection(
        String.format(
            "jdbc:arrow-flight-sql://localhost:%s",
            FLIGHT_SERVER_TEST_RULE.getPort()),
        properties);
    Assert.assertTrue(connection.isValid(0));
    connection.close();
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
    properties.put(ArrowFlightConnectionProperty.USER.camelName(),
        userTest);
    properties.put(ArrowFlightConnectionProperty.PASSWORD.camelName(),
        passTest);
    properties.put(ArrowFlightConnectionProperty.USE_ENCRYPTION.camelName(), false);

    Connection connection = DriverManager.getConnection(
        String.format(
            "jdbc:arrow-flight-sql://localhost:%s",
            FLIGHT_SERVER_TEST_RULE.getPort()),
        properties);
    Assert.assertTrue(connection.isValid(0));
    connection.close();
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

    Connection connection = DriverManager.getConnection(
        String.format(
            "jdbc:arrow-flight-sql://localhost:%s?user=%s&password=%s&useEncryption=0",
            FLIGHT_SERVER_TEST_RULE.getPort(),
            userTest,
            passTest));
    Assert.assertTrue(connection.isValid(0));
    connection.close();
  }

  /**
   * Check if an non-encrypted connection can be established successfully when connecting through
   * the DriverManager using a connection url and properties with String K-V pairs and using
   * 0 and 1 as ssl values.
   *
   * @throws Exception on error.
   */
  @Test
  public void testTLSConnectionPropertyFalseIntegerCorrectCastUrlAndPropertiesUsingSetPropertyWithDriverManager()
      throws Exception {
    final Driver driver = new ArrowFlightJdbcDriver();
    DriverManager.registerDriver(driver);
    Properties properties = new Properties();

    properties.setProperty(ArrowFlightConnectionProperty.USER.camelName(),
        userTest);
    properties.setProperty(ArrowFlightConnectionProperty.PASSWORD.camelName(),
        passTest);
    properties.setProperty(ArrowFlightConnectionProperty.USE_ENCRYPTION.camelName(), "0");

    Connection connection = DriverManager.getConnection(
        String.format(
            "jdbc:arrow-flight-sql://localhost:%s",
            FLIGHT_SERVER_TEST_RULE.getPort()),
        properties);
    Assert.assertTrue(connection.isValid(0));
    connection.close();
  }

  /**
   * Check if an non-encrypted connection can be established successfully when connecting through
   * the DriverManager using a connection url and properties with Object K-V pairs and using
   * 0 and 1 as ssl values.
   *
   * @throws Exception on error.
   */
  @Test
  public void testTLSConnectionPropertyFalseIntegerCorrectCastUrlAndPropertiesUsingPutWithDriverManager()
      throws Exception {
    final Driver driver = new ArrowFlightJdbcDriver();
    DriverManager.registerDriver(driver);

    Properties properties = new Properties();
    properties.put(ArrowFlightConnectionProperty.USER.camelName(),
        userTest);
    properties.put(ArrowFlightConnectionProperty.PASSWORD.camelName(),
        passTest);
    properties.put(ArrowFlightConnectionProperty.USE_ENCRYPTION.camelName(), 0);

    Connection connection = DriverManager.getConnection(
        String.format(
            "jdbc:arrow-flight-sql://localhost:%s",
            FLIGHT_SERVER_TEST_RULE.getPort()),
        properties);
    Assert.assertTrue(connection.isValid(0));
    connection.close();
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

    Connection connection = DriverManager.getConnection(
        String.format(
            "jdbc:arrow-flight-sql://localhost:%s?user=%s&password=%s&threadPoolSize=1&useEncryption=%s",
            FLIGHT_SERVER_TEST_RULE.getPort(),
            userTest,
            passTest,
            false));
    Assert.assertTrue(connection.isValid(0));
    connection.close();
  }

  /**
   * Check if an non-encrypted connection can be established successfully when connecting through
   * the DriverManager using a connection url and properties with String K-V pairs and using
   * 0 and 1 as ssl values.
   *
   * @throws Exception on error.
   */
  @Test
  public void testThreadPoolSizeConnectionPropertyCorrectCastUrlAndPropertiesUsingSetPropertyWithDriverManager()
      throws Exception {
    final Driver driver = new ArrowFlightJdbcDriver();
    DriverManager.registerDriver(driver);
    Properties properties = new Properties();

    properties.setProperty(ArrowFlightConnectionProperty.USER.camelName(),
        userTest);
    properties.setProperty(ArrowFlightConnectionProperty.PASSWORD.camelName(),
        passTest);
    properties.setProperty(ArrowFlightConnectionProperty.THREAD_POOL_SIZE.camelName(), "1");
    properties.put("useEncryption", false);

    Connection connection = DriverManager.getConnection(
        String.format(
            "jdbc:arrow-flight-sql://localhost:%s",
            FLIGHT_SERVER_TEST_RULE.getPort()),
        properties);
    Assert.assertTrue(connection.isValid(0));
    connection.close();
  }

  /**
   * Check if an non-encrypted connection can be established successfully when connecting through
   * the DriverManager using a connection url and properties with Object K-V pairs and using
   * 0 and 1 as ssl values.
   *
   * @throws Exception on error.
   */
  @Test
  public void testThreadPoolSizeConnectionPropertyCorrectCastUrlAndPropertiesUsingPutWithDriverManager()
      throws Exception {
    final Driver driver = new ArrowFlightJdbcDriver();
    DriverManager.registerDriver(driver);

    Properties properties = new Properties();
    properties.put(ArrowFlightConnectionProperty.USER.camelName(),
        userTest);
    properties.put(ArrowFlightConnectionProperty.PASSWORD.camelName(),
        passTest);
    properties.put(ArrowFlightConnectionProperty.THREAD_POOL_SIZE.camelName(), 1);
    properties.put("useEncryption", false);

    Connection connection = DriverManager.getConnection(
        String.format(
            "jdbc:arrow-flight-sql://localhost:%s",
            FLIGHT_SERVER_TEST_RULE.getPort()),
        properties);
    Assert.assertTrue(connection.isValid(0));
    connection.close();
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

    Connection connection = DriverManager.getConnection(
        String.format(
            "jdbc:arrow-flight-sql://localhost:%s?user=%s&password=%s&useEncryption=%s",
            FLIGHT_SERVER_TEST_RULE.getPort(),
            userTest,
            passTest,
            false));
    Assert.assertTrue(connection.isValid(0));
    connection.close();
  }

  /**
   * Check if an non-encrypted connection can be established successfully when connecting through
   * the DriverManager using a connection url and properties with String K-V pairs and using
   * 0 and 1 as ssl values.
   *
   * @throws Exception on error.
   */
  @Test
  public void testPasswordConnectionPropertyIntegerCorrectCastUrlAndPropertiesUsingSetPropertyWithDriverManager()
      throws Exception {
    final Driver driver = new ArrowFlightJdbcDriver();
    DriverManager.registerDriver(driver);
    Properties properties = new Properties();

    properties.setProperty(ArrowFlightConnectionProperty.USER.camelName(),
        userTest);
    properties.setProperty(ArrowFlightConnectionProperty.PASSWORD.camelName(),
        passTest);
    properties.put("useEncryption", false);

    Connection connection = DriverManager.getConnection(
        String.format(
            "jdbc:arrow-flight-sql://localhost:%s",
            FLIGHT_SERVER_TEST_RULE.getPort()),
        properties);
    Assert.assertTrue(connection.isValid(0));
    connection.close();
  }

  /**
   * Check if an non-encrypted connection can be established successfully when connecting through
   * the DriverManager using a connection url and properties with Object K-V pairs and using
   * 0 and 1 as ssl values.
   *
   * @throws Exception on error.
   */
  @Test
  public void testPasswordConnectionPropertyIntegerCorrectCastUrlAndPropertiesUsingPutWithDriverManager()
      throws Exception {
    final Driver driver = new ArrowFlightJdbcDriver();
    DriverManager.registerDriver(driver);

    Properties properties = new Properties();
    properties.put(ArrowFlightConnectionProperty.USER.camelName(),
        userTest);
    properties.put(ArrowFlightConnectionProperty.PASSWORD.camelName(),
        passTest);
    properties.put("useEncryption", false);

    Connection connection = DriverManager.getConnection(
        String.format(
            "jdbc:arrow-flight-sql://localhost:%s",
            FLIGHT_SERVER_TEST_RULE.getPort()),
        properties);
    Assert.assertTrue(connection.isValid(0));
    connection.close();
  }
}
