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
import java.util.Collection;
import java.util.Properties;

import org.apache.arrow.driver.jdbc.adhoc.MockFlightSqlProducer;
import org.apache.arrow.driver.jdbc.authentication.UserPasswordAuthentication;
import org.apache.arrow.driver.jdbc.client.ArrowFlightSqlClientHandler;
import org.apache.arrow.driver.jdbc.utils.ArrowFlightConnectionConfigImpl.ArrowFlightConnectionProperty;
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

  static {
    UserPasswordAuthentication authentication =
        new UserPasswordAuthentication.Builder().user("user1", "pass1").user("user2", "pass2")
            .build();

    FLIGHT_SERVER_TEST_RULE = new FlightServerTestRule.Builder().host("localhost").randomPort()
        .authentication(authentication).producer(PRODUCER).build();
  }

  private BufferAllocator allocator;
  private ArrowFlightJdbcConnectionPoolDataSource dataSource;

  @Before
  public void setUp() throws Exception {
    allocator = new RootAllocator(Long.MAX_VALUE);
    dataSource = FLIGHT_SERVER_TEST_RULE.createConnectionPoolDataSource();
  }

  @After
  public void tearDown() throws Exception {
    Collection<BufferAllocator> childAllocators = allocator.getChildAllocators();
    AutoCloseables.close(childAllocators.toArray(new AutoCloseable[0]));
    AutoCloseables.close(dataSource, allocator);
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
        dataSource.getConfig().getPort());
    properties.put(ArrowFlightConnectionProperty.USER.camelName(),
        dataSource.getConfig().getUser());
    properties.put(ArrowFlightConnectionProperty.PASSWORD.camelName(),
        dataSource.getConfig().getPassword());

    try (Connection connection = DriverManager.getConnection(
        "jdbc:arrow-flight://" + dataSource.getConfig().getHost() + ":" +
            dataSource.getConfig().getPort(), properties)) {
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

    properties.put("user", dataSource.getConfig().getUser());
    properties.put("password", dataSource.getConfig().getPassword());
    final String invalidUrl = "jdbc:arrow-flight://";

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
                 .withHost(dataSource.getConfig().getHost())
                 .withPort(dataSource.getConfig().getPort())
                 .withUsername(dataSource.getConfig().getUser())
                 .withPassword(dataSource.getConfig().getPassword())
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
        dataSource.getConfig().getUser());
    properties.put(ArrowFlightConnectionProperty.PASSWORD.camelName(),
        dataSource.getConfig().getPassword());
    final String invalidUrl =
        dataSource.getConfig().getPort() + dataSource.getConfig().getHost() + ":" + 65537;

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
                 .withHost(dataSource.getConfig().getHost())
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
        dataSource.getConfig().getPort());
    try (Connection connection = DriverManager
        .getConnection("jdbc:arrow-flight://localhost:32010", properties)) {
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
    properties.put(ArrowFlightConnectionProperty.PORT.camelName(),
        dataSource.getConfig().getPort());
    properties.put(ArrowFlightConnectionProperty.USER.camelName(),
        "invalidUser");
    properties.put(ArrowFlightConnectionProperty.PASSWORD.camelName(),
        "invalidPassword");

    try (Connection connection = DriverManager.getConnection("jdbc:arrow-flight://localhost:32010",
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

    Assert.assertTrue(DriverManager.getConnection(
            String.format(
                "jdbc:arrow-flight://localhost:%s?user=%s&password=%s&useTls=false",
                dataSource.getConfig().getPort(),
                dataSource.getConfig().getUser(),
                dataSource.getConfig().getPassword()))
        .isValid(0));
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
        dataSource.getConfig().getUser());
    properties.setProperty(ArrowFlightConnectionProperty.PASSWORD.camelName(),
        dataSource.getConfig().getPassword());
    properties.setProperty(ArrowFlightConnectionProperty.USE_TLS.camelName(), "false");

    Assert.assertTrue(DriverManager.getConnection(
        String.format(
            "jdbc:arrow-flight://localhost:%s",
            dataSource.getConfig().getPort()),
        properties).isValid(0));
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
        dataSource.getConfig().getUser());
    properties.put(ArrowFlightConnectionProperty.PASSWORD.camelName(),
        dataSource.getConfig().getPassword());
    properties.put(ArrowFlightConnectionProperty.USE_TLS.camelName(), false);

    Assert.assertTrue(DriverManager.getConnection(
        String.format(
            "jdbc:arrow-flight://localhost:%s",
            dataSource.getConfig().getPort()),
        properties).isValid(0));
  }

  /**
   * Check if an non-encrypted connection can be established successfully when connecting through
   * the DriverManager using just a connection url and using 0 and 1 as useTls values.
   *
   * @throws Exception on error.
   */
  @Test
  public void testTLSConnectionPropertyFalseIntegerCorrectCastUrlWithDriverManager()
      throws Exception {
    final Driver driver = new ArrowFlightJdbcDriver();
    DriverManager.registerDriver(driver);

    Assert.assertTrue(DriverManager.getConnection(
            String.format(
                "jdbc:arrow-flight://localhost:%s?user=%s&password=%s&useTls=0",
                dataSource.getConfig().getPort(),
                dataSource.getConfig().getUser(),
                dataSource.getConfig().getPassword()))
        .isValid(0));
  }

  /**
   * Check if an non-encrypted connection can be established successfully when connecting through
   * the DriverManager using a connection url and properties with String K-V pairs and using
   * 0 and 1 as useTls values.
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
        dataSource.getConfig().getUser());
    properties.setProperty(ArrowFlightConnectionProperty.PASSWORD.camelName(),
        dataSource.getConfig().getPassword());
    properties.setProperty(ArrowFlightConnectionProperty.USE_TLS.camelName(), "0");

    Assert.assertTrue(DriverManager.getConnection(
        String.format(
            "jdbc:arrow-flight://localhost:%s",
            dataSource.getConfig().getPort()),
        properties).isValid(0));
  }

  /**
   * Check if an non-encrypted connection can be established successfully when connecting through
   * the DriverManager using a connection url and properties with Object K-V pairs and using
   * 0 and 1 as useTls values.
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
        dataSource.getConfig().getUser());
    properties.put(ArrowFlightConnectionProperty.PASSWORD.camelName(),
        dataSource.getConfig().getPassword());
    properties.put(ArrowFlightConnectionProperty.USE_TLS.camelName(), 0);

    Assert.assertTrue(DriverManager.getConnection(
        String.format(
            "jdbc:arrow-flight://localhost:%s",
            dataSource.getConfig().getPort()),
        properties).isValid(0));
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

    Assert.assertTrue(DriverManager.getConnection(
            String.format(
                "jdbc:arrow-flight://localhost:%s?user=%s&password=%s&threadPoolSize=1",
                dataSource.getConfig().getPort(),
                dataSource.getConfig().getUser(),
                dataSource.getConfig().getPassword()))
        .isValid(0));
  }

  /**
   * Check if an non-encrypted connection can be established successfully when connecting through
   * the DriverManager using a connection url and properties with String K-V pairs and using
   * 0 and 1 as useTls values.
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
        dataSource.getConfig().getUser());
    properties.setProperty(ArrowFlightConnectionProperty.PASSWORD.camelName(),
        dataSource.getConfig().getPassword());
    properties.setProperty(ArrowFlightConnectionProperty.THREAD_POOL_SIZE.camelName(), "1");

    Assert.assertTrue(DriverManager.getConnection(
        String.format(
            "jdbc:arrow-flight://localhost:%s",
            dataSource.getConfig().getPort()),
        properties).isValid(0));
  }

  /**
   * Check if an non-encrypted connection can be established successfully when connecting through
   * the DriverManager using a connection url and properties with Object K-V pairs and using
   * 0 and 1 as useTls values.
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
        dataSource.getConfig().getUser());
    properties.put(ArrowFlightConnectionProperty.PASSWORD.camelName(),
        dataSource.getConfig().getPassword());
    properties.put(ArrowFlightConnectionProperty.THREAD_POOL_SIZE.camelName(), 1);

    Assert.assertTrue(DriverManager.getConnection(
        String.format(
            "jdbc:arrow-flight://localhost:%s",
            dataSource.getConfig().getPort()),
        properties).isValid(0));
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

    Assert.assertTrue(DriverManager.getConnection(
            String.format(
                "jdbc:arrow-flight://localhost:%s?user=%s&password=%s",
                dataSource.getConfig().getPort(),
                dataSource.getConfig().getUser(),
                dataSource.getConfig().getPassword()))
        .isValid(0));
  }

  /**
   * Check if an non-encrypted connection can be established successfully when connecting through
   * the DriverManager using a connection url and properties with String K-V pairs and using
   * 0 and 1 as useTls values.
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
        dataSource.getConfig().getUser());
    properties.setProperty(ArrowFlightConnectionProperty.PASSWORD.camelName(),
        dataSource.getConfig().getPassword());

    Assert.assertTrue(DriverManager.getConnection(
        String.format(
            "jdbc:arrow-flight://localhost:%s",
            dataSource.getConfig().getPort()),
        properties).isValid(0));
  }

  /**
   * Check if an non-encrypted connection can be established successfully when connecting through
   * the DriverManager using a connection url and properties with Object K-V pairs and using
   * 0 and 1 as useTls values.
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
        dataSource.getConfig().getUser());
    properties.put(ArrowFlightConnectionProperty.PASSWORD.camelName(),
        dataSource.getConfig().getPassword());

    Assert.assertTrue(DriverManager.getConnection(
        String.format(
            "jdbc:arrow-flight://localhost:%s",
            dataSource.getConfig().getPort()),
        properties).isValid(0));
  }
}
