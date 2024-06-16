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

import java.net.URLEncoder;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;
import org.apache.arrow.driver.jdbc.authentication.UserPasswordAuthentication;
import org.apache.arrow.driver.jdbc.client.ArrowFlightSqlClientHandler;
import org.apache.arrow.driver.jdbc.utils.ArrowFlightConnectionConfigImpl.ArrowFlightConnectionProperty;
import org.apache.arrow.driver.jdbc.utils.FlightSqlTestCertificates;
import org.apache.arrow.driver.jdbc.utils.MockFlightSqlProducer;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.util.AutoCloseables;
import org.apache.arrow.util.Preconditions;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

/** Tests encrypted connections. */
public class ConnectionTlsTest {

  @RegisterExtension public static final FlightServerTestExtension FLIGHT_SERVER_TEST_EXTENSION;
  private static final MockFlightSqlProducer PRODUCER = new MockFlightSqlProducer();
  private static final String userTest = "user1";
  private static final String passTest = "pass1";

  static {
    final FlightSqlTestCertificates.CertKeyPair certKey =
        FlightSqlTestCertificates.exampleTlsCerts().get(0);

    UserPasswordAuthentication authentication =
        new UserPasswordAuthentication.Builder().user(userTest, passTest).build();

    FLIGHT_SERVER_TEST_EXTENSION =
        new FlightServerTestExtension.Builder()
            .authentication(authentication)
            .useEncryption(certKey.cert, certKey.key)
            .producer(PRODUCER)
            .build();
  }

  private String trustStorePath;
  private String noCertificateKeyStorePath;
  private final String trustStorePass = "flight";
  private BufferAllocator allocator;

  @BeforeEach
  public void setUp() throws Exception {
    trustStorePath =
        Paths.get(Preconditions.checkNotNull(getClass().getResource("/keys/keyStore.jks")).toURI())
            .toString();
    noCertificateKeyStorePath =
        Paths.get(
                Preconditions.checkNotNull(getClass().getResource("/keys/noCertificate.jks"))
                    .toURI())
            .toString();
    allocator = new RootAllocator(Long.MAX_VALUE);
  }

  @AfterEach
  public void tearDown() throws Exception {
    allocator.getChildAllocators().forEach(BufferAllocator::close);
    AutoCloseables.close(allocator);
  }

  /**
   * Try to instantiate an encrypted FlightClient.
   *
   * @throws Exception on error.
   */
  @Test
  public void testGetEncryptedClientAuthenticatedWithDisableCertVerification() throws Exception {

    try (ArrowFlightSqlClientHandler client =
        new ArrowFlightSqlClientHandler.Builder()
            .withHost(FLIGHT_SERVER_TEST_EXTENSION.getHost())
            .withPort(FLIGHT_SERVER_TEST_EXTENSION.getPort())
            .withUsername(userTest)
            .withPassword(passTest)
            .withDisableCertificateVerification(true)
            .withBufferAllocator(allocator)
            .withEncryption(true)
            .build()) {
      assertNotNull(client);
    }
  }

  /**
   * Try to instantiate an encrypted FlightClient.
   *
   * @throws Exception on error.
   */
  @Test
  public void testGetEncryptedClientAuthenticated() throws Exception {

    try (ArrowFlightSqlClientHandler client =
        new ArrowFlightSqlClientHandler.Builder()
            .withHost(FLIGHT_SERVER_TEST_EXTENSION.getHost())
            .withPort(FLIGHT_SERVER_TEST_EXTENSION.getPort())
            .withSystemTrustStore(false)
            .withUsername(userTest)
            .withPassword(passTest)
            .withTrustStorePath(trustStorePath)
            .withTrustStorePassword(trustStorePass)
            .withBufferAllocator(allocator)
            .withEncryption(true)
            .build()) {
      assertNotNull(client);
    }
  }

  /**
   * Try to instantiate an encrypted FlightClient providing a keystore without certificate. It's
   * expected to receive the SQLException.
   *
   * @throws Exception on error.
   */
  @Test
  public void testGetEncryptedClientWithNoCertificateOnKeyStore() throws Exception {
    final String noCertificateKeyStorePassword = "flight1";

    assertThrows(
        SQLException.class,
        () -> {
          try (ArrowFlightSqlClientHandler ignored =
              new ArrowFlightSqlClientHandler.Builder()
                  .withHost(FLIGHT_SERVER_TEST_EXTENSION.getHost())
                  .withTrustStorePath(noCertificateKeyStorePath)
                  .withTrustStorePassword(noCertificateKeyStorePassword)
                  .withSystemTrustStore(false)
                  .withBufferAllocator(allocator)
                  .withEncryption(true)
                  .build()) {
            fail();
          }
        });
  }

  /**
   * Try to instantiate an encrypted FlightClient without credentials.
   *
   * @throws Exception on error.
   */
  @Test
  public void testGetNonAuthenticatedEncryptedClientNoAuth() throws Exception {
    try (ArrowFlightSqlClientHandler client =
        new ArrowFlightSqlClientHandler.Builder()
            .withHost(FLIGHT_SERVER_TEST_EXTENSION.getHost())
            .withSystemTrustStore(false)
            .withTrustStorePath(trustStorePath)
            .withTrustStorePassword(trustStorePass)
            .withBufferAllocator(allocator)
            .withEncryption(true)
            .build()) {
      assertNotNull(client);
    }
  }

  /**
   * Try to instantiate an encrypted FlightClient with an invalid password to the keystore file.
   * It's expected to receive the SQLException.
   *
   * @throws Exception on error.
   */
  @Test
  public void testGetEncryptedClientWithKeyStoreBadPasswordAndNoAuth() throws Exception {
    String keyStoreBadPassword = "badPassword";

    assertThrows(
        SQLException.class,
        () -> {
          try (ArrowFlightSqlClientHandler ignored =
              new ArrowFlightSqlClientHandler.Builder()
                  .withHost(FLIGHT_SERVER_TEST_EXTENSION.getHost())
                  .withSystemTrustStore(false)
                  .withTrustStorePath(trustStorePath)
                  .withTrustStorePassword(keyStoreBadPassword)
                  .withBufferAllocator(allocator)
                  .withEncryption(true)
                  .build()) {
            fail();
          }
        });
  }

  /**
   * Check if an encrypted connection can be established successfully when the provided valid
   * credentials and a valid Keystore.
   *
   * @throws Exception on error.
   */
  @Test
  public void testGetEncryptedConnectionWithValidCredentialsAndKeyStore() throws Exception {
    final Properties properties = new Properties();

    properties.put(ArrowFlightConnectionProperty.HOST.camelName(), "localhost");
    properties.put(
        ArrowFlightConnectionProperty.PORT.camelName(), FLIGHT_SERVER_TEST_EXTENSION.getPort());
    properties.put(ArrowFlightConnectionProperty.USER.camelName(), userTest);
    properties.put(ArrowFlightConnectionProperty.PASSWORD.camelName(), passTest);
    properties.put(ArrowFlightConnectionProperty.TRUST_STORE.camelName(), trustStorePath);
    properties.put(ArrowFlightConnectionProperty.USE_SYSTEM_TRUST_STORE.camelName(), false);
    properties.put(ArrowFlightConnectionProperty.TRUST_STORE_PASSWORD.camelName(), trustStorePass);

    final ArrowFlightJdbcDataSource dataSource =
        ArrowFlightJdbcDataSource.createNewDataSource(properties);
    try (final Connection connection = dataSource.getConnection()) {
      assertTrue(connection.isValid(300));
    }
  }

  /**
   * Check if the SQLException is thrown when trying to establish an encrypted connection providing
   * valid credentials but invalid password to the Keystore.
   *
   * @throws SQLException on error.
   */
  @Test
  public void testGetAuthenticatedEncryptedConnectionWithKeyStoreBadPassword() throws Exception {
    final Properties properties = new Properties();

    properties.put(
        ArrowFlightConnectionProperty.HOST.camelName(), FLIGHT_SERVER_TEST_EXTENSION.getHost());
    properties.put(
        ArrowFlightConnectionProperty.PORT.camelName(), FLIGHT_SERVER_TEST_EXTENSION.getPort());
    properties.put(ArrowFlightConnectionProperty.USER.camelName(), userTest);
    properties.put(ArrowFlightConnectionProperty.PASSWORD.camelName(), passTest);
    properties.put(ArrowFlightConnectionProperty.USE_ENCRYPTION.camelName(), true);
    properties.put(ArrowFlightConnectionProperty.TRUST_STORE.camelName(), trustStorePath);
    properties.put(ArrowFlightConnectionProperty.TRUST_STORE_PASSWORD.camelName(), "badpassword");

    final ArrowFlightJdbcDataSource dataSource =
        ArrowFlightJdbcDataSource.createNewDataSource(properties);

    assertThrows(
        SQLException.class,
        () -> {
          try (final Connection ignored = dataSource.getConnection()) {
            fail();
          }
        });
  }

  /**
   * Check if an encrypted connection can be established successfully when not providing
   * authentication.
   *
   * @throws Exception on error.
   */
  @Test
  public void testGetNonAuthenticatedEncryptedConnection() throws Exception {
    final Properties properties = new Properties();

    properties.put(
        ArrowFlightConnectionProperty.HOST.camelName(), FLIGHT_SERVER_TEST_EXTENSION.getHost());
    properties.put(
        ArrowFlightConnectionProperty.PORT.camelName(), FLIGHT_SERVER_TEST_EXTENSION.getPort());
    properties.put(ArrowFlightConnectionProperty.USE_ENCRYPTION.camelName(), true);
    properties.put(ArrowFlightConnectionProperty.USE_SYSTEM_TRUST_STORE.camelName(), false);
    properties.put(ArrowFlightConnectionProperty.TRUST_STORE.camelName(), trustStorePath);
    properties.put(ArrowFlightConnectionProperty.TRUST_STORE_PASSWORD.camelName(), trustStorePass);

    final ArrowFlightJdbcDataSource dataSource =
        ArrowFlightJdbcDataSource.createNewDataSource(properties);
    try (final Connection connection = dataSource.getConnection()) {
      assertTrue(connection.isValid(300));
    }
  }

  /**
   * Check if an encrypted connection can be established successfully when connecting through the
   * DriverManager using just a connection url.
   *
   * @throws Exception on error.
   */
  @Test
  public void testTLSConnectionPropertyTrueCorrectCastUrlWithDriverManager() throws Exception {
    final Driver driver = new ArrowFlightJdbcDriver();
    DriverManager.registerDriver(driver);

    try (final Connection connection =
        DriverManager.getConnection(
            String.format(
                "jdbc:arrow-flight-sql://localhost:%s?user=%s&password=%s"
                    + "&useEncryption=true&useSystemTrustStore=false&%s=%s&%s=%s",
                FLIGHT_SERVER_TEST_EXTENSION.getPort(),
                userTest,
                passTest,
                ArrowFlightConnectionProperty.TRUST_STORE.camelName(),
                URLEncoder.encode(trustStorePath, "UTF-8"),
                ArrowFlightConnectionProperty.TRUST_STORE_PASSWORD.camelName(),
                URLEncoder.encode(trustStorePass, "UTF-8")))) {
      assertTrue(connection.isValid(0));
    }
  }

  /**
   * Check if an encrypted connection can be established successfully when connecting through the
   * DriverManager using a connection url and properties with String K-V pairs.
   *
   * @throws Exception on error.
   */
  @Test
  public void
      testTLSConnectionPropertyTrueCorrectCastUrlAndPropertiesUsingSetPropertyWithDriverManager()
          throws Exception {
    final Driver driver = new ArrowFlightJdbcDriver();
    DriverManager.registerDriver(driver);

    Properties properties = new Properties();

    properties.setProperty(ArrowFlightConnectionProperty.USER.camelName(), userTest);
    properties.setProperty(ArrowFlightConnectionProperty.PASSWORD.camelName(), passTest);
    properties.setProperty(ArrowFlightConnectionProperty.TRUST_STORE.camelName(), trustStorePath);
    properties.setProperty(
        ArrowFlightConnectionProperty.TRUST_STORE_PASSWORD.camelName(), trustStorePass);
    properties.setProperty(ArrowFlightConnectionProperty.USE_ENCRYPTION.camelName(), "true");
    properties.setProperty(
        ArrowFlightConnectionProperty.USE_SYSTEM_TRUST_STORE.camelName(), "false");

    try (final Connection connection =
        DriverManager.getConnection(
            String.format(
                "jdbc:arrow-flight-sql://localhost:%s", FLIGHT_SERVER_TEST_EXTENSION.getPort()),
            properties)) {
      assertTrue(connection.isValid(0));
    }
  }

  /**
   * Check if an encrypted connection can be established successfully when connecting through the
   * DriverManager using a connection url and properties with Object K-V pairs.
   *
   * @throws Exception on error.
   */
  @Test
  public void testTLSConnectionPropertyTrueCorrectCastUrlAndPropertiesUsingPutWithDriverManager()
      throws Exception {
    final Driver driver = new ArrowFlightJdbcDriver();
    DriverManager.registerDriver(driver);

    Properties properties = new Properties();

    properties.put(ArrowFlightConnectionProperty.USER.camelName(), userTest);
    properties.put(ArrowFlightConnectionProperty.PASSWORD.camelName(), passTest);
    properties.put(ArrowFlightConnectionProperty.USE_ENCRYPTION.camelName(), true);
    properties.put(ArrowFlightConnectionProperty.USE_SYSTEM_TRUST_STORE.camelName(), false);
    properties.put(ArrowFlightConnectionProperty.TRUST_STORE.camelName(), trustStorePath);
    properties.put(ArrowFlightConnectionProperty.TRUST_STORE_PASSWORD.camelName(), trustStorePass);

    try (final Connection connection =
        DriverManager.getConnection(
            String.format(
                "jdbc:arrow-flight-sql://localhost:%s", FLIGHT_SERVER_TEST_EXTENSION.getPort()),
            properties)) {
      assertTrue(connection.isValid(0));
    }
  }

  /**
   * Check if an encrypted connection can be established successfully when connecting through the
   * DriverManager using just a connection url and using 0 and 1 as ssl values.
   *
   * @throws Exception on error.
   */
  @Test
  public void testTLSConnectionPropertyTrueIntegerCorrectCastUrlWithDriverManager()
      throws Exception {
    final Driver driver = new ArrowFlightJdbcDriver();
    DriverManager.registerDriver(driver);

    try (final Connection connection =
        DriverManager.getConnection(
            String.format(
                "jdbc:arrow-flight-sql://localhost:%s?user=%s&password=%s"
                    + "&useEncryption=1&useSystemTrustStore=0&%s=%s&%s=%s",
                FLIGHT_SERVER_TEST_EXTENSION.getPort(),
                userTest,
                passTest,
                ArrowFlightConnectionProperty.TRUST_STORE.camelName(),
                URLEncoder.encode(trustStorePath, "UTF-8"),
                ArrowFlightConnectionProperty.TRUST_STORE_PASSWORD.camelName(),
                URLEncoder.encode(trustStorePass, "UTF-8")))) {
      assertTrue(connection.isValid(0));
    }
  }

  /**
   * Check if an encrypted connection can be established successfully when connecting through the
   * DriverManager using a connection url and properties with String K-V pairs and using 0 and 1 as
   * ssl values.
   *
   * @throws Exception on error.
   */
  @Test
  public void
      testTLSConnectionPropertyTrueIntegerCorrectCastUrlAndPropertiesUsingSetPropertyWithDriverManager()
          throws Exception {
    final Driver driver = new ArrowFlightJdbcDriver();
    DriverManager.registerDriver(driver);

    Properties properties = new Properties();

    properties.setProperty(ArrowFlightConnectionProperty.USER.camelName(), userTest);
    properties.setProperty(ArrowFlightConnectionProperty.PASSWORD.camelName(), passTest);
    properties.setProperty(ArrowFlightConnectionProperty.TRUST_STORE.camelName(), trustStorePath);
    properties.setProperty(
        ArrowFlightConnectionProperty.TRUST_STORE_PASSWORD.camelName(), trustStorePass);
    properties.setProperty(ArrowFlightConnectionProperty.USE_ENCRYPTION.camelName(), "1");
    properties.setProperty(ArrowFlightConnectionProperty.USE_SYSTEM_TRUST_STORE.camelName(), "0");

    try (final Connection connection =
        DriverManager.getConnection(
            String.format(
                "jdbc:arrow-flight-sql://localhost:%s", FLIGHT_SERVER_TEST_EXTENSION.getPort()),
            properties)) {
      assertTrue(connection.isValid(0));
    }
  }

  /**
   * Check if an encrypted connection can be established successfully when connecting through the
   * DriverManager using a connection url and properties with Object K-V pairs and using 0 and 1 as
   * ssl values.
   *
   * @throws Exception on error.
   */
  @Test
  public void
      testTLSConnectionPropertyTrueIntegerCorrectCastUrlAndPropertiesUsingPutWithDriverManager()
          throws Exception {
    final Driver driver = new ArrowFlightJdbcDriver();
    DriverManager.registerDriver(driver);

    Properties properties = new Properties();

    properties.put(ArrowFlightConnectionProperty.USER.camelName(), userTest);
    properties.put(ArrowFlightConnectionProperty.PASSWORD.camelName(), passTest);
    properties.put(ArrowFlightConnectionProperty.USE_ENCRYPTION.camelName(), 1);
    properties.put(ArrowFlightConnectionProperty.USE_SYSTEM_TRUST_STORE.camelName(), 0);
    properties.put(ArrowFlightConnectionProperty.TRUST_STORE.camelName(), trustStorePath);
    properties.put(ArrowFlightConnectionProperty.TRUST_STORE_PASSWORD.camelName(), trustStorePass);

    try (final Connection connection =
        DriverManager.getConnection(
            String.format(
                "jdbc:arrow-flight-sql://localhost:%s", FLIGHT_SERVER_TEST_EXTENSION.getPort()),
            properties)) {
      assertTrue(connection.isValid(0));
    }
  }
}
