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

import java.io.IOException;
import java.sql.Connection;
import java.sql.Driver;
import java.sql.SQLException;
import java.sql.SQLOutput;
import java.util.Properties;
import java.sql.DriverManager;

import org.apache.arrow.driver.jdbc.client.ArrowFlightSqlClientHandler;
import org.apache.arrow.driver.jdbc.utils.FlightTestUtils;
import org.apache.arrow.driver.jdbc.utils.ArrowFlightConnectionConfigImpl.ArrowFlightConnectionProperty;
import org.apache.arrow.flight.CallStatus;
import org.apache.arrow.flight.FlightProducer;
import org.apache.arrow.flight.FlightServer;
import org.apache.arrow.flight.auth2.BasicCallHeaderAuthenticator;
import org.apache.arrow.flight.auth2.CallHeaderAuthenticator;
import org.apache.arrow.flight.auth2.GeneratedBearerTokenAuthenticator;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.util.AutoCloseables;
import org.apache.calcite.avatica.BuiltInConnectionProperty;
import org.apache.calcite.avatica.org.apache.http.auth.UsernamePasswordCredentials;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.google.common.base.Strings;

/**
 * Tests encrypted connections.
 * TODO Update to use {@link FlightServerTestRule} instead of {@link FlightTestUtils}
 */
public class ConnectionTlsTest {
  private final String keyStorePath = this.getClass().getResource("/keys/keyStore.jks")
      .getPath();
  private final String noCertificateKeyStorePath = this.getClass().getResource("/keys/noCertificate.jks")
      .getPath();
  private final String keyStorePass = "flight";
  private FlightServer tlsServer;
  private String serverUrl;
  private BufferAllocator allocator;
  private FlightTestUtils flightTestUtils;

  @Before
  public void setUp() throws Exception {
    flightTestUtils = new FlightTestUtils("localhost", "flight1", "woho1",
        "invalid", "wrong");

    allocator = new RootAllocator(Long.MAX_VALUE);

    final FlightTestUtils.CertKeyPair certKey = FlightTestUtils
        .exampleTlsCerts().get(0);

    final FlightProducer flightProducer = flightTestUtils
        .getFlightProducer(allocator);
    this.tlsServer = flightTestUtils.getStartedServer(location -> {
      try {
        return FlightServer.builder(allocator, location, flightProducer)
            .useTls(certKey.cert, certKey.key)
            .headerAuthenticator(new GeneratedBearerTokenAuthenticator(
                new BasicCallHeaderAuthenticator(this::validate)))
            .build();
      } catch (final IOException e) {
        e.printStackTrace();
      }
      return null;
    });
    serverUrl = flightTestUtils.getConnectionPrefix() +
        flightTestUtils.getUrl() + ":" + this.tlsServer.getPort();
  }

  @After
  public void tearDown() throws Exception {
    AutoCloseables.close(tlsServer, allocator);
  }

  /**
   * Validate the user's credential on a FlightServer.
   *
   * @param username flight server username.
   * @param password flight server password.
   * @return the result of validation.
   */
  private CallHeaderAuthenticator.AuthResult validate(final String username,
                                                      final String password) {
    if (Strings.isNullOrEmpty(username)) {
      throw CallStatus.UNAUTHENTICATED
          .withDescription("Credentials not supplied.").toRuntimeException();
    }
    final String identity;
    if (flightTestUtils.getUsername1().equals(username) &&
        flightTestUtils.getPassword1().equals(password)) {
      identity = flightTestUtils.getUsername1();
    } else {
      throw CallStatus.UNAUTHENTICATED
          .withDescription("Username or password is invalid.")
          .toRuntimeException();
    }
    return () -> identity;
  }

  /**
   * Try to instantiate an encrypted FlightClient.
   *
   * @throws Exception on error.
   */
  @Test
  public void testGetEncryptedClientAuthenticated() throws Exception {
    final UsernamePasswordCredentials credentials = new UsernamePasswordCredentials(
        flightTestUtils.getUsername1(), flightTestUtils.getPassword1());

    try (ArrowFlightSqlClientHandler client =
             new ArrowFlightSqlClientHandler.Builder()
                 .withHost("localhost")
                 .withPort(tlsServer.getPort())
                 .withUsername(credentials.getUserName())
                 .withPassword(credentials.getPassword())
                 .withKeyStorePath(keyStorePath)
                 .withKeyStorePassword(keyStorePass)
                 .withBufferAllocator(allocator)
                 .withTlsEncryption(true)
                 .build()) {
      assertNotNull(client);
    }
  }

  /**
   * Try to instantiate an encrypted FlightClient providing a keystore without certificate. It's expected to
   * receive the SQLException.
   *
   * @throws Exception on error.
   */
  @Test(expected = SQLException.class)
  public void testGetEncryptedClientWithNoCertificateOnKeyStore() throws Exception {
    final String noCertificateKeyStorePassword = "flight1";

    try (ArrowFlightSqlClientHandler client =
             new ArrowFlightSqlClientHandler.Builder()
                 .withHost(flightTestUtils.getUrl())
                 .withKeyStorePath(noCertificateKeyStorePath)
                 .withKeyStorePassword(noCertificateKeyStorePassword)
                 .withBufferAllocator(allocator)
                 .withTlsEncryption(true)
                 .build()) {
      Assert.fail();
    }
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
                 .withHost(flightTestUtils.getUrl())
                 .withKeyStorePath(keyStorePath)
                 .withKeyStorePassword(keyStorePass)
                 .withBufferAllocator(allocator)
                 .withTlsEncryption(true)
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
  @Test(expected = SQLException.class)
  public void testGetEncryptedClientWithKeyStoreBadPasswordAndNoAuth() throws Exception {
    String keyStoreBadPassword = "badPassword";

    try (ArrowFlightSqlClientHandler client =
             new ArrowFlightSqlClientHandler.Builder()
                 .withHost(flightTestUtils.getUrl())
                 .withKeyStorePath(keyStorePath)
                 .withKeyStorePassword(keyStoreBadPassword)
                 .withBufferAllocator(allocator)
                 .withTlsEncryption(true)
                 .build()) {
      Assert.fail();
    }
  }

  /**
   * Check if an encrypted connection can be established successfully when the
   * provided valid credentials and a valid Keystore.
   *
   * @throws Exception on error.
   */
  @Test
  public void testGetEncryptedConnectionWithValidCredentialsAndKeyStore() throws Exception {
    final Properties properties = new Properties();

    properties.put(ArrowFlightConnectionProperty.HOST.camelName(), "localhost");
    properties.put(ArrowFlightConnectionProperty.PORT.camelName(), tlsServer.getPort());
    properties.put(ArrowFlightConnectionProperty.USER.camelName(), flightTestUtils.getUsername1());
    properties.put(ArrowFlightConnectionProperty.PASSWORD.camelName(), flightTestUtils.getPassword1());
    properties.put(ArrowFlightConnectionProperty.USE_TLS.camelName(), true);
    properties.put(BuiltInConnectionProperty.KEYSTORE.camelName(), keyStorePath);
    properties.put(BuiltInConnectionProperty.KEYSTORE_PASSWORD.camelName(), keyStorePass);

    final ArrowFlightJdbcDataSource dataSource =
        ArrowFlightJdbcDataSource.createNewDataSource(properties);
    try (final Connection connection = dataSource.getConnection()) {
      assert connection.isValid(300);
    }
  }

  /**
   * Check if the SQLException is thrown when trying to establish an encrypted connection
   * providing valid credentials but invalid password to the Keystore.
   *
   * @throws SQLException on error.
   */
  @Test(expected = SQLException.class)
  public void testGetAuthenticatedEncryptedConnectionWithKeyStoreBadPassword() throws Exception {
    final Properties properties = new Properties();

    properties.put(ArrowFlightConnectionProperty.HOST.camelName(), "localhost");
    properties.put(ArrowFlightConnectionProperty.PORT.camelName(), tlsServer.getPort());
    properties.put(ArrowFlightConnectionProperty.USER.camelName(), flightTestUtils.getUsername1());
    properties.put(ArrowFlightConnectionProperty.PASSWORD.camelName(), flightTestUtils.getPassword1());
    properties.put(ArrowFlightConnectionProperty.USE_TLS.camelName(), true);
    properties.put(BuiltInConnectionProperty.KEYSTORE.camelName(), keyStorePath);
    properties.put(BuiltInConnectionProperty.KEYSTORE_PASSWORD.camelName(), "badpassword");

    final ArrowFlightJdbcDataSource dataSource =
        ArrowFlightJdbcDataSource.createNewDataSource(properties);
    try (final Connection connection = dataSource.getConnection()) {
      Assert.fail();
    }
  }

  /**
   * Check if an encrypted connection can be established successfully when not providing authentication.
   *
   * @throws Exception on error.
   */
  @Test
  public void testGetNonAuthenticatedEncryptedConnection() throws Exception {
    final Properties properties = new Properties();

    properties.put(ArrowFlightConnectionProperty.HOST.camelName(), "localhost");
    properties.put(ArrowFlightConnectionProperty.PORT.camelName(), tlsServer.getPort());
    properties.put(ArrowFlightConnectionProperty.USE_TLS.camelName(), true);
    properties.put(BuiltInConnectionProperty.KEYSTORE.camelName(), keyStorePath);
    properties.put(BuiltInConnectionProperty.KEYSTORE_PASSWORD.camelName(), keyStorePass);

    final ArrowFlightJdbcDataSource dataSource =
        ArrowFlightJdbcDataSource.createNewDataSource(properties);
    try (final Connection connection = dataSource.getConnection()) {
      assert connection.isValid(300);
    }
  }

  /**
   * Check if an encrypted connection can be established successfully when connecting through the DriverManager using
   * just a connection url.
   *
   * @throws Exception on error.
   */
  @Test
  public void testTLSConnectionPropertyTrueCorrectCastUrlWithDriverManager() throws Exception {
    final Driver driver = new ArrowFlightJdbcDriver();
    DriverManager.registerDriver(driver);

    Assert.assertTrue(DriverManager.getConnection(
            String.format(
                    "jdbc:arrow-flight://localhost:%s?user=%s&password=%s&useTls=true&%s=%s&%s=%s",
                    tlsServer.getPort(),
                    flightTestUtils.getUsername1(),
                    flightTestUtils.getPassword1(),
                    BuiltInConnectionProperty.KEYSTORE.camelName(),
                    keyStorePath,
                    BuiltInConnectionProperty.KEYSTORE_PASSWORD.camelName(),
                    keyStorePass)).isValid(0));
  }

  /**
   * Check if an encrypted connection can be established successfully when connecting through the DriverManager using
   * a connection url and properties with String K-V pairs.
   *
   * @throws Exception on error.
   */
  @Test
  public void testTLSConnectionPropertyTrueCorrectCastUrlAndPropertiesUsingSetPropertyWithDriverManager()
          throws Exception {
    final Driver driver = new ArrowFlightJdbcDriver();
    DriverManager.registerDriver(driver);

    Properties properties = new Properties();

    properties.setProperty(ArrowFlightConnectionProperty.USER.camelName(),flightTestUtils.getUsername1());
    properties.setProperty(ArrowFlightConnectionProperty.PASSWORD.camelName(),flightTestUtils.getPassword1());
    properties.setProperty(BuiltInConnectionProperty.KEYSTORE.camelName(), keyStorePath);
    properties.setProperty(BuiltInConnectionProperty.KEYSTORE_PASSWORD.camelName(), keyStorePass);
    properties.setProperty(ArrowFlightConnectionProperty.USE_TLS.camelName(),"true");

    Assert.assertTrue(DriverManager.getConnection(
            String.format(
                    "jdbc:arrow-flight://localhost:%s",
                    tlsServer.getPort()),
            properties).isValid(0));
  }

  /**
   * Check if an encrypted connection can be established successfully when connecting through the DriverManager using
   * a connection url and properties with Object K-V pairs.
   *
   * @throws Exception on error.
   */
  @Test
  public void testTLSConnectionPropertyTrueCorrectCastUrlAndPropertiesUsingPutWithDriverManager() throws Exception {
    final Driver driver = new ArrowFlightJdbcDriver();
    DriverManager.registerDriver(driver);

    Properties properties = new Properties();

    properties.put(ArrowFlightConnectionProperty.USER.camelName(),flightTestUtils.getUsername1());
    properties.put(ArrowFlightConnectionProperty.PASSWORD.camelName(),flightTestUtils.getPassword1());
    properties.put(ArrowFlightConnectionProperty.USE_TLS.camelName(),true);
    properties.put(BuiltInConnectionProperty.KEYSTORE.camelName(), keyStorePath);
    properties.put(BuiltInConnectionProperty.KEYSTORE_PASSWORD.camelName(), keyStorePass);

    Assert.assertTrue(DriverManager.getConnection(
            String.format(
                    "jdbc:arrow-flight://localhost:%s",
                    tlsServer.getPort()),
            properties).isValid(0));
  }

  /**
   * Check if an encrypted connection can be established successfully when connecting through the DriverManager using
   * just a connection url and using 0 and 1 as useTls values.
   *
   * @throws Exception on error.
   */
  @Test
  public void testTLSConnectionPropertyTrueIntegerCorrectCastUrlWithDriverManager() throws Exception {
    final Driver driver = new ArrowFlightJdbcDriver();
    DriverManager.registerDriver(driver);

    Assert.assertTrue(DriverManager.getConnection(
            String.format(
                    "jdbc:arrow-flight://localhost:%s?user=%s&password=%s&useTls=1&%s=%s&%s=%s",
                    tlsServer.getPort(),
                    flightTestUtils.getUsername1(),
                    flightTestUtils.getPassword1(),
                    BuiltInConnectionProperty.KEYSTORE.camelName(),
                    keyStorePath,
                    BuiltInConnectionProperty.KEYSTORE_PASSWORD.camelName(),
                    keyStorePass)).isValid(0));
  }

  /**
   * Check if an encrypted connection can be established successfully when connecting through the DriverManager using
   * a connection url and properties with String K-V pairs and using 0 and 1 as useTls values.
   *
   * @throws Exception on error.
   */
  @Test
  public void testTLSConnectionPropertyTrueIntegerCorrectCastUrlAndPropertiesUsingSetPropertyWithDriverManager()
          throws Exception {
    final Driver driver = new ArrowFlightJdbcDriver();
    DriverManager.registerDriver(driver);

    Properties properties = new Properties();

    properties.setProperty(ArrowFlightConnectionProperty.USER.camelName(),flightTestUtils.getUsername1());
    properties.setProperty(ArrowFlightConnectionProperty.PASSWORD.camelName(),flightTestUtils.getPassword1());
    properties.setProperty(BuiltInConnectionProperty.KEYSTORE.camelName(), keyStorePath);
    properties.setProperty(BuiltInConnectionProperty.KEYSTORE_PASSWORD.camelName(), keyStorePass);
    properties.setProperty(ArrowFlightConnectionProperty.USE_TLS.camelName(),"1");

    Assert.assertTrue(DriverManager.getConnection(
            String.format("jdbc:arrow-flight://localhost:%s", tlsServer.getPort()),
            properties).isValid(0));
  }

  /**
   * Check if an encrypted connection can be established successfully when connecting through the DriverManager using
   * a connection url and properties with Object K-V pairs and using 0 and 1 as useTls values.
   *
   * @throws Exception on error.
   */
  @Test
  public void testTLSConnectionPropertyTrueIntegerCorrectCastUrlAndPropertiesUsingPutWithDriverManager()
          throws Exception {
    final Driver driver = new ArrowFlightJdbcDriver();
    DriverManager.registerDriver(driver);

    Properties properties = new Properties();

    properties.put(ArrowFlightConnectionProperty.USER.camelName(),flightTestUtils.getUsername1());
    properties.put(ArrowFlightConnectionProperty.PASSWORD.camelName(),flightTestUtils.getPassword1());
    properties.put(ArrowFlightConnectionProperty.USE_TLS.camelName(),1);
    properties.put(BuiltInConnectionProperty.KEYSTORE.camelName(), keyStorePath);
    properties.put(BuiltInConnectionProperty.KEYSTORE_PASSWORD.camelName(), keyStorePass);

    Assert.assertTrue(DriverManager.getConnection(
            String.format("jdbc:arrow-flight://localhost:%s",
            tlsServer.getPort()),
            properties).isValid(0));
  }
}
