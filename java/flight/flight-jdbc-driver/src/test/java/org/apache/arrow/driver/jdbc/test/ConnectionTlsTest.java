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

package org.apache.arrow.driver.jdbc.test;

import static org.junit.Assert.assertNotNull;

import java.io.IOException;
import java.net.URI;
import java.security.cert.CertificateException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;

import org.apache.arrow.driver.jdbc.client.ArrowFlightClientHandler;
import org.apache.arrow.driver.jdbc.test.utils.FlightTestUtils;
import org.apache.arrow.flight.CallStatus;
import org.apache.arrow.flight.FlightProducer;
import org.apache.arrow.flight.FlightServer;
import org.apache.arrow.flight.auth2.BasicCallHeaderAuthenticator;
import org.apache.arrow.flight.auth2.CallHeaderAuthenticator;
import org.apache.arrow.flight.auth2.GeneratedBearerTokenAuthenticator;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.util.AutoCloseables;
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
  private FlightServer tlsServer;
  private String serverUrl;
  private BufferAllocator allocator;
  private FlightTestUtils flightTestUtils;
  private final String keyStorePath = this.getClass().getResource("/keys/keyStore.jks")
      .getPath();
  private final String noCertificateKeyStorePath = this.getClass().getResource("/keys/noCertificate.jks")
      .getPath();
  private final String keyStorePass = "flight";

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
        flightTestUtils.getLocalhost() + ":" + this.tlsServer.getPort();

    Class.forName("org.apache.arrow.driver.jdbc.ArrowFlightJdbcDriver");
  }

  @After
  public void tearDown() throws Exception {
    AutoCloseables.close(tlsServer, allocator);
  }

  /**
   * Validate the user's credential on a FlightServer.
   *
   * @param username
   *          flight server username.
   * @param password
   *          flight server password.
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
   * @throws Exception
   *           on error.
   */
  @Test
  public void testGetEncryptedClientAuthenticated() throws Exception {
    final URI address = new URI("jdbc",
        flightTestUtils.getUsername1() + ":" + flightTestUtils.getPassword1(),
        flightTestUtils.getLocalhost(), this.tlsServer.getPort(), null, null,
        null);

    final UsernamePasswordCredentials credentials = new UsernamePasswordCredentials(
        flightTestUtils.getUsername1(), flightTestUtils.getPassword1());

    try (ArrowFlightClientHandler client =
           ArrowFlightClientHandler
             .getClient(
                allocator, address.getHost(), address.getPort(),
                credentials.getUserName(), credentials.getPassword(),
                null, keyStorePath, keyStorePass)) {

      assertNotNull(client);
    }
  }

  /**
   * Try to instantiate an encrypted FlightClient providing a keystore without certificate. It's expected to
   * receive the SQLException.
   *
   * @throws Exception
   *           on error.
   */
  @Test(expected = CertificateException.class)
  public void testGetEncryptedClientWithNoCertificateOnKeyStore() throws Exception {
    final String noCertificateKeyStorePassword = "flight1";

    try (ArrowFlightClientHandler client =
           ArrowFlightClientHandler
             .getClient(allocator, flightTestUtils.getLocalhost(), this.tlsServer.getPort(),
                null, noCertificateKeyStorePath,
                noCertificateKeyStorePassword)) {
      Assert.fail();
    }
  }

  /**
   * Try to instantiate an encrypted FlightClient without credentials.
   *
   * @throws Exception
   *           on error.
   */
  @Test
  public void testGetNonAuthenticatedEncryptedClientNoAuth() throws Exception {
    try (ArrowFlightClientHandler client =
           ArrowFlightClientHandler
             .getClient(
                allocator, flightTestUtils.getLocalhost(), this.tlsServer.getPort(),
                null, keyStorePath,
                keyStorePass)) {

      assertNotNull(client);
    }
  }

  /**
   * Try to instantiate an encrypted FlightClient with an invalid password to the keystore file.
   * It's expected to receive the SQLException.
   *
   * @throws Exception
   *           on error.
   */
  @Test(expected = IOException.class)
  public void testGetEncryptedClientWithKeyStoreBadPasswordAndNoAuth() throws Exception {
    String keyStoreBadPassword = "badPassword";

    try (ArrowFlightClientHandler client =
           ArrowFlightClientHandler.getClient(
             allocator, flightTestUtils.getLocalhost(), this.tlsServer.getPort(),
             null, keyStorePath,
             keyStoreBadPassword)) {
      Assert.fail();
    }
  }

  /**
   * Check if an encrypted connection can be established successfully when the
   * provided valid credentials and a valid Keystore.
   *
   * @throws Exception
   *           on error.
   */
  @Test
  public void testGetEncryptedConnectionWithValidCredentialsAndKeyStore() throws Exception {
    final Properties properties = new Properties();

    properties.put("user", flightTestUtils.getUsername1());
    properties.put("password", flightTestUtils.getPassword1());
    properties.put("useTls", "true");
    properties.put("keyStorePath", keyStorePath);
    properties.put("keyStorePass", keyStorePass);

    try (Connection connection = DriverManager
        .getConnection(serverUrl, properties)) {

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

    properties.put("user", flightTestUtils.getUsername1());
    properties.put("password", flightTestUtils.getPassword1());
    properties.put("useTls", "true");
    properties.put("keyStorePath", keyStorePath);
    properties.put("keyStorePass", "badpassword");

    try (Connection connection = DriverManager
        .getConnection(serverUrl, properties)) {
      Assert.fail();
    }
  }

  /**
   * Check if an encrypted connection can be established successfully when not providing authentication.
   *
   * @throws Exception
   *           on error.
   */
  @Test
  public void testGetNonAuthenticatedEncryptedConnection() throws Exception {
    final Properties properties = new Properties();

    properties.put("useTls", "true");
    properties.put("keyStorePath", keyStorePath);
    properties.put("keyStorePass", keyStorePass);

    try (Connection connection = DriverManager.getConnection(serverUrl,
        properties)) {

      assert connection.isValid(300);
    }
  }
}
