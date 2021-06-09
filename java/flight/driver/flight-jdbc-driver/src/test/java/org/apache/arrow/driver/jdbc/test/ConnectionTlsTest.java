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

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;

import java.io.IOException;
import java.net.URI;
import java.sql.Connection;
import java.sql.DriverManager;
import java.util.Properties;

import org.apache.arrow.driver.jdbc.ArrowFlightClient;
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
import org.junit.Before;
import org.junit.Test;

import com.google.common.base.Strings;

public class ConnectionTlsTest {
  private FlightServer tlsServer;
  private String serverUrl;
  private BufferAllocator allocator;
  private FlightTestUtils flightTestUtils;

  @Before
  public void setUp() throws Exception {
    flightTestUtils = new FlightTestUtils("localhost", "flight1",
            "woho1", "invalid", "wrong");

    allocator = new RootAllocator(Long.MAX_VALUE);

    final FlightTestUtils.CertKeyPair certKey = FlightTestUtils
        .exampleTlsCerts().get(0);

    final FlightProducer flightProducer = flightTestUtils.getFlightProducer(allocator);
    this.tlsServer = flightTestUtils.getStartedServer((location -> {
      try {
        return FlightServer
            .builder(allocator, location, flightProducer)
            .useTls(certKey.cert, certKey.key)
            .headerAuthenticator(new GeneratedBearerTokenAuthenticator(
                new BasicCallHeaderAuthenticator(this::validate)))
            .build();
      } catch (IOException e) {
        e.printStackTrace();
      }
      return null;
    }));
    serverUrl = flightTestUtils.getConnectionPrefix() +
            flightTestUtils.getLocalhost() + ":" + this.tlsServer.getPort();

    Class.forName("org.apache.arrow.driver.jdbc.ArrowFlightJdbcDriver");
  }

  @After
  public void tearDown() throws Exception {
    AutoCloseables.close(tlsServer);
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
  private CallHeaderAuthenticator.AuthResult validate(String username,
      String password) {
    if (Strings.isNullOrEmpty(username)) {
      throw CallStatus.UNAUTHENTICATED
          .withDescription("Credentials not supplied.").toRuntimeException();
    }
    final String identity;
    if (flightTestUtils.getUsername1().equals(username) && flightTestUtils
          .getPassword1().equals(password)) {
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

    Properties properties = new Properties();

    properties.put("useTls", "true");
    properties.put("keyStorePath", "src/test/resources/keys/keyStore.jks");
    properties.put("keyStorePass", "flight");

    URI address = new URI("jdbc",
            flightTestUtils.getUsername1() + ":" + flightTestUtils.getPassword1(),
            flightTestUtils.getLocalhost(), this.tlsServer.getPort(), null, null,
        null);

    UsernamePasswordCredentials credentials = new UsernamePasswordCredentials(
            flightTestUtils.getUsername1(), flightTestUtils.getPassword1());

    ArrowFlightClient client = ArrowFlightClient
        .getEncryptedClientAuthenticated(
          allocator, address.getHost(), address.getPort(),
          null, credentials.getUserName(), credentials.getPassword(),
          properties.getProperty("keyStorePath"),
          properties.getProperty("keyStorePass"));

    assertNotNull(client);
  }

  /**
   * Try to instantiate an encrypted FlightClient.
   *
   * @throws Exception
   *           on error.
   */
  @Test
  public void testGetEncryptedClientNoAuth() throws Exception {

    Properties properties = new Properties();

    properties.put("useTls", "true");
    properties.put("keyStorePath", "src/test/resources/keys/keyStore.jks");
    properties.put("keyStorePass", "flight");

    ArrowFlightClient client = ArrowFlightClient
        .getEncryptedClientNoAuth(
          allocator, flightTestUtils.getLocalhost(), this.tlsServer.getPort(),
          null, properties.getProperty("keyStorePath"),
          properties.getProperty("keyStorePass"));

    assertNotNull(client);
  }

  /**
   * Check if an encrypted connection can be established successfully when the
   * provided valid credentials and a valid Keystore.
   *
   * @throws Exception
   *           on error.
   */
  @Test
  public void connectTls() throws Exception {
    Properties properties = new Properties();

    properties.put("user", flightTestUtils.getUsername1());
    properties.put("password", flightTestUtils.getPassword1());
    properties.put("useTls", "true");
    properties.put("keyStorePath", "src/test/resources/keys/keyStore.jks");
    properties.put("keyStorePass", "flight");

    Connection connection = DriverManager.getConnection(serverUrl, properties);

    assertFalse(connection.isClosed());
  }
}
