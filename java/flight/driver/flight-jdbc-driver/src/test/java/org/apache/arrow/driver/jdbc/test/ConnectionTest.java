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

import java.net.URISyntaxException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;

import org.apache.arrow.driver.jdbc.ArrowFlightClient;
import org.apache.arrow.driver.jdbc.ArrowFlightJdbcDriver;
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
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.google.common.base.Strings;

/**
 * Tests for {@link Connection}.
 */
public class ConnectionTest {

  private FlightServer server;
  private String serverUrl;
  private BufferAllocator allocator;
  private FlightTestUtils flightTestUtils;

  /**
   * Setup for all tests.
   *
   * @throws ClassNotFoundException
   *           If the {@link ArrowFlightJdbcDriver} cannot be loaded.
   */
  @Before
  public void setUp() throws Exception {
    allocator = new RootAllocator(Long.MAX_VALUE);

    flightTestUtils = new FlightTestUtils("localhost", "flight1",
            "woho1", "invalid", "wrong");

    final FlightProducer flightProducer = flightTestUtils.getFlightProducer(allocator);
    this.server = flightTestUtils.getStartedServer((location -> FlightServer
        .builder(allocator, location, flightProducer)
        .headerAuthenticator(new GeneratedBearerTokenAuthenticator(
            new BasicCallHeaderAuthenticator(this::validate)))
        .build()));
    serverUrl = flightTestUtils.getConnectionPrefix() +
            flightTestUtils.getLocalhost() + ":" + this.server.getPort();

    // TODO Double-check this later.
    Class.forName("org.apache.arrow.driver.jdbc.ArrowFlightJdbcDriver");
  }

  @After
  public void tearDown() throws Exception {
    AutoCloseables.close(server);
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
   * Checks if an unencrypted connection can be established successfully when
   * the provided valid credentials.
   *
   * @throws SQLException
   *           on error.
   */
  @Test
  public void testUnencryptedConnectionShouldOpenSuccessfullyWhenProvidedValidCredentials()
      throws Exception {
    Properties properties = new Properties();

    properties.put("user", flightTestUtils.getUsername1());
    properties.put("password", flightTestUtils.getPassword1());
    Connection connection = DriverManager.getConnection(serverUrl, properties);
    assertFalse(connection.isClosed());
  }

  /**
   * Try to instantiate a basic FlightClient.
   *
   * @throws URISyntaxException
   *           on error.
   */
  @Test
  public void testGetBasicClientAuthenticatedShouldOpenConnection() throws Exception {

    ArrowFlightClient client = ArrowFlightClient.getBasicClientAuthenticated(
            allocator, flightTestUtils.getLocalhost(), this.server.getPort(),
              flightTestUtils.getUsername1(), flightTestUtils.getPassword1(),
              null);

    assertNotNull(client);
  }

  /**
   * Try to instantiate a basic FlightClient.
   *
   * @throws URISyntaxException
   *           on error.
   */
  @Test
  public void testGetBasicClientNoAuthShouldOpenConnection() throws Exception {

    ArrowFlightClient client = ArrowFlightClient.getBasicClientNoAuth(
            allocator, flightTestUtils.getLocalhost(), this.server.getPort(),
              null);

    assertNotNull(client);
  }

  /**
   * Check if an unencrypted connection throws an exception when provided with
   * invalid credentials.
   *
   * @throws SQLException
   *           The exception expected to be thrown.
   */
  @Test(expected = SQLException.class)
  public void testUnencryptedConnectionShouldThrowExceptionWhenProvidedWithInvalidCredentials()
      throws Exception {

    Properties properties = new Properties();

    properties.put("user", flightTestUtils.getUsernameInvalid());
    properties.put("password", flightTestUtils.getPasswordInvalid());
    DriverManager.getConnection(serverUrl, properties);
  }
}
