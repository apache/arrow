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
import java.net.URISyntaxException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;

import com.google.common.base.Strings;
import org.apache.arrow.driver.jdbc.ArrowFlightClient;
import org.apache.arrow.driver.jdbc.ArrowFlightJdbcDriver;
import org.apache.arrow.flight.CallStatus;
import org.apache.arrow.flight.FlightProducer;
import org.apache.arrow.flight.FlightRuntimeException;
import org.apache.arrow.flight.FlightServer;
import org.apache.arrow.flight.auth2.BasicCallHeaderAuthenticator;
import org.apache.arrow.flight.auth2.CallHeaderAuthenticator;
import org.apache.arrow.flight.auth2.GeneratedBearerTokenAuthenticator;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.calcite.avatica.org.apache.http.auth.UsernamePasswordCredentials;
import org.junit.Before;
import org.junit.Test;

/**
 * Tests for {@link Connection}.
 */
public class ConnectionTest {

  private BufferAllocator allocator;
  private FlightServer server;
  private static final String CONNECTION_PREFIX = "jdbc:arrow-flight://";
  public static final String LOCALHOST = "localhost";
  private static final String USERNAME_1 = "flight1";
  private static final String PASSWORD_1 = "woohoo1";
  private static final String USERNAME_INVALID = "bad";
  private static final String PASSWORD_INVALID = "wrong";
  private static String serverUrl;

  /**
   * Setup for all tests.
   *
   * @throws ClassNotFoundException
   *           If the {@link ArrowFlightJdbcDriver} cannot be loaded.
   */
  @Before
  public void setUp() throws ClassNotFoundException, IOException {
    allocator = new RootAllocator(Long.MAX_VALUE);
    final FlightProducer flightProducer = FlightTestUtils.getFlightProducer();
    this.server = FlightTestUtils.getStartedServer((location -> FlightServer
            .builder(allocator, location, flightProducer)
            .headerAuthenticator(new GeneratedBearerTokenAuthenticator(
                    new BasicCallHeaderAuthenticator(this::validate)))
            .build()
    ));
    this.serverUrl = CONNECTION_PREFIX + LOCALHOST + ":"  + this.server.getPort();

    Class.forName("org.apache.arrow.driver.jdbc.ArrowFlightJdbcDriver");
  }

  /**
   * Checks if an unencrypted connection can be established successfully when
   * the provided valid credentials.
   *
   * @throws SQLException on error.
   */
  @Test
  public void testUnencryptedConnectionShouldOpenSuccessfullyWhenProvidedValidCredentials()
          throws SQLException {
    Properties properties = new Properties();

    properties.put("user", USERNAME_1);
    properties.put("pass", PASSWORD_1);
    Connection connection = DriverManager.getConnection(serverUrl, properties);
    assertFalse(connection.isClosed());
  }

  /**
   * Try to instantiate a basic FlightClient.
   *
   * @throws URISyntaxException on error.
   */
  @Test
  public void testGetBasicClient() throws URISyntaxException {
    URI address = new URI("jdbc",
            USERNAME_1+ ":" + PASSWORD_1,
            LOCALHOST, this.server.getPort(),
            null, null, null);

    UsernamePasswordCredentials credentials = new UsernamePasswordCredentials(
            USERNAME_1, PASSWORD_1);

    ArrowFlightClient client = ArrowFlightClient.getBasicClient(allocator, address, credentials, null);

    assertNotNull(client);
  }

  /**
   * Check if an unencrypted connection throws an exception when provided with
   * invalid credentials.
   *
   * @throws SQLException
   *           The exception expected to be thrown.
   */
  @Test(expected = FlightRuntimeException.class)
  public void testUnencryptedConnectionShouldThrowExceptionWhenProvidedWithInvalidCredentials()
          throws SQLException {

    Properties properties = new Properties();

    properties.put("user", USERNAME_INVALID);
    properties.put("pass", PASSWORD_INVALID);
    DriverManager.getConnection(serverUrl, properties);
  }

  /**
   * Validate the user's credential on a FlightServer.
   *
   * @param username flight server username.
   * @param password flight server password.
   * @return the result of validation.
   */
  private CallHeaderAuthenticator.AuthResult validate(String username, String password) {
    if (Strings.isNullOrEmpty(username)) {
      throw CallStatus.UNAUTHENTICATED.withDescription("Credentials not supplied.").toRuntimeException();
    }
    final String identity;
    if (USERNAME_1.equals(username) && PASSWORD_1.equals(password)) {
      identity = USERNAME_1;
    } else {
      throw CallStatus.UNAUTHENTICATED.withDescription("Username or password is invalid.").toRuntimeException();
    }
    return () -> identity;
  }
}
