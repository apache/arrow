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

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.URI;
import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Collection;
import java.util.Map;
import java.util.Properties;

import org.apache.arrow.driver.jdbc.ArrowFlightJdbcDriver;
import org.apache.arrow.driver.jdbc.test.utils.FlightTestUtils;
import org.apache.arrow.driver.jdbc.test.utils.PropertiesSample;
import org.apache.arrow.driver.jdbc.test.utils.UrlSample;
import org.apache.arrow.driver.jdbc.utils.DefaultProperty;
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
 * Tests for {@link ArrowFlightJdbcDriver}.
 * TODO Update to use {@link FlightServerTestRule} instead of {@link FlightTestUtils}
 */
public class ArrowFlightJdbcDriverTest {

  private BufferAllocator allocator;
  private FlightServer server;
  FlightTestUtils testUtils;

  @Before
  public void setUp() throws Exception {
    // TODO Replace this.
    Class.forName("org.apache.arrow.driver.jdbc.ArrowFlightJdbcDriver");

    allocator = new RootAllocator(Long.MAX_VALUE);

    final UrlSample url = UrlSample.CONFORMING;

    final Properties propertiesConforming = PropertiesSample.CONFORMING
        .getProperties();

    final Properties propertiesUnsupported = PropertiesSample.UNSUPPORTED
        .getProperties();

    testUtils = new FlightTestUtils(url.getHost(),
        propertiesConforming.getProperty("user"),
        propertiesConforming.getProperty("password"),
        propertiesUnsupported.getProperty("user"),
        propertiesUnsupported.getProperty("password"));

    final FlightProducer flightProducer = testUtils
        .getFlightProducer(allocator);

    server = testUtils
        .getStartedServer(
            (location -> FlightServer
                .builder(allocator, location, flightProducer)
                .headerAuthenticator(new GeneratedBearerTokenAuthenticator(
                    new BasicCallHeaderAuthenticator(this::validate)))
                .build()));
  }

  @After
  public void tearDown() throws Exception {
    Collection<BufferAllocator> childAllocators = allocator.getChildAllocators();
    AutoCloseables.close(childAllocators
            .toArray(new AutoCloseable[childAllocators.size()]));
    AutoCloseables.close(server, allocator);
  }

  /**
   * Tests whether the {@link ArrowFlightJdbcDriver} is registered in the
   * {@link DriverManager}.
   *
   * @throws SQLException
   *           If an error occurs. (This is not supposed to happen.)
   */
  @Test
  public void testDriverIsRegisteredInDriverManager() throws Exception {
    assert DriverManager.getDriver(
        UrlSample.CONFORMING.getPrefix()) instanceof ArrowFlightJdbcDriver;
  }

  /**
   * Tests whether the {@link ArrowFlightJdbcDriver} fails when provided with an
   * unsupported URL prefix.
   *
   * @throws SQLException
   *           If the test passes.
   */
  @Test(expected = SQLException.class)
  public void testShouldDeclineUrlWithUnsupportedPrefix() throws Exception {
    final Driver driver = new ArrowFlightJdbcDriver();

    driver.connect(UrlSample.UNSUPPORTED.getPath(),
        PropertiesSample.UNSUPPORTED.getProperties()).close();
  }

  /**
   * Tests whether the {@link ArrowFlightJdbcDriver} can establish a successful
   * connection to the Arrow Flight client.
   *
   * @throws Exception
   *           If the connection fails to be established.
   */
  @Test
  public void testShouldConnectWhenProvidedWithValidUrl() throws Exception {
    // Get the Arrow Flight JDBC driver by providing a URL with a valid prefix.
    final Driver driver = new ArrowFlightJdbcDriver();

    final URI uri = server.getLocation().getUri();

    try (Connection connection = driver.connect(
        "jdbc:arrow-flight://" + uri.getHost() + ":" + uri.getPort(),
        PropertiesSample.CONFORMING.getProperties())) {
      assert connection.isValid(300);
    }
  }

  /**
   * Tests whether an exception is thrown upon attempting to connect to a
   * malformed URI.
   *
   * @throws Exception If an error occurs.
   */
  @Test(expected = SQLException.class)
  public void testShouldThrowExceptionWhenAttemptingToConnectToMalformedUrl()
      throws Exception {
    final Driver driver = new ArrowFlightJdbcDriver();
    final String malformedUri = "yes:??/chainsaw.i=T333";
    driver.connect(malformedUri, PropertiesSample.UNSUPPORTED.getProperties());
  }

  /**
   * Tests whether an exception is thrown upon attempting to connect to a
   * malformed URI.
   *
   * @throws Exception If an error occurs.
   */
  @Test(expected = SQLException.class)
  public void testShouldThrowExceptionWhenAttemptingToConnectToUrlNoPrefix()
      throws Exception {
    final Driver driver = new ArrowFlightJdbcDriver();
    final String malformedUri = server.getLocation().getUri().toString();
    driver.connect(malformedUri, PropertiesSample.UNSUPPORTED.getProperties());
  }

  /**
   * Tests whether an exception is thrown upon attempting to connect to a
   * malformed URI.
   *
   * @throws Exception If an error occurs.
   */
  @Test(expected = SQLException.class)
  public void testShouldThrowExceptionWhenAttemptingToConnectToUrlNoPort()
      throws Exception {
    final Driver driver = new ArrowFlightJdbcDriver();
    final String malformedUri = "arrow-jdbc://" +
        server.getLocation().getUri().getHost();
    driver.connect(malformedUri, PropertiesSample.UNSUPPORTED.getProperties());
  }

  /**
   * Tests whether an exception is thrown upon attempting to connect to a
   * malformed URI.
   *
   * @throws Exception If an error occurs.
   */
  @Test(expected = SQLException.class)
  public void testShouldThrowExceptionWhenAttemptingToConnectToUrlNoHost()
      throws Exception {
    final Driver driver = new ArrowFlightJdbcDriver();

    final String malformedUri = "arrow-jdbc://" +
        ":" + server.getLocation().getUri().getPort();
    driver.connect(malformedUri, PropertiesSample.UNSUPPORTED.getProperties());
  }

  /**
   * Tests whether {@code ArrowFlightJdbcDriverTest#getUrlsArgs} returns the
   * correct URL parameters.
   *
   * @throws Exception If an error occurs.
   */
  @SuppressWarnings("unchecked")
  @Test
  public void testDriverUrlParsingMechanismShouldReturnTheDesiredArgsFromUrl()
      throws Exception {
    final Driver driver = new ArrowFlightJdbcDriver();

    final Method getUrlsArgs = driver.getClass()
        .getDeclaredMethod("getUrlsArgs", String.class);

    getUrlsArgs.setAccessible(true);

    final Map<Object, Object> parsedArgs = (Map<Object, Object>) getUrlsArgs
        .invoke(driver,
            "jdbc:arrow-flight://localhost:2222/?key1=value1&key2=value2&a=b");

    // Check size == the amount of args provided (prefix not included!)
    assertEquals(5, parsedArgs.size());

    // Check host == the provided host
    assertEquals(parsedArgs.get(DefaultProperty.HOST.toString()), "localhost");

    // Check port == the provided port
    assertEquals(parsedArgs.get(DefaultProperty.PORT.toString()), "2222");

    // Check all other non-default arguments
    assertEquals(parsedArgs.get("key1"), "value1");
    assertEquals(parsedArgs.get("key2"), "value2");
    assertEquals(parsedArgs.get("a"), "b");
  }

  /**
   * Tests whether an exception is thrown upon attempting to connect to a
   * malformed URI.
   *
   * @throws Exception If an error occurs.
   */
  @SuppressWarnings("unchecked")
  @Test(expected = SQLException.class)
  public void testDriverUrlParsingMechanismShouldThrowExceptionUponProvidedWithMalformedUrl()
      throws Exception {
    final Driver driver = new ArrowFlightJdbcDriver();

    final Method getUrlsArgs = driver.getClass()
        .getDeclaredMethod("getUrlsArgs", String.class);

    getUrlsArgs.setAccessible(true);

    try {
      final Map<String, String> parsedArgs = (Map<String, String>) getUrlsArgs
          .invoke(driver,
            "jdbc:arrow-flight://localhost:2222/?k1=v1&m=");
    } catch (InvocationTargetException e) {
      throw (SQLException) e.getCause();
    }
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
    if (testUtils.getUsername1().equals(username) &&
        testUtils.getPassword1().equals(password)) {
      identity = testUtils.getUsername1();
    } else {
      throw CallStatus.UNAUTHENTICATED
      .withDescription("Username or password is invalid.")
      .toRuntimeException();
    }
    return () -> identity;
  }

}
