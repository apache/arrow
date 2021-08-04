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

import java.lang.reflect.Constructor;
import java.sql.Connection;
import java.util.Properties;

import org.apache.arrow.driver.jdbc.ArrowFlightJdbcDriver;
import org.apache.arrow.driver.jdbc.ArrowFlightJdbcFactory;
import org.apache.arrow.driver.jdbc.test.utils.FlightTestUtils;
import org.apache.arrow.driver.jdbc.test.utils.PropertiesSample;
import org.apache.arrow.driver.jdbc.test.utils.UrlSample;
import org.apache.arrow.flight.CallStatus;
import org.apache.arrow.flight.FlightProducer;
import org.apache.arrow.flight.FlightServer;
import org.apache.arrow.flight.auth2.BasicCallHeaderAuthenticator;
import org.apache.arrow.flight.auth2.CallHeaderAuthenticator;
import org.apache.arrow.flight.auth2.GeneratedBearerTokenAuthenticator;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.util.AutoCloseables;
import org.apache.calcite.avatica.UnregisteredDriver;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.google.common.base.Strings;

/**
 * Tests for {@link ArrowFlightJdbcDriver}.
 * TODO Update to use {@link FlightServerTestRule} instead of {@link FlightTestUtils}
 */
public class ArrowFlightJdbcFactoryTest {

  private BufferAllocator allocator;
  private FlightServer server;
  FlightTestUtils testUtils;

  @Before
  public void setUp() throws Exception {
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

    server = testUtils.getStartedServer(
        location -> FlightServer.builder(allocator, location, flightProducer)
            .headerAuthenticator(new GeneratedBearerTokenAuthenticator(
                new BasicCallHeaderAuthenticator(this::validate)))
            .build());
  }

  @After
  public void tearDown() throws Exception {
    AutoCloseables.close(server, allocator);
  }

  @Test
  public void testShouldBeAbleToEstablishAConnectionSuccessfully()
          throws Exception {
    UnregisteredDriver driver = new ArrowFlightJdbcDriver();
    Constructor<ArrowFlightJdbcFactory> constructor =
            ArrowFlightJdbcFactory.class
                    .getConstructor();
    constructor.setAccessible(true);
    ArrowFlightJdbcFactory factory = constructor.newInstance();

    try (Connection connection = factory.newConnection(driver,
            constructor.newInstance(),
            "jdbc:arrow-flight://localhost:32010",
            new Properties())) {
      assert connection.isValid(300);
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
