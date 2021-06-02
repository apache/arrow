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

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;

import org.apache.arrow.driver.jdbc.ArrowFlightJdbcDriver;
import org.apache.arrow.flight.FlightRuntimeException;
import org.junit.Before;
import org.junit.Test;

/**
 * Tests for {@link Connection}.
 */
public class ConnectionTest {

  private String goodUrl;

  @SuppressWarnings("unused")
  private String badUrl; // TODO Test cases with this later.

  private ConnectionTest() {
    // Prevent instantiation.
  }

  /**
   * Setup for all tests.
   *
   * @throws ClassNotFoundException
   *           If the {@link ArrowFlightJdbcDriver} cannot be loaded.
   */
  @Before
  public void setUp() throws ClassNotFoundException {
    Class.forName("org.apache.arrow.driver.jdbc.ArrowFlightJdbcDriver");
    goodUrl = "jdbc:arrow-flight://localhost:32010";
    badUrl = "jdbc:mysql://localhost:3306"; // Not from Arrow Flight.
  }

  /**
   * Checks if an unencrypted connection can be established successfully when
   * the provided arguments are valid.
   *
   * @throws ClassNotFoundException
   *           when the class can not be loaded.
   * @throws SQLException
   *           on error.
   */
  @Test
  public void testUnencryptedConnectionShouldOpenSuccessfullyWhenProvidedWithGoodArgs()
      throws SQLException {
    Properties properties = new Properties();

    // Insert good (valid) args here.
    properties.put("user", "flight");
    properties.put("pass", "flight123");

    // Attempt to establish a connection to the Arrow Flight server.
    Connection connection = DriverManager.getConnection(goodUrl, properties);
    assertFalse(connection.isClosed());
    connection.close();
  }

  /**
   * Check if an unencrypted connection throws an exception when provided with
   * bad arguments for properties.
   *
   * @throws SQLException
   *           The exception expected to be thrown.
   */
  @Test(expected = FlightRuntimeException.class)
  public void testUnencryptedConnectionShouldThrowExceptionWhenProvidedWithBadArgs()
      throws SQLException {

    Properties properties = new Properties();

    properties.put("user", "_baduser");
    properties.put("pass", "_badpass");

    DriverManager.getConnection(goodUrl, properties);
  }

}
