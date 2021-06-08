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

import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.SQLException;

import org.apache.arrow.driver.jdbc.ArrowFlightJdbcDriver;
import org.apache.arrow.driver.jdbc.test.utils.PropertiesSample;
import org.apache.arrow.driver.jdbc.test.utils.UrlSample;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * Tests for {@link ArrowFlightJdbcDriver}.
 */
public class ArrowFlightJdbcDriverTest {

  @Before
  public void setUp() throws Exception {
    // TODO Replace this.
    Class.forName("org.apache.arrow.driver.jdbc.ArrowFlightJdbcDriver");
  }

  @After
  public void tearDown() throws Exception {
  }

  /**
   * Tests whether the {@link ArrowFlightJdbcDriver} is registered in the
   * {@link DriverManager}.
   * 
   * @throws SQLException
   *           If an error occurs. (This is not supposed to happen.)
   */
  @Test
  public void testDriverIsRegisteredInDriverManager() throws SQLException {
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
  public void testShouldDeclineUrlWithUnsupportedPrefix() throws SQLException {
    Driver driver = new ArrowFlightJdbcDriver();

    driver.connect(UrlSample.UNSUPPORTED.getPath(),
        PropertiesSample.UNSUPPORTED.getProperties()).close();
  }

  /**
   * Tests whether the {@link ArrowFlightJdbcDriver} can establish a successful
   * connection to the Arrow Flight client.
   * 
   * @throws SQLException
   *           If the connection fails to be established.
   */
  @Test
  public void testShouldConnectWhenProvidedWithValidUrl() throws SQLException {
    // Get the Arrow Flight JDBC driver by providing a URL with a valid prefix.
    Driver driver = DriverManager.getDriver(UrlSample.CONFORMING.getPath());

    try (Connection connection = driver.connect(UrlSample.CONFORMING.getPath(),
        PropertiesSample.CONFORMING.getProperties())) {
      assert connection.isValid(300);
    }
  }

}
