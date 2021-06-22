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

import static org.apache.arrow.driver.jdbc.utils.BaseProperty.HOST;
import static org.apache.arrow.driver.jdbc.utils.BaseProperty.PASSWORD;
import static org.apache.arrow.driver.jdbc.utils.BaseProperty.PORT;
import static org.apache.arrow.driver.jdbc.utils.BaseProperty.USERNAME;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.fail;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;

import org.apache.arrow.driver.jdbc.ArrowFlightJdbcDriver;
import org.apache.arrow.driver.jdbc.utils.BaseProperty;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;

import me.alexpanov.net.FreePortFinder;

public class ResultSetTest {
  private static Map<BaseProperty, Object> properties;

  @ClassRule
  public static FlightServerTestRule rule;

  private static Connection connection;

  static {
    properties = new HashMap<>();
    properties.put(HOST, "localhost");
    properties.put(PORT, FreePortFinder.findFreeLocalPort());
    properties.put(USERNAME, "flight-test-user");
    properties.put(PASSWORD, "flight-test-password");

    rule = new FlightServerTestRule(properties);
  }

  @BeforeClass
  public static void setup() throws SQLException {
    connection = rule.getConnection();
  }

  @AfterClass
  public static void tearDown() throws SQLException {
    connection.close();
  }

  /**
   * Tests whether the {@link ArrowFlightJdbcDriver} can run a query successfully.
   *
   * @throws Exception
   *           If the connection fails to be established.
   */
  @Test
  public void testShouldRunSelectQuery() throws Exception {
    Statement statement = connection.createStatement();
    ResultSet resultSet = statement.executeQuery("SELECT * FROM TEST");

    while (resultSet.next()) {
      assertNotNull(resultSet.getObject(1));
    }
  }

  /**
   * Tests whether the {@link ArrowFlightJdbcDriver} fails upon attempting
   * to run an invalid query.
   *
   * @throws Exception
   *           If the connection fails to be established.
   */
  @Test(expected = SQLException.class)
  public void testShouldThrowExceptionUponAttemptingToExecuteAnInvalidSelectQuery()
      throws Exception {
    Statement statement = connection.createStatement();
    ResultSet resultSet = statement.executeQuery("SELECT * FROM SHOULD-FAIL");
    fail();
  }
}
