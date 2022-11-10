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

import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import org.apache.arrow.driver.jdbc.utils.CoreMockedSqlProducers;
import org.apache.arrow.driver.jdbc.utils.MockFlightSqlProducer;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ErrorCollector;

public class ArrowFlightPreparedStatementTest {

  public static final MockFlightSqlProducer PRODUCER = CoreMockedSqlProducers.getLegacyProducer();
  @ClassRule
  public static final FlightServerTestRule FLIGHT_SERVER_TEST_RULE = FlightServerTestRule
      .createStandardTestRule(PRODUCER);

  private static Connection connection;

  @Rule
  public final ErrorCollector collector = new ErrorCollector();

  @BeforeClass
  public static void setup() throws SQLException {
    connection = FLIGHT_SERVER_TEST_RULE.getConnection(false);
  }

  @AfterClass
  public static void tearDown() throws SQLException {
    connection.close();
  }

  @Test
  public void testSimpleQueryNoParameterBinding() throws SQLException {
    final String query = CoreMockedSqlProducers.LEGACY_REGULAR_SQL_CMD;
    try (final PreparedStatement preparedStatement = connection.prepareStatement(query);
         final ResultSet resultSet = preparedStatement.executeQuery()) {
      CoreMockedSqlProducers.assertLegacyRegularSqlResultSet(resultSet, collector);
    }
  }

  @Test
  public void testReturnColumnCount() throws SQLException {
    final String query = CoreMockedSqlProducers.LEGACY_REGULAR_SQL_CMD;
    try (final PreparedStatement psmt = connection.prepareStatement(query)) {
      collector.checkThat("ID", equalTo(psmt.getMetaData().getColumnName(1)));
      collector.checkThat("Name", equalTo(psmt.getMetaData().getColumnName(2)));
      collector.checkThat("Age", equalTo(psmt.getMetaData().getColumnName(3)));
      collector.checkThat("Salary", equalTo(psmt.getMetaData().getColumnName(4)));
      collector.checkThat("Hire Date", equalTo(psmt.getMetaData().getColumnName(5)));
      collector.checkThat("Last Sale", equalTo(psmt.getMetaData().getColumnName(6)));
      collector.checkThat(6, equalTo(psmt.getMetaData().getColumnCount()));
    }
  }

  @Test
  public void testUpdateQuery() throws SQLException {
    String query = "Fake update";
    PRODUCER.addUpdateQuery(query, /*updatedRows*/42);
    try (final PreparedStatement stmt = connection.prepareStatement(query)) {
      int updated = stmt.executeUpdate();
      assertEquals(42, updated);
    }
  }
}
