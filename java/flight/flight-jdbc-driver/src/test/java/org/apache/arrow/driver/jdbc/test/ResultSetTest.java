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
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.sql.Connection;
import java.sql.Date;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.apache.arrow.driver.jdbc.ArrowFlightJdbcDriver;
import org.apache.arrow.driver.jdbc.utils.BaseProperty;
import org.hamcrest.CoreMatchers;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ErrorCollector;

import com.google.common.collect.ImmutableSet;

import me.alexpanov.net.FreePortFinder;

public class ResultSetTest {
  @ClassRule
  public static FlightServerTestRule rule;
  private static Map<BaseProperty, Object> properties;
  private static Connection connection;

  static {
    properties = new HashMap<>();
    properties.put(HOST, "localhost");
    properties.put(PORT, FreePortFinder.findFreeLocalPort());
    properties.put(USERNAME, "flight-test-user");
    properties.put(PASSWORD, "flight-test-password");

    rule = new FlightServerTestRule(properties);
  }

  @Rule
  public final ErrorCollector collector = new ErrorCollector();

  @BeforeClass
  public static void setup() throws SQLException {
    connection = rule.getConnection();
  }

  @AfterClass
  public static void tearDown() throws SQLException {
    connection.close();
  }

  private static void resultSetNextUntilDone(ResultSet resultSet) throws SQLException {
    while (resultSet.next()) {
      // TODO: implement resultSet.last()
      // Pass to the next until resultSet is done
    }
  }

  private static void setMaxRowsLimit(int maxRowsLimit, Statement statement) throws SQLException {
    statement.setLargeMaxRows(maxRowsLimit);
  }

  /**
   * Tests whether the {@link ArrowFlightJdbcDriver} can run a query successfully.
   *
   * @throws Exception If the connection fails to be established.
   */
  @Test
  public void testShouldRunSelectQuery() throws Exception {
    try (Statement statement = connection.createStatement();
         ResultSet resultSet = statement.executeQuery(FlightServerTestRule.QUERY_STRING)) {
      int count = 0;
      int expectedRows = 50000;

      Set<String> testNames =
          IntStream.range(0, expectedRows).mapToObj(i -> "Test Name #" + i).collect(Collectors.toSet());

      for (; resultSet.next(); count++) {
        collector.checkThat(resultSet.getObject(1), CoreMatchers.instanceOf(Long.class));
        collector.checkThat(testNames.remove(resultSet.getString(2)), CoreMatchers.is(true));
        collector.checkThat(resultSet.getObject(3), CoreMatchers.instanceOf(Integer.class));
        collector.checkThat(resultSet.getObject(4), CoreMatchers.instanceOf(Double.class));
        collector.checkThat(resultSet.getObject(5), CoreMatchers.instanceOf(Date.class));
        collector.checkThat(resultSet.getObject(6), CoreMatchers.instanceOf(Timestamp.class));
      }

      collector.checkThat(testNames.isEmpty(), CoreMatchers.is(true));
      collector.checkThat(expectedRows, CoreMatchers.is(count));
    }
  }

  @Test
  public void testShouldExecuteQueryNotBlockIfClosedBeforeEnd() throws Exception {
    try (Statement statement = connection.createStatement();
         ResultSet resultSet = statement.executeQuery(FlightServerTestRule.QUERY_STRING)) {

      for (int i = 0; i < 7500; i++) {
        assertTrue(resultSet.next());
      }
    }
  }

  /**
   * Tests whether the {@link ArrowFlightJdbcDriver} query only returns only the
   * amount of value set by {@link org.apache.calcite.avatica.AvaticaStatement#setMaxRows(int)}.
   *
   * @throws Exception If the connection fails to be established.
   */
  @Test
  public void testShouldRunSelectQuerySettingMaxRowLimit() throws Exception {
    try (Statement statement = connection.createStatement();
         ResultSet resultSet = statement.executeQuery("SELECT * FROM TEST")) {

      final int maxRowsLimit = 3;
      statement.setMaxRows(maxRowsLimit);

      collector.checkThat(statement.getMaxRows(), is(maxRowsLimit));

      int count = 0;
      int columns = 6;
      for (; resultSet.next(); count++) {
        for (int column = 1; column <= columns; column++) {
          resultSet.getObject(column);
        }
        collector.checkThat("Test Name #" + count, is(resultSet.getString(2)));
      }

      collector.checkThat(maxRowsLimit, is(count));
    }
  }

  /**
   * Tests whether the {@link ArrowFlightJdbcDriver} fails upon attempting
   * to run an invalid query.
   *
   * @throws Exception If the connection fails to be established.
   */
  @Test(expected = SQLException.class)
  public void testShouldThrowExceptionUponAttemptingToExecuteAnInvalidSelectQuery()
      throws Exception {
    Statement statement = connection.createStatement();
    ResultSet resultSet = statement.executeQuery("SELECT * FROM SHOULD-FAIL");
    fail();
  }

  /**
   * Tests whether the {@link ArrowFlightJdbcDriver} query only returns only the
   * amount of value set by {@link org.apache.calcite.avatica.AvaticaStatement#setLargeMaxRows(long)} (int)}.
   *
   * @throws Exception If the connection fails to be established.
   */
  @Test
  public void testShouldRunSelectQuerySettingLargeMaxRowLimit() throws Exception {
    try (Statement statement = connection.createStatement();
         ResultSet resultSet = statement.executeQuery("SELECT * FROM TEST")) {

      final long maxRowsLimit = 3;
      statement.setLargeMaxRows(maxRowsLimit);

      collector.checkThat(statement.getLargeMaxRows(), is(maxRowsLimit));

      int count = 0;
      int columns = resultSet.getMetaData().getColumnCount();
      for (; resultSet.next(); count++) {
        for (int column = 1; column <= columns; column++) {
          resultSet.getObject(column);
        }
        assertEquals("Test Name #" + count, resultSet.getString(2));
      }

      assertEquals(maxRowsLimit, count);
    }
  }

  @Test
  public void testColumnCountShouldRemainConsistentForResultSetThroughoutEntireDuration() throws SQLException {
    final Set<Integer> counts = new HashSet<>();
    try (final Statement statement = connection.createStatement();
         final ResultSet resultSet = statement.executeQuery(FlightServerTestRule.QUERY_STRING)) {
      while (resultSet.next()) {
        counts.add(resultSet.getMetaData().getColumnCount());
      }
    }
    collector.checkThat(counts, is(ImmutableSet.of(6)));
  }

  /**
   * Tests whether the {@link ArrowFlightJdbcDriver} close the statement after complete ResultSet
   * when call {@link org.apache.calcite.avatica.AvaticaStatement#closeOnCompletion()}.
   *
   * @throws Exception If the connection fails to be established.
   */
  @Test
  public void testShouldCloseStatementWhenIsCloseOnCompletion() throws Exception {
    Statement statement = connection.createStatement();
    ResultSet resultSet = statement.executeQuery("SELECT * FROM TEST");

    statement.closeOnCompletion();

    resultSetNextUntilDone(resultSet);

    collector.checkThat(statement.isClosed(), is(true));
  }

  /**
   * Tests whether the {@link ArrowFlightJdbcDriver} close the statement after complete ResultSet with max rows limit
   * when call {@link org.apache.calcite.avatica.AvaticaStatement#closeOnCompletion()}.
   *
   * @throws Exception If the connection fails to be established.
   */
  @Test
  public void testShouldCloseStatementWhenIsCloseOnCompletionWithMaxRowsLimit() throws Exception {
    Statement statement = connection.createStatement();
    ResultSet resultSet = statement.executeQuery("SELECT * FROM TEST");

    setMaxRowsLimit(3, statement);
    statement.closeOnCompletion();

    resultSetNextUntilDone(resultSet);

    collector.checkThat(statement.isClosed(), is(true));
  }

  /**
   * Tests whether the {@link ArrowFlightJdbcDriver} not close the statement after complete ResultSet with max rows
   * limit when call {@link org.apache.calcite.avatica.AvaticaStatement#closeOnCompletion()}.
   *
   * @throws Exception If the connection fails to be established.
   */
  @Test
  public void testShouldNotCloseStatementWhenIsNotCloseOnCompletionWithMaxRowsLimit() throws Exception {
    try (Statement statement = connection.createStatement();
         ResultSet resultSet = statement.executeQuery("SELECT * FROM TEST")) {

      setMaxRowsLimit(3, statement);

      resultSetNextUntilDone(resultSet);

      collector.checkThat(statement.isClosed(), is(false));
    }
  }
}
