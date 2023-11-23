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

import static java.lang.String.format;
import static java.util.Collections.synchronizedSet;
import static org.apache.arrow.flight.Location.forGrpcInsecure;
import static org.hamcrest.CoreMatchers.allOf;
import static org.hamcrest.CoreMatchers.anyOf;
import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLTimeoutException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.CountDownLatch;

import org.apache.arrow.driver.jdbc.utils.CoreMockedSqlProducers;
import org.apache.arrow.driver.jdbc.utils.PartitionedFlightSqlProducer;
import org.apache.arrow.flight.FlightEndpoint;
import org.apache.arrow.flight.FlightProducer;
import org.apache.arrow.flight.FlightServer;
import org.apache.arrow.flight.Ticket;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ErrorCollector;

import com.google.common.collect.ImmutableSet;

public class ResultSetTest {
  private static final Random RANDOM = new Random(10);
  @ClassRule
  public static final FlightServerTestRule SERVER_TEST_RULE = FlightServerTestRule
      .createStandardTestRule(CoreMockedSqlProducers.getLegacyProducer());
  private static Connection connection;

  @Rule
  public final ErrorCollector collector = new ErrorCollector();

  @BeforeClass
  public static void setup() throws SQLException {
    connection = SERVER_TEST_RULE.getConnection(false);
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
         ResultSet resultSet = statement.executeQuery(
             CoreMockedSqlProducers.LEGACY_REGULAR_SQL_CMD)) {
      CoreMockedSqlProducers.assertLegacyRegularSqlResultSet(resultSet, collector);
    }
  }

  @Test
  public void testShouldExecuteQueryNotBlockIfClosedBeforeEnd() throws Exception {
    try (Statement statement = connection.createStatement();
         ResultSet resultSet = statement.executeQuery(
             CoreMockedSqlProducers.LEGACY_REGULAR_SQL_CMD)) {

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
         ResultSet resultSet = statement.executeQuery(
             CoreMockedSqlProducers.LEGACY_REGULAR_SQL_CMD)) {

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
    try (Statement statement = connection.createStatement();
         ResultSet result = statement.executeQuery("SELECT * FROM SHOULD-FAIL")) {
      fail();
    }
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
         ResultSet resultSet = statement.executeQuery(
             CoreMockedSqlProducers.LEGACY_REGULAR_SQL_CMD)) {
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
  public void testColumnCountShouldRemainConsistentForResultSetThroughoutEntireDuration()
      throws SQLException {
    final Set<Integer> counts = new HashSet<>();
    try (final Statement statement = connection.createStatement();
         final ResultSet resultSet = statement.executeQuery(
             CoreMockedSqlProducers.LEGACY_REGULAR_SQL_CMD)) {
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
    try (Statement statement = connection.createStatement();
        ResultSet resultSet = statement.executeQuery(CoreMockedSqlProducers.LEGACY_REGULAR_SQL_CMD)) {

      statement.closeOnCompletion();

      resultSetNextUntilDone(resultSet);

      collector.checkThat(statement.isClosed(), is(true));
    }
  }

  /**
   * Tests whether the {@link ArrowFlightJdbcDriver} close the statement after complete ResultSet with max rows limit
   * when call {@link org.apache.calcite.avatica.AvaticaStatement#closeOnCompletion()}.
   *
   * @throws Exception If the connection fails to be established.
   */
  @Test
  public void testShouldCloseStatementWhenIsCloseOnCompletionWithMaxRowsLimit() throws Exception {
    try (Statement statement = connection.createStatement();
         ResultSet resultSet = statement.executeQuery(CoreMockedSqlProducers.LEGACY_REGULAR_SQL_CMD)) {

      final long maxRowsLimit = 3;
      statement.setLargeMaxRows(maxRowsLimit);
      statement.closeOnCompletion();

      resultSetNextUntilDone(resultSet);

      collector.checkThat(statement.isClosed(), is(true));
    }
  }

  /**
   * Tests whether the {@link ArrowFlightJdbcDriver} not close the statement after complete ResultSet with max rows
   * limit when call {@link org.apache.calcite.avatica.AvaticaStatement#closeOnCompletion()}.
   *
   * @throws Exception If the connection fails to be established.
   */
  @Test
  public void testShouldNotCloseStatementWhenIsNotCloseOnCompletionWithMaxRowsLimit()
      throws Exception {
    try (Statement statement = connection.createStatement();
         ResultSet resultSet = statement.executeQuery(
             CoreMockedSqlProducers.LEGACY_REGULAR_SQL_CMD)) {

      final long maxRowsLimit = 3;
      statement.setLargeMaxRows(maxRowsLimit);

      collector.checkThat(statement.isClosed(), is(false));
      resultSetNextUntilDone(resultSet);
      collector.checkThat(resultSet.isClosed(), is(false));
      collector.checkThat(resultSet, is(instanceOf(ArrowFlightJdbcFlightStreamResultSet.class)));
    }
  }

  @Test
  public void testShouldCancelQueryUponCancelAfterQueryingResultSet() throws SQLException {
    try (final Statement statement = connection.createStatement();
         final ResultSet resultSet = statement.executeQuery(
             CoreMockedSqlProducers.LEGACY_REGULAR_SQL_CMD)) {
      final int column = RANDOM.nextInt(resultSet.getMetaData().getColumnCount()) + 1;
      collector.checkThat(resultSet.isClosed(), is(false));
      collector.checkThat(resultSet.next(), is(true));
      collector.checkSucceeds(() -> resultSet.getObject(column));
      statement.cancel();
      // Should reset `ResultSet`; keep both `ResultSet` and `Connection` open.
      collector.checkThat(statement.isClosed(), is(false));
      collector.checkThat(resultSet.isClosed(), is(false));
      collector.checkThat(resultSet.getMetaData().getColumnCount(), is(0));
    }
  }

  @Test
  public void testShouldInterruptFlightStreamsIfQueryIsCancelledMidQuerying()
      throws SQLException, InterruptedException {
    try (final Statement statement = connection.createStatement()) {
      final CountDownLatch latch = new CountDownLatch(1);
      final Set<Exception> exceptions = synchronizedSet(new HashSet<>(1));
      final Thread thread = new Thread(() -> {
        try (final ResultSet resultSet = statement.executeQuery(
            CoreMockedSqlProducers.LEGACY_REGULAR_SQL_CMD)) {
          final int cachedColumnCount = resultSet.getMetaData().getColumnCount();
          Thread.sleep(300);
          while (resultSet.next()) {
            resultSet.getObject(RANDOM.nextInt(cachedColumnCount) + 1);
          }
        } catch (final SQLException | InterruptedException e) {
          exceptions.add(e);
        } finally {
          latch.countDown();
        }
      });
      thread.setName("Test Case: interrupt query execution before first retrieval");
      thread.start();
      statement.cancel();
      thread.join();
      collector.checkThat(
          exceptions.stream()
              .map(Exception::getMessage)
              .map(StringBuilder::new)
              .reduce(StringBuilder::append)
              .orElseThrow(IllegalArgumentException::new)
              .toString(),
          is("Statement canceled"));
    }
  }

  @Test
  public void testShouldInterruptFlightStreamsIfQueryIsCancelledMidProcessingForTimeConsumingQueries()
      throws SQLException, InterruptedException {
    final String query = CoreMockedSqlProducers.LEGACY_CANCELLATION_SQL_CMD;
    try (final Statement statement = connection.createStatement()) {
      final Set<Exception> exceptions = synchronizedSet(new HashSet<>(1));
      final Thread thread = new Thread(() -> {
        try (final ResultSet ignored = statement.executeQuery(query)) {
          fail();
        } catch (final SQLException e) {
          exceptions.add(e);
        }
      });
      thread.setName("Test Case: interrupt query execution mid-process");
      thread.setPriority(Thread.MAX_PRIORITY);
      thread.start();
      Thread.sleep(5000); // Let the other thread attempt to retrieve results.
      statement.cancel();
      thread.join();
      collector.checkThat(
          exceptions.stream()
              .map(Exception::getMessage)
              .map(StringBuilder::new)
              .reduce(StringBuilder::append)
              .orElseThrow(IllegalStateException::new)
              .toString(),
          anyOf(is(format("Error while executing SQL \"%s\": Query canceled", query)),
              allOf(containsString(format("Error while executing SQL \"%s\"", query)),
                  containsString("CANCELLED"))));
    }
  }

  @Test
  public void testShouldInterruptFlightStreamsIfQueryTimeoutIsOver() throws SQLException {
    final String query = CoreMockedSqlProducers.LEGACY_CANCELLATION_SQL_CMD;
    final int timeoutValue = 2;
    final String timeoutUnit = "SECONDS";
    try (final Statement statement = connection.createStatement()) {
      statement.setQueryTimeout(timeoutValue);
      final Set<Exception> exceptions = new HashSet<>(1);
      try {
        statement.executeQuery(query);
      } catch (final Exception e) {
        exceptions.add(e);
      }
      final Throwable comparisonCause = exceptions.stream()
          .findFirst()
          .orElseThrow(RuntimeException::new)
          .getCause()
          .getCause();
      collector.checkThat(comparisonCause,
          is(instanceOf(SQLTimeoutException.class)));
      collector.checkThat(comparisonCause.getMessage(),
          is(format("Query timed out after %d %s", timeoutValue, timeoutUnit)));
    }
  }

  @Test
  public void testFlightStreamsQueryShouldNotTimeout() throws SQLException {
    final String query = CoreMockedSqlProducers.LEGACY_REGULAR_SQL_CMD;
    final int timeoutValue = 5;
    try (Statement statement = connection.createStatement()) {
      statement.setQueryTimeout(timeoutValue);
      try (ResultSet resultSet = statement.executeQuery(query)) {
        CoreMockedSqlProducers.assertLegacyRegularSqlResultSet(resultSet, collector);
      }
    }
  }

  @Test
  public void testPartitionedFlightServer() throws Exception {
    // Arrange
    final Schema schema = new Schema(
        Arrays.asList(Field.nullablePrimitive("int_column", new ArrowType.Int(32, true))));
    try (BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE);
        VectorSchemaRoot firstPartition = VectorSchemaRoot.create(schema, allocator);
        VectorSchemaRoot secondPartition = VectorSchemaRoot.create(schema, allocator)) {
      firstPartition.setRowCount(1);
      ((IntVector) firstPartition.getVector(0)).set(0, 1);
      secondPartition.setRowCount(1);
      ((IntVector) secondPartition.getVector(0)).set(0, 2);

      // Construct the data-only nodes first.
      FlightProducer firstProducer = new PartitionedFlightSqlProducer.DataOnlyFlightSqlProducer(
          new Ticket("first".getBytes()), firstPartition);
      FlightProducer secondProducer = new PartitionedFlightSqlProducer.DataOnlyFlightSqlProducer(
          new Ticket("second".getBytes()), secondPartition);

      final FlightServer.Builder firstBuilder = FlightServer.builder(
          allocator, forGrpcInsecure("localhost", 0), firstProducer);

      final FlightServer.Builder secondBuilder = FlightServer.builder(
          allocator, forGrpcInsecure("localhost", 0), secondProducer);

      // Run the data-only nodes so that we can get the Locations they are running at.
      try (FlightServer firstServer = firstBuilder.build();
           FlightServer secondServer = secondBuilder.build()) {
        firstServer.start();
        secondServer.start();
        final FlightEndpoint firstEndpoint =
            new FlightEndpoint(new Ticket("first".getBytes()), firstServer.getLocation());

        final FlightEndpoint secondEndpoint =
            new FlightEndpoint(new Ticket("second".getBytes()), secondServer.getLocation());

        // Finally start the root node.
        try (final PartitionedFlightSqlProducer rootProducer = new PartitionedFlightSqlProducer(
            schema, firstEndpoint, secondEndpoint);
             FlightServer rootServer = FlightServer.builder(
                 allocator, forGrpcInsecure("localhost", 0), rootProducer)
                 .build()
                 .start();
             Connection newConnection = DriverManager.getConnection(String.format(
                 "jdbc:arrow-flight-sql://%s:%d/?useEncryption=false",
                 rootServer.getLocation().getUri().getHost(), rootServer.getPort()));
             Statement newStatement = newConnection.createStatement();
             // Act
             ResultSet result = newStatement.executeQuery("Select partitioned_data")) {
          List<Integer> resultData = new ArrayList<>();
          while (result.next()) {
            resultData.add(result.getInt(1));
          }

          // Assert
          assertEquals(firstPartition.getRowCount() + secondPartition.getRowCount(), resultData.size());
          assertTrue(resultData.contains(((IntVector) firstPartition.getVector(0)).get(0)));
          assertTrue(resultData.contains(((IntVector) secondPartition.getVector(0)).get(0)));
        }
      }
    }
  }

  @Test
  public void testShouldRunSelectQueryWithEmptyVectorsEmbedded() throws Exception {
    try (Statement statement = connection.createStatement();
         ResultSet resultSet = statement.executeQuery(
             CoreMockedSqlProducers.LEGACY_REGULAR_WITH_EMPTY_SQL_CMD)) {
      long rowCount = 0;
      while (resultSet.next()) {
        ++rowCount;
      }
      assertEquals(2, rowCount);
    }
  }
}
