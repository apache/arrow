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
import static org.hamcrest.CoreMatchers.allOf;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.not;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.Statement;
import java.util.Collections;

import org.apache.arrow.driver.jdbc.utils.MockFlightSqlProducer;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.util.AutoCloseables;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.Types.MinorType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.calcite.avatica.AvaticaUtils;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ErrorCollector;

/**
 * Tests for {@link ArrowFlightStatement#executeUpdate}.
 */
public class ArrowFlightStatementExecuteUpdateTest {
  private static final String UPDATE_SAMPLE_QUERY =
      "UPDATE sample_table SET sample_col = sample_val WHERE sample_condition";
  private static final int UPDATE_SAMPLE_QUERY_AFFECTED_COLS = 10;
  private static final String LARGE_UPDATE_SAMPLE_QUERY =
      "UPDATE large_sample_table SET large_sample_col = large_sample_val WHERE large_sample_condition";
  private static final long LARGE_UPDATE_SAMPLE_QUERY_AFFECTED_COLS = (long) Integer.MAX_VALUE + 1;
  private static final String REGULAR_QUERY_SAMPLE = "SELECT * FROM NOT_UPDATE_QUERY";
  private static final Schema REGULAR_QUERY_SCHEMA =
      new Schema(
          Collections.singletonList(Field.nullable("placeholder", MinorType.VARCHAR.getType())));
  private static final MockFlightSqlProducer PRODUCER = new MockFlightSqlProducer();
  @ClassRule
  public static final FlightServerTestRule SERVER_TEST_RULE = FlightServerTestRule.createStandardTestRule(PRODUCER);

  @Rule
  public final ErrorCollector collector = new ErrorCollector();
  public Connection connection;
  public Statement statement;

  @BeforeClass
  public static void setUpBeforeClass() {
    PRODUCER.addUpdateQuery(UPDATE_SAMPLE_QUERY, UPDATE_SAMPLE_QUERY_AFFECTED_COLS);
    PRODUCER.addUpdateQuery(LARGE_UPDATE_SAMPLE_QUERY, LARGE_UPDATE_SAMPLE_QUERY_AFFECTED_COLS);
    PRODUCER.addSelectQuery(
        REGULAR_QUERY_SAMPLE,
        REGULAR_QUERY_SCHEMA,
        Collections.singletonList(listener -> {
          try (final BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE);
               final VectorSchemaRoot root = VectorSchemaRoot.create(REGULAR_QUERY_SCHEMA,
                   allocator)) {
            listener.start(root);
            listener.putNext();
          } catch (final Throwable throwable) {
            listener.error(throwable);
          } finally {
            listener.completed();
          }
        }));
  }

  @Before
  public void setUp() throws SQLException {
    connection = SERVER_TEST_RULE.getConnection(false);
    statement = connection.createStatement();
  }

  @After
  public void tearDown() throws Exception {
    AutoCloseables.close(statement, connection);
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    AutoCloseables.close(PRODUCER);
  }

  @Test
  public void testExecuteUpdateShouldReturnNumColsAffectedForNumRowsFittingInt()
      throws SQLException {
    collector.checkThat(statement.executeUpdate(UPDATE_SAMPLE_QUERY),
        is(UPDATE_SAMPLE_QUERY_AFFECTED_COLS));
  }

  @Test
  public void testExecuteUpdateShouldReturnSaturatedNumColsAffectedIfDoesNotFitInInt()
      throws SQLException {
    final long result = statement.executeUpdate(LARGE_UPDATE_SAMPLE_QUERY);
    final long expectedRowCountRaw = LARGE_UPDATE_SAMPLE_QUERY_AFFECTED_COLS;
    collector.checkThat(
        result,
        is(allOf(
            not(equalTo(expectedRowCountRaw)),
            equalTo((long) AvaticaUtils.toSaturatedInt(
                expectedRowCountRaw))))); // Because of long-to-integer overflow.
  }

  @Test
  public void testExecuteLargeUpdateShouldReturnNumColsAffected() throws SQLException {
    collector.checkThat(
        statement.executeLargeUpdate(LARGE_UPDATE_SAMPLE_QUERY),
        is(LARGE_UPDATE_SAMPLE_QUERY_AFFECTED_COLS));
  }

  @Test(expected = SQLFeatureNotSupportedException.class)
  // TODO Implement `Statement#executeUpdate(String, int)`
  public void testExecuteUpdateUnsupportedWithDriverFlag() throws SQLException {
    collector.checkThat(
        statement.executeUpdate(UPDATE_SAMPLE_QUERY, Statement.RETURN_GENERATED_KEYS),
        is(UPDATE_SAMPLE_QUERY_AFFECTED_COLS));
  }

  @Test(expected = SQLFeatureNotSupportedException.class)
  // TODO Implement `Statement#executeUpdate(String, int[])`
  public void testExecuteUpdateUnsupportedWithArrayOfInts() throws SQLException {
    collector.checkThat(
        statement.executeUpdate(UPDATE_SAMPLE_QUERY, new int[0]),
        is(UPDATE_SAMPLE_QUERY_AFFECTED_COLS));
  }

  @Test(expected = SQLFeatureNotSupportedException.class)
  // TODO Implement `Statement#executeUpdate(String, String[])`
  public void testExecuteUpdateUnsupportedWithArraysOfStrings() throws SQLException {
    collector.checkThat(
        statement.executeUpdate(UPDATE_SAMPLE_QUERY, new String[0]),
        is(UPDATE_SAMPLE_QUERY_AFFECTED_COLS));
  }

  @Test
  public void testExecuteShouldExecuteUpdateQueryAutomatically() throws SQLException {
    collector.checkThat(statement.execute(UPDATE_SAMPLE_QUERY),
        is(false)); // Meaning there was an update query.
    collector.checkThat(statement.execute(REGULAR_QUERY_SAMPLE),
        is(true)); // Meaning there was a select query.
  }

  @Test
  public void testShouldFailToPrepareStatementForNullQuery() {
    int count = 0;
    try {
      collector.checkThat(statement.execute(null), is(false));
    } catch (final SQLException e) {
      count++;
      collector.checkThat(e.getCause(), is(instanceOf(NullPointerException.class)));
    }
    collector.checkThat(count, is(1));
  }

  @Test
  public void testShouldFailToPrepareStatementForClosedStatement() throws SQLException {
    statement.close();
    collector.checkThat(statement.isClosed(), is(true));
    int count = 0;
    try {
      statement.execute(UPDATE_SAMPLE_QUERY);
    } catch (final SQLException e) {
      count++;
      collector.checkThat(e.getMessage(), is("Statement closed"));
    }
    collector.checkThat(count, is(1));
  }

  @Test
  public void testShouldFailToPrepareStatementForBadStatement() {
    final String badQuery = "BAD INVALID STATEMENT";
    int count = 0;
    try {
      statement.execute(badQuery);
    } catch (final SQLException e) {
      count++;
      /*
       * The error message is up to whatever implementation of `FlightSqlProducer`
       * the driver is communicating with. However, for the purpose of this test,
       * we simply throw an `IllegalArgumentException` for queries not registered
       * in our `MockFlightSqlProducer`.
       */
      collector.checkThat(
          e.getMessage(),
          is(format("Error while executing SQL \"%s\": Query not found", badQuery)));
    }
    collector.checkThat(count, is(1));
  }
}
