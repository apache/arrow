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
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

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
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

/** Tests for {@link ArrowFlightStatement#executeUpdate}. */
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

  @RegisterExtension
  public static final FlightServerTestExtension FLIGHT_SERVER_TEST_EXTENSION =
      FlightServerTestExtension.createStandardTestExtension(PRODUCER);

  public Connection connection;
  public Statement statement;

  @BeforeAll
  public static void setUpBeforeClass() {
    PRODUCER.addUpdateQuery(UPDATE_SAMPLE_QUERY, UPDATE_SAMPLE_QUERY_AFFECTED_COLS);
    PRODUCER.addUpdateQuery(LARGE_UPDATE_SAMPLE_QUERY, LARGE_UPDATE_SAMPLE_QUERY_AFFECTED_COLS);
    PRODUCER.addSelectQuery(
        REGULAR_QUERY_SAMPLE,
        REGULAR_QUERY_SCHEMA,
        Collections.singletonList(
            listener -> {
              try (final BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE);
                  final VectorSchemaRoot root =
                      VectorSchemaRoot.create(REGULAR_QUERY_SCHEMA, allocator)) {
                listener.start(root);
                listener.putNext();
              } catch (final Throwable throwable) {
                listener.error(throwable);
              } finally {
                listener.completed();
              }
            }));
  }

  @BeforeEach
  public void setUp() throws SQLException {
    connection = FLIGHT_SERVER_TEST_EXTENSION.getConnection(false);
    statement = connection.createStatement();
  }

  @AfterEach
  public void tearDown() throws Exception {
    AutoCloseables.close(statement, connection);
  }

  @AfterAll
  public static void tearDownAfterClass() throws Exception {
    AutoCloseables.close(PRODUCER);
  }

  @Test
  public void testExecuteUpdateShouldReturnNumColsAffectedForNumRowsFittingInt()
      throws SQLException {
    assertThat(statement.executeUpdate(UPDATE_SAMPLE_QUERY), is(UPDATE_SAMPLE_QUERY_AFFECTED_COLS));
  }

  @Test
  public void testExecuteUpdateShouldReturnSaturatedNumColsAffectedIfDoesNotFitInInt()
      throws SQLException {
    final long result = statement.executeUpdate(LARGE_UPDATE_SAMPLE_QUERY);
    final long expectedRowCountRaw = LARGE_UPDATE_SAMPLE_QUERY_AFFECTED_COLS;
    assertThat(
        result,
        is(
            allOf(
                not(equalTo(expectedRowCountRaw)),
                equalTo(
                    (long)
                        AvaticaUtils.toSaturatedInt(
                            expectedRowCountRaw))))); // Because of long-to-integer overflow.
  }

  @Test
  public void testExecuteLargeUpdateShouldReturnNumColsAffected() throws SQLException {
    assertThat(
        statement.executeLargeUpdate(LARGE_UPDATE_SAMPLE_QUERY),
        is(LARGE_UPDATE_SAMPLE_QUERY_AFFECTED_COLS));
  }

  @Test
  // TODO Implement `Statement#executeUpdate(String, int)`
  public void testExecuteUpdateUnsupportedWithDriverFlag() throws SQLException {
    assertThrows(
        SQLFeatureNotSupportedException.class,
        () -> {
          assertThat(
              statement.executeUpdate(UPDATE_SAMPLE_QUERY, Statement.NO_GENERATED_KEYS),
              is(UPDATE_SAMPLE_QUERY_AFFECTED_COLS));
        });
  }

  @Test
  // TODO Implement `Statement#executeUpdate(String, int[])`
  public void testExecuteUpdateUnsupportedWithArrayOfInts() throws SQLException {
    assertThrows(
        SQLFeatureNotSupportedException.class,
        () -> {
          assertThat(
              statement.executeUpdate(UPDATE_SAMPLE_QUERY, new int[0]),
              is(UPDATE_SAMPLE_QUERY_AFFECTED_COLS));
        });
  }

  @Test
  // TODO Implement `Statement#executeUpdate(String, String[])`
  public void testExecuteUpdateUnsupportedWithArraysOfStrings() throws SQLException {
    assertThrows(
        SQLFeatureNotSupportedException.class,
        () -> {
          assertThat(
              statement.executeUpdate(UPDATE_SAMPLE_QUERY, new String[0]),
              is(UPDATE_SAMPLE_QUERY_AFFECTED_COLS));
        });
  }

  @Test
  public void testExecuteShouldExecuteUpdateQueryAutomatically() throws SQLException {
    assertThat(
        statement.execute(UPDATE_SAMPLE_QUERY), is(false)); // Meaning there was an update query.
    assertThat(
        statement.execute(REGULAR_QUERY_SAMPLE), is(true)); // Meaning there was a select query.
  }

  @Test
  public void testShouldFailToPrepareStatementForNullQuery() {
    int count = 0;
    try {
      assertThat(statement.execute(null), is(false));
    } catch (final SQLException e) {
      count++;
      assertThat(e.getCause(), is(instanceOf(NullPointerException.class)));
    }
    assertThat(count, is(1));
  }

  @Test
  public void testShouldFailToPrepareStatementForClosedStatement() throws SQLException {
    statement.close();
    assertThat(statement.isClosed(), is(true));
    int count = 0;
    try {
      statement.execute(UPDATE_SAMPLE_QUERY);
    } catch (final SQLException e) {
      count++;
      assertThat(e.getMessage(), is("Statement closed"));
    }
    assertThat(count, is(1));
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
      assertThat(
          e.getMessage(),
          is(format("Error while executing SQL \"%s\": Query not found", badQuery)));
    }
    assertThat(count, is(1));
  }
}
