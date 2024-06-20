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

import static org.hamcrest.CoreMatchers.allOf;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.arrow.driver.jdbc.utils.MockFlightSqlProducer;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.util.AutoCloseables;
import org.apache.arrow.vector.UInt1Vector;
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

/** Tests for {@link ArrowFlightStatement#execute}. */
public class ArrowFlightStatementExecuteTest {
  private static final String SAMPLE_QUERY_CMD = "SELECT * FROM this_test";
  private static final int SAMPLE_QUERY_ROWS = Byte.MAX_VALUE;
  private static final String VECTOR_NAME = "Unsigned Byte";
  private static final Schema SAMPLE_QUERY_SCHEMA =
      new Schema(Collections.singletonList(Field.nullable(VECTOR_NAME, MinorType.UINT1.getType())));
  private static final String SAMPLE_UPDATE_QUERY =
      "UPDATE this_table SET this_field = that_field FROM this_test WHERE this_condition";
  private static final long SAMPLE_UPDATE_COUNT = 100L;
  private static final String SAMPLE_LARGE_UPDATE_QUERY =
      "UPDATE this_large_table SET this_large_field = that_large_field FROM this_large_test WHERE this_large_condition";
  private static final long SAMPLE_LARGE_UPDATE_COUNT = Long.MAX_VALUE;
  private static final MockFlightSqlProducer PRODUCER = new MockFlightSqlProducer();

  @RegisterExtension
  public static final FlightServerTestExtension FLIGHT_SERVER_TEST_EXTENSION =
      FlightServerTestExtension.createStandardTestExtension(PRODUCER);

  private Connection connection;
  private Statement statement;

  @BeforeAll
  public static void setUpBeforeClass() {
    PRODUCER.addSelectQuery(
        SAMPLE_QUERY_CMD,
        SAMPLE_QUERY_SCHEMA,
        Collections.singletonList(
            listener -> {
              try (final BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE);
                  final VectorSchemaRoot root =
                      VectorSchemaRoot.create(SAMPLE_QUERY_SCHEMA, allocator)) {
                final UInt1Vector vector = (UInt1Vector) root.getVector(VECTOR_NAME);
                IntStream.range(0, SAMPLE_QUERY_ROWS)
                    .forEach(index -> vector.setSafe(index, index));
                vector.setValueCount(SAMPLE_QUERY_ROWS);
                root.setRowCount(SAMPLE_QUERY_ROWS);
                listener.start(root);
                listener.putNext();
              } catch (final Throwable throwable) {
                listener.error(throwable);
              } finally {
                listener.completed();
              }
            }));
    PRODUCER.addUpdateQuery(SAMPLE_UPDATE_QUERY, SAMPLE_UPDATE_COUNT);
    PRODUCER.addUpdateQuery(SAMPLE_LARGE_UPDATE_QUERY, SAMPLE_LARGE_UPDATE_COUNT);
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
  public void testExecuteShouldRunSelectQuery() throws SQLException {
    assertThat(statement.execute(SAMPLE_QUERY_CMD), is(true)); // Means this is a SELECT query.
    final Set<Byte> numbers =
        IntStream.range(0, SAMPLE_QUERY_ROWS)
            .boxed()
            .map(Integer::byteValue)
            .collect(Collectors.toCollection(HashSet::new));
    try (final ResultSet resultSet = statement.getResultSet()) {
      final int columnCount = resultSet.getMetaData().getColumnCount();
      assertThat(columnCount, is(1));
      int rowCount = 0;
      for (; resultSet.next(); rowCount++) {
        assertThat(numbers.remove(resultSet.getByte(1)), is(true));
      }
      assertThat(rowCount, is(equalTo(SAMPLE_QUERY_ROWS)));
    }
    assertThat(numbers, is(Collections.emptySet()));
    assertThat(
        (long) statement.getUpdateCount(),
        is(allOf(equalTo(statement.getLargeUpdateCount()), equalTo(-1L))));
  }

  @Test
  public void testExecuteShouldRunUpdateQueryForSmallUpdate() throws SQLException {
    assertThat(statement.execute(SAMPLE_UPDATE_QUERY), is(false)); // Means this is an UPDATE query.
    assertThat(
        (long) statement.getUpdateCount(),
        is(allOf(equalTo(statement.getLargeUpdateCount()), equalTo(SAMPLE_UPDATE_COUNT))));
    assertThat(statement.getResultSet(), is(nullValue()));
  }

  @Test
  public void testExecuteShouldRunUpdateQueryForLargeUpdate() throws SQLException {
    assertThat(statement.execute(SAMPLE_LARGE_UPDATE_QUERY), is(false)); // UPDATE query.
    final long updateCountSmall = statement.getUpdateCount();
    final long updateCountLarge = statement.getLargeUpdateCount();
    assertThat(updateCountLarge, is(equalTo(SAMPLE_LARGE_UPDATE_COUNT)));
    assertThat(
        updateCountSmall,
        is(
            allOf(
                equalTo((long) AvaticaUtils.toSaturatedInt(updateCountLarge)),
                not(equalTo(updateCountLarge)))));
    assertThat(statement.getResultSet(), is(nullValue()));
  }

  @Test
  public void testUpdateCountShouldStartOnZero() throws SQLException {
    assertThat(
        (long) statement.getUpdateCount(),
        is(allOf(equalTo(statement.getLargeUpdateCount()), equalTo(0L))));
    assertThat(statement.getResultSet(), is(nullValue()));
  }
}
