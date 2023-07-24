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

package org.apache.arrow.driver.jdbc.utils;

import static java.util.stream.IntStream.range;
import static org.hamcrest.CoreMatchers.is;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;

import org.apache.arrow.util.Preconditions;
import org.junit.rules.ErrorCollector;

/**
 * Utility class for testing that require asserting that the values in a {@link ResultSet} are expected.
 */
public final class ResultSetTestUtils {
  private final ErrorCollector collector;

  public ResultSetTestUtils(final ErrorCollector collector) {
    this.collector =
        Preconditions.checkNotNull(collector, "Error collector cannot be null.");
  }

  /**
   * Checks that the values (rows and columns) in the provided {@link ResultSet} are expected.
   *
   * @param resultSet       the {@code ResultSet} to assert.
   * @param expectedResults the rows and columns representing the only values the {@code resultSet}
   *                        is expected to have.
   * @param collector       the {@link ErrorCollector} to use for asserting that the {@code resultSet}
   *                        has the expected values.
   * @param <T>             the type to be found in the expected results for the {@code resultSet}.
   * @throws SQLException if querying the {@code ResultSet} fails at some point unexpectedly.
   */
  public static <T> void testData(final ResultSet resultSet, final List<List<T>> expectedResults,
                                  final ErrorCollector collector)
      throws SQLException {
    testData(
        resultSet,
        range(1, resultSet.getMetaData().getColumnCount() + 1).toArray(),
        expectedResults,
        collector);
  }

  /**
   * Checks that the values (rows and columns) in the provided {@link ResultSet} are expected.
   *
   * @param resultSet       the {@code ResultSet} to assert.
   * @param columnNames     the column names to fetch in the {@code ResultSet} for comparison.
   * @param expectedResults the rows and columns representing the only values the {@code resultSet}
   *                        is expected to have.
   * @param collector       the {@link ErrorCollector} to use for asserting that the {@code resultSet}
   *                        has the expected values.
   * @param <T>             the type to be found in the expected results for the {@code resultSet}.
   * @throws SQLException if querying the {@code ResultSet} fails at some point unexpectedly.
   */
  @SuppressWarnings("unchecked")
  public static <T> void testData(final ResultSet resultSet, final List<String> columnNames,
                                  final List<List<T>> expectedResults,
                                  final ErrorCollector collector)
      throws SQLException {
    testData(
        resultSet,
        data -> {
          final List<T> columns = new ArrayList<>();
          for (final String columnName : columnNames) {
            try {
              columns.add((T) resultSet.getObject(columnName));
            } catch (final SQLException e) {
              collector.addError(e);
            }
          }
          return columns;
        },
        expectedResults,
        collector);
  }

  /**
   * Checks that the values (rows and columns) in the provided {@link ResultSet} are expected.
   *
   * @param resultSet       the {@code ResultSet} to assert.
   * @param columnIndices   the column indices to fetch in the {@code ResultSet} for comparison.
   * @param expectedResults the rows and columns representing the only values the {@code resultSet}
   *                        is expected to have.
   * @param collector       the {@link ErrorCollector} to use for asserting that the {@code resultSet}
   *                        has the expected values.
   * @param <T>             the type to be found in the expected results for the {@code resultSet}.
   * @throws SQLException if querying the {@code ResultSet} fails at some point unexpectedly.
   */
  @SuppressWarnings("unchecked")
  public static <T> void testData(final ResultSet resultSet, final int[] columnIndices,
                                  final List<List<T>> expectedResults,
                                  final ErrorCollector collector)
      throws SQLException {
    testData(
        resultSet,
        data -> {
          final List<T> columns = new ArrayList<>();
          for (final int columnIndex : columnIndices) {
            try {
              columns.add((T) resultSet.getObject(columnIndex));
            } catch (final SQLException e) {
              collector.addError(e);
            }
          }
          return columns;
        },
        expectedResults,
        collector);
  }

  /**
   * Checks that the values (rows and columns) in the provided {@link ResultSet} are expected.
   *
   * @param resultSet       the {@code ResultSet} to assert.
   * @param dataConsumer    the column indices to fetch in the {@code ResultSet} for comparison.
   * @param expectedResults the rows and columns representing the only values the {@code resultSet}
   *                        is expected to have.
   * @param collector       the {@link ErrorCollector} to use for asserting that the {@code resultSet}
   *                        has the expected values.
   * @param <T>             the type to be found in the expected results for the {@code resultSet}.
   * @throws SQLException if querying the {@code ResultSet} fails at some point unexpectedly.
   */
  public static <T> void testData(final ResultSet resultSet,
                                  final Function<ResultSet, List<T>> dataConsumer,
                                  final List<List<T>> expectedResults,
                                  final ErrorCollector collector)
      throws SQLException {
    final List<List<T>> actualResults = new ArrayList<>();
    while (resultSet.next()) {
      actualResults.add(dataConsumer.apply(resultSet));
    }
    collector.checkThat(actualResults, is(expectedResults));
  }

  /**
   * Checks that the values (rows and columns) in the provided {@link ResultSet} are expected.
   *
   * @param resultSet       the {@code ResultSet} to assert.
   * @param expectedResults the rows and columns representing the only values the {@code resultSet} is expected to have.
   * @param <T>             the type to be found in the expected results for the {@code resultSet}.
   * @throws SQLException if querying the {@code ResultSet} fails at some point unexpectedly.
   */
  public <T> void testData(final ResultSet resultSet, final List<List<T>> expectedResults)
      throws SQLException {
    testData(resultSet, expectedResults, collector);
  }

  /**
   * Checks that the values (rows and columns) in the provided {@link ResultSet} are expected.
   *
   * @param resultSet       the {@code ResultSet} to assert.
   * @param columnNames     the column names to fetch in the {@code ResultSet} for comparison.
   * @param expectedResults the rows and columns representing the only values the {@code resultSet} is expected to have.
   * @param <T>             the type to be found in the expected results for the {@code resultSet}.
   * @throws SQLException if querying the {@code ResultSet} fails at some point unexpectedly.
   */
  @SuppressWarnings("unchecked")
  public <T> void testData(final ResultSet resultSet, final List<String> columnNames,
                           final List<List<T>> expectedResults) throws SQLException {
    testData(resultSet, columnNames, expectedResults, collector);
  }

  /**
   * Checks that the values (rows and columns) in the provided {@link ResultSet} are expected.
   *
   * @param resultSet       the {@code ResultSet} to assert.
   * @param columnIndices   the column indices to fetch in the {@code ResultSet} for comparison.
   * @param expectedResults the rows and columns representing the only values the {@code resultSet} is expected to have.
   * @param <T>             the type to be found in the expected results for the {@code resultSet}.
   * @throws SQLException if querying the {@code ResultSet} fails at some point unexpectedly.
   */
  @SuppressWarnings("unchecked")
  public <T> void testData(final ResultSet resultSet, final int[] columnIndices,
                           final List<List<T>> expectedResults) throws SQLException {
    testData(resultSet, columnIndices, expectedResults, collector);
  }

  /**
   * Checks that the values (rows and columns) in the provided {@link ResultSet} are expected.
   *
   * @param resultSet       the {@code ResultSet} to assert.
   * @param dataConsumer    the column indices to fetch in the {@code ResultSet} for comparison.
   * @param expectedResults the rows and columns representing the only values the {@code resultSet} is expected to have.
   * @param <T>             the type to be found in the expected results for the {@code resultSet}.
   * @throws SQLException if querying the {@code ResultSet} fails at some point unexpectedly.
   */
  public <T> void testData(final ResultSet resultSet,
                           final Function<ResultSet, List<T>> dataConsumer,
                           final List<List<T>> expectedResults) throws SQLException {
    testData(resultSet, dataConsumer, expectedResults, collector);
  }
}
