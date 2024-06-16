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
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import org.apache.arrow.driver.jdbc.utils.CoreMockedSqlProducers;
import org.hamcrest.CoreMatchers;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

public class ResultSetMetadataTest {
  private static ResultSetMetaData metadata;

  private static Connection connection;

  @RegisterExtension
  public static final FlightServerTestExtension FLIGHT_SERVER_TEST_EXTENSION =
      FlightServerTestExtension.createStandardTestExtension(
          CoreMockedSqlProducers.getLegacyProducer());

  @BeforeAll
  public static void setup() throws SQLException {
    connection = FLIGHT_SERVER_TEST_EXTENSION.getConnection(false);

    try (Statement statement = connection.createStatement();
        ResultSet resultSet =
            statement.executeQuery(CoreMockedSqlProducers.LEGACY_METADATA_SQL_CMD)) {
      metadata = resultSet.getMetaData();
    }
  }

  @AfterAll
  public static void teardown() throws SQLException {
    connection.close();
  }

  /** Test if {@link ResultSetMetaData} object is not null. */
  @Test
  public void testShouldGetResultSetMetadata() {
    assertThat(metadata, CoreMatchers.is(notNullValue()));
  }

  /**
   * Test if {@link ResultSetMetaData#getColumnCount()} returns the correct values.
   *
   * @throws SQLException in case of error.
   */
  @Test
  public void testShouldGetColumnCount() throws SQLException {
    final int columnCount = metadata.getColumnCount();

    assert columnCount == 3;
  }

  /**
   * Test if {@link ResultSetMetaData#getColumnTypeName(int)} returns the correct type name for each
   * column.
   *
   * @throws SQLException in case of error.
   */
  @Test
  public void testShouldGetColumnTypesName() throws SQLException {
    final String firstColumn = metadata.getColumnTypeName(1);
    final String secondColumn = metadata.getColumnTypeName(2);
    final String thirdColumn = metadata.getColumnTypeName(3);

    assertThat(firstColumn, equalTo("BIGINT"));
    assertThat(secondColumn, equalTo("VARCHAR"));
    assertThat(thirdColumn, equalTo("FLOAT"));
  }

  /**
   * Test if {@link ResultSetMetaData#getColumnTypeName(int)} passing an column index that does not
   * exist.
   */
  @Test
  public void testShouldGetColumnTypesNameFromOutOfBoundIndex() {
    assertThrows(
        IndexOutOfBoundsException.class,
        () -> {
          metadata.getColumnTypeName(4);
        });
  }

  /**
   * Test if {@link ResultSetMetaData#getColumnName(int)} returns the correct name for each column.
   *
   * @throws SQLException in case of error.
   */
  @Test
  public void testShouldGetColumnNames() throws SQLException {
    final String firstColumn = metadata.getColumnName(1);
    final String secondColumn = metadata.getColumnName(2);
    final String thirdColumn = metadata.getColumnName(3);

    assertThat(firstColumn, equalTo("integer0"));
    assertThat(secondColumn, equalTo("string1"));
    assertThat(thirdColumn, equalTo("float2"));
  }

  /**
   * Test {@link ResultSetMetaData#getColumnTypeName(int)} passing an column index that does not
   * exist.
   */
  @Test
  public void testShouldGetColumnNameFromOutOfBoundIndex() {
    assertThrows(IndexOutOfBoundsException.class, () -> metadata.getColumnName(4));
  }

  /**
   * Test if {@link ResultSetMetaData#getColumnType(int)}returns the correct values.
   *
   * @throws SQLException in case of error.
   */
  @Test
  public void testShouldGetColumnType() throws SQLException {
    final int firstColumn = metadata.getColumnType(1);
    final int secondColumn = metadata.getColumnType(2);
    final int thirdColumn = metadata.getColumnType(3);

    assertThat(firstColumn, equalTo(Types.BIGINT));
    assertThat(secondColumn, equalTo(Types.VARCHAR));
    assertThat(thirdColumn, equalTo(Types.FLOAT));
  }

  @Test
  public void testShouldGetPrecision() throws SQLException {
    assertThat(metadata.getPrecision(1), equalTo(10));
    assertThat(metadata.getPrecision(2), equalTo(65535));
    assertThat(metadata.getPrecision(3), equalTo(15));
  }

  @Test
  public void testShouldGetScale() throws SQLException {
    assertThat(metadata.getScale(1), equalTo(0));
    assertThat(metadata.getScale(2), equalTo(0));
    assertThat(metadata.getScale(3), equalTo(20));
  }

  @Test
  public void testShouldGetCatalogName() throws SQLException {
    assertThat(metadata.getCatalogName(1), equalTo("CATALOG_NAME_1"));
    assertThat(metadata.getCatalogName(2), equalTo("CATALOG_NAME_2"));
    assertThat(metadata.getCatalogName(3), equalTo("CATALOG_NAME_3"));
  }

  @Test
  public void testShouldGetSchemaName() throws SQLException {
    assertThat(metadata.getSchemaName(1), equalTo("SCHEMA_NAME_1"));
    assertThat(metadata.getSchemaName(2), equalTo("SCHEMA_NAME_2"));
    assertThat(metadata.getSchemaName(3), equalTo("SCHEMA_NAME_3"));
  }

  @Test
  public void testShouldGetTableName() throws SQLException {
    assertThat(metadata.getTableName(1), equalTo("TABLE_NAME_1"));
    assertThat(metadata.getTableName(2), equalTo("TABLE_NAME_2"));
    assertThat(metadata.getTableName(3), equalTo("TABLE_NAME_3"));
  }

  @Test
  public void testShouldIsAutoIncrement() throws SQLException {
    assertThat(metadata.isAutoIncrement(1), equalTo(true));
    assertThat(metadata.isAutoIncrement(2), equalTo(false));
    assertThat(metadata.isAutoIncrement(3), equalTo(false));
  }

  @Test
  public void testShouldIsCaseSensitive() throws SQLException {
    assertThat(metadata.isCaseSensitive(1), equalTo(false));
    assertThat(metadata.isCaseSensitive(2), equalTo(true));
    assertThat(metadata.isCaseSensitive(3), equalTo(false));
  }

  @Test
  public void testShouldIsReadonly() throws SQLException {
    assertThat(metadata.isReadOnly(1), equalTo(true));
    assertThat(metadata.isReadOnly(2), equalTo(false));
    assertThat(metadata.isReadOnly(3), equalTo(false));
  }

  @Test
  public void testShouldIsSearchable() throws SQLException {
    assertThat(metadata.isSearchable(1), equalTo(true));
    assertThat(metadata.isSearchable(2), equalTo(true));
    assertThat(metadata.isSearchable(3), equalTo(true));
  }

  /**
   * Test if {@link ResultSetMetaData#getColumnTypeName(int)} passing an column index that does not
   * exist.
   */
  @Test
  public void testShouldGetColumnTypesFromOutOfBoundIndex() {
    assertThrows(IndexOutOfBoundsException.class, () -> metadata.getColumnType(4));
  }
}
