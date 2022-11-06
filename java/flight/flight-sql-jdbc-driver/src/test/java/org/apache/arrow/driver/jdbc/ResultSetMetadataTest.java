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

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;

import org.apache.arrow.driver.jdbc.utils.CoreMockedSqlProducers;
import org.hamcrest.CoreMatchers;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ErrorCollector;

public class ResultSetMetadataTest {
  private static ResultSetMetaData metadata;

  private static Connection connection;

  @Rule
  public ErrorCollector collector = new ErrorCollector();

  @ClassRule
  public static final FlightServerTestRule SERVER_TEST_RULE = FlightServerTestRule
      .createStandardTestRule(CoreMockedSqlProducers.getLegacyProducer());

  @BeforeClass
  public static void setup() throws SQLException {
    connection = SERVER_TEST_RULE.getConnection(false);

    try (Statement statement = connection.createStatement();
         ResultSet resultSet = statement.executeQuery(
             CoreMockedSqlProducers.LEGACY_METADATA_SQL_CMD)) {
      metadata = resultSet.getMetaData();
    }
  }

  @AfterClass
  public static void teardown() throws SQLException {
    connection.close();
  }

  /**
   * Test if {@link ResultSetMetaData} object is not null.
   */
  @Test
  public void testShouldGetResultSetMetadata() {
    collector.checkThat(metadata, CoreMatchers.is(notNullValue()));
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
   * Test if {@link ResultSetMetaData#getColumnTypeName(int)}  returns the correct type name for each
   * column.
   *
   * @throws SQLException in case of error.
   */
  @Test
  public void testShouldGetColumnTypesName() throws SQLException {
    final String firstColumn = metadata.getColumnTypeName(1);
    final String secondColumn = metadata.getColumnTypeName(2);
    final String thirdColumn = metadata.getColumnTypeName(3);

    collector.checkThat(firstColumn, equalTo("BIGINT"));
    collector.checkThat(secondColumn, equalTo("VARCHAR"));
    collector.checkThat(thirdColumn, equalTo("FLOAT"));
  }

  /**
   * Test if {@link ResultSetMetaData#getColumnTypeName(int)} passing an column index that does not exist.
   *
   * @throws SQLException in case of error.
   */
  @Test(expected = IndexOutOfBoundsException.class)
  public void testShouldGetColumnTypesNameFromOutOfBoundIndex() throws SQLException {
    metadata.getColumnTypeName(4);

    Assert.fail();
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

    collector.checkThat(firstColumn, equalTo("integer0"));
    collector.checkThat(secondColumn, equalTo("string1"));
    collector.checkThat(thirdColumn, equalTo("float2"));
  }


  /**
   * Test {@link ResultSetMetaData#getColumnTypeName(int)} passing an column index that does not exist.
   *
   * @throws SQLException in case of error.
   */
  @Test(expected = IndexOutOfBoundsException.class)
  public void testShouldGetColumnNameFromOutOfBoundIndex() throws SQLException {
    metadata.getColumnName(4);

    Assert.fail();
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

    collector.checkThat(firstColumn, equalTo(Types.BIGINT));
    collector.checkThat(secondColumn, equalTo(Types.VARCHAR));
    collector.checkThat(thirdColumn, equalTo(Types.FLOAT));
  }

  @Test
  public void testShouldGetPrecision() throws SQLException {
    collector.checkThat(metadata.getPrecision(1), equalTo(10));
    collector.checkThat(metadata.getPrecision(2), equalTo(65535));
    collector.checkThat(metadata.getPrecision(3), equalTo(15));
  }

  @Test
  public void testShouldGetScale() throws SQLException {
    collector.checkThat(metadata.getScale(1), equalTo(0));
    collector.checkThat(metadata.getScale(2), equalTo(0));
    collector.checkThat(metadata.getScale(3), equalTo(20));
  }

  @Test
  public void testShouldGetCatalogName() throws SQLException {
    collector.checkThat(metadata.getCatalogName(1), equalTo("CATALOG_NAME_1"));
    collector.checkThat(metadata.getCatalogName(2), equalTo("CATALOG_NAME_2"));
    collector.checkThat(metadata.getCatalogName(3), equalTo("CATALOG_NAME_3"));
  }

  @Test
  public void testShouldGetSchemaName() throws SQLException {
    collector.checkThat(metadata.getSchemaName(1), equalTo("SCHEMA_NAME_1"));
    collector.checkThat(metadata.getSchemaName(2), equalTo("SCHEMA_NAME_2"));
    collector.checkThat(metadata.getSchemaName(3), equalTo("SCHEMA_NAME_3"));
  }

  @Test
  public void testShouldGetTableName() throws SQLException {
    collector.checkThat(metadata.getTableName(1), equalTo("TABLE_NAME_1"));
    collector.checkThat(metadata.getTableName(2), equalTo("TABLE_NAME_2"));
    collector.checkThat(metadata.getTableName(3), equalTo("TABLE_NAME_3"));
  }

  @Test
  public void testShouldIsAutoIncrement() throws SQLException {
    collector.checkThat(metadata.isAutoIncrement(1), equalTo(true));
    collector.checkThat(metadata.isAutoIncrement(2), equalTo(false));
    collector.checkThat(metadata.isAutoIncrement(3), equalTo(false));
  }

  @Test
  public void testShouldIsCaseSensitive() throws SQLException {
    collector.checkThat(metadata.isCaseSensitive(1), equalTo(false));
    collector.checkThat(metadata.isCaseSensitive(2), equalTo(true));
    collector.checkThat(metadata.isCaseSensitive(3), equalTo(false));
  }

  @Test
  public void testShouldIsReadonly() throws SQLException {
    collector.checkThat(metadata.isReadOnly(1), equalTo(true));
    collector.checkThat(metadata.isReadOnly(2), equalTo(false));
    collector.checkThat(metadata.isReadOnly(3), equalTo(false));
  }

  @Test
  public void testShouldIsSearchable() throws SQLException {
    collector.checkThat(metadata.isSearchable(1), equalTo(true));
    collector.checkThat(metadata.isSearchable(2), equalTo(true));
    collector.checkThat(metadata.isSearchable(3), equalTo(true));
  }

  /**
   * Test if {@link ResultSetMetaData#getColumnTypeName(int)} passing an column index that does not exist.
   *
   * @throws SQLException in case of error.
   */
  @Test(expected = IndexOutOfBoundsException.class)
  public void testShouldGetColumnTypesFromOutOfBoundIndex() throws SQLException {
    metadata.getColumnType(4);

    Assert.fail();
  }
}
