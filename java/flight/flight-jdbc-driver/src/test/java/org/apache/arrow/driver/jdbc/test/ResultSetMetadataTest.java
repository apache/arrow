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
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.notNullValue;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.util.HashMap;
import java.util.Map;

import org.apache.arrow.driver.jdbc.utils.BaseProperty;
import org.hamcrest.CoreMatchers;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ErrorCollector;

import me.alexpanov.net.FreePortFinder;

public class ResultSetMetadataTest {

  private static final Map<BaseProperty, Object> properties;
  private static ResultSetMetaData metadata;

  private static Connection connection;

  @Rule
  public ErrorCollector collector = new ErrorCollector();

  @ClassRule
  public static final FlightServerTestRule rule;

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

    final Statement statement = connection.createStatement();
    ResultSet resultSet = statement.executeQuery(FlightServerTestRule.METADATA_TEST_SQL_CMD);

    metadata = resultSet.getMetaData();
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

    collector.checkThat(firstColumn, equalTo("Int"));
    collector.checkThat(secondColumn, equalTo("Utf8"));
    collector.checkThat(thirdColumn, equalTo("FloatingPoint"));
  }

  /**
   * Test if {@link ResultSetMetaData#getColumnTypeName(int)} passing an column index that does not exist.
   *
   * @throws SQLException in case of error.
   */
  @Test(expected = IndexOutOfBoundsException.class)
  public void testShouldGetColumnTypesNameFromOutOfBoundIndex() throws SQLException {
    final String outOfBoundColumn = metadata.getColumnTypeName(4);

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
    final String outOfBoundColumn = metadata.getColumnName(4);

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


  /**
   * Test if {@link ResultSetMetaData#getColumnTypeName(int)} passing an column index that does not exist.
   *
   * @throws SQLException in case of error.
   */
  @Test(expected = IndexOutOfBoundsException.class)
  public void testShouldGetColumnTypesFromOutOfBoundIndex() throws SQLException {
    final int outOfBoundColumn = metadata.getColumnType(4);

    Assert.fail();
  }
}
