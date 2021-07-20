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
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.util.HashMap;
import java.util.Map;

import org.apache.arrow.driver.jdbc.ArrowFlightJdbcDriver;
import org.apache.arrow.driver.jdbc.ArrowFlightResultSet;
import org.apache.arrow.driver.jdbc.utils.BaseProperty;
import org.apache.arrow.vector.types.DateUnit;
import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.IntervalUnit;
import org.apache.arrow.vector.types.TimeUnit;
import org.apache.arrow.vector.types.UnionMode;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ErrorCollector;

import me.alexpanov.net.FreePortFinder;

public class ResultSetTest {
  private static Map<BaseProperty, Object> properties;

  @ClassRule
  public static FlightServerTestRule rule;

  @Rule
  public final ErrorCollector collector = new ErrorCollector();

  private static Connection connection;

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
  }

  @AfterClass
  public static void tearDown() throws SQLException {
    connection.close();
  }

  /**
   * Tests whether the {@link ArrowFlightJdbcDriver} can run a query successfully.
   *
   * @throws Exception If the connection fails to be established.
   */
  @Test
  public void testShouldRunSelectQuery() throws Exception {
    try (Statement statement = connection.createStatement();
         ResultSet resultSet = statement.executeQuery("SELECT * FROM TEST")) {
      int count = 0;
      int columns = 6;
      for (; resultSet.next(); count++) {
        for (int column = 1; column <= columns; column++) {
          resultSet.getObject(column);
        }
      }

      assertEquals(count, Byte.MAX_VALUE);
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

  @Test
  public void testGetSqlTypeIdFromArrowType() {
    assertEquals(Types.TINYINT, ArrowFlightResultSet.getSqlTypeIdFromArrowType(new ArrowType.Int(8, true)));
    assertEquals(Types.SMALLINT, ArrowFlightResultSet.getSqlTypeIdFromArrowType(new ArrowType.Int(16, true)));
    assertEquals(Types.INTEGER, ArrowFlightResultSet.getSqlTypeIdFromArrowType(new ArrowType.Int(32, true)));
    assertEquals(Types.BIGINT, ArrowFlightResultSet.getSqlTypeIdFromArrowType(new ArrowType.Int(64, true)));

    assertEquals(Types.BINARY, ArrowFlightResultSet.getSqlTypeIdFromArrowType(new ArrowType.FixedSizeBinary(1024)));
    assertEquals(Types.VARBINARY, ArrowFlightResultSet.getSqlTypeIdFromArrowType(new ArrowType.Binary()));
    assertEquals(Types.LONGVARBINARY, ArrowFlightResultSet.getSqlTypeIdFromArrowType(new ArrowType.LargeBinary()));

    assertEquals(Types.VARCHAR, ArrowFlightResultSet.getSqlTypeIdFromArrowType(new ArrowType.Utf8()));
    assertEquals(Types.LONGVARCHAR, ArrowFlightResultSet.getSqlTypeIdFromArrowType(new ArrowType.LargeUtf8()));

    assertEquals(Types.DATE, ArrowFlightResultSet.getSqlTypeIdFromArrowType(new ArrowType.Date(DateUnit.MILLISECOND)));
    assertEquals(Types.TIME,
        ArrowFlightResultSet.getSqlTypeIdFromArrowType(new ArrowType.Time(TimeUnit.MILLISECOND, 32)));
    assertEquals(Types.TIMESTAMP,
        ArrowFlightResultSet.getSqlTypeIdFromArrowType(new ArrowType.Timestamp(TimeUnit.MILLISECOND, "")));

    assertEquals(Types.BOOLEAN, ArrowFlightResultSet.getSqlTypeIdFromArrowType(new ArrowType.Bool()));

    assertEquals(Types.DECIMAL, ArrowFlightResultSet.getSqlTypeIdFromArrowType(new ArrowType.Decimal(0, 0, 64)));
    assertEquals(Types.DOUBLE,
        ArrowFlightResultSet.getSqlTypeIdFromArrowType(new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE)));
    assertEquals(Types.FLOAT,
        ArrowFlightResultSet.getSqlTypeIdFromArrowType(new ArrowType.FloatingPoint(FloatingPointPrecision.SINGLE)));

    assertEquals(Types.ARRAY, ArrowFlightResultSet.getSqlTypeIdFromArrowType(new ArrowType.List()));
    assertEquals(Types.ARRAY, ArrowFlightResultSet.getSqlTypeIdFromArrowType(new ArrowType.LargeList()));
    assertEquals(Types.ARRAY, ArrowFlightResultSet.getSqlTypeIdFromArrowType(new ArrowType.FixedSizeList(10)));

    assertEquals(Types.STRUCT, ArrowFlightResultSet.getSqlTypeIdFromArrowType(new ArrowType.Struct()));

    assertEquals(Types.JAVA_OBJECT,
        ArrowFlightResultSet.getSqlTypeIdFromArrowType(new ArrowType.Duration(TimeUnit.MILLISECOND)));
    assertEquals(Types.JAVA_OBJECT,
        ArrowFlightResultSet.getSqlTypeIdFromArrowType(new ArrowType.Interval(IntervalUnit.DAY_TIME)));
    assertEquals(Types.JAVA_OBJECT,
        ArrowFlightResultSet.getSqlTypeIdFromArrowType(new ArrowType.Union(UnionMode.Dense, null)));
    assertEquals(Types.JAVA_OBJECT, ArrowFlightResultSet.getSqlTypeIdFromArrowType(new ArrowType.Map(true)));

    assertEquals(Types.NULL, ArrowFlightResultSet.getSqlTypeIdFromArrowType(new ArrowType.Null()));
  }
}
