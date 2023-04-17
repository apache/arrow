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

package org.apache.arrow.adapter.jdbc;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.util.Calendar;
import java.util.HashMap;
import java.util.Map;
import java.util.TimeZone;
import java.util.function.Function;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.util.Preconditions;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.util.ValueVectorUtility;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;

/**
 * Class to abstract out some common test functionality for testing JDBC to Arrow.
 */
public abstract class AbstractJdbcToArrowTest {

  protected static final String BIGINT = "BIGINT_FIELD5";
  protected static final String BINARY = "BINARY_FIELD12";
  protected static final String BIT = "BIT_FIELD17";
  protected static final String BLOB = "BLOB_FIELD14";
  protected static final String BOOL = "BOOL_FIELD2";
  protected static final String CHAR = "CHAR_FIELD16";
  protected static final String CLOB = "CLOB_FIELD15";
  protected static final String DATE = "DATE_FIELD10";
  protected static final String DECIMAL = "DECIMAL_FIELD6";
  protected static final String DOUBLE = "DOUBLE_FIELD7";
  protected static final String INT = "INT_FIELD1";
  protected static final String LIST = "LIST_FIELD19";
  protected static final String MAP = "MAP_FIELD20";
  protected static final String REAL = "REAL_FIELD8";
  protected static final String SMALLINT = "SMALLINT_FIELD4";
  protected static final String TIME = "TIME_FIELD9";
  protected static final String TIMESTAMP = "TIMESTAMP_FIELD11";
  protected static final String TINYINT = "TINYINT_FIELD3";
  protected static final String VARCHAR = "VARCHAR_FIELD13";
  protected static final String NULL = "NULL_FIELD18";
  protected static final Map<String, JdbcFieldInfo> ARRAY_SUB_TYPE_BY_COLUMN_NAME_MAP = new HashMap<>();

  static {
    ARRAY_SUB_TYPE_BY_COLUMN_NAME_MAP.put(LIST, new JdbcFieldInfo(Types.INTEGER));
  }

  protected Connection conn = null;
  protected Table table;
  protected boolean reuseVectorSchemaRoot;

  /**
   * This method creates Table object after reading YAML file.
   *
   * @param ymlFilePath path to file
   * @return Table object
   * @throws IOException on error
   */
  protected static Table getTable(String ymlFilePath, @SuppressWarnings("rawtypes") Class clss) throws IOException {
    return new ObjectMapper(new YAMLFactory()).readValue(
            clss.getClassLoader().getResourceAsStream(ymlFilePath), Table.class);
  }


  /**
   * This method creates Connection object and DB table and also populate data into table for test.
   *
   * @throws SQLException on error
   * @throws ClassNotFoundException on error
   */
  @Before
  public void setUp() throws SQLException, ClassNotFoundException {
    TimeZone.setDefault(TimeZone.getTimeZone("UTC"));
    String url = "jdbc:h2:mem:JdbcToArrowTest";
    String driver = "org.h2.Driver";
    Class.forName(driver);
    conn = DriverManager.getConnection(url);
    try (Statement stmt = conn.createStatement();) {
      stmt.executeUpdate(table.getCreate());
      for (String insert : table.getData()) {
        stmt.executeUpdate(insert);
      }
    }
  }

  /**
   * Clean up method to close connection after test completes.
   *
   * @throws SQLException on error
   */
  @After
  public void destroy() throws SQLException {
    if (conn != null) {
      conn.close();
      conn = null;
    }
  }

  /**
   * Prepares test data and returns collection of Table object for each test iteration.
   *
   * @param testFiles files for test
   * @param clss Class type
   * @return Collection of Table objects
   * @throws SQLException on error
   * @throws ClassNotFoundException on error
   * @throws IOException on error
   */
  public static Object[][] prepareTestData(String[] testFiles, @SuppressWarnings("rawtypes") Class clss)
      throws SQLException, ClassNotFoundException, IOException {
    Object[][] tableArr = new Object[testFiles.length][];
    int i = 0;
    for (String testFile : testFiles) {
      tableArr[i++] = new Object[]{getTable(testFile, clss)};
    }
    return tableArr;
  }

  /**
   * Abstract method to implement test Functionality to test JdbcToArrow methods.
   *
   * @throws SQLException on error
   * @throws IOException on error
   */
  @Test
  public abstract void testJdbcToArrowValues() throws SQLException, IOException;

  /**
   * Abstract method to implement logic to assert test various datatype values.
   *
   * @param root VectorSchemaRoot for test
   * @param isIncludeMapVector is this dataset checks includes map column.
   *          Jdbc type to 'map' mapping declared in configuration only manually
   */
  public abstract void testDataSets(VectorSchemaRoot root, boolean isIncludeMapVector);

  /**
   * For the given SQL query, execute and fetch the data from Relational DB and convert it to Arrow objects.
   * This method uses the default Calendar instance with default TimeZone and Locale as returned by the JVM.
   * If you wish to use specific TimeZone or Locale for any Date, Time and Timestamp datasets, you may want use
   * overloaded API that taken Calendar object instance.
   *
   * This method is for test only.
   *
   * @param connection Database connection to be used. This method will not close the passed connection object. Since
   *                   the caller has passed the connection object it's the responsibility of the caller to close or
   *                   return the connection to the pool.
   * @param query      The DB Query to fetch the data.
   * @param allocator  Memory allocator
   * @return Arrow Data Objects {@link VectorSchemaRoot}
   * @throws SQLException Propagate any SQL Exceptions to the caller after closing any resources opened such as
   *                      ResultSet and Statement objects.
   */
  public VectorSchemaRoot sqlToArrow(Connection connection, String query, BufferAllocator allocator)
      throws SQLException, IOException {
    Preconditions.checkNotNull(allocator, "Memory allocator object can not be null");

    JdbcToArrowConfig config = new JdbcToArrowConfigBuilder(allocator, JdbcToArrowUtils.getUtcCalendar())
        .setArraySubTypeByColumnNameMap(ARRAY_SUB_TYPE_BY_COLUMN_NAME_MAP)
        .build();
    return sqlToArrow(connection, query, config);
  }

  /**
   * For the given SQL query, execute and fetch the data from Relational DB and convert it to Arrow objects.
   *
   * This method is for test only.
   *
   * @param connection Database connection to be used. This method will not close the passed connection object. Since
   *                   the caller has passed the connection object it's the responsibility of the caller to close or
   *                   return the connection to the pool.
   * @param query      The DB Query to fetch the data.
   * @param allocator  Memory allocator
   * @param calendar   Calendar object to use to handle Date, Time and Timestamp datasets.
   * @return Arrow Data Objects {@link VectorSchemaRoot}
   * @throws SQLException Propagate any SQL Exceptions to the caller after closing any resources opened such as
   *                      ResultSet and Statement objects.
   */
  public VectorSchemaRoot sqlToArrow(
      Connection connection,
      String query,
      BufferAllocator allocator,
      Calendar calendar) throws SQLException, IOException {

    Preconditions.checkNotNull(allocator, "Memory allocator object can not be null");
    Preconditions.checkNotNull(calendar, "Calendar object can not be null");

    JdbcToArrowConfig config = new JdbcToArrowConfigBuilder(allocator, calendar)
        .setArraySubTypeByColumnNameMap(ARRAY_SUB_TYPE_BY_COLUMN_NAME_MAP)
        .build();
    return sqlToArrow(connection, query, config);
  }

  /**
   * For the given SQL query, execute and fetch the data from Relational DB and convert it to Arrow objects.
   *
   * This method is for test only.
   *
   * @param connection Database connection to be used. This method will not close the passed connection object.
   *                   Since the caller has passed the connection object it's the responsibility of the caller
   *                   to close or return the connection to the pool.
   * @param query      The DB Query to fetch the data.
   * @param config     Configuration
   * @return Arrow Data Objects {@link VectorSchemaRoot}
   * @throws SQLException Propagate any SQL Exceptions to the caller after closing any resources opened such as
   *                      ResultSet and Statement objects.
   */
  public static VectorSchemaRoot sqlToArrow(Connection connection, String query, JdbcToArrowConfig config)
      throws SQLException, IOException {
    Preconditions.checkNotNull(connection, "JDBC connection object can not be null");
    Preconditions.checkArgument(query != null && query.length() > 0, "SQL query can not be null or empty");

    try (Statement stmt = connection.createStatement()) {
      return sqlToArrow(stmt.executeQuery(query), config);
    }
  }

  /**
   * For the given JDBC {@link ResultSet}, fetch the data from Relational DB and convert it to Arrow objects. This
   * method uses the default RootAllocator and Calendar object.
   *
   * This method is for test only.
   *
   * @param resultSet ResultSet to use to fetch the data from underlying database
   * @return Arrow Data Objects {@link VectorSchemaRoot}
   * @throws SQLException on error
   */
  public static VectorSchemaRoot sqlToArrow(ResultSet resultSet) throws SQLException, IOException {
    Preconditions.checkNotNull(resultSet, "JDBC ResultSet object can not be null");

    return sqlToArrow(resultSet, JdbcToArrowUtils.getUtcCalendar());
  }

  /**
   * For the given JDBC {@link ResultSet}, fetch the data from Relational DB and convert it to Arrow objects.
   *
   * This method is for test only.
   *
   * @param resultSet ResultSet to use to fetch the data from underlying database
   * @param allocator Memory allocator
   * @return Arrow Data Objects {@link VectorSchemaRoot}
   * @throws SQLException on error
   */
  public static VectorSchemaRoot sqlToArrow(ResultSet resultSet, BufferAllocator allocator)
      throws SQLException, IOException {
    Preconditions.checkNotNull(allocator, "Memory Allocator object can not be null");

    JdbcToArrowConfig config = new JdbcToArrowConfigBuilder(allocator, JdbcToArrowUtils.getUtcCalendar())
        .setArraySubTypeByColumnNameMap(ARRAY_SUB_TYPE_BY_COLUMN_NAME_MAP)
        .build();
    return sqlToArrow(resultSet, config);
  }

  /**
   * For the given JDBC {@link ResultSet}, fetch the data from Relational DB and convert it to Arrow objects.
   *
   * This method is for test only.
   *
   * @param resultSet ResultSet to use to fetch the data from underlying database
   * @param calendar  Calendar instance to use for Date, Time and Timestamp datasets, or <code>null</code> if none.
   * @return Arrow Data Objects {@link VectorSchemaRoot}
   * @throws SQLException on error
   */
  public static VectorSchemaRoot sqlToArrow(ResultSet resultSet, Calendar calendar) throws SQLException, IOException {
    Preconditions.checkNotNull(resultSet, "JDBC ResultSet object can not be null");

    JdbcToArrowConfig config = new JdbcToArrowConfigBuilder(new RootAllocator(Integer.MAX_VALUE), calendar)
        .setArraySubTypeByColumnNameMap(ARRAY_SUB_TYPE_BY_COLUMN_NAME_MAP)
        .build();
    return sqlToArrow(resultSet, config);
  }

  /**
   * For the given JDBC {@link ResultSet}, fetch the data from Relational DB and convert it to Arrow objects.
   *
   * This method is for test only.
   *
   * @param resultSet ResultSet to use to fetch the data from underlying database
   * @param allocator Memory allocator to use.
   * @param calendar  Calendar instance to use for Date, Time and Timestamp datasets, or <code>null</code> if none.
   * @return Arrow Data Objects {@link VectorSchemaRoot}
   * @throws SQLException on error
   */
  public static VectorSchemaRoot sqlToArrow(
      ResultSet resultSet,
      BufferAllocator allocator,
      Calendar calendar)
      throws SQLException, IOException {
    Preconditions.checkNotNull(allocator, "Memory Allocator object can not be null");

    JdbcToArrowConfig config = new JdbcToArrowConfigBuilder(allocator, calendar)
        .setArraySubTypeByColumnNameMap(ARRAY_SUB_TYPE_BY_COLUMN_NAME_MAP)
        .build();
    return sqlToArrow(resultSet, config);
  }

  /**
   * For the given JDBC {@link ResultSet}, fetch the data from Relational DB and convert it to Arrow objects.
   *
   * This method is for test only.
   *
   * @param resultSet ResultSet to use to fetch the data from underlying database
   * @param config    Configuration of the conversion from JDBC to Arrow.
   * @return Arrow Data Objects {@link VectorSchemaRoot}
   * @throws SQLException on error
   */
  public static VectorSchemaRoot sqlToArrow(ResultSet resultSet, JdbcToArrowConfig config)
      throws SQLException, IOException {
    Preconditions.checkNotNull(resultSet, "JDBC ResultSet object can not be null");
    Preconditions.checkNotNull(config, "The configuration cannot be null");

    VectorSchemaRoot root = VectorSchemaRoot.create(
        JdbcToArrowUtils.jdbcToArrowSchema(resultSet.getMetaData(), config), config.getAllocator());
    if (config.getTargetBatchSize() != JdbcToArrowConfig.NO_LIMIT_BATCH_SIZE) {
      ValueVectorUtility.preAllocate(root, config.getTargetBatchSize());
    }
    JdbcToArrowUtils.jdbcToArrowVectors(resultSet, root, config);
    return root;
  }

  /**
   * Register MAP_FIELD20 as ArrowType.Map
   * @param calendar  Calendar instance to use for Date, Time and Timestamp datasets, or <code>null</code> if none.
   * @param rsmd ResultSetMetaData to lookup column name from result set metadata
   * @return typeConverter instance with mapping column to Map type
   */
  protected Function<JdbcFieldInfo, ArrowType> jdbcToArrowTypeConverter(
          Calendar calendar, ResultSetMetaData rsmd) {
    return (jdbcFieldInfo) -> {
      String columnLabel = null;
      try {
        int columnIndex = jdbcFieldInfo.getColumn();
        if (columnIndex != 0) {
          columnLabel = rsmd.getColumnLabel(columnIndex);
        }
      } catch (SQLException e) {
        throw new RuntimeException(e);
      }
      if (MAP.equals(columnLabel)) {
        return new ArrowType.Map(false);
      } else {
        return JdbcToArrowUtils.getArrowTypeFromJdbcType(jdbcFieldInfo, calendar);
      }
    };
  }

  protected ResultSetMetaData getQueryMetaData(String query) throws SQLException {
    return conn.createStatement().executeQuery(query).getMetaData();
  }

}
