/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.arrow.adapter.jdbc;

import java.io.IOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Calendar;
import java.util.Locale;
import java.util.TimeZone;

import org.apache.arrow.memory.BaseAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;

import com.google.common.base.Preconditions;

/**
 * Utility class to convert JDBC objects to columnar Arrow format objects.
 * <p>
 * This utility uses following data mapping to map JDBC/SQL datatype to Arrow data types.
 * <p>
 * CHAR	--> ArrowType.Utf8
 * NCHAR	--> ArrowType.Utf8
 * VARCHAR --> ArrowType.Utf8
 * NVARCHAR --> ArrowType.Utf8
 * LONGVARCHAR --> ArrowType.Utf8
 * LONGNVARCHAR --> ArrowType.Utf8
 * NUMERIC --> ArrowType.Decimal(precision, scale)
 * DECIMAL --> ArrowType.Decimal(precision, scale)
 * BIT --> ArrowType.Bool
 * TINYINT --> ArrowType.Int(8, signed)
 * SMALLINT --> ArrowType.Int(16, signed)
 * INTEGER --> ArrowType.Int(32, signed)
 * BIGINT --> ArrowType.Int(64, signed)
 * REAL --> ArrowType.FloatingPoint(FloatingPointPrecision.SINGLE)
 * FLOAT --> ArrowType.FloatingPoint(FloatingPointPrecision.SINGLE)
 * DOUBLE --> ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE)
 * BINARY --> ArrowType.Binary
 * VARBINARY --> ArrowType.Binary
 * LONGVARBINARY --> ArrowType.Binary
 * DATE --> ArrowType.Date(DateUnit.MILLISECOND)
 * TIME --> ArrowType.Time(TimeUnit.MILLISECOND, 32)
 * TIMESTAMP --> ArrowType.Timestamp(TimeUnit.MILLISECOND, timezone=null)
 * CLOB --> ArrowType.Utf8
 * BLOB --> ArrowType.Binary
 * <p>
 * TODO: At this time, SQL Data type java.sql.Types.ARRAY is still not supported.
 *
 * @since 0.10.0
 */
public class JdbcToArrow {

  /**
   * For the given SQL query, execute and fetch the data from Relational DB and convert it to Arrow objects.
   * This method uses the default Calendar instance with default TimeZone and Locale as returned by the JVM.
   * If you wish to use specific TimeZone or Locale for any Date, Time and Timestamp datasets, you may want use
   * overloaded API that taken Calendar object instance.
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
  public static VectorSchemaRoot sqlToArrow(Connection connection, String query, BaseAllocator allocator)
      throws SQLException, IOException {
    Preconditions.checkNotNull(connection, "JDBC connection object can not be null");
    Preconditions.checkArgument(query != null && query.length() > 0, "SQL query can not be null or empty");
    Preconditions.checkNotNull(allocator, "Memory allocator object can not be null");

    return sqlToArrow(connection, query, allocator, Calendar.getInstance(TimeZone.getTimeZone("UTC"), Locale.ROOT));
  }

  /**
   * For the given SQL query, execute and fetch the data from Relational DB and convert it to Arrow objects.
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
  public static VectorSchemaRoot sqlToArrow(
      Connection connection,
      String query,
      BaseAllocator allocator,
      Calendar calendar) throws SQLException, IOException {
    Preconditions.checkNotNull(connection, "JDBC connection object can not be null");
    Preconditions.checkArgument(query != null && query.length() > 0, "SQL query can not be null or empty");
    Preconditions.checkNotNull(allocator, "Memory allocator object can not be null");
    Preconditions.checkNotNull(calendar, "Calendar object can not be null");

    try (Statement stmt = connection.createStatement()) {
      return sqlToArrow(stmt.executeQuery(query), allocator, calendar);
    }
  }

  /**
   * For the given JDBC {@link ResultSet}, fetch the data from Relational DB and convert it to Arrow objects. This
   * method uses the default RootAllocator and Calendar object.
   *
   * @param resultSet
   * @return Arrow Data Objects {@link VectorSchemaRoot}
   * @throws SQLException
   */
  public static VectorSchemaRoot sqlToArrow(ResultSet resultSet) throws SQLException, IOException {
    Preconditions.checkNotNull(resultSet, "JDBC ResultSet object can not be null");

    return sqlToArrow(resultSet, Calendar.getInstance(TimeZone.getTimeZone("UTC"), Locale.ROOT));
  }

  /**
   * For the given JDBC {@link ResultSet}, fetch the data from Relational DB and convert it to Arrow objects.
   *
   * @param resultSet
   * @param allocator Memory allocator
   * @return Arrow Data Objects {@link VectorSchemaRoot}
   * @throws SQLException
   */
  public static VectorSchemaRoot sqlToArrow(ResultSet resultSet, BaseAllocator allocator)
      throws SQLException, IOException {
    Preconditions.checkNotNull(resultSet, "JDBC ResultSet object can not be null");
    Preconditions.checkNotNull(allocator, "Memory Allocator object can not be null");

    return sqlToArrow(resultSet, allocator, Calendar.getInstance(TimeZone.getTimeZone("UTC"), Locale.ROOT));
  }

  /**
   * For the given JDBC {@link ResultSet}, fetch the data from Relational DB and convert it to Arrow objects.
   *
   * @param resultSet
   * @param calendar  Calendar instance to use for Date, Time and Timestamp datasets.
   * @return Arrow Data Objects {@link VectorSchemaRoot}
   * @throws SQLException
   */
  public static VectorSchemaRoot sqlToArrow(ResultSet resultSet, Calendar calendar) throws SQLException, IOException {
    Preconditions.checkNotNull(resultSet, "JDBC ResultSet object can not be null");
    Preconditions.checkNotNull(calendar, "Calendar object can not be null");

    RootAllocator rootAllocator = new RootAllocator(Integer.MAX_VALUE);
    VectorSchemaRoot root = sqlToArrow(resultSet, rootAllocator, calendar);

    return root;
  }

  /**
   * For the given JDBC {@link ResultSet}, fetch the data from Relational DB and convert it to Arrow objects.
   *
   * @param resultSet
   * @param allocator Memory allocator to use.
   * @param calendar  Calendar instance to use for Date, Time and Timestamp datasets.
   * @return Arrow Data Objects {@link VectorSchemaRoot}
   * @throws SQLException
   */
  public static VectorSchemaRoot sqlToArrow(ResultSet resultSet, BaseAllocator allocator, Calendar calendar)
      throws SQLException, IOException {
    Preconditions.checkNotNull(resultSet, "JDBC ResultSet object can not be null");
    Preconditions.checkNotNull(allocator, "Memory Allocator object can not be null");
    Preconditions.checkNotNull(calendar, "Calendar object can not be null");

    VectorSchemaRoot root = VectorSchemaRoot.create(
            JdbcToArrowUtils.jdbcToArrowSchema(resultSet.getMetaData(), calendar), allocator);
    JdbcToArrowUtils.jdbcToArrowVectors(resultSet, root, calendar);
    return root;
  }
}
