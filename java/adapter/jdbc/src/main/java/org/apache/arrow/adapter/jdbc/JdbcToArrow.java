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
import java.sql.ResultSet;
import java.sql.SQLException;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.util.Preconditions;

/**
 * Utility class to convert JDBC objects to columnar Arrow format objects.
 *
 * <p>This utility uses following data mapping to map JDBC/SQL datatype to Arrow data types.
 *
 * <p>CHAR --> ArrowType.Utf8
 * NCHAR --> ArrowType.Utf8
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
 *
 * @since 0.10.0
 */
public class JdbcToArrow {

  /*----------------------------------------------------------------*
   |                                                                |
   |          Partial Convert API                        |
   |                                                                |
   *----------------------------------------------------------------*/

  /**
   * For the given JDBC {@link ResultSet}, fetch the data from Relational DB and convert it to Arrow objects.
   * Note here uses the default targetBatchSize = 1024.
   *
   * @param resultSet ResultSet to use to fetch the data from underlying database
   * @param allocator Memory allocator
   * @return Arrow Data Objects {@link ArrowVectorIterator}
   * @throws SQLException on error
   */
  public static ArrowVectorIterator sqlToArrowVectorIterator(
      ResultSet resultSet,
      BufferAllocator allocator)
      throws SQLException, IOException {
    Preconditions.checkNotNull(allocator, "Memory Allocator object cannot be null");

    JdbcToArrowConfig config =
        new JdbcToArrowConfig(allocator, JdbcToArrowUtils.getUtcCalendar());
    return sqlToArrowVectorIterator(resultSet, config);
  }

  /**
   * For the given JDBC {@link ResultSet}, fetch the data from Relational DB and convert it to Arrow objects.
   * Note if not specify {@link JdbcToArrowConfig#targetBatchSize}, will use default value 1024.
   * @param resultSet ResultSet to use to fetch the data from underlying database
   * @param config    Configuration of the conversion from JDBC to Arrow.
   * @return Arrow Data Objects {@link ArrowVectorIterator}
   * @throws SQLException on error
   */
  public static ArrowVectorIterator sqlToArrowVectorIterator(
      ResultSet resultSet,
      JdbcToArrowConfig config)
      throws SQLException, IOException {
    Preconditions.checkNotNull(resultSet, "JDBC ResultSet object cannot be null");
    Preconditions.checkNotNull(config, "The configuration cannot be null");
    return ArrowVectorIterator.create(resultSet, config);
  }
}
