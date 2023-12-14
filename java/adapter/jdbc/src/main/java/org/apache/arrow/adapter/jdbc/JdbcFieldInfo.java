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

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Types;

import org.apache.arrow.util.Preconditions;

/**
 * This class represents the information about a JDBC ResultSet Field that is
 * needed to construct an {@link org.apache.arrow.vector.types.pojo.ArrowType}.
 * Currently, this is:
 * <ul>
 *   <li>The JDBC {@link java.sql.Types} type.</li>
 *   <li>The nullability.</li>
 *   <li>The field's precision (used for {@link java.sql.Types#DECIMAL} and {@link java.sql.Types#NUMERIC} types).</li>
 *   <li>The field's scale (used for {@link java.sql.Types#DECIMAL} and {@link java.sql.Types#NUMERIC} types).</li>
 * </ul>
 */
public class JdbcFieldInfo {
  private final int column;
  private final int jdbcType;
  private final int nullability;
  private final int precision;
  private final int scale;
  private final String typeName;
  private final int displaySize;

  /**
   * Builds a <code>JdbcFieldInfo</code> using only the {@link java.sql.Types} type.  Do not use this constructor
   * if the field type is {@link java.sql.Types#DECIMAL} or {@link java.sql.Types#NUMERIC}; the precision and
   * scale will be set to <code>0</code>.
   *
   * @param jdbcType The {@link java.sql.Types} type.
   * @throws IllegalArgumentException if jdbcType is {@link java.sql.Types#DECIMAL} or {@link java.sql.Types#NUMERIC}.
   */
  public JdbcFieldInfo(int jdbcType) {
    Preconditions.checkArgument(
        (jdbcType != Types.DECIMAL && jdbcType != Types.NUMERIC),
        "DECIMAL and NUMERIC types require a precision and scale; please use another constructor.");
    this.column = 0;
    this.jdbcType = jdbcType;
    this.nullability = ResultSetMetaData.columnNullableUnknown;
    this.precision = 0;
    this.scale = 0;
    this.typeName = "";
    this.displaySize = 0;
  }

  /**
   * Builds a <code>JdbcFieldInfo</code> from the {@link java.sql.Types} type, precision, and scale.
   * Use this constructor for {@link java.sql.Types#DECIMAL} and {@link java.sql.Types#NUMERIC} types.
   *
   * @param jdbcType The {@link java.sql.Types} type.
   * @param precision The field's numeric precision.
   * @param scale The field's numeric scale.
   */
  public JdbcFieldInfo(int jdbcType, int precision, int scale) {
    this.column = 0;
    this.jdbcType = jdbcType;
    this.nullability = ResultSetMetaData.columnNullableUnknown;
    this.precision = precision;
    this.scale = scale;
    this.typeName = "";
    this.displaySize = 0;
  }

  /**
   * Builds a <code>JdbcFieldInfo</code> from the {@link java.sql.Types} type, nullability, precision, and scale.
   *
   * @param jdbcType The {@link java.sql.Types} type.
   * @param nullability The nullability. Must be one of {@link ResultSetMetaData#columnNoNulls},
   *     {@link ResultSetMetaData#columnNullable}, or {@link ResultSetMetaData#columnNullableUnknown}.
   * @param precision The field's numeric precision.
   * @param scale The field's numeric scale.
   */
  public JdbcFieldInfo(int jdbcType, int nullability, int precision, int scale) {
    this.column = 0;
    this.jdbcType = jdbcType;
    this.nullability = nullability;
    this.precision = precision;
    this.scale = scale;
    this.typeName = "";
    this.displaySize = 0;
  }

  /**
   * Builds a <code>JdbcFieldInfo</code> from the corresponding {@link java.sql.ResultSetMetaData} column.
   *
   * @param rsmd The {@link java.sql.ResultSetMetaData} to get the field information from.
   * @param column The column to get the field information for (on a 1-based index).
   * @throws SQLException If the column information cannot be retrieved.
   * @throws NullPointerException if <code>rsmd</code> is <code>null</code>.
   * @throws IllegalArgumentException if <code>column</code> is out of bounds.
   */
  public JdbcFieldInfo(ResultSetMetaData rsmd, int column) throws SQLException {
    Preconditions.checkNotNull(rsmd, "ResultSetMetaData cannot be null.");
    Preconditions.checkArgument(column > 0, "ResultSetMetaData columns have indices starting at 1.");
    Preconditions.checkArgument(
        column <= rsmd.getColumnCount(),
        "The index must be within the number of columns (1 to %s, inclusive)", rsmd.getColumnCount());

    this.column = column;
    this.jdbcType = rsmd.getColumnType(column);
    this.nullability = rsmd.isNullable(column);
    this.precision = rsmd.getPrecision(column);
    this.scale = rsmd.getScale(column);
    this.typeName = rsmd.getColumnTypeName(column);
    this.displaySize = rsmd.getColumnDisplaySize(column);
  }

  /**
   * Builds a <code>JdbcFieldInfo</code> from the corresponding row from a {@link java.sql.DatabaseMetaData#getColumns}
   * ResultSet.
   *
   * @param rs The {@link java.sql.ResultSet} to get the field information from.
   * @throws SQLException If the column information cannot be retrieved.
   */
  public JdbcFieldInfo(ResultSet rs) throws SQLException {
    this.column = rs.getInt("ORDINAL_POSITION");
    this.jdbcType = rs.getInt("DATA_TYPE");
    this.nullability = rs.getInt("NULLABLE");
    this.precision = rs.getInt("COLUMN_SIZE");
    this.scale = rs.getInt("DECIMAL_DIGITS");
    this.typeName = rs.getString("TYPE_NAME");
    this.displaySize = rs.getInt("CHAR_OCTET_LENGTH");
  }

  /**
   * The {@link java.sql.Types} type.
   */
  public int getJdbcType() {
    return jdbcType;
  }

  /**
   * The nullability.
   */
  public int isNullable() {
    return nullability;
  }

  /**
   * The numeric precision, for {@link java.sql.Types#NUMERIC}  and {@link java.sql.Types#DECIMAL} types.
   */
  public int getPrecision() {
    return precision;
  }

  /**
   * The numeric scale, for {@link java.sql.Types#NUMERIC}  and {@link java.sql.Types#DECIMAL} types.
   */
  public int getScale() {
    return scale;
  }

  /**
   * The column index for query column.
   */
  public int getColumn() {
    return column;
  }

  /**
   * The type name as reported by the database.
   */
  public String getTypeName() {
    return typeName;
  }

  /**
   * The max number of characters for the column.
   */
  public int getDisplaySize() {
    return displaySize;
  }
}
