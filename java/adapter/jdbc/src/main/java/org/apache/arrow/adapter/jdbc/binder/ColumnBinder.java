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

package org.apache.arrow.adapter.jdbc.binder;

import java.sql.PreparedStatement;
import java.sql.SQLException;

import org.apache.arrow.vector.FieldVector;

/**
 * A helper to bind values from a wrapped Arrow vector to a JDBC PreparedStatement.
 */
public interface ColumnBinder {
  /**
   * Bind the given row to the given parameter.
   *
   * @param statement The statement to bind to.
   * @param parameterIndex The parameter to bind to (1-indexed)
   * @param rowIndex The row to bind values from (0-indexed)
   * @throws SQLException if an error occurs
   */
  void bind(PreparedStatement statement, int parameterIndex, int rowIndex) throws SQLException;

  /**
   * Get the JDBC type code used by this binder.
   *
   * @return A type code from {@link java.sql.Types}.
   */
  int getJdbcType();

  /**
   * Get the vector used by this binder.
   */
  FieldVector getVector();

  /**
   * Create a column binder for a vector, using the default JDBC type code for null values.
   */
  static ColumnBinder forVector(FieldVector vector) {
    return forVector(vector, /*jdbcType*/ null);
  }

  /**
   * Create a column binder for a vector, overriding the JDBC type code used for null values.
   *
   * @param vector The vector that the column binder will wrap.
   * @param jdbcType The JDBC type code to use (or null to use the default).
   */
  static ColumnBinder forVector(FieldVector vector, Integer jdbcType) {
    final ColumnBinder binder = vector.getField().getType().accept(new ColumnBinderArrowTypeVisitor(vector, jdbcType));
    if (vector.getField().isNullable()) {
      return new NullableColumnBinder(binder);
    }
    return binder;
  }
}
