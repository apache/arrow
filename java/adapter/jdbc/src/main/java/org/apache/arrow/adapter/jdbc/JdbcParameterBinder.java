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

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

import org.apache.arrow.adapter.jdbc.binder.ColumnBinder;
import org.apache.arrow.util.Preconditions;
import org.apache.arrow.vector.VectorSchemaRoot;

/**
 * A binder binds JDBC prepared statement parameters to rows of Arrow data from a VectorSchemaRoot.
 *
 * Each row of the VectorSchemaRoot will be bound to the configured parameters of the PreparedStatement.
 * One row of data is bound at a time.
 */
public class JdbcParameterBinder {
  private final PreparedStatement statement;
  private final VectorSchemaRoot root;
  private final ColumnBinder[] binders;
  private final int[] parameterIndices;
  private int nextRowIndex;

  /**
   * Create a new parameter binder.
   *
   * @param statement The statement to bind parameters to.
   * @param root The VectorSchemaRoot to pull data from.
   * @param binders Column binders to translate from Arrow data to JDBC parameters, one per parameter.
   * @param parameterIndices For each binder in <tt>binders</tt>, the index of the parameter to bind to.
   */
  private JdbcParameterBinder(
      final PreparedStatement statement,
      final VectorSchemaRoot root,
      final ColumnBinder[] binders,
      int[] parameterIndices) {
    Preconditions.checkArgument(
        binders.length == parameterIndices.length,
        "Number of column binders (%s) must equal number of parameter indices (%s)",
        binders.length, parameterIndices.length);
    this.statement = statement;
    this.root = root;
    this.binders = binders;
    this.parameterIndices = parameterIndices;
    this.nextRowIndex = 0;
  }

  /**
   * Initialize a binder with a builder.
   *
   * @param statement The statement to bind to. The binder does not maintain ownership of the statement.
   * @param root The {@link VectorSchemaRoot} to pull data from. The binder does not maintain ownership
   *             of the vector schema root.
   */
  public static Builder builder(final PreparedStatement statement, final VectorSchemaRoot root) {
    return new Builder(statement, root);
  }

  /** Reset the binder (so the root can be updated with new data). */
  public void reset() {
    nextRowIndex = 0;
  }

  /**
   * Bind the next row of data to the parameters of the statement.
   *
   * After this, the application should call the desired method on the prepared statement,
   * such as {@link PreparedStatement#executeUpdate()}, or {@link PreparedStatement#addBatch()}.
   *
   * @return true if a row was bound, false if rows were exhausted
   */
  public boolean next() throws SQLException {
    if (nextRowIndex >= root.getRowCount()) {
      return false;
    }
    for (int i = 0; i < parameterIndices.length; i++) {
      final int parameterIndex = parameterIndices[i];
      binders[i].bind(statement, parameterIndex, nextRowIndex);
    }
    nextRowIndex++;
    return true;
  }

  /**
   * A builder for a {@link JdbcParameterBinder}.
   */
  public static class Builder {
    private final PreparedStatement statement;
    private final VectorSchemaRoot root;
    private final Map<Integer, ColumnBinder> bindings;

    Builder(PreparedStatement statement, VectorSchemaRoot root) {
      this.statement = statement;
      this.root = root;
      this.bindings = new HashMap<>();
    }

    /** Bind each column to the corresponding parameter in order. */
    public Builder bindAll() {
      for (int i = 0; i < root.getFieldVectors().size(); i++) {
        bind(/*parameterIndex=*/ i + 1, /*columnIndex=*/ i);
      }
      return this;
    }

    /** Bind the given parameter to the given column using the default binder. */
    public Builder bind(int parameterIndex, int columnIndex) {
      return bind(
          parameterIndex,
          ColumnBinder.forVector(root.getVector(columnIndex)));
    }

    /** Bind the given parameter using the given binder. */
    public Builder bind(int parameterIndex, ColumnBinder binder) {
      Preconditions.checkArgument(
          parameterIndex > 0, "parameterIndex %d must be positive", parameterIndex);
      bindings.put(parameterIndex, binder);
      return this;
    }

    /** Build the binder. */
    public JdbcParameterBinder build() {
      ColumnBinder[] binders = new ColumnBinder[bindings.size()];
      int[] parameterIndices = new int[bindings.size()];
      int index = 0;
      for (Map.Entry<Integer, ColumnBinder> entry : bindings.entrySet()) {
        binders[index] = entry.getValue();
        parameterIndices[index] = entry.getKey();
        index++;
      }
      return new JdbcParameterBinder(statement, root, binders, parameterIndices);
    }
  }
}
