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
 */
public class JdbcParameterBinder {
  private final PreparedStatement statement;
  private final VectorSchemaRoot root;
  private final ColumnBinder[] binders;
  private final int[] parameterIndices;
  private int nextRowIndex;

  JdbcParameterBinder(
      final PreparedStatement statement,
      final VectorSchemaRoot root,
      final ColumnBinder[] binders,
      int[] parameterIndices) {
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
   * @param root The root to pull data from. The binder does not maintain ownership of the root.
   */
  public static Builder builder(final PreparedStatement statement, final VectorSchemaRoot root) {
    return new Builder(statement, root);
  }

  /** Reset the binder (so the root can be updated with new data). */
  public void reset() {
    nextRowIndex = 0;
  }

  /**
   * Bind the next row to the statement.
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
