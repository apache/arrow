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

package org.apache.arrow.adapter.jdbc.consumer;

import java.io.IOException;
import java.sql.Array;
import java.sql.ResultSet;
import java.sql.SQLException;

import org.apache.arrow.vector.complex.ListVector;

/**
 * Wrapper for consumers which consume array type values from {@link ResultSet}.
 * Write the data to {@link ListVector}.
 */
public class ArrayConsumer {

  /**
   * Creates a consumer for {@link ListVector}.
   */
  public static JdbcConsumer<ListVector> createConsumer(
          ListVector vector, JdbcConsumer delegate, int index, boolean nullable) {
    if (nullable) {
      return new NullableArrayConsumer(vector, delegate, index);
    } else {
      return new NonNullableArrayConsumer(vector, delegate, index);
    }
  }

  /**
   * Nullable consumer for {@link ListVector}.
   */
  static class NullableArrayConsumer extends BaseJdbcConsumer<ListVector> {

    private final JdbcConsumer delegate;

    /**
     * Instantiate a nullable array consumer.
     */
    public NullableArrayConsumer(ListVector vector, JdbcConsumer delegate, int index) {
      super(vector, index);
      this.delegate = delegate;
    }

    @Override
    public void consume(ResultSet resultSet) throws SQLException, IOException {
      final Array array = resultSet.getArray(columnIndexInResultSet);
      if (!resultSet.wasNull()) {
        vector.startNewValue(currentIndex);
        int count = 0;
        try (ResultSet rs = array.getResultSet()) {
          while (rs.next()) {
            delegate.consume(rs);
            count++;
          }
        }
        vector.endValue(currentIndex, count);
      }
      currentIndex++;
    }

    @Override
    public void close() throws Exception {
      this.vector.close();
      this.delegate.close();
    }
  }

  /**
   * Non-nullable consumer for {@link ListVector}.
   */
  static class NonNullableArrayConsumer extends BaseJdbcConsumer<ListVector> {

    private final JdbcConsumer delegate;

    /**
     * Instantiate a nullable array consumer.
     */
    public NonNullableArrayConsumer(ListVector vector, JdbcConsumer delegate, int index) {
      super(vector, index);
      this.delegate = delegate;
    }

    @Override
    public void consume(ResultSet resultSet) throws SQLException, IOException {
      final Array array = resultSet.getArray(columnIndexInResultSet);

      vector.startNewValue(currentIndex);
      int count = 0;
      try (ResultSet rs = array.getResultSet()) {
        while (rs.next()) {
          delegate.consume(rs);
          count++;
        }
      }
      vector.endValue(currentIndex, count);
      currentIndex++;
    }

    @Override
    public void close() throws Exception {
      this.vector.close();
      this.delegate.close();
    }
  }
}
