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

import java.sql.ResultSet;
import java.sql.SQLException;

import org.apache.arrow.vector.BitVector;

/**
 * Consumer which consume bit type values from {@link ResultSet}.
 * Write the data to {@link BitVector}.
 */
public abstract class BitConsumer implements JdbcConsumer<BitVector> {

  /**
   * Creates a consumer for {@link BitVector}.
   */
  public static BitConsumer createConsumer(BitVector vector, int index, boolean nullable) {
    if (nullable) {
      return new NullableBitConsumer(vector, index);
    } else {
      return new NonNullableBitConsumer(vector, index);
    }
  }

  protected BitVector vector;
  protected final int columnIndexInResultSet;

  protected int currentIndex;

  /**
   * Instantiate a BitConsumer.
   */
  public BitConsumer(BitVector vector, int index) {
    this.vector = vector;
    this.columnIndexInResultSet = index;
  }

  @Override
  public void close() throws Exception {
    this.vector.close();
  }

  @Override
  public void resetValueVector(BitVector vector) {
    this.vector = vector;
    this.currentIndex = 0;
  }

  /**
   * Nullable consumer for {@link BitVector}.
   */
  static class NullableBitConsumer extends BitConsumer {

    /**
     * Instantiate a BitConsumer.
     */
    public NullableBitConsumer(BitVector vector, int index) {
      super(vector, index);
    }

    @Override
    public void consume(ResultSet resultSet) throws SQLException {
      boolean value = resultSet.getBoolean(columnIndexInResultSet);
      if (!resultSet.wasNull()) {
        vector.setSafe(currentIndex, value ? 1 : 0);
      }
      currentIndex++;
    }
  }

  /**
   * Non-nullable consumer for {@link BitVector}.
   */
  static class NonNullableBitConsumer extends BitConsumer {

    /**
     * Instantiate a BitConsumer.
     */
    public NonNullableBitConsumer(BitVector vector, int index) {
      super(vector, index);
    }

    @Override
    public void consume(ResultSet resultSet) throws SQLException {
      boolean value = resultSet.getBoolean(columnIndexInResultSet);
      vector.setSafe(currentIndex, value ? 1 : 0);
      currentIndex++;
    }
  }
}
