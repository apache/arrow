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

import org.apache.arrow.vector.IntVector;

/**
 * Consumer which consume int type values from {@link ResultSet}.
 * Write the data to {@link org.apache.arrow.vector.IntVector}.
 */
public class IntConsumer {

  /**
   * Creates a consumer for {@link IntVector}.
   */
  public static JdbcConsumer<IntVector> createConsumer(IntVector vector, int index, boolean nullable) {
    if (nullable) {
      return new NullableIntConsumer(vector, index);
    } else {
      return new NonNullableIntConsumer(vector, index);
    }
  }

  /**
   * Nullable consumer for int.
   */
  static class NullableIntConsumer extends BaseConsumer<IntVector> {

    /**
     * Instantiate a IntConsumer.
     */
    public NullableIntConsumer(IntVector vector, int index) {
      super(vector, index);
    }

    @Override
    public void consume(ResultSet resultSet) throws SQLException {
      int value = resultSet.getInt(columnIndexInResultSet);
      if (!resultSet.wasNull()) {
        // for fixed width vectors, we have allocated enough memory proactively,
        // so there is no need to call the setSafe method here.
        vector.set(currentIndex, value);
      }
      currentIndex++;
    }
  }

  /**
   * Non-nullable consumer for int.
   */
  static class NonNullableIntConsumer extends BaseConsumer<IntVector> {

    /**
     * Instantiate a IntConsumer.
     */
    public NonNullableIntConsumer(IntVector vector, int index) {
      super(vector, index);
    }

    @Override
    public void consume(ResultSet resultSet) throws SQLException {
      int value = resultSet.getInt(columnIndexInResultSet);
      // for fixed width vectors, we have allocated enough memory proactively,
      // so there is no need to call the setSafe method here.
      vector.set(currentIndex, value);
      currentIndex++;
    }
  }
}
