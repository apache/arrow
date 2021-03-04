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

import java.math.BigDecimal;
import java.sql.ResultSet;
import java.sql.SQLException;

import org.apache.arrow.vector.DecimalVector;

/**
 * Consumer which consume decimal type values from {@link ResultSet}.
 * Write the data to {@link org.apache.arrow.vector.DecimalVector}.
 */
public class DecimalConsumer {

  /**
   * Creates a consumer for {@link DecimalVector}.
   */
  public static JdbcConsumer<DecimalVector> createConsumer(DecimalVector vector, int index, boolean nullable) {
    if (nullable) {
      return new NullableDecimalConsumer(vector, index);
    } else {
      return new NonNullableDecimalConsumer(vector, index);
    }
  }

  /**
   * Consumer for nullable decimal.
   */
  static class NullableDecimalConsumer extends BaseConsumer<DecimalVector> {

    /**
     * Instantiate a DecimalConsumer.
     */
    public NullableDecimalConsumer(DecimalVector vector, int index) {
      super(vector, index);
    }

    @Override
    public void consume(ResultSet resultSet) throws SQLException {
      BigDecimal value = resultSet.getBigDecimal(columnIndexInResultSet);
      if (!resultSet.wasNull()) {
        // for fixed width vectors, we have allocated enough memory proactively,
        // so there is no need to call the setSafe method here.
        vector.set(currentIndex, value);
      }
      currentIndex++;
    }
  }

  /**
   * Consumer for non-nullable decimal.
   */
  static class NonNullableDecimalConsumer extends BaseConsumer<DecimalVector> {

    /**
     * Instantiate a DecimalConsumer.
     */
    public NonNullableDecimalConsumer(DecimalVector vector, int index) {
      super(vector, index);
    }

    @Override
    public void consume(ResultSet resultSet) throws SQLException {
      BigDecimal value = resultSet.getBigDecimal(columnIndexInResultSet);
      // for fixed width vectors, we have allocated enough memory proactively,
      // so there is no need to call the setSafe method here.
      vector.set(currentIndex, value);
      currentIndex++;
    }
  }
}
