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
import java.sql.Timestamp;

import org.apache.arrow.vector.TimeStampMilliVector;

/**
 * Consumer which consume timestamp type values from {@link ResultSet}.
 * Write the data to {@link TimeStampMilliVector}.
 */
public abstract class TimestampConsumer {

  /**
   * Creates a consumer for {@link TimeStampMilliVector}.
   */
  public static JdbcConsumer<TimeStampMilliVector> createConsumer(
          TimeStampMilliVector vector, int index, boolean nullable) {
    if (nullable) {
      return new NullableTimestampConsumer(vector, index);
    } else {
      return new NonNullableTimestampConsumer(vector, index);
    }
  }

  /**
   * Nullable consumer for timestamp.
   */
  static class NullableTimestampConsumer extends BaseConsumer<TimeStampMilliVector> {

    /**
     * Instantiate a TimestampConsumer.
     */
    public NullableTimestampConsumer(TimeStampMilliVector vector, int index) {
      super(vector, index);
    }

    @Override
    public void consume(ResultSet resultSet) throws SQLException {
      Timestamp timestamp = resultSet.getTimestamp(columnIndexInResultSet);
      if (!resultSet.wasNull()) {
        // for fixed width vectors, we have allocated enough memory proactively,
        // so there is no need to call the setSafe method here.
        vector.set(currentIndex, timestamp.getTime());
      }
      currentIndex++;
    }
  }

  /**
   * Non-nullable consumer for timestamp.
   */
  static class NonNullableTimestampConsumer extends BaseConsumer<TimeStampMilliVector> {

    /**
     * Instantiate a TimestampConsumer.
     */
    public NonNullableTimestampConsumer(TimeStampMilliVector vector, int index) {
      super(vector, index);
    }

    @Override
    public void consume(ResultSet resultSet) throws SQLException {
      Timestamp timestamp = resultSet.getTimestamp(columnIndexInResultSet);
      // for fixed width vectors, we have allocated enough memory proactively,
      // so there is no need to call the setSafe method here.
      vector.set(currentIndex, timestamp.getTime());
      currentIndex++;
    }
  }
}
