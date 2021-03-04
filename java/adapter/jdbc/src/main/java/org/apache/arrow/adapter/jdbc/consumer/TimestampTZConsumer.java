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
import java.util.Calendar;

import org.apache.arrow.util.Preconditions;
import org.apache.arrow.vector.TimeStampMilliTZVector;

/**
 * Consumer which consume timestamp (with time zone) type values from {@link ResultSet}.
 * Write the data to {@link TimeStampMilliTZVector}.
 */
public class TimestampTZConsumer {
  /**
   * Creates a consumer for {@link TimeStampMilliTZVector}.
   */
  public static JdbcConsumer<TimeStampMilliTZVector> createConsumer(
      TimeStampMilliTZVector vector, int index, boolean nullable, Calendar calendar) {
    Preconditions.checkArgument(calendar != null, "Calendar cannot be null");
    if (nullable) {
      return new TimestampTZConsumer.NullableTimestampTZConsumer(vector, index, calendar);
    } else {
      return new TimestampTZConsumer.NonNullableTimestampConsumer(vector, index, calendar);
    }
  }

  /**
   * Nullable consumer for timestamp (with time zone).
   */
  static class NullableTimestampTZConsumer extends BaseConsumer<TimeStampMilliTZVector> {

    protected final Calendar calendar;

    /**
     * Instantiate a TimestampConsumer.
     */
    public NullableTimestampTZConsumer(TimeStampMilliTZVector vector, int index, Calendar calendar) {
      super(vector, index);
      this.calendar = calendar;
    }

    @Override
    public void consume(ResultSet resultSet) throws SQLException {
      Timestamp timestamp = resultSet.getTimestamp(columnIndexInResultSet, calendar);
      if (!resultSet.wasNull()) {
        // for fixed width vectors, we have allocated enough memory proactively,
        // so there is no need to call the setSafe method here.
        vector.set(currentIndex, timestamp.getTime());
      }
      currentIndex++;
    }
  }

  /**
   * Non-nullable consumer for timestamp (with time zone).
   */
  static class NonNullableTimestampConsumer extends BaseConsumer<TimeStampMilliTZVector> {

    protected final Calendar calendar;

    /**
     * Instantiate a TimestampConsumer.
     */
    public NonNullableTimestampConsumer(TimeStampMilliTZVector vector, int index, Calendar calendar) {
      super(vector, index);
      this.calendar = calendar;
    }

    @Override
    public void consume(ResultSet resultSet) throws SQLException {
      Timestamp timestamp = resultSet.getTimestamp(columnIndexInResultSet, calendar);
      // for fixed width vectors, we have allocated enough memory proactively,
      // so there is no need to call the setSafe method here.
      vector.set(currentIndex, timestamp.getTime());
      currentIndex++;
    }
  }
}
