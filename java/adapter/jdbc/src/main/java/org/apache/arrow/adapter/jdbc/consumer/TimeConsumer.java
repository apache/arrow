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
import java.sql.Time;
import java.util.Calendar;

import org.apache.arrow.vector.TimeMilliVector;

/**
 * Consumer which consume time type values from {@link ResultSet}.
 * Write the data to {@link org.apache.arrow.vector.TimeMilliVector}.
 */
public abstract class TimeConsumer {

  /**
   * Creates a consumer for {@link TimeMilliVector}.
   */
  public static JdbcConsumer<TimeMilliVector> createConsumer(
          TimeMilliVector vector, int index, boolean nullable, Calendar calendar) {
    if (nullable) {
      return new NullableTimeConsumer(vector, index, calendar);
    } else {
      return new NonNullableTimeConsumer(vector, index, calendar);
    }
  }

  /**
   * Nullable consumer for {@link TimeMilliVector}.
   */
  static class NullableTimeConsumer extends BaseConsumer<TimeMilliVector> {

    protected final Calendar calendar;

    /**
     * Instantiate a TimeConsumer.
     */
    public NullableTimeConsumer(TimeMilliVector vector, int index) {
      this(vector, index, /* calendar */null);
    }

    /**
     * Instantiate a TimeConsumer.
     */
    public NullableTimeConsumer(TimeMilliVector vector, int index, Calendar calendar) {
      super(vector, index);
      this.calendar = calendar;
    }

    @Override
    public void consume(ResultSet resultSet) throws SQLException {
      Time time = calendar == null ? resultSet.getTime(columnIndexInResultSet) :
          resultSet.getTime(columnIndexInResultSet, calendar);
      if (!resultSet.wasNull()) {
        // for fixed width vectors, we have allocated enough memory proactively,
        // so there is no need to call the setSafe method here.
        vector.set(currentIndex, (int) time.getTime());
      }
      currentIndex++;
    }
  }

  /**
   * Non-nullable consumer for {@link TimeMilliVector}.
   */
  static class NonNullableTimeConsumer extends BaseConsumer<TimeMilliVector> {

    protected final Calendar calendar;

    /**
     * Instantiate a TimeConsumer.
     */
    public NonNullableTimeConsumer(TimeMilliVector vector, int index) {
      this(vector, index, /* calendar */null);
    }

    /**
     * Instantiate a TimeConsumer.
     */
    public NonNullableTimeConsumer(TimeMilliVector vector, int index, Calendar calendar) {
      super(vector, index);
      this.calendar = calendar;
    }

    @Override
    public void consume(ResultSet resultSet) throws SQLException {
      Time time = calendar == null ? resultSet.getTime(columnIndexInResultSet) :
          resultSet.getTime(columnIndexInResultSet, calendar);
      // for fixed width vectors, we have allocated enough memory proactively,
      // so there is no need to call the setSafe method here.
      vector.set(currentIndex, (int) time.getTime());
      currentIndex++;
    }
  }
}
