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
import org.apache.arrow.vector.complex.impl.TimeMilliWriterImpl;
import org.apache.arrow.vector.complex.writer.TimeMilliWriter;

/**
 * Consumer which consume time type values from {@link ResultSet}.
 * Write the data to {@link org.apache.arrow.vector.TimeMilliVector}.
 */
public class TimeConsumer implements JdbcConsumer {

  private final TimeMilliWriter writer;
  private final int columnIndexInResultSet;
  private final Calendar calendar;

  /**
   * Instantiate a TimeConsumer.
   */
  public TimeConsumer(TimeMilliVector vector, int index) {
    this(vector, index, null);
  }

  /**
   * Instantiate a TimeConsumer.
   */
  public TimeConsumer(TimeMilliVector vector, int index, Calendar calendar) {
    this.writer = new TimeMilliWriterImpl(vector);
    this.columnIndexInResultSet = index;
    this.calendar = calendar;
  }

  @Override
  public void consume(ResultSet resultSet) throws SQLException {
    Time time = calendar == null ? resultSet.getTime(columnIndexInResultSet) :
        resultSet.getTime(columnIndexInResultSet, calendar);
    if (!resultSet.wasNull()) {
      writer.writeTimeMilli((int) time.getTime());
    }
    writer.setPosition(writer.getPosition() + 1);
  }
}
