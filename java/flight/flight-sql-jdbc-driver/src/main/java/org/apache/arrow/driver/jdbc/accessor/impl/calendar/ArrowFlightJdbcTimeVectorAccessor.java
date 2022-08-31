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

package org.apache.arrow.driver.jdbc.accessor.impl.calendar;

import static org.apache.arrow.driver.jdbc.accessor.impl.calendar.ArrowFlightJdbcTimeVectorGetter.Getter;
import static org.apache.arrow.driver.jdbc.accessor.impl.calendar.ArrowFlightJdbcTimeVectorGetter.Holder;
import static org.apache.arrow.driver.jdbc.accessor.impl.calendar.ArrowFlightJdbcTimeVectorGetter.createGetter;

import java.sql.Time;
import java.sql.Timestamp;
import java.util.Calendar;
import java.util.concurrent.TimeUnit;
import java.util.function.IntSupplier;

import org.apache.arrow.driver.jdbc.ArrowFlightJdbcTime;
import org.apache.arrow.driver.jdbc.accessor.ArrowFlightJdbcAccessor;
import org.apache.arrow.driver.jdbc.accessor.ArrowFlightJdbcAccessorFactory;
import org.apache.arrow.driver.jdbc.utils.DateTimeUtils;
import org.apache.arrow.vector.TimeMicroVector;
import org.apache.arrow.vector.TimeMilliVector;
import org.apache.arrow.vector.TimeNanoVector;
import org.apache.arrow.vector.TimeSecVector;
import org.apache.arrow.vector.ValueVector;

/**
 * Accessor for the Arrow types: {@link TimeNanoVector}, {@link TimeMicroVector}, {@link TimeMilliVector}
 * and {@link TimeSecVector}.
 */
public class ArrowFlightJdbcTimeVectorAccessor extends ArrowFlightJdbcAccessor {

  private final Getter getter;
  private final TimeUnit timeUnit;
  private final Holder holder;

  /**
   * Instantiate an accessor for a {@link TimeNanoVector}.
   *
   * @param vector             an instance of a TimeNanoVector.
   * @param currentRowSupplier the supplier to track the lines.
   * @param setCursorWasNull   the consumer to set if value was null.
   */
  public ArrowFlightJdbcTimeVectorAccessor(TimeNanoVector vector, IntSupplier currentRowSupplier,
                                           ArrowFlightJdbcAccessorFactory.WasNullConsumer setCursorWasNull) {
    super(currentRowSupplier, setCursorWasNull);
    this.holder = new Holder();
    this.getter = createGetter(vector);
    this.timeUnit = getTimeUnitForVector(vector);
  }

  /**
   * Instantiate an accessor for a {@link TimeMicroVector}.
   *
   * @param vector             an instance of a TimeMicroVector.
   * @param currentRowSupplier the supplier to track the lines.
   * @param setCursorWasNull   the consumer to set if value was null.
   */
  public ArrowFlightJdbcTimeVectorAccessor(TimeMicroVector vector, IntSupplier currentRowSupplier,
                                           ArrowFlightJdbcAccessorFactory.WasNullConsumer setCursorWasNull) {
    super(currentRowSupplier, setCursorWasNull);
    this.holder = new Holder();
    this.getter = createGetter(vector);
    this.timeUnit = getTimeUnitForVector(vector);
  }

  /**
   * Instantiate an accessor for a {@link TimeMilliVector}.
   *
   * @param vector             an instance of a TimeMilliVector.
   * @param currentRowSupplier the supplier to track the lines.
   */
  public ArrowFlightJdbcTimeVectorAccessor(TimeMilliVector vector, IntSupplier currentRowSupplier,
                                           ArrowFlightJdbcAccessorFactory.WasNullConsumer setCursorWasNull) {
    super(currentRowSupplier, setCursorWasNull);
    this.holder = new Holder();
    this.getter = createGetter(vector);
    this.timeUnit = getTimeUnitForVector(vector);
  }

  /**
   * Instantiate an accessor for a {@link TimeSecVector}.
   *
   * @param vector             an instance of a TimeSecVector.
   * @param currentRowSupplier the supplier to track the lines.
   */
  public ArrowFlightJdbcTimeVectorAccessor(TimeSecVector vector, IntSupplier currentRowSupplier,
                                           ArrowFlightJdbcAccessorFactory.WasNullConsumer setCursorWasNull) {
    super(currentRowSupplier, setCursorWasNull);
    this.holder = new Holder();
    this.getter = createGetter(vector);
    this.timeUnit = getTimeUnitForVector(vector);
  }

  @Override
  public Class<?> getObjectClass() {
    return Time.class;
  }

  @Override
  public Object getObject() {
    return this.getTime(null);
  }

  @Override
  public Time getTime(Calendar calendar) {
    fillHolder();
    if (this.wasNull) {
      return null;
    }

    long value = holder.value;
    long milliseconds = this.timeUnit.toMillis(value);

    return new ArrowFlightJdbcTime(DateTimeUtils.applyCalendarOffset(milliseconds, calendar));
  }

  private void fillHolder() {
    getter.get(getCurrentRow(), holder);
    this.wasNull = holder.isSet == 0;
    this.wasNullConsumer.setWasNull(this.wasNull);
  }

  @Override
  public Timestamp getTimestamp(Calendar calendar) {
    Time time = getTime(calendar);
    if (time == null) {
      return null;
    }
    return new Timestamp(time.getTime());
  }

  protected static TimeUnit getTimeUnitForVector(ValueVector vector) {
    if (vector instanceof TimeNanoVector) {
      return TimeUnit.NANOSECONDS;
    } else if (vector instanceof TimeMicroVector) {
      return TimeUnit.MICROSECONDS;
    } else if (vector instanceof TimeMilliVector) {
      return TimeUnit.MILLISECONDS;
    } else if (vector instanceof TimeSecVector) {
      return TimeUnit.SECONDS;
    }

    throw new IllegalArgumentException("Invalid Arrow vector");
  }
}
