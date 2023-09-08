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

import static org.apache.arrow.driver.jdbc.accessor.impl.calendar.ArrowFlightJdbcDateVectorGetter.Getter;
import static org.apache.arrow.driver.jdbc.accessor.impl.calendar.ArrowFlightJdbcDateVectorGetter.Holder;
import static org.apache.arrow.driver.jdbc.accessor.impl.calendar.ArrowFlightJdbcDateVectorGetter.createGetter;
import static org.apache.arrow.driver.jdbc.utils.DateTimeUtils.getTimestampValue;
import static org.apache.calcite.avatica.util.DateTimeUtils.MILLIS_PER_DAY;
import static org.apache.calcite.avatica.util.DateTimeUtils.unixDateToString;

import java.sql.Date;
import java.sql.Timestamp;
import java.util.Calendar;
import java.util.concurrent.TimeUnit;
import java.util.function.IntSupplier;

import org.apache.arrow.driver.jdbc.accessor.ArrowFlightJdbcAccessor;
import org.apache.arrow.driver.jdbc.accessor.ArrowFlightJdbcAccessorFactory;
import org.apache.arrow.driver.jdbc.utils.DateTimeUtils;
import org.apache.arrow.vector.DateDayVector;
import org.apache.arrow.vector.DateMilliVector;
import org.apache.arrow.vector.ValueVector;

/**
 * Accessor for the Arrow types: {@link DateDayVector} and {@link DateMilliVector}.
 */
public class ArrowFlightJdbcDateVectorAccessor extends ArrowFlightJdbcAccessor {

  private final Getter getter;
  private final TimeUnit timeUnit;
  private final Holder holder;

  /**
   * Instantiate an accessor for a {@link DateDayVector}.
   *
   * @param vector             an instance of a DateDayVector.
   * @param currentRowSupplier the supplier to track the lines.
   * @param setCursorWasNull   the consumer to set if value was null.
   */
  public ArrowFlightJdbcDateVectorAccessor(DateDayVector vector, IntSupplier currentRowSupplier,
                                           ArrowFlightJdbcAccessorFactory.WasNullConsumer setCursorWasNull) {
    super(currentRowSupplier, setCursorWasNull);
    this.holder = new Holder();
    this.getter = createGetter(vector);
    this.timeUnit = getTimeUnitForVector(vector);
  }

  /**
   * Instantiate an accessor for a {@link DateMilliVector}.
   *
   * @param vector             an instance of a DateMilliVector.
   * @param currentRowSupplier the supplier to track the lines.
   */
  public ArrowFlightJdbcDateVectorAccessor(DateMilliVector vector, IntSupplier currentRowSupplier,
                                           ArrowFlightJdbcAccessorFactory.WasNullConsumer setCursorWasNull) {
    super(currentRowSupplier, setCursorWasNull);
    this.holder = new Holder();
    this.getter = createGetter(vector);
    this.timeUnit = getTimeUnitForVector(vector);
  }

  @Override
  public Class<?> getObjectClass() {
    return Date.class;
  }

  @Override
  public Object getObject() {
    return this.getDate(null);
  }

  @Override
  public Date getDate(Calendar calendar) {
    fillHolder();
    if (this.wasNull) {
      return null;
    }

    long value = holder.value;
    long milliseconds = this.timeUnit.toMillis(value);

    long millisWithCalendar = DateTimeUtils.applyCalendarOffset(milliseconds, calendar);

    return new Date(getTimestampValue(millisWithCalendar).getTime());
  }

  private void fillHolder() {
    getter.get(getCurrentRow(), holder);
    this.wasNull = holder.isSet == 0;
    this.wasNullConsumer.setWasNull(this.wasNull);
  }

  @Override
  public Timestamp getTimestamp(Calendar calendar) {
    Date date = getDate(calendar);
    if (date == null) {
      return null;
    }
    return new Timestamp(date.getTime());
  }

  @Override
  public String getString() {
    fillHolder();
    if (wasNull) {
      return null;
    }
    long milliseconds = timeUnit.toMillis(holder.value);
    return unixDateToString((int) (milliseconds / MILLIS_PER_DAY));
  }

  protected static TimeUnit getTimeUnitForVector(ValueVector vector) {
    if (vector instanceof DateDayVector) {
      return TimeUnit.DAYS;
    } else if (vector instanceof DateMilliVector) {
      return TimeUnit.MILLISECONDS;
    }

    throw new IllegalArgumentException("Invalid Arrow vector");
  }
}
