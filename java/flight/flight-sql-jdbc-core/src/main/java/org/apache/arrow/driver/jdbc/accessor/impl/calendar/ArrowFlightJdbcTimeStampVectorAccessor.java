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

import static org.apache.arrow.driver.jdbc.accessor.impl.calendar.ArrowFlightJdbcTimeStampVectorGetter.Getter;
import static org.apache.arrow.driver.jdbc.accessor.impl.calendar.ArrowFlightJdbcTimeStampVectorGetter.Holder;
import static org.apache.arrow.driver.jdbc.accessor.impl.calendar.ArrowFlightJdbcTimeStampVectorGetter.createGetter;

import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.Calendar;
import java.util.TimeZone;
import java.util.concurrent.TimeUnit;
import java.util.function.IntSupplier;

import org.apache.arrow.driver.jdbc.accessor.ArrowFlightJdbcAccessor;
import org.apache.arrow.driver.jdbc.accessor.ArrowFlightJdbcAccessorFactory;
import org.apache.arrow.vector.TimeStampVector;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.util.DateUtility;

/**
 * Accessor for the Arrow types extending from {@link TimeStampVector}.
 */
public class ArrowFlightJdbcTimeStampVectorAccessor extends ArrowFlightJdbcAccessor {

  private final TimeZone timeZone;
  private final Getter getter;
  private final TimeUnit timeUnit;
  private final LongToUTCDateTime longToUTCDateTime;
  private final Holder holder;

  /**
   * Functional interface used to convert a number (in any time resolution) to LocalDateTime.
   */
  interface LongToUTCDateTime {
    LocalDateTime fromLong(long value);
  }

  /**
   * Instantiate a ArrowFlightJdbcTimeStampVectorAccessor for given vector.
   */
  public ArrowFlightJdbcTimeStampVectorAccessor(TimeStampVector vector,
                                                IntSupplier currentRowSupplier,
                                                ArrowFlightJdbcAccessorFactory.WasNullConsumer setCursorWasNull) {
    super(currentRowSupplier, setCursorWasNull);
    this.holder = new Holder();
    this.getter = createGetter(vector);

    this.timeZone = getTimeZoneForVector(vector);
    this.timeUnit = getTimeUnitForVector(vector);
    this.longToUTCDateTime = getLongToUTCDateTimeForVector(vector);
  }

  @Override
  public Class<?> getObjectClass() {
    return Timestamp.class;
  }

  @Override
  public Object getObject() {
    return this.getTimestamp(null);
  }

  private LocalDateTime getLocalDateTime(Calendar calendar) {
    getter.get(getCurrentRow(), holder);
    this.wasNull = holder.isSet == 0;
    this.wasNullConsumer.setWasNull(this.wasNull);
    if (this.wasNull) {
      return null;
    }

    long value = holder.value;

    LocalDateTime localDateTime = this.longToUTCDateTime.fromLong(value);
    ZoneId defaultTimeZone = Calendar.getInstance().getTimeZone().toZoneId();
    ZoneId sourceTimeZone;

    if (this.timeZone != null) {
      sourceTimeZone = this.timeZone.toZoneId();
    } else if (calendar != null) {
      sourceTimeZone = calendar.getTimeZone().toZoneId();
    } else {
      sourceTimeZone = defaultTimeZone;
    }

    return localDateTime.atZone(sourceTimeZone).withZoneSameInstant(defaultTimeZone).toLocalDateTime();
  }

  @Override
  public Date getDate(Calendar calendar) {
    LocalDateTime localDateTime = getLocalDateTime(calendar);
    if (localDateTime == null) {
      return null;
    }

    return new Date(Timestamp.valueOf(localDateTime).getTime());
  }

  @Override
  public Time getTime(Calendar calendar) {
    LocalDateTime localDateTime = getLocalDateTime(calendar);
    if (localDateTime == null) {
      return null;
    }

    return new Time(Timestamp.valueOf(localDateTime).getTime());
  }

  @Override
  public Timestamp getTimestamp(Calendar calendar) {
    LocalDateTime localDateTime = getLocalDateTime(calendar);
    if (localDateTime == null) {
      return null;
    }

    return Timestamp.valueOf(localDateTime);
  }

  protected static TimeUnit getTimeUnitForVector(TimeStampVector vector) {
    ArrowType.Timestamp arrowType =
        (ArrowType.Timestamp) vector.getField().getFieldType().getType();

    switch (arrowType.getUnit()) {
      case NANOSECOND:
        return TimeUnit.NANOSECONDS;
      case MICROSECOND:
        return TimeUnit.MICROSECONDS;
      case MILLISECOND:
        return TimeUnit.MILLISECONDS;
      case SECOND:
        return TimeUnit.SECONDS;
      default:
        throw new UnsupportedOperationException("Invalid Arrow time unit");
    }
  }

  protected static LongToUTCDateTime getLongToUTCDateTimeForVector(TimeStampVector vector) {
    String timeZoneID = "UTC";

    ArrowType.Timestamp arrowType =
        (ArrowType.Timestamp) vector.getField().getFieldType().getType();

    switch (arrowType.getUnit()) {
      case NANOSECOND:
        return DateUtility::getLocalDateTimeFromEpochNano;
      case MICROSECOND:
        return DateUtility::getLocalDateTimeFromEpochMicro;
      case MILLISECOND:
        return DateUtility::getLocalDateTimeFromEpochMilli;
      case SECOND:
        return seconds -> DateUtility.getLocalDateTimeFromEpochMilli(
            TimeUnit.SECONDS.toMillis(seconds));
      default:
        throw new UnsupportedOperationException("Invalid Arrow time unit");
    }
  }

  protected static TimeZone getTimeZoneForVector(TimeStampVector vector) {
    ArrowType.Timestamp arrowType =
        (ArrowType.Timestamp) vector.getField().getFieldType().getType();

    String timezoneName = arrowType.getTimezone();
    if (timezoneName == null) {
      return null;
    }

    return TimeZone.getTimeZone(timezoneName);
  }
}
