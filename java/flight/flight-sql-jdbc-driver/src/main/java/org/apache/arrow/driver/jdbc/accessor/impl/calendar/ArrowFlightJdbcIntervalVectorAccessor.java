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

import static org.apache.arrow.driver.jdbc.utils.IntervalStringUtils.formatIntervalDay;
import static org.apache.arrow.driver.jdbc.utils.IntervalStringUtils.formatIntervalYear;
import static org.apache.arrow.vector.util.DateUtility.yearsToMonths;

import java.sql.SQLException;
import java.util.function.IntSupplier;

import org.apache.arrow.driver.jdbc.accessor.ArrowFlightJdbcAccessor;
import org.apache.arrow.driver.jdbc.accessor.ArrowFlightJdbcAccessorFactory;
import org.apache.arrow.vector.BaseFixedWidthVector;
import org.apache.arrow.vector.IntervalDayVector;
import org.apache.arrow.vector.IntervalYearVector;
import org.apache.arrow.vector.holders.NullableIntervalDayHolder;
import org.apache.arrow.vector.holders.NullableIntervalYearHolder;
import org.joda.time.Period;

/**
 * Accessor for the Arrow type {@link IntervalDayVector}.
 */
public class ArrowFlightJdbcIntervalVectorAccessor extends ArrowFlightJdbcAccessor {

  private final BaseFixedWidthVector vector;
  private final StringGetter stringGetter;
  private final Class<?> objectClass;

  /**
   * Instantiate an accessor for a {@link IntervalDayVector}.
   *
   * @param vector             an instance of a IntervalDayVector.
   * @param currentRowSupplier the supplier to track the rows.
   * @param setCursorWasNull   the consumer to set if value was null.
   */
  public ArrowFlightJdbcIntervalVectorAccessor(IntervalDayVector vector,
                                               IntSupplier currentRowSupplier,
                                               ArrowFlightJdbcAccessorFactory.WasNullConsumer setCursorWasNull) {
    super(currentRowSupplier, setCursorWasNull);
    this.vector = vector;
    stringGetter = (index) -> {
      final NullableIntervalDayHolder holder = new NullableIntervalDayHolder();
      vector.get(index, holder);
      if (holder.isSet == 0) {
        return null;
      } else {
        final int days = holder.days;
        final int millis = holder.milliseconds;
        return formatIntervalDay(new Period().plusDays(days).plusMillis(millis));
      }
    };
    objectClass = java.time.Duration.class;
  }

  /**
   * Instantiate an accessor for a {@link IntervalYearVector}.
   *
   * @param vector             an instance of a IntervalYearVector.
   * @param currentRowSupplier the supplier to track the rows.
   * @param setCursorWasNull   the consumer to set if value was null.
   */
  public ArrowFlightJdbcIntervalVectorAccessor(IntervalYearVector vector,
                                               IntSupplier currentRowSupplier,
                                               ArrowFlightJdbcAccessorFactory.WasNullConsumer setCursorWasNull) {
    super(currentRowSupplier, setCursorWasNull);
    this.vector = vector;
    stringGetter = (index) -> {
      final NullableIntervalYearHolder holder = new NullableIntervalYearHolder();
      vector.get(index, holder);
      if (holder.isSet == 0) {
        return null;
      } else {
        final int interval = holder.value;
        final int years = (interval / yearsToMonths);
        final int months = (interval % yearsToMonths);
        return formatIntervalYear(new Period().plusYears(years).plusMonths(months));
      }
    };
    objectClass = java.time.Period.class;
  }

  @Override
  public Class<?> getObjectClass() {
    return objectClass;
  }

  @Override
  public String getString() throws SQLException {
    String result = stringGetter.get(getCurrentRow());
    wasNull = result == null;
    wasNullConsumer.setWasNull(wasNull);
    return result;
  }

  @Override
  public Object getObject() {
    Object object = vector.getObject(getCurrentRow());
    wasNull = object == null;
    wasNullConsumer.setWasNull(wasNull);
    return object;
  }

  /**
   * Functional interface used to unify Interval*Vector#getAsStringBuilder implementations.
   */
  @FunctionalInterface
  interface StringGetter {
    String get(int index);
  }
}
