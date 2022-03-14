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
import static org.joda.time.Period.parse;

import java.sql.SQLException;
import java.time.Duration;
import java.time.Period;
import java.util.function.IntSupplier;

import org.apache.arrow.driver.jdbc.accessor.ArrowFlightJdbcAccessor;
import org.apache.arrow.driver.jdbc.accessor.ArrowFlightJdbcAccessorFactory;
import org.apache.arrow.vector.BaseFixedWidthVector;
import org.apache.arrow.vector.IntervalDayVector;
import org.apache.arrow.vector.IntervalYearVector;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

/**
 * Accessor for the Arrow type {@link IntervalDayVector}.
 */
public class ArrowFlightJdbcIntervalVectorAccessor extends ArrowFlightJdbcAccessor {

  /**
   * Functional interface used to unify Interval*Vector#getAsStringBuilder implementations.
   */
  @FunctionalInterface
  interface StringBuilderGetter {
    StringBuilder get(int index);
  }

  private final BaseFixedWidthVector vector;
  private final StringBuilderGetter stringBuilderGetter;
  private final Class<?> objectClass;

  /**
   * Instantiate an accessor for a {@link IntervalDayVector}.
   *
   * @param vector             an instance of a IntervalDayVector.
   * @param currentRowSupplier the supplier to track the rows.
   * @param setCursorWasNull   the consumer to set if value was null.
   */
  @SuppressFBWarnings(value = "EI_EXPOSE_REP2", justification = "We shouldn't make copies of vectors")
  public ArrowFlightJdbcIntervalVectorAccessor(IntervalDayVector vector,
                                               IntSupplier currentRowSupplier,
                                               ArrowFlightJdbcAccessorFactory.WasNullConsumer setCursorWasNull) {
    super(currentRowSupplier, setCursorWasNull);
    this.vector = vector;
    this.stringBuilderGetter = vector::getAsStringBuilder;
    this.objectClass = Duration.class;
  }

  /**
   * Instantiate an accessor for a {@link IntervalYearVector}.
   *
   * @param vector             an instance of a IntervalYearVector.
   * @param currentRowSupplier the supplier to track the rows.
   * @param setCursorWasNull   the consumer to set if value was null.
   */
  @SuppressFBWarnings(value = "EI_EXPOSE_REP2", justification = "We shouldn't make copies of vectors")
  public ArrowFlightJdbcIntervalVectorAccessor(IntervalYearVector vector,
                                               IntSupplier currentRowSupplier,
                                               ArrowFlightJdbcAccessorFactory.WasNullConsumer setCursorWasNull) {
    super(currentRowSupplier, setCursorWasNull);
    this.vector = vector;
    this.stringBuilderGetter = vector::getAsStringBuilder;
    this.objectClass = Period.class;
  }

  @Override
  public Object getObject() {
    Object object = this.vector.getObject(getCurrentRow());
    this.wasNull = object == null;
    this.wasNullConsumer.setWasNull(this.wasNull);

    return object;
  }

  @Override
  public Class<?> getObjectClass() {
    return this.objectClass;
  }

  @Override
  public String getString() {
    StringBuilder stringBuilder = this.stringBuilderGetter.get(getCurrentRow());

    this.wasNull = stringBuilder == null;
    this.wasNullConsumer.setWasNull(wasNull);
    if (stringBuilder == null) {
      return null;
    }
    if (vector instanceof IntervalDayVector) {
      return formatIntervalDay(parse(object.toString()));
    } else if (vector instanceof IntervalYearVector) {
      return formatIntervalYear(parse(object.toString()));
    } else {
      throw new SQLException("Invalid Interval vector instance");
    }
  }
}
