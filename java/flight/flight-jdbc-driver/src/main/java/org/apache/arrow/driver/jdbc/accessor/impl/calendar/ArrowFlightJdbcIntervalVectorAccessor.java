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

import java.time.Duration;
import java.time.Period;
import java.util.function.IntSupplier;

import org.apache.arrow.driver.jdbc.accessor.ArrowFlightJdbcAccessor;
import org.apache.arrow.vector.IntervalDayVector;
import org.apache.arrow.vector.IntervalYearVector;

/**
 * Accessor for the Arrow type {@link IntervalDayVector}.
 */
public class ArrowFlightJdbcIntervalVectorAccessor extends ArrowFlightJdbcAccessor {

  /**
   * Functional interface used to unify Interval*Vector#getObject implementations.
   */
  @FunctionalInterface
  interface ObjectGetter {
    Object get(int index);
  }

  /**
   * Functional interface used to unify Interval*Vector#getAsStringBuilder implementations.
   */
  @FunctionalInterface
  interface StringBuilderGetter {
    StringBuilder get(int index);
  }

  private final ObjectGetter objectGetter;
  private final StringBuilderGetter stringBuilderGetter;
  private final Class<?> objectClass;

  /**
   * Instantiate an accessor for a {@link IntervalDayVector}.
   *
   * @param vector             an instance of a IntervalDayVector.
   * @param currentRowSupplier the supplier to track the rows.
   */
  public ArrowFlightJdbcIntervalVectorAccessor(IntervalDayVector vector, IntSupplier currentRowSupplier) {
    super(currentRowSupplier);
    this.objectGetter = vector::getObject;
    this.stringBuilderGetter = vector::getAsStringBuilder;
    this.objectClass = Duration.class;
  }

  /**
   * Instantiate an accessor for a {@link IntervalYearVector}.
   *
   * @param vector             an instance of a IntervalYearVector.
   * @param currentRowSupplier the supplier to track the rows.
   */
  public ArrowFlightJdbcIntervalVectorAccessor(IntervalYearVector vector, IntSupplier currentRowSupplier) {
    super(currentRowSupplier);
    this.objectGetter = vector::getObject;
    this.stringBuilderGetter = vector::getAsStringBuilder;
    this.objectClass = Period.class;
  }

  @Override
  public Object getObject() {
    Object object = objectGetter.get(getCurrentRow());
    this.wasNull = object == null;

    return object;
  }

  @Override
  public Class<?> getObjectClass() {
    return this.objectClass;
  }

  @Override
  public String getString() {
    StringBuilder stringBuilder = stringBuilderGetter.get(getCurrentRow());
    if (this.wasNull = (stringBuilder == null)) {
      return null;
    }

    return stringBuilder.toString();
  }
}
