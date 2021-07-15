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
import java.util.function.IntSupplier;

import org.apache.arrow.driver.jdbc.accessor.ArrowFlightJdbcAccessor;
import org.apache.arrow.vector.IntervalDayVector;

/**
 * Accessor for the Arrow type {@link IntervalDayVector}.
 */
public class ArrowFlightJdbcIntervalDayVectorAccessor extends ArrowFlightJdbcAccessor {

  private final IntervalDayVector vector;

  public ArrowFlightJdbcIntervalDayVectorAccessor(IntervalDayVector vector, IntSupplier currentRowSupplier) {
    super(currentRowSupplier);
    this.vector = vector;
  }

  @Override
  public Object getObject() {
    Duration duration = vector.getObject(getCurrentRow());
    this.wasNull = duration == null;

    return duration;
  }

  @Override
  public Class<?> getObjectClass() {
    return Duration.class;
  }

  @Override
  public String getString() {
    StringBuilder stringBuilder = vector.getAsStringBuilder(getCurrentRow());
    if (this.wasNull = (stringBuilder == null)) {
      return null;
    }

    return stringBuilder.toString();
  }
}
