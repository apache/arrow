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

package org.apache.arrow.driver.jdbc.accessor.impl.numeric;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.function.IntSupplier;

import org.apache.arrow.driver.jdbc.accessor.ArrowFlightJdbcAccessor;
import org.apache.arrow.driver.jdbc.accessor.ArrowFlightJdbcAccessorFactory;
import org.apache.arrow.vector.Decimal256Vector;
import org.apache.arrow.vector.DecimalVector;

/**
 * Accessor for {@link DecimalVector} and {@link Decimal256Vector}.
 */
public class ArrowFlightJdbcDecimalVectorAccessor extends ArrowFlightJdbcAccessor {

  private final Getter getter;

  /**
   * Functional interface used to unify Decimal*Vector#getObject implementations.
   */
  @FunctionalInterface
  interface Getter {
    BigDecimal getObject(int index);
  }

  public ArrowFlightJdbcDecimalVectorAccessor(DecimalVector vector, IntSupplier currentRowSupplier,
                                              ArrowFlightJdbcAccessorFactory.WasNullConsumer setCursorWasNull) {
    super(currentRowSupplier, setCursorWasNull);
    this.getter = vector::getObject;
  }

  public ArrowFlightJdbcDecimalVectorAccessor(Decimal256Vector vector,
                                              IntSupplier currentRowSupplier,
                                              ArrowFlightJdbcAccessorFactory.WasNullConsumer setCursorWasNull) {
    super(currentRowSupplier, setCursorWasNull);
    this.getter = vector::getObject;
  }

  @Override
  public Class<?> getObjectClass() {
    return BigDecimal.class;
  }

  @Override
  public BigDecimal getBigDecimal() {
    final BigDecimal value = getter.getObject(getCurrentRow());
    this.wasNull = value == null;
    this.wasNullConsumer.setWasNull(this.wasNull);
    return value;
  }

  @Override
  public String getString() {
    final BigDecimal value = this.getBigDecimal();
    return this.wasNull ? null : value.toString();
  }

  @Override
  public boolean getBoolean() {
    final BigDecimal value = this.getBigDecimal();

    return !this.wasNull && !value.equals(BigDecimal.ZERO);
  }

  @Override
  public byte getByte() {
    final BigDecimal value = this.getBigDecimal();

    return this.wasNull ? 0 : value.byteValue();
  }

  @Override
  public short getShort() {
    final BigDecimal value = this.getBigDecimal();

    return this.wasNull ? 0 : value.shortValue();
  }

  @Override
  public int getInt() {
    final BigDecimal value = this.getBigDecimal();

    return this.wasNull ? 0 : value.intValue();
  }

  @Override
  public long getLong() {
    final BigDecimal value = this.getBigDecimal();

    return this.wasNull ? 0 : value.longValue();
  }

  @Override
  public float getFloat() {
    final BigDecimal value = this.getBigDecimal();

    return this.wasNull ? 0 : value.floatValue();
  }

  @Override
  public double getDouble() {
    final BigDecimal value = this.getBigDecimal();

    return this.wasNull ? 0 : value.doubleValue();
  }

  @Override
  public BigDecimal getBigDecimal(int scale) {
    final BigDecimal value = this.getBigDecimal();

    return this.wasNull ? null : value.setScale(scale, RoundingMode.HALF_UP);
  }

  @Override
  public Object getObject() {
    return this.getBigDecimal();
  }
}
