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
import org.apache.arrow.vector.Decimal256Vector;

/**
 * Accessor for the Decimal256Vector.
 */
public class ArrowFlightJdbcDecimal256VectorAccessor extends ArrowFlightJdbcAccessor {

  private Decimal256Vector vector;

  public ArrowFlightJdbcDecimal256VectorAccessor(Decimal256Vector vector, IntSupplier currentRowSupplier) {
    super(currentRowSupplier);
    this.vector = vector;
  }

  @Override
  public Class<?> getObjectClass() {
    return BigDecimal.class;
  }

  @Override
  public BigDecimal getBigDecimal() {
    final BigDecimal value = vector.getObject(getCurrentRow());
    this.wasNull = value == null;
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
  public BigDecimal getBigDecimal(int i) {
    final BigDecimal value = this.getBigDecimal();
    return this.wasNull ? null : value.setScale(i, RoundingMode.UNNECESSARY);
  }

  @Override
  public byte[] getBytes() {
    final BigDecimal value = this.getBigDecimal();
    return this.wasNull ? null : value.unscaledValue().toByteArray();
  }

  @Override
  public Object getObject() {
    return this.getBigDecimal();
  }
}
