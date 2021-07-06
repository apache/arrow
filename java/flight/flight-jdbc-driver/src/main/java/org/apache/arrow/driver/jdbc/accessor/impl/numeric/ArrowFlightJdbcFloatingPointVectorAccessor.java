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
import java.sql.SQLException;
import java.util.function.IntSupplier;

import org.apache.arrow.driver.jdbc.accessor.ArrowFlightJdbcAccessor;
import org.apache.arrow.vector.FloatingPointVector;

/**
 * Accessor for the arrow types: Float4Vector and Float8Vector.
 */
public class ArrowFlightJdbcFloatingPointVectorAccessor extends ArrowFlightJdbcAccessor {

  private final FloatingPointVector vector;
  private final IntSupplier currentRowSupplier;

  public ArrowFlightJdbcFloatingPointVectorAccessor(FloatingPointVector vector, IntSupplier currentRowSupplier) {
    this.vector = vector;
    this.currentRowSupplier = currentRowSupplier;
  }

  @Override
  public double getDouble() {
    return vector.getValueAsDouble(currentRowSupplier.getAsInt());
  }

  @Override
  public Object getObject() throws SQLException {
    return this.getDouble();
  }

  @Override
  public String getString() throws SQLException {
    return Double.toString(getDouble());
  }

  @Override
  public boolean getBoolean() throws SQLException {
    return this.getDouble() != 0.0;
  }

  @Override
  public byte getByte() throws SQLException {
    return (byte) this.getDouble();
  }

  @Override
  public short getShort() throws SQLException {
    return (short) this.getDouble();
  }

  @Override
  public int getInt() throws SQLException {
    return (int) this.getDouble();
  }

  @Override
  public long getLong() throws SQLException {
    return (long) this.getDouble();
  }

  @Override
  public float getFloat() throws SQLException {
    return (float) this.getDouble();
  }

  @Override
  public BigDecimal getBigDecimal() throws SQLException {
    return BigDecimal.valueOf(this.getDouble());
  }

  @Override
  public BigDecimal getBigDecimal(int scale) throws SQLException {
    if (scale != 0) {
      throw new UnsupportedOperationException("Can not use getBigDecimal(int scale) on a decimal accessor.");
    }
    return BigDecimal.valueOf(this.getDouble());
  }
}
