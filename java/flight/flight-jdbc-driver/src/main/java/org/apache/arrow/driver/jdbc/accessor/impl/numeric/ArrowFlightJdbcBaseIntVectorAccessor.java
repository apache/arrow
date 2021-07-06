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
import java.nio.ByteBuffer;
import java.util.function.IntSupplier;

import org.apache.arrow.driver.jdbc.accessor.ArrowFlightJdbcAccessor;
import org.apache.arrow.vector.BaseIntVector;

/**
 * Accessor for the arrow types: TinyIntVector, SmallIntVector, IntVector, BigIntVector,
 * UInt1Vector, UInt2Vector, UInt4Vector and UInt8Vector.
 */
public class ArrowFlightJdbcBaseIntVectorAccessor extends ArrowFlightJdbcAccessor {

  private final BaseIntVector vector;
  private final IntSupplier currentRowSupplier;

  public ArrowFlightJdbcBaseIntVectorAccessor(BaseIntVector vector, IntSupplier currentRowSupplier) {
    this.vector = vector;
    this.currentRowSupplier = currentRowSupplier;
  }

  @Override
  public long getLong() {
    return vector.getValueAsLong(currentRowSupplier.getAsInt());
  }

  @Override
  public String getString() {
    return Long.toString(getLong());
  }

  @Override
  public byte getByte() {
    return (byte) getLong();
  }

  @Override
  public short getShort() {
    return (short) getLong();
  }

  @Override
  public int getInt() {
    return (int) getLong();
  }

  @Override
  public float getFloat() {
    return (float) getLong();
  }

  @Override
  public double getDouble() {
    return (double) getLong();
  }

  @Override
  public byte[] getBytes() {
    return ByteBuffer.allocate(Long.BYTES).putLong(getLong()).array();
  }

  @Override
  public BigDecimal getBigDecimal() {
    return BigDecimal.valueOf(getLong());
  }
}
