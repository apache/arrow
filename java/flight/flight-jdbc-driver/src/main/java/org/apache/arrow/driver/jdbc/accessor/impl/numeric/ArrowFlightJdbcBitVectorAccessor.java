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
import org.apache.arrow.vector.BitVector;
import org.apache.arrow.vector.holders.NullableBitHolder;

/**
 * Accessor for the arrow {@link BitVector}.
 */
public class ArrowFlightJdbcBitVectorAccessor extends ArrowFlightJdbcAccessor {

  private final BitVector vector;
  private final NullableBitHolder holder;
  private final int BYTES_T0_ALLOCATE = 1 ;

  /**
   * Constructor for the BitVectorAccessor.
   *
   * @param vector an instance of a {@link BitVector}.
   * @param currentRowSupplier a supplier to check which row is being accessed.
   */
  public ArrowFlightJdbcBitVectorAccessor(BitVector vector, IntSupplier currentRowSupplier) {
    super(currentRowSupplier);
    this.vector = vector;
    this.holder = new NullableBitHolder();
  }

  @Override
  public Class<?> getObjectClass() {
    return Long.class;
  }

  @Override
  public String getString() {
    final long number = getLong();

    if (this.wasNull) {
      return null;
    } else {
      return number == 0 ? "false" : "true";
    }
  }

  @Override
  public boolean getBoolean() {
    return this.getLong() != 0;
  }

  @Override
  public byte getByte() {
    return (byte) this.getLong();
  }

  @Override
  public short getShort() {
    return (short) this.getLong();
  }

  @Override
  public int getInt() {
    return (int) this.getLong();
  }

  @Override
  public long getLong() {
    vector.get(getCurrentRow(), holder);
    if (this.wasNull = holder.isSet == 0) {
      return 0;
    }

    return holder.value;
  }

  @Override
  public float getFloat() {
    return this.getLong();
  }

  @Override
  public double getDouble() {
    return this.getLong();
  }

  @Override
  public BigDecimal getBigDecimal() {
    final long value = this.getLong();

    return this.wasNull ? null : BigDecimal.valueOf(value);
  }

  @Override
  public byte[] getBytes() {
    final byte value = (byte) getLong();
    return this.wasNull ? null : ByteBuffer.allocate(BYTES_T0_ALLOCATE).put(value).array();
  }

  @Override
  public Object getObject() {
    final long value = this.getLong();
    return this.wasNull ? null : value;
  }
}
