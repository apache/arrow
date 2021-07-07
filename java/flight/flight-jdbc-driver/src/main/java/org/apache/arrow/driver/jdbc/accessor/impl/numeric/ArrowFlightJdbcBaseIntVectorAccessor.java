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
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.SmallIntVector;
import org.apache.arrow.vector.TinyIntVector;
import org.apache.arrow.vector.UInt1Vector;
import org.apache.arrow.vector.UInt2Vector;
import org.apache.arrow.vector.UInt4Vector;
import org.apache.arrow.vector.UInt8Vector;

/**
 * Accessor for the arrow types: TinyIntVector, SmallIntVector, IntVector, BigIntVector,
 * UInt1Vector, UInt2Vector, UInt4Vector and UInt8Vector.
 */
public class ArrowFlightJdbcBaseIntVectorAccessor extends ArrowFlightJdbcAccessor {

  private final BaseIntVector vector;
  private final IntSupplier currentRowSupplier;
  private boolean isUnassigned;
  private int bytesToAllocate;

  public ArrowFlightJdbcBaseIntVectorAccessor(UInt1Vector vector,
                                       IntSupplier currentRowSupplier) {
    this(vector, currentRowSupplier, true, 1);
  }

  public ArrowFlightJdbcBaseIntVectorAccessor(UInt2Vector vector,
                                              IntSupplier currentRowSupplier) {
    this(vector, currentRowSupplier, true, 2);
  }

  public ArrowFlightJdbcBaseIntVectorAccessor(UInt4Vector vector,
                                              IntSupplier currentRowSupplier) {
    this(vector, currentRowSupplier, true, 4);
  }

  public ArrowFlightJdbcBaseIntVectorAccessor(UInt8Vector vector,
                                              IntSupplier currentRowSupplier) {
    this(vector, currentRowSupplier, true, 8);
  }

  public ArrowFlightJdbcBaseIntVectorAccessor(TinyIntVector vector,
                                              IntSupplier currentRowSupplier) {
    this(vector, currentRowSupplier, true, TinyIntVector.TYPE_WIDTH);
  }

  public ArrowFlightJdbcBaseIntVectorAccessor(SmallIntVector vector,
                                              IntSupplier currentRowSupplier) {
    this(vector, currentRowSupplier, true, SmallIntVector.TYPE_WIDTH);
  }

  public ArrowFlightJdbcBaseIntVectorAccessor(IntVector vector,
                                              IntSupplier currentRowSupplier) {
    this(vector, currentRowSupplier, true, IntVector.TYPE_WIDTH);
  }

  public ArrowFlightJdbcBaseIntVectorAccessor(BigIntVector vector,
                                              IntSupplier currentRowSupplier) {
    this(vector, currentRowSupplier, true, BigIntVector.TYPE_WIDTH);
  }

  ArrowFlightJdbcBaseIntVectorAccessor(BaseIntVector vector,
                                       IntSupplier currentRowSupplier,
                                       boolean isUnassigned,
                                       int bytesToAllocate) {
    this.vector = vector;
    this.currentRowSupplier = currentRowSupplier;
    this.isUnassigned = isUnassigned;
    this.bytesToAllocate = bytesToAllocate;
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
