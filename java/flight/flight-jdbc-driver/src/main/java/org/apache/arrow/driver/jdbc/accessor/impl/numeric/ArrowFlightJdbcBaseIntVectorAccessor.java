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

import static org.apache.arrow.driver.jdbc.accessor.impl.numeric.ArrowFlightJdbcNumericGetter.Getter;
import static org.apache.arrow.driver.jdbc.accessor.impl.numeric.ArrowFlightJdbcNumericGetter.createGetter;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.function.IntSupplier;

import org.apache.arrow.driver.jdbc.accessor.ArrowFlightJdbcAccessor;
import org.apache.arrow.driver.jdbc.accessor.ArrowFlightJdbcAccessorFactory;
import org.apache.arrow.driver.jdbc.accessor.impl.numeric.ArrowFlightJdbcNumericGetter.NumericHolder;
import org.apache.arrow.vector.BaseIntVector;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.SmallIntVector;
import org.apache.arrow.vector.TinyIntVector;
import org.apache.arrow.vector.UInt1Vector;
import org.apache.arrow.vector.UInt2Vector;
import org.apache.arrow.vector.UInt4Vector;
import org.apache.arrow.vector.UInt8Vector;
import org.apache.arrow.vector.types.Types.MinorType;

/**
 * Accessor for the arrow types: TinyIntVector, SmallIntVector, IntVector, BigIntVector,
 * UInt1Vector, UInt2Vector, UInt4Vector and UInt8Vector.
 */
public class ArrowFlightJdbcBaseIntVectorAccessor extends ArrowFlightJdbcAccessor {

  private final MinorType type;
  private final boolean isUnsigned;
  private final int bytesToAllocate;
  private final Getter getter;
  private final NumericHolder holder;

  public ArrowFlightJdbcBaseIntVectorAccessor(UInt1Vector vector, IntSupplier currentRowSupplier,
                                              ArrowFlightJdbcAccessorFactory.WasNullConsumer setCursorWasNull) {
    this(vector, currentRowSupplier, true, UInt1Vector.TYPE_WIDTH, setCursorWasNull);
  }

  public ArrowFlightJdbcBaseIntVectorAccessor(UInt2Vector vector, IntSupplier currentRowSupplier,
                                              ArrowFlightJdbcAccessorFactory.WasNullConsumer setCursorWasNull) {
    this(vector, currentRowSupplier, true, UInt2Vector.TYPE_WIDTH, setCursorWasNull);
  }

  public ArrowFlightJdbcBaseIntVectorAccessor(UInt4Vector vector, IntSupplier currentRowSupplier,
                                              ArrowFlightJdbcAccessorFactory.WasNullConsumer setCursorWasNull) {
    this(vector, currentRowSupplier, true, UInt4Vector.TYPE_WIDTH, setCursorWasNull);
  }

  public ArrowFlightJdbcBaseIntVectorAccessor(UInt8Vector vector, IntSupplier currentRowSupplier,
                                              ArrowFlightJdbcAccessorFactory.WasNullConsumer setCursorWasNull) {
    this(vector, currentRowSupplier, true, UInt8Vector.TYPE_WIDTH, setCursorWasNull);
  }

  public ArrowFlightJdbcBaseIntVectorAccessor(TinyIntVector vector, IntSupplier currentRowSupplier,
                                              ArrowFlightJdbcAccessorFactory.WasNullConsumer setCursorWasNull) {
    this(vector, currentRowSupplier, false, TinyIntVector.TYPE_WIDTH, setCursorWasNull);
  }

  public ArrowFlightJdbcBaseIntVectorAccessor(SmallIntVector vector, IntSupplier currentRowSupplier,
                                              ArrowFlightJdbcAccessorFactory.WasNullConsumer setCursorWasNull) {
    this(vector, currentRowSupplier, false, SmallIntVector.TYPE_WIDTH, setCursorWasNull);
  }

  public ArrowFlightJdbcBaseIntVectorAccessor(IntVector vector, IntSupplier currentRowSupplier,
                                              ArrowFlightJdbcAccessorFactory.WasNullConsumer setCursorWasNull) {
    this(vector, currentRowSupplier, false, IntVector.TYPE_WIDTH, setCursorWasNull);
  }

  public ArrowFlightJdbcBaseIntVectorAccessor(BigIntVector vector, IntSupplier currentRowSupplier,
                                              ArrowFlightJdbcAccessorFactory.WasNullConsumer setCursorWasNull) {
    this(vector, currentRowSupplier, false, BigIntVector.TYPE_WIDTH, setCursorWasNull);
  }

  private ArrowFlightJdbcBaseIntVectorAccessor(BaseIntVector vector, IntSupplier currentRowSupplier,
                                               boolean isUnsigned, int bytesToAllocate,
                                               ArrowFlightJdbcAccessorFactory.WasNullConsumer setCursorWasNull) {
    super(currentRowSupplier, setCursorWasNull);
    this.type = vector.getMinorType();
    this.holder = new NumericHolder();
    this.getter = createGetter(vector);
    this.isUnsigned = isUnsigned;
    this.bytesToAllocate = bytesToAllocate;
  }

  @Override
  public long getLong() {
    getter.get(getCurrentRow(), holder);

    this.wasNull = holder.isSet == 0;
    this.wasNullConsumer.setWasNull(this.wasNull);
    if (this.wasNull) {
      return 0;
    }

    return holder.value;
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
      return isUnsigned ? Long.toUnsignedString(number) : Long.toString(number);
    }
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
  public BigDecimal getBigDecimal() {
    final BigDecimal value = BigDecimal.valueOf(getLong());
    return this.wasNull ? null : value;
  }

  @Override
  public BigDecimal getBigDecimal(int scale) {
    final BigDecimal value =
        BigDecimal.valueOf(this.getDouble()).setScale(scale, RoundingMode.HALF_UP);
    return this.wasNull ? null : value;
  }

  @Override
  public Number getObject() {
    final Number number;
    switch (type) {
      case TINYINT:
      case UINT1:
        number = getByte();
        break;
      case SMALLINT:
      case UINT2:
        number = getShort();
        break;
      case INT:
      case UINT4:
        number = getInt();
        break;
      case BIGINT:
      case UINT8:
        number = getLong();
        break;
      default:
        throw new IllegalStateException("No valid MinorType was provided.");
    }
    return wasNull ? null : number;
  }

  @Override
  public boolean getBoolean() {
    final long value = getLong();

    return value != 0;
  }
}
