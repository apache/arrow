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
import java.nio.ByteBuffer;
import java.util.function.IntSupplier;

import org.apache.arrow.driver.jdbc.accessor.ArrowFlightJdbcAccessor;
import org.apache.arrow.vector.Float4Vector;
import org.apache.arrow.vector.holders.NullableFloat4Holder;

/**
 * Accessor for the Float4Vector.
 */
public class ArrowFlightJdbcFloat4VectorAccessor extends ArrowFlightJdbcAccessor {

  private final Float4Vector vector;
  private final NullableFloat4Holder holder;

  /**
   * Instantiate a accessor for the {@link Float4Vector}.
   *
   * @param vector an instance of a Float4Vector.
   * @param currentRowSupplier the supplier to track the lines.
   */
  public ArrowFlightJdbcFloat4VectorAccessor(Float4Vector vector,
                                             IntSupplier currentRowSupplier) {
    super(currentRowSupplier);
    this.holder = new NullableFloat4Holder();
    this.vector = vector;
  }

  @Override
  public Class<?> getObjectClass() {
    return Float.class;
  }

  @Override
  public String getString() {
    final float value = this.getFloat();

    return this.wasNull ? null : Float.toString(value);
  }

  @Override
  public boolean getBoolean() {
    return this.getFloat() != 0.0;
  }

  @Override
  public byte getByte() {
    return (byte) this.getFloat();
  }

  @Override
  public short getShort() {
    return (short) this.getFloat();
  }

  @Override
  public int getInt() {
    return (int) this.getFloat();
  }

  @Override
  public long getLong() {
    return (long) this.getFloat();
  }

  @Override
  public float getFloat() {
    vector.get(getCurrentRow(), holder);

    if (this.wasNull = holder.isSet == 0) {
      return 0;
    }

    return holder.value;
  }

  @Override
  public double getDouble() {
    return this.getFloat();
  }

  @Override
  public BigDecimal getBigDecimal() {
    final float value = this.getFloat();

    final boolean infinite = Float.isInfinite(value);
    if (infinite) {
      throw new UnsupportedOperationException();
    }

    return this.wasNull ? null : BigDecimal.valueOf(value);
  }

  @Override
  public byte[] getBytes() {
    final float value = this.getFloat();
    return this.wasNull ? null : ByteBuffer.allocate(Float4Vector.TYPE_WIDTH).putFloat(value).array();
  }

  @Override
  public BigDecimal getBigDecimal(int scale) {
    final BigDecimal value = BigDecimal.valueOf(this.getFloat()).setScale(scale, RoundingMode.HALF_UP);
    return this.wasNull ? null : value;
  }

  @Override
  public Object getObject() {
    final float value = this.getFloat();
    return this.wasNull ? null : value;
  }
}
