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

package org.apache.arrow.driver.jdbc.accessor;

import java.util.function.IntSupplier;

import org.apache.arrow.driver.jdbc.accessor.impl.binary.ArrowFlightJdbcBinaryVectorAccessor;
import org.apache.arrow.driver.jdbc.accessor.impl.calendar.ArrowFlightJdbcDurationVectorAccessor;
import org.apache.arrow.driver.jdbc.accessor.impl.numeric.ArrowFlightJdbcBaseIntVectorAccessor;
import org.apache.arrow.driver.jdbc.accessor.impl.numeric.ArrowFlightJdbcBitVectorAccessor;
import org.apache.arrow.driver.jdbc.accessor.impl.numeric.ArrowFlightJdbcFloat4VectorAccessor;
import org.apache.arrow.driver.jdbc.accessor.impl.numeric.ArrowFlightJdbcFloat8VectorAccessor;
import org.apache.arrow.driver.jdbc.accessor.impl.text.ArrowFlightJdbcVarCharVectorAccessor;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.DurationVector;
import org.apache.arrow.vector.FixedSizeBinaryVector;
import org.apache.arrow.vector.BitVector;
import org.apache.arrow.vector.Float4Vector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.LargeVarBinaryVector;
import org.apache.arrow.vector.LargeVarCharVector;
import org.apache.arrow.vector.SmallIntVector;
import org.apache.arrow.vector.TinyIntVector;
import org.apache.arrow.vector.UInt1Vector;
import org.apache.arrow.vector.UInt2Vector;
import org.apache.arrow.vector.UInt4Vector;
import org.apache.arrow.vector.UInt8Vector;
import org.apache.arrow.vector.ValueVector;
import org.apache.arrow.vector.VarBinaryVector;
import org.apache.arrow.vector.VarCharVector;

/**
 * Factory to instantiate the accessors.
 */
public class ArrowFlightJdbcAccessorFactory {

  /**
   * Create an accessor according to the its type.
   *
   * @param vector        an instance of an arrow vector.
   * @param getCurrentRow a supplier to check which row is being accessed.
   * @return an instance of one of the accessors.
   */
  public static ArrowFlightJdbcAccessor createAccessor(ValueVector vector, IntSupplier getCurrentRow) {
    if (vector instanceof UInt1Vector) {
      return new ArrowFlightJdbcBaseIntVectorAccessor((UInt1Vector) vector, getCurrentRow);
    } else if (vector instanceof UInt2Vector) {
      return new ArrowFlightJdbcBaseIntVectorAccessor((UInt2Vector) vector, getCurrentRow);
    } else if (vector instanceof UInt4Vector) {
      return new ArrowFlightJdbcBaseIntVectorAccessor((UInt4Vector) vector, getCurrentRow);
    } else if (vector instanceof UInt8Vector) {
      return new ArrowFlightJdbcBaseIntVectorAccessor((UInt8Vector) vector, getCurrentRow);
    } else if (vector instanceof TinyIntVector) {
      return new ArrowFlightJdbcBaseIntVectorAccessor((TinyIntVector) vector, getCurrentRow);
    } else if (vector instanceof SmallIntVector) {
      return new ArrowFlightJdbcBaseIntVectorAccessor((SmallIntVector) vector, getCurrentRow);
    } else if (vector instanceof IntVector) {
      return new ArrowFlightJdbcBaseIntVectorAccessor((IntVector) vector, getCurrentRow);
    } else if (vector instanceof BigIntVector) {
      return new ArrowFlightJdbcBaseIntVectorAccessor((BigIntVector) vector, getCurrentRow);
    } else if (vector instanceof Float4Vector) {
      return new ArrowFlightJdbcFloat4VectorAccessor((Float4Vector) vector, getCurrentRow);
    } else if (vector instanceof Float8Vector) {
      return new ArrowFlightJdbcFloat8VectorAccessor((Float8Vector) vector, getCurrentRow);
    } else if (vector instanceof BitVector) {
      return new ArrowFlightJdbcBitVectorAccessor((BitVector) vector, getCurrentRow);
    } else if (vector instanceof VarBinaryVector) {
      return new ArrowFlightJdbcBinaryVectorAccessor((VarBinaryVector) vector, getCurrentRow);
    } else if (vector instanceof LargeVarBinaryVector) {
      return new ArrowFlightJdbcBinaryVectorAccessor((LargeVarBinaryVector) vector, getCurrentRow);
    } else if (vector instanceof FixedSizeBinaryVector) {
      return new ArrowFlightJdbcBinaryVectorAccessor((FixedSizeBinaryVector) vector, getCurrentRow);
    } else if (vector instanceof VarCharVector) {
      return new ArrowFlightJdbcVarCharVectorAccessor(((VarCharVector) vector), getCurrentRow);
    } else if (vector instanceof LargeVarCharVector) {
      return new ArrowFlightJdbcVarCharVectorAccessor(((LargeVarCharVector) vector), getCurrentRow);
    } else if (vector instanceof DurationVector) {
      return new ArrowFlightJdbcDurationVectorAccessor(((DurationVector) vector), getCurrentRow);
    }

    throw new UnsupportedOperationException();
  }
}
