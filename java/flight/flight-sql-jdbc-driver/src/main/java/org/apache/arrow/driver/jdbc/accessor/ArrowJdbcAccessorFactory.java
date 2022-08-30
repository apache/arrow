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

import org.apache.arrow.driver.jdbc.accessor.impl.ArrowJdbcNullVectorAccessor;
import org.apache.arrow.driver.jdbc.accessor.impl.binary.ArrowJdbcBinaryVectorAccessor;
import org.apache.arrow.driver.jdbc.accessor.impl.calendar.ArrowJdbcDateVectorAccessor;
import org.apache.arrow.driver.jdbc.accessor.impl.calendar.ArrowJdbcDurationVectorAccessor;
import org.apache.arrow.driver.jdbc.accessor.impl.calendar.ArrowJdbcIntervalVectorAccessor;
import org.apache.arrow.driver.jdbc.accessor.impl.calendar.ArrowJdbcTimeStampVectorAccessor;
import org.apache.arrow.driver.jdbc.accessor.impl.calendar.ArrowJdbcTimeVectorAccessor;
import org.apache.arrow.driver.jdbc.accessor.impl.complex.ArrowJdbcDenseUnionVectorAccessor;
import org.apache.arrow.driver.jdbc.accessor.impl.complex.ArrowJdbcFixedSizeListVectorAccessor;
import org.apache.arrow.driver.jdbc.accessor.impl.complex.ArrowJdbcLargeListVectorAccessor;
import org.apache.arrow.driver.jdbc.accessor.impl.complex.ArrowJdbcListVectorAccessor;
import org.apache.arrow.driver.jdbc.accessor.impl.complex.ArrowJdbcMapVectorAccessor;
import org.apache.arrow.driver.jdbc.accessor.impl.complex.ArrowJdbcStructVectorAccessor;
import org.apache.arrow.driver.jdbc.accessor.impl.complex.ArrowJdbcUnionVectorAccessor;
import org.apache.arrow.driver.jdbc.accessor.impl.numeric.ArrowJdbcBaseIntVectorAccessor;
import org.apache.arrow.driver.jdbc.accessor.impl.numeric.ArrowJdbcBitVectorAccessor;
import org.apache.arrow.driver.jdbc.accessor.impl.numeric.ArrowJdbcDecimalVectorAccessor;
import org.apache.arrow.driver.jdbc.accessor.impl.numeric.ArrowJdbcFloat4VectorAccessor;
import org.apache.arrow.driver.jdbc.accessor.impl.numeric.ArrowJdbcFloat8VectorAccessor;
import org.apache.arrow.driver.jdbc.accessor.impl.text.ArrowJdbcVarCharVectorAccessor;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.BitVector;
import org.apache.arrow.vector.DateDayVector;
import org.apache.arrow.vector.DateMilliVector;
import org.apache.arrow.vector.Decimal256Vector;
import org.apache.arrow.vector.DecimalVector;
import org.apache.arrow.vector.DurationVector;
import org.apache.arrow.vector.FixedSizeBinaryVector;
import org.apache.arrow.vector.Float4Vector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.IntervalDayVector;
import org.apache.arrow.vector.IntervalYearVector;
import org.apache.arrow.vector.LargeVarBinaryVector;
import org.apache.arrow.vector.LargeVarCharVector;
import org.apache.arrow.vector.NullVector;
import org.apache.arrow.vector.SmallIntVector;
import org.apache.arrow.vector.TimeMicroVector;
import org.apache.arrow.vector.TimeMilliVector;
import org.apache.arrow.vector.TimeNanoVector;
import org.apache.arrow.vector.TimeSecVector;
import org.apache.arrow.vector.TimeStampVector;
import org.apache.arrow.vector.TinyIntVector;
import org.apache.arrow.vector.UInt1Vector;
import org.apache.arrow.vector.UInt2Vector;
import org.apache.arrow.vector.UInt4Vector;
import org.apache.arrow.vector.UInt8Vector;
import org.apache.arrow.vector.ValueVector;
import org.apache.arrow.vector.VarBinaryVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.complex.DenseUnionVector;
import org.apache.arrow.vector.complex.FixedSizeListVector;
import org.apache.arrow.vector.complex.LargeListVector;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.complex.MapVector;
import org.apache.arrow.vector.complex.StructVector;
import org.apache.arrow.vector.complex.UnionVector;

/**
 * Factory to instantiate the accessors.
 */
public class ArrowJdbcAccessorFactory {

  /**
   * Create an accessor according to its type.
   *
   * @param vector        an instance of an arrow vector.
   * @param getCurrentRow a supplier to check which row is being accessed.
   * @return an instance of one of the accessors.
   */
  public static ArrowJdbcAccessor createAccessor(ValueVector vector,
                                                 IntSupplier getCurrentRow,
                                                 WasNullConsumer setCursorWasNull) {
    if (vector instanceof UInt1Vector) {
      return new ArrowJdbcBaseIntVectorAccessor((UInt1Vector) vector, getCurrentRow,
          setCursorWasNull);
    } else if (vector instanceof UInt2Vector) {
      return new ArrowJdbcBaseIntVectorAccessor((UInt2Vector) vector, getCurrentRow,
          setCursorWasNull);
    } else if (vector instanceof UInt4Vector) {
      return new ArrowJdbcBaseIntVectorAccessor((UInt4Vector) vector, getCurrentRow,
          setCursorWasNull);
    } else if (vector instanceof UInt8Vector) {
      return new ArrowJdbcBaseIntVectorAccessor((UInt8Vector) vector, getCurrentRow,
          setCursorWasNull);
    } else if (vector instanceof TinyIntVector) {
      return new ArrowJdbcBaseIntVectorAccessor((TinyIntVector) vector, getCurrentRow,
          setCursorWasNull);
    } else if (vector instanceof SmallIntVector) {
      return new ArrowJdbcBaseIntVectorAccessor((SmallIntVector) vector, getCurrentRow,
          setCursorWasNull);
    } else if (vector instanceof IntVector) {
      return new ArrowJdbcBaseIntVectorAccessor((IntVector) vector, getCurrentRow,
          setCursorWasNull);
    } else if (vector instanceof BigIntVector) {
      return new ArrowJdbcBaseIntVectorAccessor((BigIntVector) vector, getCurrentRow,
          setCursorWasNull);
    } else if (vector instanceof Float4Vector) {
      return new ArrowJdbcFloat4VectorAccessor((Float4Vector) vector, getCurrentRow,
          setCursorWasNull);
    } else if (vector instanceof Float8Vector) {
      return new ArrowJdbcFloat8VectorAccessor((Float8Vector) vector, getCurrentRow,
          setCursorWasNull);
    } else if (vector instanceof BitVector) {
      return new ArrowJdbcBitVectorAccessor((BitVector) vector, getCurrentRow,
          setCursorWasNull);
    } else if (vector instanceof DecimalVector) {
      return new ArrowJdbcDecimalVectorAccessor((DecimalVector) vector, getCurrentRow,
          setCursorWasNull);
    } else if (vector instanceof Decimal256Vector) {
      return new ArrowJdbcDecimalVectorAccessor((Decimal256Vector) vector, getCurrentRow,
          setCursorWasNull);
    } else if (vector instanceof VarBinaryVector) {
      return new ArrowJdbcBinaryVectorAccessor((VarBinaryVector) vector, getCurrentRow,
          setCursorWasNull);
    } else if (vector instanceof LargeVarBinaryVector) {
      return new ArrowJdbcBinaryVectorAccessor((LargeVarBinaryVector) vector, getCurrentRow,
          setCursorWasNull);
    } else if (vector instanceof FixedSizeBinaryVector) {
      return new ArrowJdbcBinaryVectorAccessor((FixedSizeBinaryVector) vector, getCurrentRow,
          setCursorWasNull);
    } else if (vector instanceof TimeStampVector) {
      return new ArrowJdbcTimeStampVectorAccessor((TimeStampVector) vector, getCurrentRow,
          setCursorWasNull);
    } else if (vector instanceof TimeNanoVector) {
      return new ArrowJdbcTimeVectorAccessor((TimeNanoVector) vector, getCurrentRow,
          setCursorWasNull);
    } else if (vector instanceof TimeMicroVector) {
      return new ArrowJdbcTimeVectorAccessor((TimeMicroVector) vector, getCurrentRow,
          setCursorWasNull);
    } else if (vector instanceof TimeMilliVector) {
      return new ArrowJdbcTimeVectorAccessor((TimeMilliVector) vector, getCurrentRow,
          setCursorWasNull);
    } else if (vector instanceof TimeSecVector) {
      return new ArrowJdbcTimeVectorAccessor((TimeSecVector) vector, getCurrentRow,
          setCursorWasNull);
    } else if (vector instanceof DateDayVector) {
      return new ArrowJdbcDateVectorAccessor(((DateDayVector) vector), getCurrentRow,
          setCursorWasNull);
    } else if (vector instanceof DateMilliVector) {
      return new ArrowJdbcDateVectorAccessor(((DateMilliVector) vector), getCurrentRow,
          setCursorWasNull);
    } else if (vector instanceof VarCharVector) {
      return new ArrowJdbcVarCharVectorAccessor((VarCharVector) vector, getCurrentRow,
          setCursorWasNull);
    } else if (vector instanceof LargeVarCharVector) {
      return new ArrowJdbcVarCharVectorAccessor((LargeVarCharVector) vector, getCurrentRow,
          setCursorWasNull);
    } else if (vector instanceof DurationVector) {
      return new ArrowJdbcDurationVectorAccessor((DurationVector) vector, getCurrentRow,
          setCursorWasNull);
    } else if (vector instanceof IntervalDayVector) {
      return new ArrowJdbcIntervalVectorAccessor(((IntervalDayVector) vector), getCurrentRow,
          setCursorWasNull);
    } else if (vector instanceof IntervalYearVector) {
      return new ArrowJdbcIntervalVectorAccessor(((IntervalYearVector) vector), getCurrentRow,
          setCursorWasNull);
    } else if (vector instanceof StructVector) {
      return new ArrowJdbcStructVectorAccessor((StructVector) vector, getCurrentRow,
          setCursorWasNull);
    } else if (vector instanceof MapVector) {
      return new ArrowJdbcMapVectorAccessor((MapVector) vector, getCurrentRow,
          setCursorWasNull);
    } else if (vector instanceof ListVector) {
      return new ArrowJdbcListVectorAccessor((ListVector) vector, getCurrentRow,
          setCursorWasNull);
    } else if (vector instanceof LargeListVector) {
      return new ArrowJdbcLargeListVectorAccessor((LargeListVector) vector, getCurrentRow,
          setCursorWasNull);
    } else if (vector instanceof FixedSizeListVector) {
      return new ArrowJdbcFixedSizeListVectorAccessor((FixedSizeListVector) vector,
          getCurrentRow, setCursorWasNull);
    } else if (vector instanceof UnionVector) {
      return new ArrowJdbcUnionVectorAccessor((UnionVector) vector, getCurrentRow,
          setCursorWasNull);
    } else if (vector instanceof DenseUnionVector) {
      return new ArrowJdbcDenseUnionVectorAccessor((DenseUnionVector) vector, getCurrentRow,
          setCursorWasNull);
    } else if (vector instanceof NullVector || vector == null) {
      return new ArrowJdbcNullVectorAccessor(setCursorWasNull);
    }

    throw new UnsupportedOperationException("Unsupported vector type: " + vector.getClass().getName());
  }

  /**
   * Functional interface used to propagate that the value accessed was null or not.
   */
  @FunctionalInterface
  public interface WasNullConsumer {
    void setWasNull(boolean wasNull);
  }
}
