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

import org.apache.arrow.driver.jdbc.accessor.impl.ArrowFlightJdbcNullVectorAccessor;
import org.apache.arrow.driver.jdbc.accessor.impl.binary.ArrowFlightJdbcBinaryVectorAccessor;
import org.apache.arrow.driver.jdbc.accessor.impl.calendar.ArrowFlightJdbcDateVectorAccessor;
import org.apache.arrow.driver.jdbc.accessor.impl.calendar.ArrowFlightJdbcDurationVectorAccessor;
import org.apache.arrow.driver.jdbc.accessor.impl.calendar.ArrowFlightJdbcIntervalVectorAccessor;
import org.apache.arrow.driver.jdbc.accessor.impl.calendar.ArrowFlightJdbcTimeStampVectorAccessor;
import org.apache.arrow.driver.jdbc.accessor.impl.calendar.ArrowFlightJdbcTimeVectorAccessor;
import org.apache.arrow.driver.jdbc.accessor.impl.complex.ArrowFlightJdbcDenseUnionVectorAccessor;
import org.apache.arrow.driver.jdbc.accessor.impl.complex.ArrowFlightJdbcFixedSizeListVectorAccessor;
import org.apache.arrow.driver.jdbc.accessor.impl.complex.ArrowFlightJdbcLargeListVectorAccessor;
import org.apache.arrow.driver.jdbc.accessor.impl.complex.ArrowFlightJdbcListVectorAccessor;
import org.apache.arrow.driver.jdbc.accessor.impl.complex.ArrowFlightJdbcMapVectorAccessor;
import org.apache.arrow.driver.jdbc.accessor.impl.complex.ArrowFlightJdbcStructVectorAccessor;
import org.apache.arrow.driver.jdbc.accessor.impl.complex.ArrowFlightJdbcUnionVectorAccessor;
import org.apache.arrow.driver.jdbc.accessor.impl.numeric.ArrowFlightJdbcBaseIntVectorAccessor;
import org.apache.arrow.driver.jdbc.accessor.impl.numeric.ArrowFlightJdbcBitVectorAccessor;
import org.apache.arrow.driver.jdbc.accessor.impl.numeric.ArrowFlightJdbcDecimalVectorAccessor;
import org.apache.arrow.driver.jdbc.accessor.impl.numeric.ArrowFlightJdbcFloat4VectorAccessor;
import org.apache.arrow.driver.jdbc.accessor.impl.numeric.ArrowFlightJdbcFloat8VectorAccessor;
import org.apache.arrow.driver.jdbc.accessor.impl.text.ArrowFlightJdbcVarCharVectorAccessor;
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
public class ArrowFlightJdbcAccessorFactory {

  /**
   * Create an accessor according to its type.
   *
   * @param vector        an instance of an arrow vector.
   * @param getCurrentRow a supplier to check which row is being accessed.
   * @return an instance of one of the accessors.
   */
  public static ArrowFlightJdbcAccessor createAccessor(ValueVector vector,
                                                       IntSupplier getCurrentRow,
                                                       WasNullConsumer setCursorWasNull) {
    if (vector instanceof UInt1Vector) {
      return new ArrowFlightJdbcBaseIntVectorAccessor((UInt1Vector) vector, getCurrentRow,
          setCursorWasNull);
    } else if (vector instanceof UInt2Vector) {
      return new ArrowFlightJdbcBaseIntVectorAccessor((UInt2Vector) vector, getCurrentRow,
          setCursorWasNull);
    } else if (vector instanceof UInt4Vector) {
      return new ArrowFlightJdbcBaseIntVectorAccessor((UInt4Vector) vector, getCurrentRow,
          setCursorWasNull);
    } else if (vector instanceof UInt8Vector) {
      return new ArrowFlightJdbcBaseIntVectorAccessor((UInt8Vector) vector, getCurrentRow,
          setCursorWasNull);
    } else if (vector instanceof TinyIntVector) {
      return new ArrowFlightJdbcBaseIntVectorAccessor((TinyIntVector) vector, getCurrentRow,
          setCursorWasNull);
    } else if (vector instanceof SmallIntVector) {
      return new ArrowFlightJdbcBaseIntVectorAccessor((SmallIntVector) vector, getCurrentRow,
          setCursorWasNull);
    } else if (vector instanceof IntVector) {
      return new ArrowFlightJdbcBaseIntVectorAccessor((IntVector) vector, getCurrentRow,
          setCursorWasNull);
    } else if (vector instanceof BigIntVector) {
      return new ArrowFlightJdbcBaseIntVectorAccessor((BigIntVector) vector, getCurrentRow,
          setCursorWasNull);
    } else if (vector instanceof Float4Vector) {
      return new ArrowFlightJdbcFloat4VectorAccessor((Float4Vector) vector, getCurrentRow,
          setCursorWasNull);
    } else if (vector instanceof Float8Vector) {
      return new ArrowFlightJdbcFloat8VectorAccessor((Float8Vector) vector, getCurrentRow,
          setCursorWasNull);
    } else if (vector instanceof BitVector) {
      return new ArrowFlightJdbcBitVectorAccessor((BitVector) vector, getCurrentRow,
          setCursorWasNull);
    } else if (vector instanceof DecimalVector) {
      return new ArrowFlightJdbcDecimalVectorAccessor((DecimalVector) vector, getCurrentRow,
          setCursorWasNull);
    } else if (vector instanceof Decimal256Vector) {
      return new ArrowFlightJdbcDecimalVectorAccessor((Decimal256Vector) vector, getCurrentRow,
          setCursorWasNull);
    } else if (vector instanceof VarBinaryVector) {
      return new ArrowFlightJdbcBinaryVectorAccessor((VarBinaryVector) vector, getCurrentRow,
          setCursorWasNull);
    } else if (vector instanceof LargeVarBinaryVector) {
      return new ArrowFlightJdbcBinaryVectorAccessor((LargeVarBinaryVector) vector, getCurrentRow,
          setCursorWasNull);
    } else if (vector instanceof FixedSizeBinaryVector) {
      return new ArrowFlightJdbcBinaryVectorAccessor((FixedSizeBinaryVector) vector, getCurrentRow,
          setCursorWasNull);
    } else if (vector instanceof TimeStampVector) {
      return new ArrowFlightJdbcTimeStampVectorAccessor((TimeStampVector) vector, getCurrentRow,
          setCursorWasNull);
    } else if (vector instanceof TimeNanoVector) {
      return new ArrowFlightJdbcTimeVectorAccessor((TimeNanoVector) vector, getCurrentRow,
          setCursorWasNull);
    } else if (vector instanceof TimeMicroVector) {
      return new ArrowFlightJdbcTimeVectorAccessor((TimeMicroVector) vector, getCurrentRow,
          setCursorWasNull);
    } else if (vector instanceof TimeMilliVector) {
      return new ArrowFlightJdbcTimeVectorAccessor((TimeMilliVector) vector, getCurrentRow,
          setCursorWasNull);
    } else if (vector instanceof TimeSecVector) {
      return new ArrowFlightJdbcTimeVectorAccessor((TimeSecVector) vector, getCurrentRow,
          setCursorWasNull);
    } else if (vector instanceof DateDayVector) {
      return new ArrowFlightJdbcDateVectorAccessor(((DateDayVector) vector), getCurrentRow,
          setCursorWasNull);
    } else if (vector instanceof DateMilliVector) {
      return new ArrowFlightJdbcDateVectorAccessor(((DateMilliVector) vector), getCurrentRow,
          setCursorWasNull);
    } else if (vector instanceof VarCharVector) {
      return new ArrowFlightJdbcVarCharVectorAccessor((VarCharVector) vector, getCurrentRow,
          setCursorWasNull);
    } else if (vector instanceof LargeVarCharVector) {
      return new ArrowFlightJdbcVarCharVectorAccessor((LargeVarCharVector) vector, getCurrentRow,
          setCursorWasNull);
    } else if (vector instanceof DurationVector) {
      return new ArrowFlightJdbcDurationVectorAccessor((DurationVector) vector, getCurrentRow,
          setCursorWasNull);
    } else if (vector instanceof IntervalDayVector) {
      return new ArrowFlightJdbcIntervalVectorAccessor(((IntervalDayVector) vector), getCurrentRow,
          setCursorWasNull);
    } else if (vector instanceof IntervalYearVector) {
      return new ArrowFlightJdbcIntervalVectorAccessor(((IntervalYearVector) vector), getCurrentRow,
          setCursorWasNull);
    } else if (vector instanceof StructVector) {
      return new ArrowFlightJdbcStructVectorAccessor((StructVector) vector, getCurrentRow,
          setCursorWasNull);
    } else if (vector instanceof MapVector) {
      return new ArrowFlightJdbcMapVectorAccessor((MapVector) vector, getCurrentRow,
          setCursorWasNull);
    } else if (vector instanceof ListVector) {
      return new ArrowFlightJdbcListVectorAccessor((ListVector) vector, getCurrentRow,
          setCursorWasNull);
    } else if (vector instanceof LargeListVector) {
      return new ArrowFlightJdbcLargeListVectorAccessor((LargeListVector) vector, getCurrentRow,
          setCursorWasNull);
    } else if (vector instanceof FixedSizeListVector) {
      return new ArrowFlightJdbcFixedSizeListVectorAccessor((FixedSizeListVector) vector,
          getCurrentRow, setCursorWasNull);
    } else if (vector instanceof UnionVector) {
      return new ArrowFlightJdbcUnionVectorAccessor((UnionVector) vector, getCurrentRow,
          setCursorWasNull);
    } else if (vector instanceof DenseUnionVector) {
      return new ArrowFlightJdbcDenseUnionVectorAccessor((DenseUnionVector) vector, getCurrentRow,
          setCursorWasNull);
    } else if (vector instanceof NullVector || vector == null) {
      return new ArrowFlightJdbcNullVectorAccessor(setCursorWasNull);
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
