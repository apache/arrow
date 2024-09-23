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

import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.function.IntSupplier;
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
import org.apache.arrow.driver.jdbc.utils.RootAllocatorTestExtension;
import org.apache.arrow.vector.DurationVector;
import org.apache.arrow.vector.IntervalDayVector;
import org.apache.arrow.vector.IntervalMonthDayNanoVector;
import org.apache.arrow.vector.IntervalYearVector;
import org.apache.arrow.vector.LargeVarCharVector;
import org.apache.arrow.vector.ValueVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.complex.DenseUnionVector;
import org.apache.arrow.vector.complex.MapVector;
import org.apache.arrow.vector.complex.StructVector;
import org.apache.arrow.vector.complex.UnionVector;
import org.apache.arrow.vector.types.TimeUnit;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

public class ArrowFlightJdbcAccessorFactoryTest {
  public static final IntSupplier GET_CURRENT_ROW = () -> 0;

  @RegisterExtension
  public static RootAllocatorTestExtension rootAllocatorTestExtension =
      new RootAllocatorTestExtension();

  @Test
  public void createAccessorForUInt1Vector() {
    try (ValueVector valueVector = rootAllocatorTestExtension.createUInt1Vector()) {
      ArrowFlightJdbcAccessor accessor =
          ArrowFlightJdbcAccessorFactory.createAccessor(
              valueVector, GET_CURRENT_ROW, (boolean wasNull) -> {});

      assertTrue(accessor instanceof ArrowFlightJdbcBaseIntVectorAccessor);
    }
  }

  @Test
  public void createAccessorForUInt2Vector() {
    try (ValueVector valueVector = rootAllocatorTestExtension.createUInt2Vector()) {
      ArrowFlightJdbcAccessor accessor =
          ArrowFlightJdbcAccessorFactory.createAccessor(
              valueVector, GET_CURRENT_ROW, (boolean wasNull) -> {});

      assertTrue(accessor instanceof ArrowFlightJdbcBaseIntVectorAccessor);
    }
  }

  @Test
  public void createAccessorForUInt4Vector() {
    try (ValueVector valueVector = rootAllocatorTestExtension.createUInt4Vector()) {
      ArrowFlightJdbcAccessor accessor =
          ArrowFlightJdbcAccessorFactory.createAccessor(
              valueVector, GET_CURRENT_ROW, (boolean wasNull) -> {});

      assertTrue(accessor instanceof ArrowFlightJdbcBaseIntVectorAccessor);
    }
  }

  @Test
  public void createAccessorForUInt8Vector() {
    try (ValueVector valueVector = rootAllocatorTestExtension.createUInt8Vector()) {
      ArrowFlightJdbcAccessor accessor =
          ArrowFlightJdbcAccessorFactory.createAccessor(
              valueVector, GET_CURRENT_ROW, (boolean wasNull) -> {});

      assertTrue(accessor instanceof ArrowFlightJdbcBaseIntVectorAccessor);
    }
  }

  @Test
  public void createAccessorForTinyIntVector() {
    try (ValueVector valueVector = rootAllocatorTestExtension.createTinyIntVector()) {
      ArrowFlightJdbcAccessor accessor =
          ArrowFlightJdbcAccessorFactory.createAccessor(
              valueVector, GET_CURRENT_ROW, (boolean wasNull) -> {});

      assertTrue(accessor instanceof ArrowFlightJdbcBaseIntVectorAccessor);
    }
  }

  @Test
  public void createAccessorForSmallIntVector() {
    try (ValueVector valueVector = rootAllocatorTestExtension.createSmallIntVector()) {
      ArrowFlightJdbcAccessor accessor =
          ArrowFlightJdbcAccessorFactory.createAccessor(
              valueVector, GET_CURRENT_ROW, (boolean wasNull) -> {});

      assertTrue(accessor instanceof ArrowFlightJdbcBaseIntVectorAccessor);
    }
  }

  @Test
  public void createAccessorForIntVector() {
    try (ValueVector valueVector = rootAllocatorTestExtension.createIntVector()) {
      ArrowFlightJdbcAccessor accessor =
          ArrowFlightJdbcAccessorFactory.createAccessor(
              valueVector, GET_CURRENT_ROW, (boolean wasNull) -> {});

      assertTrue(accessor instanceof ArrowFlightJdbcBaseIntVectorAccessor);
    }
  }

  @Test
  public void createAccessorForBigIntVector() {
    try (ValueVector valueVector = rootAllocatorTestExtension.createBigIntVector()) {
      ArrowFlightJdbcAccessor accessor =
          ArrowFlightJdbcAccessorFactory.createAccessor(
              valueVector, GET_CURRENT_ROW, (boolean wasNull) -> {});

      assertTrue(accessor instanceof ArrowFlightJdbcBaseIntVectorAccessor);
    }
  }

  @Test
  public void createAccessorForFloat4Vector() {
    try (ValueVector valueVector = rootAllocatorTestExtension.createFloat4Vector()) {
      ArrowFlightJdbcAccessor accessor =
          ArrowFlightJdbcAccessorFactory.createAccessor(
              valueVector, GET_CURRENT_ROW, (boolean wasNull) -> {});

      assertTrue(accessor instanceof ArrowFlightJdbcFloat4VectorAccessor);
    }
  }

  @Test
  public void createAccessorForFloat8Vector() {
    try (ValueVector valueVector = rootAllocatorTestExtension.createFloat8Vector()) {
      ArrowFlightJdbcAccessor accessor =
          ArrowFlightJdbcAccessorFactory.createAccessor(
              valueVector, GET_CURRENT_ROW, (boolean wasNull) -> {});

      assertTrue(accessor instanceof ArrowFlightJdbcFloat8VectorAccessor);
    }
  }

  @Test
  public void createAccessorForBitVector() {
    try (ValueVector valueVector = rootAllocatorTestExtension.createBitVector()) {
      ArrowFlightJdbcAccessor accessor =
          ArrowFlightJdbcAccessorFactory.createAccessor(
              valueVector, GET_CURRENT_ROW, (boolean wasNull) -> {});

      assertTrue(accessor instanceof ArrowFlightJdbcBitVectorAccessor);
    }
  }

  @Test
  public void createAccessorForDecimalVector() {
    try (ValueVector valueVector = rootAllocatorTestExtension.createDecimalVector()) {
      ArrowFlightJdbcAccessor accessor =
          ArrowFlightJdbcAccessorFactory.createAccessor(
              valueVector, GET_CURRENT_ROW, (boolean wasNull) -> {});

      assertTrue(accessor instanceof ArrowFlightJdbcDecimalVectorAccessor);
    }
  }

  @Test
  public void createAccessorForDecimal256Vector() {
    try (ValueVector valueVector = rootAllocatorTestExtension.createDecimal256Vector()) {
      ArrowFlightJdbcAccessor accessor =
          ArrowFlightJdbcAccessorFactory.createAccessor(
              valueVector, GET_CURRENT_ROW, (boolean wasNull) -> {});

      assertTrue(accessor instanceof ArrowFlightJdbcDecimalVectorAccessor);
    }
  }

  @Test
  public void createAccessorForVarBinaryVector() {
    try (ValueVector valueVector = rootAllocatorTestExtension.createVarBinaryVector()) {
      ArrowFlightJdbcAccessor accessor =
          ArrowFlightJdbcAccessorFactory.createAccessor(
              valueVector, GET_CURRENT_ROW, (boolean wasNull) -> {});

      assertTrue(accessor instanceof ArrowFlightJdbcBinaryVectorAccessor);
    }
  }

  @Test
  public void createAccessorForLargeVarBinaryVector() {
    try (ValueVector valueVector = rootAllocatorTestExtension.createLargeVarBinaryVector()) {
      ArrowFlightJdbcAccessor accessor =
          ArrowFlightJdbcAccessorFactory.createAccessor(
              valueVector, GET_CURRENT_ROW, (boolean wasNull) -> {});

      assertTrue(accessor instanceof ArrowFlightJdbcBinaryVectorAccessor);
    }
  }

  @Test
  public void createAccessorForFixedSizeBinaryVector() {
    try (ValueVector valueVector = rootAllocatorTestExtension.createFixedSizeBinaryVector()) {
      ArrowFlightJdbcAccessor accessor =
          ArrowFlightJdbcAccessorFactory.createAccessor(
              valueVector, GET_CURRENT_ROW, (boolean wasNull) -> {});

      assertTrue(accessor instanceof ArrowFlightJdbcBinaryVectorAccessor);
    }
  }

  @Test
  public void createAccessorForTimeStampVector() {
    try (ValueVector valueVector = rootAllocatorTestExtension.createTimeStampMilliVector()) {
      ArrowFlightJdbcAccessor accessor =
          ArrowFlightJdbcAccessorFactory.createAccessor(
              valueVector, GET_CURRENT_ROW, (boolean wasNull) -> {});

      assertTrue(accessor instanceof ArrowFlightJdbcTimeStampVectorAccessor);
    }
  }

  @Test
  public void createAccessorForTimeNanoVector() {
    try (ValueVector valueVector = rootAllocatorTestExtension.createTimeNanoVector()) {
      ArrowFlightJdbcAccessor accessor =
          ArrowFlightJdbcAccessorFactory.createAccessor(
              valueVector, GET_CURRENT_ROW, (boolean wasNull) -> {});

      assertTrue(accessor instanceof ArrowFlightJdbcTimeVectorAccessor);
    }
  }

  @Test
  public void createAccessorForTimeMicroVector() {
    try (ValueVector valueVector = rootAllocatorTestExtension.createTimeMicroVector()) {
      ArrowFlightJdbcAccessor accessor =
          ArrowFlightJdbcAccessorFactory.createAccessor(
              valueVector, GET_CURRENT_ROW, (boolean wasNull) -> {});

      assertTrue(accessor instanceof ArrowFlightJdbcTimeVectorAccessor);
    }
  }

  @Test
  public void createAccessorForTimeMilliVector() {
    try (ValueVector valueVector = rootAllocatorTestExtension.createTimeMilliVector()) {
      ArrowFlightJdbcAccessor accessor =
          ArrowFlightJdbcAccessorFactory.createAccessor(
              valueVector, GET_CURRENT_ROW, (boolean wasNull) -> {});

      assertTrue(accessor instanceof ArrowFlightJdbcTimeVectorAccessor);
    }
  }

  @Test
  public void createAccessorForTimeSecVector() {
    try (ValueVector valueVector = rootAllocatorTestExtension.createTimeSecVector()) {
      ArrowFlightJdbcAccessor accessor =
          ArrowFlightJdbcAccessorFactory.createAccessor(
              valueVector, GET_CURRENT_ROW, (boolean wasNull) -> {});

      assertTrue(accessor instanceof ArrowFlightJdbcTimeVectorAccessor);
    }
  }

  @Test
  public void createAccessorForDateDayVector() {
    try (ValueVector valueVector = rootAllocatorTestExtension.createDateDayVector()) {
      ArrowFlightJdbcAccessor accessor =
          ArrowFlightJdbcAccessorFactory.createAccessor(
              valueVector, GET_CURRENT_ROW, (boolean wasNull) -> {});

      assertTrue(accessor instanceof ArrowFlightJdbcDateVectorAccessor);
    }
  }

  @Test
  public void createAccessorForDateMilliVector() {
    try (ValueVector valueVector = rootAllocatorTestExtension.createDateMilliVector()) {
      ArrowFlightJdbcAccessor accessor =
          ArrowFlightJdbcAccessorFactory.createAccessor(
              valueVector, GET_CURRENT_ROW, (boolean wasNull) -> {});

      assertTrue(accessor instanceof ArrowFlightJdbcDateVectorAccessor);
    }
  }

  @Test
  public void createAccessorForVarCharVector() {
    try (ValueVector valueVector =
        new VarCharVector("", rootAllocatorTestExtension.getRootAllocator())) {
      ArrowFlightJdbcAccessor accessor =
          ArrowFlightJdbcAccessorFactory.createAccessor(
              valueVector, GET_CURRENT_ROW, (boolean wasNull) -> {});

      assertTrue(accessor instanceof ArrowFlightJdbcVarCharVectorAccessor);
    }
  }

  @Test
  public void createAccessorForLargeVarCharVector() {
    try (ValueVector valueVector =
        new LargeVarCharVector("", rootAllocatorTestExtension.getRootAllocator())) {
      ArrowFlightJdbcAccessor accessor =
          ArrowFlightJdbcAccessorFactory.createAccessor(
              valueVector, GET_CURRENT_ROW, (boolean wasNull) -> {});

      assertTrue(accessor instanceof ArrowFlightJdbcVarCharVectorAccessor);
    }
  }

  @Test
  public void createAccessorForDurationVector() {
    try (ValueVector valueVector =
        new DurationVector(
            "",
            new FieldType(true, new ArrowType.Duration(TimeUnit.MILLISECOND), null),
            rootAllocatorTestExtension.getRootAllocator())) {
      ArrowFlightJdbcAccessor accessor =
          ArrowFlightJdbcAccessorFactory.createAccessor(
              valueVector, GET_CURRENT_ROW, (boolean wasNull) -> {});

      assertTrue(accessor instanceof ArrowFlightJdbcDurationVectorAccessor);
    }
  }

  @Test
  public void createAccessorForIntervalDayVector() {
    try (ValueVector valueVector =
        new IntervalDayVector("", rootAllocatorTestExtension.getRootAllocator())) {
      ArrowFlightJdbcAccessor accessor =
          ArrowFlightJdbcAccessorFactory.createAccessor(
              valueVector, GET_CURRENT_ROW, (boolean wasNull) -> {});

      assertTrue(accessor instanceof ArrowFlightJdbcIntervalVectorAccessor);
    }
  }

  @Test
  public void createAccessorForIntervalYearVector() {
    try (ValueVector valueVector =
        new IntervalYearVector("", rootAllocatorTestExtension.getRootAllocator())) {
      ArrowFlightJdbcAccessor accessor =
          ArrowFlightJdbcAccessorFactory.createAccessor(
              valueVector, GET_CURRENT_ROW, (boolean wasNull) -> {});

      assertTrue(accessor instanceof ArrowFlightJdbcIntervalVectorAccessor);
    }
  }

  @Test
  public void createAccessorForIntervalMonthDayNanoVector() {
    try (ValueVector valueVector =
        new IntervalMonthDayNanoVector("", rootAllocatorTestExtension.getRootAllocator())) {
      ArrowFlightJdbcAccessor accessor =
          ArrowFlightJdbcAccessorFactory.createAccessor(
              valueVector, GET_CURRENT_ROW, (boolean wasNull) -> {});

      assertTrue(accessor instanceof ArrowFlightJdbcIntervalVectorAccessor);
    }
  }

  @Test
  public void createAccessorForUnionVector() {
    try (ValueVector valueVector =
        new UnionVector("", rootAllocatorTestExtension.getRootAllocator(), null, null)) {
      ArrowFlightJdbcAccessor accessor =
          ArrowFlightJdbcAccessorFactory.createAccessor(
              valueVector, GET_CURRENT_ROW, (boolean wasNull) -> {});

      assertTrue(accessor instanceof ArrowFlightJdbcUnionVectorAccessor);
    }
  }

  @Test
  public void createAccessorForDenseUnionVector() {
    try (ValueVector valueVector =
        new DenseUnionVector("", rootAllocatorTestExtension.getRootAllocator(), null, null)) {
      ArrowFlightJdbcAccessor accessor =
          ArrowFlightJdbcAccessorFactory.createAccessor(
              valueVector, GET_CURRENT_ROW, (boolean wasNull) -> {});

      assertTrue(accessor instanceof ArrowFlightJdbcDenseUnionVectorAccessor);
    }
  }

  @Test
  public void createAccessorForStructVector() {
    try (ValueVector valueVector =
        StructVector.empty("", rootAllocatorTestExtension.getRootAllocator())) {
      ArrowFlightJdbcAccessor accessor =
          ArrowFlightJdbcAccessorFactory.createAccessor(
              valueVector, GET_CURRENT_ROW, (boolean wasNull) -> {});

      assertTrue(accessor instanceof ArrowFlightJdbcStructVectorAccessor);
    }
  }

  @Test
  public void createAccessorForListVector() {
    try (ValueVector valueVector = rootAllocatorTestExtension.createListVector()) {
      ArrowFlightJdbcAccessor accessor =
          ArrowFlightJdbcAccessorFactory.createAccessor(
              valueVector, GET_CURRENT_ROW, (boolean wasNull) -> {});

      assertTrue(accessor instanceof ArrowFlightJdbcListVectorAccessor);
    }
  }

  @Test
  public void createAccessorForLargeListVector() {
    try (ValueVector valueVector = rootAllocatorTestExtension.createLargeListVector()) {
      ArrowFlightJdbcAccessor accessor =
          ArrowFlightJdbcAccessorFactory.createAccessor(
              valueVector, GET_CURRENT_ROW, (boolean wasNull) -> {});

      assertTrue(accessor instanceof ArrowFlightJdbcLargeListVectorAccessor);
    }
  }

  @Test
  public void createAccessorForFixedSizeListVector() {
    try (ValueVector valueVector = rootAllocatorTestExtension.createFixedSizeListVector()) {
      ArrowFlightJdbcAccessor accessor =
          ArrowFlightJdbcAccessorFactory.createAccessor(
              valueVector, GET_CURRENT_ROW, (boolean wasNull) -> {});

      assertTrue(accessor instanceof ArrowFlightJdbcFixedSizeListVectorAccessor);
    }
  }

  @Test
  public void createAccessorForMapVector() {
    try (ValueVector valueVector =
        MapVector.empty("", rootAllocatorTestExtension.getRootAllocator(), true)) {
      ArrowFlightJdbcAccessor accessor =
          ArrowFlightJdbcAccessorFactory.createAccessor(
              valueVector, GET_CURRENT_ROW, (boolean wasNull) -> {});

      assertTrue(accessor instanceof ArrowFlightJdbcMapVectorAccessor);
    }
  }
}
