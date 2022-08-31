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
import org.apache.arrow.driver.jdbc.utils.RootAllocatorTestRule;
import org.apache.arrow.vector.DurationVector;
import org.apache.arrow.vector.IntervalDayVector;
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
import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Test;

public class ArrowFlightJdbcAccessorFactoryTest {
  public static final IntSupplier GET_CURRENT_ROW = () -> 0;

  @ClassRule
  public static RootAllocatorTestRule rootAllocatorTestRule = new RootAllocatorTestRule();

  @Test
  public void createAccessorForUInt1Vector() {
    try (ValueVector valueVector = rootAllocatorTestRule.createUInt1Vector()) {
      ArrowFlightJdbcAccessor accessor =
          ArrowFlightJdbcAccessorFactory.createAccessor(valueVector, GET_CURRENT_ROW,
              (boolean wasNull) -> {
              });

      Assert.assertTrue(accessor instanceof ArrowFlightJdbcBaseIntVectorAccessor);
    }
  }

  @Test
  public void createAccessorForUInt2Vector() {
    try (ValueVector valueVector = rootAllocatorTestRule.createUInt2Vector()) {
      ArrowFlightJdbcAccessor accessor =
          ArrowFlightJdbcAccessorFactory.createAccessor(valueVector, GET_CURRENT_ROW,
              (boolean wasNull) -> {
              });

      Assert.assertTrue(accessor instanceof ArrowFlightJdbcBaseIntVectorAccessor);
    }
  }

  @Test
  public void createAccessorForUInt4Vector() {
    try (ValueVector valueVector = rootAllocatorTestRule.createUInt4Vector()) {
      ArrowFlightJdbcAccessor accessor =
          ArrowFlightJdbcAccessorFactory.createAccessor(valueVector, GET_CURRENT_ROW,
              (boolean wasNull) -> {
              });

      Assert.assertTrue(accessor instanceof ArrowFlightJdbcBaseIntVectorAccessor);
    }
  }

  @Test
  public void createAccessorForUInt8Vector() {
    try (ValueVector valueVector = rootAllocatorTestRule.createUInt8Vector()) {
      ArrowFlightJdbcAccessor accessor =
          ArrowFlightJdbcAccessorFactory.createAccessor(valueVector, GET_CURRENT_ROW,
              (boolean wasNull) -> {
              });

      Assert.assertTrue(accessor instanceof ArrowFlightJdbcBaseIntVectorAccessor);
    }
  }

  @Test
  public void createAccessorForTinyIntVector() {
    try (ValueVector valueVector = rootAllocatorTestRule.createTinyIntVector()) {
      ArrowFlightJdbcAccessor accessor =
          ArrowFlightJdbcAccessorFactory.createAccessor(valueVector, GET_CURRENT_ROW,
              (boolean wasNull) -> {
              });

      Assert.assertTrue(accessor instanceof ArrowFlightJdbcBaseIntVectorAccessor);
    }
  }

  @Test
  public void createAccessorForSmallIntVector() {
    try (ValueVector valueVector = rootAllocatorTestRule.createSmallIntVector()) {
      ArrowFlightJdbcAccessor accessor =
          ArrowFlightJdbcAccessorFactory.createAccessor(valueVector, GET_CURRENT_ROW,
              (boolean wasNull) -> {
              });

      Assert.assertTrue(accessor instanceof ArrowFlightJdbcBaseIntVectorAccessor);
    }
  }

  @Test
  public void createAccessorForIntVector() {
    try (ValueVector valueVector = rootAllocatorTestRule.createIntVector()) {
      ArrowFlightJdbcAccessor accessor =
          ArrowFlightJdbcAccessorFactory.createAccessor(valueVector, GET_CURRENT_ROW,
              (boolean wasNull) -> {
              });

      Assert.assertTrue(accessor instanceof ArrowFlightJdbcBaseIntVectorAccessor);
    }
  }

  @Test
  public void createAccessorForBigIntVector() {
    try (ValueVector valueVector = rootAllocatorTestRule.createBigIntVector()) {
      ArrowFlightJdbcAccessor accessor =
          ArrowFlightJdbcAccessorFactory.createAccessor(valueVector, GET_CURRENT_ROW,
              (boolean wasNull) -> {
              });

      Assert.assertTrue(accessor instanceof ArrowFlightJdbcBaseIntVectorAccessor);
    }
  }

  @Test
  public void createAccessorForFloat4Vector() {
    try (ValueVector valueVector = rootAllocatorTestRule.createFloat4Vector()) {
      ArrowFlightJdbcAccessor accessor =
          ArrowFlightJdbcAccessorFactory.createAccessor(valueVector, GET_CURRENT_ROW,
              (boolean wasNull) -> {
              });

      Assert.assertTrue(accessor instanceof ArrowFlightJdbcFloat4VectorAccessor);
    }
  }

  @Test
  public void createAccessorForFloat8Vector() {
    try (ValueVector valueVector = rootAllocatorTestRule.createFloat8Vector()) {
      ArrowFlightJdbcAccessor accessor =
          ArrowFlightJdbcAccessorFactory.createAccessor(valueVector, GET_CURRENT_ROW,
              (boolean wasNull) -> {
              });

      Assert.assertTrue(accessor instanceof ArrowFlightJdbcFloat8VectorAccessor);
    }
  }

  @Test
  public void createAccessorForBitVector() {
    try (ValueVector valueVector = rootAllocatorTestRule.createBitVector()) {
      ArrowFlightJdbcAccessor accessor =
          ArrowFlightJdbcAccessorFactory.createAccessor(valueVector, GET_CURRENT_ROW,
              (boolean wasNull) -> {
              });

      Assert.assertTrue(accessor instanceof ArrowFlightJdbcBitVectorAccessor);
    }
  }

  @Test
  public void createAccessorForDecimalVector() {
    try (ValueVector valueVector = rootAllocatorTestRule.createDecimalVector()) {
      ArrowFlightJdbcAccessor accessor =
          ArrowFlightJdbcAccessorFactory.createAccessor(valueVector, GET_CURRENT_ROW,
              (boolean wasNull) -> {
              });

      Assert.assertTrue(accessor instanceof ArrowFlightJdbcDecimalVectorAccessor);
    }
  }

  @Test
  public void createAccessorForDecimal256Vector() {
    try (ValueVector valueVector = rootAllocatorTestRule.createDecimal256Vector()) {
      ArrowFlightJdbcAccessor accessor =
          ArrowFlightJdbcAccessorFactory.createAccessor(valueVector, GET_CURRENT_ROW,
              (boolean wasNull) -> {
              });

      Assert.assertTrue(accessor instanceof ArrowFlightJdbcDecimalVectorAccessor);
    }
  }

  @Test
  public void createAccessorForVarBinaryVector() {
    try (ValueVector valueVector = rootAllocatorTestRule.createVarBinaryVector()) {
      ArrowFlightJdbcAccessor accessor =
          ArrowFlightJdbcAccessorFactory.createAccessor(valueVector, GET_CURRENT_ROW,
              (boolean wasNull) -> {
              });

      Assert.assertTrue(accessor instanceof ArrowFlightJdbcBinaryVectorAccessor);
    }
  }

  @Test
  public void createAccessorForLargeVarBinaryVector() {
    try (ValueVector valueVector = rootAllocatorTestRule.createLargeVarBinaryVector()) {
      ArrowFlightJdbcAccessor accessor =
          ArrowFlightJdbcAccessorFactory.createAccessor(valueVector, GET_CURRENT_ROW,
              (boolean wasNull) -> {
              });

      Assert.assertTrue(accessor instanceof ArrowFlightJdbcBinaryVectorAccessor);
    }
  }

  @Test
  public void createAccessorForFixedSizeBinaryVector() {
    try (ValueVector valueVector = rootAllocatorTestRule.createFixedSizeBinaryVector()) {
      ArrowFlightJdbcAccessor accessor =
          ArrowFlightJdbcAccessorFactory.createAccessor(valueVector, GET_CURRENT_ROW,
              (boolean wasNull) -> {
              });

      Assert.assertTrue(accessor instanceof ArrowFlightJdbcBinaryVectorAccessor);
    }
  }

  @Test
  public void createAccessorForTimeStampVector() {
    try (ValueVector valueVector = rootAllocatorTestRule.createTimeStampMilliVector()) {
      ArrowFlightJdbcAccessor accessor =
          ArrowFlightJdbcAccessorFactory.createAccessor(valueVector, GET_CURRENT_ROW,
              (boolean wasNull) -> {
              });

      Assert.assertTrue(accessor instanceof ArrowFlightJdbcTimeStampVectorAccessor);
    }
  }

  @Test
  public void createAccessorForTimeNanoVector() {
    try (ValueVector valueVector = rootAllocatorTestRule.createTimeNanoVector()) {
      ArrowFlightJdbcAccessor accessor =
          ArrowFlightJdbcAccessorFactory.createAccessor(valueVector, GET_CURRENT_ROW,
              (boolean wasNull) -> {
              });

      Assert.assertTrue(accessor instanceof ArrowFlightJdbcTimeVectorAccessor);
    }
  }

  @Test
  public void createAccessorForTimeMicroVector() {
    try (ValueVector valueVector = rootAllocatorTestRule.createTimeMicroVector()) {
      ArrowFlightJdbcAccessor accessor =
          ArrowFlightJdbcAccessorFactory.createAccessor(valueVector, GET_CURRENT_ROW,
              (boolean wasNull) -> {
              });

      Assert.assertTrue(accessor instanceof ArrowFlightJdbcTimeVectorAccessor);
    }
  }

  @Test
  public void createAccessorForTimeMilliVector() {
    try (ValueVector valueVector = rootAllocatorTestRule.createTimeMilliVector()) {
      ArrowFlightJdbcAccessor accessor =
          ArrowFlightJdbcAccessorFactory.createAccessor(valueVector, GET_CURRENT_ROW,
              (boolean wasNull) -> {
              });

      Assert.assertTrue(accessor instanceof ArrowFlightJdbcTimeVectorAccessor);
    }
  }

  @Test
  public void createAccessorForTimeSecVector() {
    try (ValueVector valueVector = rootAllocatorTestRule.createTimeSecVector()) {
      ArrowFlightJdbcAccessor accessor =
          ArrowFlightJdbcAccessorFactory.createAccessor(valueVector, GET_CURRENT_ROW,
              (boolean wasNull) -> {
              });

      Assert.assertTrue(accessor instanceof ArrowFlightJdbcTimeVectorAccessor);
    }
  }

  @Test
  public void createAccessorForDateDayVector() {
    try (ValueVector valueVector = rootAllocatorTestRule.createDateDayVector()) {
      ArrowFlightJdbcAccessor accessor =
          ArrowFlightJdbcAccessorFactory.createAccessor(valueVector, GET_CURRENT_ROW,
              (boolean wasNull) -> {
              });

      Assert.assertTrue(accessor instanceof ArrowFlightJdbcDateVectorAccessor);
    }
  }

  @Test
  public void createAccessorForDateMilliVector() {
    try (ValueVector valueVector = rootAllocatorTestRule.createDateMilliVector()) {
      ArrowFlightJdbcAccessor accessor =
          ArrowFlightJdbcAccessorFactory.createAccessor(valueVector, GET_CURRENT_ROW,
              (boolean wasNull) -> {
              });

      Assert.assertTrue(accessor instanceof ArrowFlightJdbcDateVectorAccessor);
    }
  }

  @Test
  public void createAccessorForVarCharVector() {
    try (
        ValueVector valueVector = new VarCharVector("", rootAllocatorTestRule.getRootAllocator())) {
      ArrowFlightJdbcAccessor accessor =
          ArrowFlightJdbcAccessorFactory.createAccessor(valueVector, GET_CURRENT_ROW,
              (boolean wasNull) -> {
              });

      Assert.assertTrue(accessor instanceof ArrowFlightJdbcVarCharVectorAccessor);
    }
  }

  @Test
  public void createAccessorForLargeVarCharVector() {
    try (ValueVector valueVector = new LargeVarCharVector("",
        rootAllocatorTestRule.getRootAllocator())) {
      ArrowFlightJdbcAccessor accessor =
          ArrowFlightJdbcAccessorFactory.createAccessor(valueVector, GET_CURRENT_ROW,
              (boolean wasNull) -> {
              });

      Assert.assertTrue(accessor instanceof ArrowFlightJdbcVarCharVectorAccessor);
    }
  }

  @Test
  public void createAccessorForDurationVector() {
    try (ValueVector valueVector =
             new DurationVector("",
                 new FieldType(true, new ArrowType.Duration(TimeUnit.MILLISECOND), null),
                 rootAllocatorTestRule.getRootAllocator())) {
      ArrowFlightJdbcAccessor accessor =
          ArrowFlightJdbcAccessorFactory.createAccessor(valueVector, GET_CURRENT_ROW,
              (boolean wasNull) -> {
              });

      Assert.assertTrue(accessor instanceof ArrowFlightJdbcDurationVectorAccessor);
    }
  }

  @Test
  public void createAccessorForIntervalDayVector() {
    try (ValueVector valueVector = new IntervalDayVector("",
        rootAllocatorTestRule.getRootAllocator())) {
      ArrowFlightJdbcAccessor accessor =
          ArrowFlightJdbcAccessorFactory.createAccessor(valueVector, GET_CURRENT_ROW,
              (boolean wasNull) -> {
              });

      Assert.assertTrue(accessor instanceof ArrowFlightJdbcIntervalVectorAccessor);
    }
  }

  @Test
  public void createAccessorForIntervalYearVector() {
    try (ValueVector valueVector = new IntervalYearVector("",
        rootAllocatorTestRule.getRootAllocator())) {
      ArrowFlightJdbcAccessor accessor =
          ArrowFlightJdbcAccessorFactory.createAccessor(valueVector, GET_CURRENT_ROW,
              (boolean wasNull) -> {
              });

      Assert.assertTrue(accessor instanceof ArrowFlightJdbcIntervalVectorAccessor);
    }
  }

  @Test
  public void createAccessorForUnionVector() {
    try (ValueVector valueVector = new UnionVector("", rootAllocatorTestRule.getRootAllocator(),
        null, null)) {
      ArrowFlightJdbcAccessor accessor =
          ArrowFlightJdbcAccessorFactory.createAccessor(valueVector, GET_CURRENT_ROW,
              (boolean wasNull) -> {
              });

      Assert.assertTrue(accessor instanceof ArrowFlightJdbcUnionVectorAccessor);
    }
  }

  @Test
  public void createAccessorForDenseUnionVector() {
    try (
        ValueVector valueVector = new DenseUnionVector("", rootAllocatorTestRule.getRootAllocator(),
            null, null)) {
      ArrowFlightJdbcAccessor accessor =
          ArrowFlightJdbcAccessorFactory.createAccessor(valueVector, GET_CURRENT_ROW,
              (boolean wasNull) -> {
              });

      Assert.assertTrue(accessor instanceof ArrowFlightJdbcDenseUnionVectorAccessor);
    }
  }

  @Test
  public void createAccessorForStructVector() {
    try (ValueVector valueVector = StructVector.empty("",
        rootAllocatorTestRule.getRootAllocator())) {
      ArrowFlightJdbcAccessor accessor =
          ArrowFlightJdbcAccessorFactory.createAccessor(valueVector, GET_CURRENT_ROW,
              (boolean wasNull) -> {
              });

      Assert.assertTrue(accessor instanceof ArrowFlightJdbcStructVectorAccessor);
    }
  }

  @Test
  public void createAccessorForListVector() {
    try (ValueVector valueVector = rootAllocatorTestRule.createListVector()) {
      ArrowFlightJdbcAccessor accessor =
          ArrowFlightJdbcAccessorFactory.createAccessor(valueVector, GET_CURRENT_ROW,
              (boolean wasNull) -> {
              });

      Assert.assertTrue(accessor instanceof ArrowFlightJdbcListVectorAccessor);
    }
  }

  @Test
  public void createAccessorForLargeListVector() {
    try (ValueVector valueVector = rootAllocatorTestRule.createLargeListVector()) {
      ArrowFlightJdbcAccessor accessor =
          ArrowFlightJdbcAccessorFactory.createAccessor(valueVector, GET_CURRENT_ROW,
              (boolean wasNull) -> {
              });

      Assert.assertTrue(accessor instanceof ArrowFlightJdbcLargeListVectorAccessor);
    }
  }

  @Test
  public void createAccessorForFixedSizeListVector() {
    try (ValueVector valueVector = rootAllocatorTestRule.createFixedSizeListVector()) {
      ArrowFlightJdbcAccessor accessor =
          ArrowFlightJdbcAccessorFactory.createAccessor(valueVector, GET_CURRENT_ROW,
              (boolean wasNull) -> {
              });

      Assert.assertTrue(accessor instanceof ArrowFlightJdbcFixedSizeListVectorAccessor);
    }
  }

  @Test
  public void createAccessorForMapVector() {
    try (ValueVector valueVector = MapVector.empty("", rootAllocatorTestRule.getRootAllocator(),
        true)) {
      ArrowFlightJdbcAccessor accessor =
          ArrowFlightJdbcAccessorFactory.createAccessor(valueVector, GET_CURRENT_ROW,
              (boolean wasNull) -> {
              });

      Assert.assertTrue(accessor instanceof ArrowFlightJdbcMapVectorAccessor);
    }
  }
}
