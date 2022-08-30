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

public class ArrowJdbcAccessorFactoryTest {
  public static final IntSupplier GET_CURRENT_ROW = () -> 0;

  @ClassRule
  public static RootAllocatorTestRule rootAllocatorTestRule = new RootAllocatorTestRule();

  @Test
  public void createAccessorForUInt1Vector() {
    try (ValueVector valueVector = rootAllocatorTestRule.createUInt1Vector()) {
      ArrowJdbcAccessor accessor =
          ArrowJdbcAccessorFactory.createAccessor(valueVector, GET_CURRENT_ROW,
              (boolean wasNull) -> {
              });

      Assert.assertTrue(accessor instanceof ArrowJdbcBaseIntVectorAccessor);
    }
  }

  @Test
  public void createAccessorForUInt2Vector() {
    try (ValueVector valueVector = rootAllocatorTestRule.createUInt2Vector()) {
      ArrowJdbcAccessor accessor =
          ArrowJdbcAccessorFactory.createAccessor(valueVector, GET_CURRENT_ROW,
              (boolean wasNull) -> {
              });

      Assert.assertTrue(accessor instanceof ArrowJdbcBaseIntVectorAccessor);
    }
  }

  @Test
  public void createAccessorForUInt4Vector() {
    try (ValueVector valueVector = rootAllocatorTestRule.createUInt4Vector()) {
      ArrowJdbcAccessor accessor =
          ArrowJdbcAccessorFactory.createAccessor(valueVector, GET_CURRENT_ROW,
              (boolean wasNull) -> {
              });

      Assert.assertTrue(accessor instanceof ArrowJdbcBaseIntVectorAccessor);
    }
  }

  @Test
  public void createAccessorForUInt8Vector() {
    try (ValueVector valueVector = rootAllocatorTestRule.createUInt8Vector()) {
      ArrowJdbcAccessor accessor =
          ArrowJdbcAccessorFactory.createAccessor(valueVector, GET_CURRENT_ROW,
              (boolean wasNull) -> {
              });

      Assert.assertTrue(accessor instanceof ArrowJdbcBaseIntVectorAccessor);
    }
  }

  @Test
  public void createAccessorForTinyIntVector() {
    try (ValueVector valueVector = rootAllocatorTestRule.createTinyIntVector()) {
      ArrowJdbcAccessor accessor =
          ArrowJdbcAccessorFactory.createAccessor(valueVector, GET_CURRENT_ROW,
              (boolean wasNull) -> {
              });

      Assert.assertTrue(accessor instanceof ArrowJdbcBaseIntVectorAccessor);
    }
  }

  @Test
  public void createAccessorForSmallIntVector() {
    try (ValueVector valueVector = rootAllocatorTestRule.createSmallIntVector()) {
      ArrowJdbcAccessor accessor =
          ArrowJdbcAccessorFactory.createAccessor(valueVector, GET_CURRENT_ROW,
              (boolean wasNull) -> {
              });

      Assert.assertTrue(accessor instanceof ArrowJdbcBaseIntVectorAccessor);
    }
  }

  @Test
  public void createAccessorForIntVector() {
    try (ValueVector valueVector = rootAllocatorTestRule.createIntVector()) {
      ArrowJdbcAccessor accessor =
          ArrowJdbcAccessorFactory.createAccessor(valueVector, GET_CURRENT_ROW,
              (boolean wasNull) -> {
              });

      Assert.assertTrue(accessor instanceof ArrowJdbcBaseIntVectorAccessor);
    }
  }

  @Test
  public void createAccessorForBigIntVector() {
    try (ValueVector valueVector = rootAllocatorTestRule.createBigIntVector()) {
      ArrowJdbcAccessor accessor =
          ArrowJdbcAccessorFactory.createAccessor(valueVector, GET_CURRENT_ROW,
              (boolean wasNull) -> {
              });

      Assert.assertTrue(accessor instanceof ArrowJdbcBaseIntVectorAccessor);
    }
  }

  @Test
  public void createAccessorForFloat4Vector() {
    try (ValueVector valueVector = rootAllocatorTestRule.createFloat4Vector()) {
      ArrowJdbcAccessor accessor =
          ArrowJdbcAccessorFactory.createAccessor(valueVector, GET_CURRENT_ROW,
              (boolean wasNull) -> {
              });

      Assert.assertTrue(accessor instanceof ArrowJdbcFloat4VectorAccessor);
    }
  }

  @Test
  public void createAccessorForFloat8Vector() {
    try (ValueVector valueVector = rootAllocatorTestRule.createFloat8Vector()) {
      ArrowJdbcAccessor accessor =
          ArrowJdbcAccessorFactory.createAccessor(valueVector, GET_CURRENT_ROW,
              (boolean wasNull) -> {
              });

      Assert.assertTrue(accessor instanceof ArrowJdbcFloat8VectorAccessor);
    }
  }

  @Test
  public void createAccessorForBitVector() {
    try (ValueVector valueVector = rootAllocatorTestRule.createBitVector()) {
      ArrowJdbcAccessor accessor =
          ArrowJdbcAccessorFactory.createAccessor(valueVector, GET_CURRENT_ROW,
              (boolean wasNull) -> {
              });

      Assert.assertTrue(accessor instanceof ArrowJdbcBitVectorAccessor);
    }
  }

  @Test
  public void createAccessorForDecimalVector() {
    try (ValueVector valueVector = rootAllocatorTestRule.createDecimalVector()) {
      ArrowJdbcAccessor accessor =
          ArrowJdbcAccessorFactory.createAccessor(valueVector, GET_CURRENT_ROW,
              (boolean wasNull) -> {
              });

      Assert.assertTrue(accessor instanceof ArrowJdbcDecimalVectorAccessor);
    }
  }

  @Test
  public void createAccessorForDecimal256Vector() {
    try (ValueVector valueVector = rootAllocatorTestRule.createDecimal256Vector()) {
      ArrowJdbcAccessor accessor =
          ArrowJdbcAccessorFactory.createAccessor(valueVector, GET_CURRENT_ROW,
              (boolean wasNull) -> {
              });

      Assert.assertTrue(accessor instanceof ArrowJdbcDecimalVectorAccessor);
    }
  }

  @Test
  public void createAccessorForVarBinaryVector() {
    try (ValueVector valueVector = rootAllocatorTestRule.createVarBinaryVector()) {
      ArrowJdbcAccessor accessor =
          ArrowJdbcAccessorFactory.createAccessor(valueVector, GET_CURRENT_ROW,
              (boolean wasNull) -> {
              });

      Assert.assertTrue(accessor instanceof ArrowJdbcBinaryVectorAccessor);
    }
  }

  @Test
  public void createAccessorForLargeVarBinaryVector() {
    try (ValueVector valueVector = rootAllocatorTestRule.createLargeVarBinaryVector()) {
      ArrowJdbcAccessor accessor =
          ArrowJdbcAccessorFactory.createAccessor(valueVector, GET_CURRENT_ROW,
              (boolean wasNull) -> {
              });

      Assert.assertTrue(accessor instanceof ArrowJdbcBinaryVectorAccessor);
    }
  }

  @Test
  public void createAccessorForFixedSizeBinaryVector() {
    try (ValueVector valueVector = rootAllocatorTestRule.createFixedSizeBinaryVector()) {
      ArrowJdbcAccessor accessor =
          ArrowJdbcAccessorFactory.createAccessor(valueVector, GET_CURRENT_ROW,
              (boolean wasNull) -> {
              });

      Assert.assertTrue(accessor instanceof ArrowJdbcBinaryVectorAccessor);
    }
  }

  @Test
  public void createAccessorForTimeStampVector() {
    try (ValueVector valueVector = rootAllocatorTestRule.createTimeStampMilliVector()) {
      ArrowJdbcAccessor accessor =
          ArrowJdbcAccessorFactory.createAccessor(valueVector, GET_CURRENT_ROW,
              (boolean wasNull) -> {
              });

      Assert.assertTrue(accessor instanceof ArrowJdbcTimeStampVectorAccessor);
    }
  }

  @Test
  public void createAccessorForTimeNanoVector() {
    try (ValueVector valueVector = rootAllocatorTestRule.createTimeNanoVector()) {
      ArrowJdbcAccessor accessor =
          ArrowJdbcAccessorFactory.createAccessor(valueVector, GET_CURRENT_ROW,
              (boolean wasNull) -> {
              });

      Assert.assertTrue(accessor instanceof ArrowJdbcTimeVectorAccessor);
    }
  }

  @Test
  public void createAccessorForTimeMicroVector() {
    try (ValueVector valueVector = rootAllocatorTestRule.createTimeMicroVector()) {
      ArrowJdbcAccessor accessor =
          ArrowJdbcAccessorFactory.createAccessor(valueVector, GET_CURRENT_ROW,
              (boolean wasNull) -> {
              });

      Assert.assertTrue(accessor instanceof ArrowJdbcTimeVectorAccessor);
    }
  }

  @Test
  public void createAccessorForTimeMilliVector() {
    try (ValueVector valueVector = rootAllocatorTestRule.createTimeMilliVector()) {
      ArrowJdbcAccessor accessor =
          ArrowJdbcAccessorFactory.createAccessor(valueVector, GET_CURRENT_ROW,
              (boolean wasNull) -> {
              });

      Assert.assertTrue(accessor instanceof ArrowJdbcTimeVectorAccessor);
    }
  }

  @Test
  public void createAccessorForTimeSecVector() {
    try (ValueVector valueVector = rootAllocatorTestRule.createTimeSecVector()) {
      ArrowJdbcAccessor accessor =
          ArrowJdbcAccessorFactory.createAccessor(valueVector, GET_CURRENT_ROW,
              (boolean wasNull) -> {
              });

      Assert.assertTrue(accessor instanceof ArrowJdbcTimeVectorAccessor);
    }
  }

  @Test
  public void createAccessorForDateDayVector() {
    try (ValueVector valueVector = rootAllocatorTestRule.createDateDayVector()) {
      ArrowJdbcAccessor accessor =
          ArrowJdbcAccessorFactory.createAccessor(valueVector, GET_CURRENT_ROW,
              (boolean wasNull) -> {
              });

      Assert.assertTrue(accessor instanceof ArrowJdbcDateVectorAccessor);
    }
  }

  @Test
  public void createAccessorForDateMilliVector() {
    try (ValueVector valueVector = rootAllocatorTestRule.createDateMilliVector()) {
      ArrowJdbcAccessor accessor =
          ArrowJdbcAccessorFactory.createAccessor(valueVector, GET_CURRENT_ROW,
              (boolean wasNull) -> {
              });

      Assert.assertTrue(accessor instanceof ArrowJdbcDateVectorAccessor);
    }
  }

  @Test
  public void createAccessorForVarCharVector() {
    try (
        ValueVector valueVector = new VarCharVector("", rootAllocatorTestRule.getRootAllocator())) {
      ArrowJdbcAccessor accessor =
          ArrowJdbcAccessorFactory.createAccessor(valueVector, GET_CURRENT_ROW,
              (boolean wasNull) -> {
              });

      Assert.assertTrue(accessor instanceof ArrowJdbcVarCharVectorAccessor);
    }
  }

  @Test
  public void createAccessorForLargeVarCharVector() {
    try (ValueVector valueVector = new LargeVarCharVector("",
        rootAllocatorTestRule.getRootAllocator())) {
      ArrowJdbcAccessor accessor =
          ArrowJdbcAccessorFactory.createAccessor(valueVector, GET_CURRENT_ROW,
              (boolean wasNull) -> {
              });

      Assert.assertTrue(accessor instanceof ArrowJdbcVarCharVectorAccessor);
    }
  }

  @Test
  public void createAccessorForDurationVector() {
    try (ValueVector valueVector =
             new DurationVector("",
                 new FieldType(true, new ArrowType.Duration(TimeUnit.MILLISECOND), null),
                 rootAllocatorTestRule.getRootAllocator())) {
      ArrowJdbcAccessor accessor =
          ArrowJdbcAccessorFactory.createAccessor(valueVector, GET_CURRENT_ROW,
              (boolean wasNull) -> {
              });

      Assert.assertTrue(accessor instanceof ArrowJdbcDurationVectorAccessor);
    }
  }

  @Test
  public void createAccessorForIntervalDayVector() {
    try (ValueVector valueVector = new IntervalDayVector("",
        rootAllocatorTestRule.getRootAllocator())) {
      ArrowJdbcAccessor accessor =
          ArrowJdbcAccessorFactory.createAccessor(valueVector, GET_CURRENT_ROW,
              (boolean wasNull) -> {
              });

      Assert.assertTrue(accessor instanceof ArrowJdbcIntervalVectorAccessor);
    }
  }

  @Test
  public void createAccessorForIntervalYearVector() {
    try (ValueVector valueVector = new IntervalYearVector("",
        rootAllocatorTestRule.getRootAllocator())) {
      ArrowJdbcAccessor accessor =
          ArrowJdbcAccessorFactory.createAccessor(valueVector, GET_CURRENT_ROW,
              (boolean wasNull) -> {
              });

      Assert.assertTrue(accessor instanceof ArrowJdbcIntervalVectorAccessor);
    }
  }

  @Test
  public void createAccessorForUnionVector() {
    try (ValueVector valueVector = new UnionVector("", rootAllocatorTestRule.getRootAllocator(),
        null, null)) {
      ArrowJdbcAccessor accessor =
          ArrowJdbcAccessorFactory.createAccessor(valueVector, GET_CURRENT_ROW,
              (boolean wasNull) -> {
              });

      Assert.assertTrue(accessor instanceof ArrowJdbcUnionVectorAccessor);
    }
  }

  @Test
  public void createAccessorForDenseUnionVector() {
    try (
        ValueVector valueVector = new DenseUnionVector("", rootAllocatorTestRule.getRootAllocator(),
            null, null)) {
      ArrowJdbcAccessor accessor =
          ArrowJdbcAccessorFactory.createAccessor(valueVector, GET_CURRENT_ROW,
              (boolean wasNull) -> {
              });

      Assert.assertTrue(accessor instanceof ArrowJdbcDenseUnionVectorAccessor);
    }
  }

  @Test
  public void createAccessorForStructVector() {
    try (ValueVector valueVector = StructVector.empty("",
        rootAllocatorTestRule.getRootAllocator())) {
      ArrowJdbcAccessor accessor =
          ArrowJdbcAccessorFactory.createAccessor(valueVector, GET_CURRENT_ROW,
              (boolean wasNull) -> {
              });

      Assert.assertTrue(accessor instanceof ArrowJdbcStructVectorAccessor);
    }
  }

  @Test
  public void createAccessorForListVector() {
    try (ValueVector valueVector = rootAllocatorTestRule.createListVector()) {
      ArrowJdbcAccessor accessor =
          ArrowJdbcAccessorFactory.createAccessor(valueVector, GET_CURRENT_ROW,
              (boolean wasNull) -> {
              });

      Assert.assertTrue(accessor instanceof ArrowJdbcListVectorAccessor);
    }
  }

  @Test
  public void createAccessorForLargeListVector() {
    try (ValueVector valueVector = rootAllocatorTestRule.createLargeListVector()) {
      ArrowJdbcAccessor accessor =
          ArrowJdbcAccessorFactory.createAccessor(valueVector, GET_CURRENT_ROW,
              (boolean wasNull) -> {
              });

      Assert.assertTrue(accessor instanceof ArrowJdbcLargeListVectorAccessor);
    }
  }

  @Test
  public void createAccessorForFixedSizeListVector() {
    try (ValueVector valueVector = rootAllocatorTestRule.createFixedSizeListVector()) {
      ArrowJdbcAccessor accessor =
          ArrowJdbcAccessorFactory.createAccessor(valueVector, GET_CURRENT_ROW,
              (boolean wasNull) -> {
              });

      Assert.assertTrue(accessor instanceof ArrowJdbcFixedSizeListVectorAccessor);
    }
  }

  @Test
  public void createAccessorForMapVector() {
    try (ValueVector valueVector = MapVector.empty("", rootAllocatorTestRule.getRootAllocator(),
        true)) {
      ArrowJdbcAccessor accessor =
          ArrowJdbcAccessorFactory.createAccessor(valueVector, GET_CURRENT_ROW,
              (boolean wasNull) -> {
              });

      Assert.assertTrue(accessor instanceof ArrowJdbcMapVectorAccessor);
    }
  }
}
