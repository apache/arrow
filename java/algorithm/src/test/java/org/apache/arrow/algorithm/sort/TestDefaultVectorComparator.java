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
package org.apache.arrow.algorithm.sort;

import static org.apache.arrow.vector.complex.BaseRepeatedValueVector.OFFSET_WIDTH;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
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
import org.apache.arrow.vector.LargeVarBinaryVector;
import org.apache.arrow.vector.LargeVarCharVector;
import org.apache.arrow.vector.NullVector;
import org.apache.arrow.vector.SmallIntVector;
import org.apache.arrow.vector.TimeMicroVector;
import org.apache.arrow.vector.TimeMilliVector;
import org.apache.arrow.vector.TimeNanoVector;
import org.apache.arrow.vector.TimeSecVector;
import org.apache.arrow.vector.TimeStampMilliVector;
import org.apache.arrow.vector.TimeStampVector;
import org.apache.arrow.vector.TinyIntVector;
import org.apache.arrow.vector.UInt1Vector;
import org.apache.arrow.vector.UInt2Vector;
import org.apache.arrow.vector.UInt4Vector;
import org.apache.arrow.vector.UInt8Vector;
import org.apache.arrow.vector.ValueVector;
import org.apache.arrow.vector.VarBinaryVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.complex.FixedSizeListVector;
import org.apache.arrow.vector.complex.LargeListVector;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.testing.ValueVectorDataPopulator;
import org.apache.arrow.vector.types.TimeUnit;
import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/** Test cases for {@link DefaultVectorComparators}. */
public class TestDefaultVectorComparator {

  private BufferAllocator allocator;

  @BeforeEach
  public void prepare() {
    allocator = new RootAllocator(1024 * 1024);
  }

  @AfterEach
  public void shutdown() {
    allocator.close();
  }

  private ListVector createListVector(int count) {
    ListVector listVector = ListVector.empty("list vector", allocator);
    Types.MinorType type = Types.MinorType.INT;
    listVector.addOrGetVector(FieldType.nullable(type.getType()));
    listVector.allocateNew();

    IntVector dataVector = (IntVector) listVector.getDataVector();

    for (int i = 0; i < count; i++) {
      dataVector.set(i, i);
    }
    dataVector.setValueCount(count);

    listVector.setNotNull(0);

    listVector.getOffsetBuffer().setInt(0, 0);
    listVector.getOffsetBuffer().setInt(OFFSET_WIDTH, count);

    listVector.setLastSet(0);
    listVector.setValueCount(1);

    return listVector;
  }

  @Test
  public void testCompareLists() {
    try (ListVector listVector1 = createListVector(10);
        ListVector listVector2 = createListVector(11)) {
      VectorValueComparator<ListVector> comparator =
          DefaultVectorComparators.createDefaultComparator(listVector1);
      comparator.attachVectors(listVector1, listVector2);

      // prefix is smaller
      assertTrue(comparator.compare(0, 0) < 0);
    }

    try (ListVector listVector1 = createListVector(11);
        ListVector listVector2 = createListVector(11)) {
      ((IntVector) listVector2.getDataVector()).set(10, 110);

      VectorValueComparator<ListVector> comparator =
          DefaultVectorComparators.createDefaultComparator(listVector1);
      comparator.attachVectors(listVector1, listVector2);

      // breaking tie by the last element
      assertTrue(comparator.compare(0, 0) < 0);
    }

    try (ListVector listVector1 = createListVector(10);
        ListVector listVector2 = createListVector(10)) {

      VectorValueComparator<ListVector> comparator =
          DefaultVectorComparators.createDefaultComparator(listVector1);
      comparator.attachVectors(listVector1, listVector2);

      // list vector elements equal
      assertTrue(comparator.compare(0, 0) == 0);
    }
  }

  @Test
  public void testCopiedComparatorForLists() {
    for (int i = 1; i < 10; i++) {
      for (int j = 1; j < 10; j++) {
        try (ListVector listVector1 = createListVector(10);
            ListVector listVector2 = createListVector(11)) {
          VectorValueComparator<ListVector> comparator =
              DefaultVectorComparators.createDefaultComparator(listVector1);
          comparator.attachVectors(listVector1, listVector2);

          VectorValueComparator<ListVector> copyComparator = comparator.createNew();
          copyComparator.attachVectors(listVector1, listVector2);

          assertEquals(comparator.compare(0, 0), copyComparator.compare(0, 0));
        }
      }
    }
  }

  private FixedSizeListVector createFixedSizeListVector(int count) {
    FixedSizeListVector listVector = FixedSizeListVector.empty("list vector", count, allocator);
    Types.MinorType type = Types.MinorType.INT;
    listVector.addOrGetVector(FieldType.nullable(type.getType()));
    listVector.allocateNew();

    IntVector dataVector = (IntVector) listVector.getDataVector();

    for (int i = 0; i < count; i++) {
      dataVector.set(i, i);
    }
    dataVector.setValueCount(count);

    listVector.setNotNull(0);
    listVector.setValueCount(1);

    return listVector;
  }

  @Test
  public void testCompareFixedSizeLists() {
    try (FixedSizeListVector listVector1 = createFixedSizeListVector(10);
        FixedSizeListVector listVector2 = createFixedSizeListVector(11)) {
      VectorValueComparator<FixedSizeListVector> comparator =
          DefaultVectorComparators.createDefaultComparator(listVector1);
      comparator.attachVectors(listVector1, listVector2);

      // prefix is smaller
      assertTrue(comparator.compare(0, 0) < 0);
    }

    try (FixedSizeListVector listVector1 = createFixedSizeListVector(11);
        FixedSizeListVector listVector2 = createFixedSizeListVector(11)) {
      ((IntVector) listVector2.getDataVector()).set(10, 110);

      VectorValueComparator<FixedSizeListVector> comparator =
          DefaultVectorComparators.createDefaultComparator(listVector1);
      comparator.attachVectors(listVector1, listVector2);

      // breaking tie by the last element
      assertTrue(comparator.compare(0, 0) < 0);
    }

    try (FixedSizeListVector listVector1 = createFixedSizeListVector(10);
        FixedSizeListVector listVector2 = createFixedSizeListVector(10)) {

      VectorValueComparator<FixedSizeListVector> comparator =
          DefaultVectorComparators.createDefaultComparator(listVector1);
      comparator.attachVectors(listVector1, listVector2);

      // list vector elements equal
      assertTrue(comparator.compare(0, 0) == 0);
    }
  }

  @Test
  public void testCompareUInt1() {
    try (UInt1Vector vec = new UInt1Vector("", allocator)) {
      vec.allocateNew(10);
      vec.setValueCount(10);

      vec.setNull(0);
      vec.set(1, -2);
      vec.set(2, -1);
      vec.set(3, 0);
      vec.set(4, 1);
      vec.set(5, 2);
      vec.set(6, -2);
      vec.setNull(7);
      vec.set(8, Byte.MAX_VALUE);
      vec.set(9, Byte.MIN_VALUE);

      VectorValueComparator<UInt1Vector> comparator =
          DefaultVectorComparators.createDefaultComparator(vec);
      comparator.attachVector(vec);

      assertTrue(comparator.compare(0, 1) < 0);
      assertTrue(comparator.compare(1, 2) < 0);
      assertTrue(comparator.compare(1, 3) > 0);
      assertTrue(comparator.compare(2, 5) > 0);
      assertTrue(comparator.compare(4, 5) < 0);
      assertTrue(comparator.compare(1, 6) == 0);
      assertTrue(comparator.compare(0, 7) == 0);
      assertTrue(comparator.compare(8, 9) < 0);
      assertTrue(comparator.compare(4, 8) < 0);
      assertTrue(comparator.compare(5, 9) < 0);
      assertTrue(comparator.compare(2, 9) > 0);
    }
  }

  @Test
  public void testCompareUInt2() {
    try (UInt2Vector vec = new UInt2Vector("", allocator)) {
      vec.allocateNew(10);

      ValueVectorDataPopulator.setVector(
          vec,
          null,
          (char) (Character.MAX_VALUE - 1),
          Character.MAX_VALUE,
          (char) 0,
          (char) 1,
          (char) 2,
          (char) (Character.MAX_VALUE - 1),
          null,
          '\u7FFF', // value for the max 16-byte signed integer
          '\u8000' // value for the min 16-byte signed integer
          );

      VectorValueComparator<UInt2Vector> comparator =
          DefaultVectorComparators.createDefaultComparator(vec);
      comparator.attachVector(vec);

      assertTrue(comparator.compare(0, 1) < 0);
      assertTrue(comparator.compare(1, 2) < 0);
      assertTrue(comparator.compare(1, 3) > 0);
      assertTrue(comparator.compare(2, 5) > 0);
      assertTrue(comparator.compare(4, 5) < 0);
      assertEquals(0, comparator.compare(1, 6));
      assertEquals(0, comparator.compare(0, 7));
      assertTrue(comparator.compare(8, 9) < 0);
      assertTrue(comparator.compare(4, 8) < 0);
      assertTrue(comparator.compare(5, 9) < 0);
      assertTrue(comparator.compare(2, 9) > 0);
    }
  }

  @Test
  public void testCompareUInt4() {
    try (UInt4Vector vec = new UInt4Vector("", allocator)) {
      vec.allocateNew(10);
      vec.setValueCount(10);

      vec.setNull(0);
      vec.set(1, -2);
      vec.set(2, -1);
      vec.set(3, 0);
      vec.set(4, 1);
      vec.set(5, 2);
      vec.set(6, -2);
      vec.setNull(7);
      vec.set(8, Integer.MAX_VALUE);
      vec.set(9, Integer.MIN_VALUE);

      VectorValueComparator<UInt4Vector> comparator =
          DefaultVectorComparators.createDefaultComparator(vec);
      comparator.attachVector(vec);

      assertTrue(comparator.compare(0, 1) < 0);
      assertTrue(comparator.compare(1, 2) < 0);
      assertTrue(comparator.compare(1, 3) > 0);
      assertTrue(comparator.compare(2, 5) > 0);
      assertTrue(comparator.compare(4, 5) < 0);
      assertTrue(comparator.compare(1, 6) == 0);
      assertTrue(comparator.compare(0, 7) == 0);
      assertTrue(comparator.compare(8, 9) < 0);
      assertTrue(comparator.compare(4, 8) < 0);
      assertTrue(comparator.compare(5, 9) < 0);
      assertTrue(comparator.compare(2, 9) > 0);
    }
  }

  @Test
  public void testCompareUInt8() {
    try (UInt8Vector vec = new UInt8Vector("", allocator)) {
      vec.allocateNew(10);
      vec.setValueCount(10);

      vec.setNull(0);
      vec.set(1, -2);
      vec.set(2, -1);
      vec.set(3, 0);
      vec.set(4, 1);
      vec.set(5, 2);
      vec.set(6, -2);
      vec.setNull(7);
      vec.set(8, Long.MAX_VALUE);
      vec.set(9, Long.MIN_VALUE);

      VectorValueComparator<UInt8Vector> comparator =
          DefaultVectorComparators.createDefaultComparator(vec);
      comparator.attachVector(vec);

      assertTrue(comparator.compare(0, 1) < 0);
      assertTrue(comparator.compare(1, 2) < 0);
      assertTrue(comparator.compare(1, 3) > 0);
      assertTrue(comparator.compare(2, 5) > 0);
      assertTrue(comparator.compare(4, 5) < 0);
      assertTrue(comparator.compare(1, 6) == 0);
      assertTrue(comparator.compare(0, 7) == 0);
      assertTrue(comparator.compare(8, 9) < 0);
      assertTrue(comparator.compare(4, 8) < 0);
      assertTrue(comparator.compare(5, 9) < 0);
      assertTrue(comparator.compare(2, 9) > 0);
    }
  }

  @Test
  public void testCompareFloat4() {
    try (Float4Vector vec = new Float4Vector("", allocator)) {
      vec.allocateNew(9);
      ValueVectorDataPopulator.setVector(
          vec,
          -1.1f,
          0.0f,
          1.0f,
          null,
          1.0f,
          2.0f,
          Float.NaN,
          Float.NaN,
          Float.POSITIVE_INFINITY,
          Float.NEGATIVE_INFINITY);

      VectorValueComparator<Float4Vector> comparator =
          DefaultVectorComparators.createDefaultComparator(vec);
      comparator.attachVector(vec);

      assertTrue(comparator.compare(0, 1) < 0);
      assertTrue(comparator.compare(0, 2) < 0);
      assertTrue(comparator.compare(2, 1) > 0);

      // test equality
      assertTrue(comparator.compare(5, 5) == 0);
      assertTrue(comparator.compare(2, 4) == 0);

      // null first
      assertTrue(comparator.compare(3, 4) < 0);
      assertTrue(comparator.compare(5, 3) > 0);
      assertTrue(comparator.compare(8, 3) > 0);

      // NaN behavior.
      assertTrue(comparator.compare(6, 7) == 0);
      assertTrue(comparator.compare(7, 6) == 0);
      assertTrue(comparator.compare(7, 7) == 0);
      assertTrue(comparator.compare(6, 0) > 0);
      assertTrue(comparator.compare(6, 8) > 0);
      assertTrue(comparator.compare(6, 3) > 0);
    }
  }

  @Test
  public void testCompareFloat8() {
    try (Float8Vector vec = new Float8Vector("", allocator)) {
      vec.allocateNew(9);
      ValueVectorDataPopulator.setVector(
          vec,
          -1.1,
          0.0,
          1.0,
          null,
          1.0,
          2.0,
          Double.NaN,
          Double.NaN,
          Double.POSITIVE_INFINITY,
          Double.NEGATIVE_INFINITY);

      VectorValueComparator<Float8Vector> comparator =
          DefaultVectorComparators.createDefaultComparator(vec);
      comparator.attachVector(vec);

      assertTrue(comparator.compare(0, 1) < 0);
      assertTrue(comparator.compare(0, 2) < 0);
      assertTrue(comparator.compare(2, 1) > 0);

      // test equality
      assertTrue(comparator.compare(5, 5) == 0);
      assertTrue(comparator.compare(2, 4) == 0);

      // null first
      assertTrue(comparator.compare(3, 4) < 0);
      assertTrue(comparator.compare(5, 3) > 0);
      assertTrue(comparator.compare(8, 3) > 0);

      // NaN behavior.
      assertTrue(comparator.compare(6, 7) == 0);
      assertTrue(comparator.compare(7, 6) == 0);
      assertTrue(comparator.compare(7, 7) == 0);
      assertTrue(comparator.compare(6, 0) > 0);
      assertTrue(comparator.compare(6, 8) > 0);
      assertTrue(comparator.compare(6, 3) > 0);
    }
  }

  @Test
  public void testCompareLong() {
    try (BigIntVector vec = new BigIntVector("", allocator)) {
      vec.allocateNew(8);
      ValueVectorDataPopulator.setVector(
          vec, -1L, 0L, 1L, null, 1L, 5L, Long.MIN_VALUE + 1L, Long.MAX_VALUE);

      VectorValueComparator<BigIntVector> comparator =
          DefaultVectorComparators.createDefaultComparator(vec);
      comparator.attachVector(vec);

      assertTrue(comparator.compare(0, 1) < 0);
      assertTrue(comparator.compare(0, 2) < 0);
      assertTrue(comparator.compare(2, 1) > 0);

      // test equality
      assertTrue(comparator.compare(5, 5) == 0);
      assertTrue(comparator.compare(2, 4) == 0);

      // null first
      assertTrue(comparator.compare(3, 4) < 0);
      assertTrue(comparator.compare(5, 3) > 0);

      // potential overflow
      assertTrue(comparator.compare(6, 7) < 0);
      assertTrue(comparator.compare(7, 6) > 0);
      assertTrue(comparator.compare(7, 7) == 0);
    }
  }

  @Test
  public void testCompareInt() {
    try (IntVector vec = new IntVector("", allocator)) {
      vec.allocateNew(8);
      ValueVectorDataPopulator.setVector(
          vec, -1, 0, 1, null, 1, 5, Integer.MIN_VALUE + 1, Integer.MAX_VALUE);

      VectorValueComparator<IntVector> comparator =
          DefaultVectorComparators.createDefaultComparator(vec);
      comparator.attachVector(vec);

      assertTrue(comparator.compare(0, 1) < 0);
      assertTrue(comparator.compare(0, 2) < 0);
      assertTrue(comparator.compare(2, 1) > 0);

      // test equality
      assertTrue(comparator.compare(5, 5) == 0);
      assertTrue(comparator.compare(2, 4) == 0);

      // null first
      assertTrue(comparator.compare(3, 4) < 0);
      assertTrue(comparator.compare(5, 3) > 0);

      // potential overflow
      assertTrue(comparator.compare(6, 7) < 0);
      assertTrue(comparator.compare(7, 6) > 0);
      assertTrue(comparator.compare(7, 7) == 0);
    }
  }

  @Test
  public void testCompareShort() {
    try (SmallIntVector vec = new SmallIntVector("", allocator)) {
      vec.allocateNew(8);
      ValueVectorDataPopulator.setVector(
          vec,
          (short) -1,
          (short) 0,
          (short) 1,
          null,
          (short) 1,
          (short) 5,
          (short) (Short.MIN_VALUE + 1),
          Short.MAX_VALUE);

      VectorValueComparator<SmallIntVector> comparator =
          DefaultVectorComparators.createDefaultComparator(vec);
      comparator.attachVector(vec);

      assertTrue(comparator.compare(0, 1) < 0);
      assertTrue(comparator.compare(0, 2) < 0);
      assertTrue(comparator.compare(2, 1) > 0);

      // test equality
      assertTrue(comparator.compare(5, 5) == 0);
      assertTrue(comparator.compare(2, 4) == 0);

      // null first
      assertTrue(comparator.compare(3, 4) < 0);
      assertTrue(comparator.compare(5, 3) > 0);

      // potential overflow
      assertTrue(comparator.compare(6, 7) < 0);
      assertTrue(comparator.compare(7, 6) > 0);
      assertTrue(comparator.compare(7, 7) == 0);
    }
  }

  @Test
  public void testCompareByte() {
    try (TinyIntVector vec = new TinyIntVector("", allocator)) {
      vec.allocateNew(8);
      ValueVectorDataPopulator.setVector(
          vec,
          (byte) -1,
          (byte) 0,
          (byte) 1,
          null,
          (byte) 1,
          (byte) 5,
          (byte) (Byte.MIN_VALUE + 1),
          Byte.MAX_VALUE);

      VectorValueComparator<TinyIntVector> comparator =
          DefaultVectorComparators.createDefaultComparator(vec);
      comparator.attachVector(vec);

      assertTrue(comparator.compare(0, 1) < 0);
      assertTrue(comparator.compare(0, 2) < 0);
      assertTrue(comparator.compare(2, 1) > 0);

      // test equality
      assertTrue(comparator.compare(5, 5) == 0);
      assertTrue(comparator.compare(2, 4) == 0);

      // null first
      assertTrue(comparator.compare(3, 4) < 0);
      assertTrue(comparator.compare(5, 3) > 0);

      // potential overflow
      assertTrue(comparator.compare(6, 7) < 0);
      assertTrue(comparator.compare(7, 6) > 0);
      assertTrue(comparator.compare(7, 7) == 0);
    }
  }

  @Test
  public void testCompareBit() {
    try (BitVector vec = new BitVector("", allocator)) {
      vec.allocateNew(6);
      ValueVectorDataPopulator.setVector(vec, 1, 2, 0, 0, -1, null);

      VectorValueComparator<BitVector> comparator =
          DefaultVectorComparators.createDefaultComparator(vec);
      comparator.attachVector(vec);

      assertTrue(comparator.compare(0, 1) == 0);
      assertTrue(comparator.compare(0, 2) > 0);
      assertTrue(comparator.compare(0, 4) == 0);
      assertTrue(comparator.compare(2, 1) < 0);
      assertTrue(comparator.compare(2, 4) < 0);

      // null first
      assertTrue(comparator.compare(5, 0) < 0);
      assertTrue(comparator.compare(5, 2) < 0);
    }
  }

  @Test
  public void testCompareDateDay() {
    try (DateDayVector vec = new DateDayVector("", allocator)) {
      vec.allocateNew(8);
      ValueVectorDataPopulator.setVector(
          vec, -1, 0, 1, null, 1, 5, Integer.MIN_VALUE + 1, Integer.MAX_VALUE);

      VectorValueComparator<DateDayVector> comparator =
          DefaultVectorComparators.createDefaultComparator(vec);
      comparator.attachVector(vec);

      assertTrue(comparator.compare(0, 1) < 0);
      assertTrue(comparator.compare(0, 2) < 0);
      assertTrue(comparator.compare(2, 1) > 0);

      // test equality
      assertTrue(comparator.compare(5, 5) == 0);
      assertTrue(comparator.compare(2, 4) == 0);

      // null first
      assertTrue(comparator.compare(3, 4) < 0);
      assertTrue(comparator.compare(5, 3) > 0);

      // potential overflow
      assertTrue(comparator.compare(6, 7) < 0);
      assertTrue(comparator.compare(7, 6) > 0);
      assertTrue(comparator.compare(7, 7) == 0);
    }
  }

  @Test
  public void testCompareDateMilli() {
    try (DateMilliVector vec = new DateMilliVector("", allocator)) {
      vec.allocateNew(8);
      ValueVectorDataPopulator.setVector(
          vec, -1L, 0L, 1L, null, 1L, 5L, Long.MIN_VALUE + 1L, Long.MAX_VALUE);

      VectorValueComparator<DateMilliVector> comparator =
          DefaultVectorComparators.createDefaultComparator(vec);
      comparator.attachVector(vec);

      assertTrue(comparator.compare(0, 1) < 0);
      assertTrue(comparator.compare(0, 2) < 0);
      assertTrue(comparator.compare(2, 1) > 0);

      // test equality
      assertTrue(comparator.compare(5, 5) == 0);
      assertTrue(comparator.compare(2, 4) == 0);

      // null first
      assertTrue(comparator.compare(3, 4) < 0);
      assertTrue(comparator.compare(5, 3) > 0);

      // potential overflow
      assertTrue(comparator.compare(6, 7) < 0);
      assertTrue(comparator.compare(7, 6) > 0);
      assertTrue(comparator.compare(7, 7) == 0);
    }
  }

  @Test
  public void testCompareDecimal() {
    try (DecimalVector vec = new DecimalVector("", allocator, 10, 1)) {
      vec.allocateNew(8);
      ValueVectorDataPopulator.setVector(
          vec, -1L, 0L, 1L, null, 1L, 5L, Long.MIN_VALUE + 1L, Long.MAX_VALUE);

      VectorValueComparator<DecimalVector> comparator =
          DefaultVectorComparators.createDefaultComparator(vec);
      comparator.attachVector(vec);

      assertTrue(comparator.compare(0, 1) < 0);
      assertTrue(comparator.compare(0, 2) < 0);
      assertTrue(comparator.compare(2, 1) > 0);

      // test equality
      assertTrue(comparator.compare(5, 5) == 0);
      assertTrue(comparator.compare(2, 4) == 0);

      // null first
      assertTrue(comparator.compare(3, 4) < 0);
      assertTrue(comparator.compare(5, 3) > 0);

      // potential overflow
      assertTrue(comparator.compare(6, 7) < 0);
      assertTrue(comparator.compare(7, 6) > 0);
      assertTrue(comparator.compare(7, 7) == 0);
    }
  }

  @Test
  public void testCompareDecimal256() {
    try (Decimal256Vector vec = new Decimal256Vector("", allocator, 10, 1)) {
      vec.allocateNew(8);
      ValueVectorDataPopulator.setVector(
          vec, -1L, 0L, 1L, null, 1L, 5L, Long.MIN_VALUE + 1L, Long.MAX_VALUE);

      VectorValueComparator<Decimal256Vector> comparator =
          DefaultVectorComparators.createDefaultComparator(vec);
      comparator.attachVector(vec);

      assertTrue(comparator.compare(0, 1) < 0);
      assertTrue(comparator.compare(0, 2) < 0);
      assertTrue(comparator.compare(2, 1) > 0);

      // test equality
      assertTrue(comparator.compare(5, 5) == 0);
      assertTrue(comparator.compare(2, 4) == 0);

      // null first
      assertTrue(comparator.compare(3, 4) < 0);
      assertTrue(comparator.compare(5, 3) > 0);

      // potential overflow
      assertTrue(comparator.compare(6, 7) < 0);
      assertTrue(comparator.compare(7, 6) > 0);
      assertTrue(comparator.compare(7, 7) == 0);
    }
  }

  @Test
  public void testCompareDuration() {
    try (DurationVector vec =
        new DurationVector(
            "", FieldType.nullable(new ArrowType.Duration(TimeUnit.MILLISECOND)), allocator)) {
      vec.allocateNew(8);
      ValueVectorDataPopulator.setVector(
          vec, -1L, 0L, 1L, null, 1L, 5L, Long.MIN_VALUE + 1L, Long.MAX_VALUE);

      VectorValueComparator<DurationVector> comparator =
          DefaultVectorComparators.createDefaultComparator(vec);
      comparator.attachVector(vec);

      assertTrue(comparator.compare(0, 1) < 0);
      assertTrue(comparator.compare(0, 2) < 0);
      assertTrue(comparator.compare(2, 1) > 0);

      // test equality
      assertTrue(comparator.compare(5, 5) == 0);
      assertTrue(comparator.compare(2, 4) == 0);

      // null first
      assertTrue(comparator.compare(3, 4) < 0);
      assertTrue(comparator.compare(5, 3) > 0);

      // potential overflow
      assertTrue(comparator.compare(6, 7) < 0);
      assertTrue(comparator.compare(7, 6) > 0);
      assertTrue(comparator.compare(7, 7) == 0);
    }
  }

  @Test
  public void testCompareIntervalDay() {
    try (IntervalDayVector vec =
        new IntervalDayVector(
            "", FieldType.nullable(new ArrowType.Duration(TimeUnit.MILLISECOND)), allocator)) {
      vec.allocateNew(8);
      vec.set(0, -1, 0);
      vec.set(1, 0, 0);
      vec.set(2, 1, 0);
      vec.setNull(3);
      vec.set(4, -1, -1);
      vec.set(5, 1, 1);
      vec.set(6, 1, 1);
      vec.set(7, -1, -1);

      VectorValueComparator<IntervalDayVector> comparator =
          DefaultVectorComparators.createDefaultComparator(vec);
      comparator.attachVector(vec);

      assertTrue(comparator.compare(0, 1) < 0);
      assertTrue(comparator.compare(0, 2) < 0);
      assertTrue(comparator.compare(2, 1) > 0);
      assertTrue(comparator.compare(2, 5) < 0);
      assertTrue(comparator.compare(0, 4) > 0);

      // test equality
      assertTrue(comparator.compare(5, 6) == 0);
      assertTrue(comparator.compare(4, 7) == 0);

      // null first
      assertTrue(comparator.compare(3, 4) < 0);
      assertTrue(comparator.compare(5, 3) > 0);
    }
  }

  @Test
  public void testCompareTimeMicro() {
    try (TimeMicroVector vec = new TimeMicroVector("", allocator)) {
      vec.allocateNew(8);
      ValueVectorDataPopulator.setVector(
          vec, -1L, 0L, 1L, null, 1L, 5L, Long.MIN_VALUE + 1L, Long.MAX_VALUE);

      VectorValueComparator<TimeMicroVector> comparator =
          DefaultVectorComparators.createDefaultComparator(vec);
      comparator.attachVector(vec);

      assertTrue(comparator.compare(0, 1) < 0);
      assertTrue(comparator.compare(0, 2) < 0);
      assertTrue(comparator.compare(2, 1) > 0);

      // test equality
      assertTrue(comparator.compare(5, 5) == 0);
      assertTrue(comparator.compare(2, 4) == 0);

      // null first
      assertTrue(comparator.compare(3, 4) < 0);
      assertTrue(comparator.compare(5, 3) > 0);

      // potential overflow
      assertTrue(comparator.compare(6, 7) < 0);
      assertTrue(comparator.compare(7, 6) > 0);
      assertTrue(comparator.compare(7, 7) == 0);
    }
  }

  @Test
  public void testCompareTimeMilli() {
    try (TimeMilliVector vec = new TimeMilliVector("", allocator)) {
      vec.allocateNew(8);
      ValueVectorDataPopulator.setVector(
          vec, -1, 0, 1, null, 1, 5, Integer.MIN_VALUE + 1, Integer.MAX_VALUE);

      VectorValueComparator<TimeMilliVector> comparator =
          DefaultVectorComparators.createDefaultComparator(vec);
      comparator.attachVector(vec);

      assertTrue(comparator.compare(0, 1) < 0);
      assertTrue(comparator.compare(0, 2) < 0);
      assertTrue(comparator.compare(2, 1) > 0);

      // test equality
      assertTrue(comparator.compare(5, 5) == 0);
      assertTrue(comparator.compare(2, 4) == 0);

      // null first
      assertTrue(comparator.compare(3, 4) < 0);
      assertTrue(comparator.compare(5, 3) > 0);

      // potential overflow
      assertTrue(comparator.compare(6, 7) < 0);
      assertTrue(comparator.compare(7, 6) > 0);
      assertTrue(comparator.compare(7, 7) == 0);
    }
  }

  @Test
  public void testCompareTimeNano() {
    try (TimeNanoVector vec = new TimeNanoVector("", allocator)) {
      vec.allocateNew(8);
      ValueVectorDataPopulator.setVector(
          vec, -1L, 0L, 1L, null, 1L, 5L, Long.MIN_VALUE + 1L, Long.MAX_VALUE);

      VectorValueComparator<TimeNanoVector> comparator =
          DefaultVectorComparators.createDefaultComparator(vec);
      comparator.attachVector(vec);

      assertTrue(comparator.compare(0, 1) < 0);
      assertTrue(comparator.compare(0, 2) < 0);
      assertTrue(comparator.compare(2, 1) > 0);

      // test equality
      assertTrue(comparator.compare(5, 5) == 0);
      assertTrue(comparator.compare(2, 4) == 0);

      // null first
      assertTrue(comparator.compare(3, 4) < 0);
      assertTrue(comparator.compare(5, 3) > 0);

      // potential overflow
      assertTrue(comparator.compare(6, 7) < 0);
      assertTrue(comparator.compare(7, 6) > 0);
      assertTrue(comparator.compare(7, 7) == 0);
    }
  }

  @Test
  public void testCompareTimeSec() {
    try (TimeSecVector vec = new TimeSecVector("", allocator)) {
      vec.allocateNew(8);
      ValueVectorDataPopulator.setVector(
          vec, -1, 0, 1, null, 1, 5, Integer.MIN_VALUE + 1, Integer.MAX_VALUE);

      VectorValueComparator<TimeSecVector> comparator =
          DefaultVectorComparators.createDefaultComparator(vec);
      comparator.attachVector(vec);

      assertTrue(comparator.compare(0, 1) < 0);
      assertTrue(comparator.compare(0, 2) < 0);
      assertTrue(comparator.compare(2, 1) > 0);

      // test equality
      assertTrue(comparator.compare(5, 5) == 0);
      assertTrue(comparator.compare(2, 4) == 0);

      // null first
      assertTrue(comparator.compare(3, 4) < 0);
      assertTrue(comparator.compare(5, 3) > 0);

      // potential overflow
      assertTrue(comparator.compare(6, 7) < 0);
      assertTrue(comparator.compare(7, 6) > 0);
      assertTrue(comparator.compare(7, 7) == 0);
    }
  }

  @Test
  public void testCompareTimeStamp() {
    try (TimeStampMilliVector vec = new TimeStampMilliVector("", allocator)) {
      vec.allocateNew(8);
      ValueVectorDataPopulator.setVector(
          vec, -1L, 0L, 1L, null, 1L, 5L, Long.MIN_VALUE + 1L, Long.MAX_VALUE);

      VectorValueComparator<TimeStampVector> comparator =
          DefaultVectorComparators.createDefaultComparator(vec);
      comparator.attachVector(vec);

      assertTrue(comparator.compare(0, 1) < 0);
      assertTrue(comparator.compare(0, 2) < 0);
      assertTrue(comparator.compare(2, 1) > 0);

      // test equality
      assertTrue(comparator.compare(5, 5) == 0);
      assertTrue(comparator.compare(2, 4) == 0);

      // null first
      assertTrue(comparator.compare(3, 4) < 0);
      assertTrue(comparator.compare(5, 3) > 0);

      // potential overflow
      assertTrue(comparator.compare(6, 7) < 0);
      assertTrue(comparator.compare(7, 6) > 0);
      assertTrue(comparator.compare(7, 7) == 0);
    }
  }

  @Test
  public void testCompareFixedSizeBinary() {
    try (FixedSizeBinaryVector vector1 = new FixedSizeBinaryVector("test1", allocator, 2);
        FixedSizeBinaryVector vector2 = new FixedSizeBinaryVector("test1", allocator, 3)) {
      vector1.allocateNew();
      vector2.allocateNew();
      vector1.set(0, new byte[] {1, 1});
      vector2.set(0, new byte[] {1, 1, 0});
      VectorValueComparator<FixedSizeBinaryVector> comparator =
          DefaultVectorComparators.createDefaultComparator(vector1);
      comparator.attachVectors(vector1, vector2);

      // prefix is smaller
      assertTrue(comparator.compare(0, 0) < 0);
    }

    try (FixedSizeBinaryVector vector1 = new FixedSizeBinaryVector("test1", allocator, 3);
        FixedSizeBinaryVector vector2 = new FixedSizeBinaryVector("test1", allocator, 3)) {
      vector1.allocateNew();
      vector2.allocateNew();
      vector1.set(0, new byte[] {1, 1, 0});
      vector2.set(0, new byte[] {1, 1, 1});
      VectorValueComparator<FixedSizeBinaryVector> comparator =
          DefaultVectorComparators.createDefaultComparator(vector1);
      comparator.attachVectors(vector1, vector2);

      // breaking tie by the last element
      assertTrue(comparator.compare(0, 0) < 0);
    }

    try (FixedSizeBinaryVector vector1 = new FixedSizeBinaryVector("test1", allocator, 3);
        FixedSizeBinaryVector vector2 = new FixedSizeBinaryVector("test1", allocator, 3)) {
      vector1.allocateNew();
      vector2.allocateNew();
      vector1.set(0, new byte[] {1, 1, 1});
      vector2.set(0, new byte[] {1, 1, 1});
      VectorValueComparator<FixedSizeBinaryVector> comparator =
          DefaultVectorComparators.createDefaultComparator(vector1);
      comparator.attachVectors(vector1, vector2);

      // list vector elements equal
      assertTrue(comparator.compare(0, 0) == 0);
    }
  }

  @Test
  public void testCompareNull() {
    try (NullVector vec =
        new NullVector("test", FieldType.notNullable(new ArrowType.Int(32, false)))) {
      vec.setValueCount(2);

      VectorValueComparator<NullVector> comparator =
          DefaultVectorComparators.createDefaultComparator(vec);
      comparator.attachVector(vec);
      assertEquals(DefaultVectorComparators.NullComparator.class, comparator.getClass());
      assertEquals(0, comparator.compare(0, 1));
    }
  }

  @Test
  public void testCheckNullsOnCompareIsFalseForNonNullableVector() {
    try (IntVector vec =
        new IntVector(
            "not nullable", FieldType.notNullable(new ArrowType.Int(32, false)), allocator)) {

      ValueVectorDataPopulator.setVector(vec, 1, 2, 3, 4);

      final VectorValueComparator<IntVector> comparator =
          DefaultVectorComparators.createDefaultComparator(vec);
      comparator.attachVector(vec);

      assertFalse(comparator.checkNullsOnCompare());
    }
  }

  @Test
  public void testCheckNullsOnCompareIsTrueForNullableVector() {
    try (IntVector vec =
            new IntVector("nullable", FieldType.nullable(new ArrowType.Int(32, false)), allocator);
        IntVector vec2 =
            new IntVector(
                "not-nullable", FieldType.notNullable(new ArrowType.Int(32, false)), allocator)) {

      ValueVectorDataPopulator.setVector(vec, 1, null, 3, 4);
      ValueVectorDataPopulator.setVector(vec2, 1, 2, 3, 4);

      final VectorValueComparator<IntVector> comparator =
          DefaultVectorComparators.createDefaultComparator(vec);
      comparator.attachVector(vec);
      assertTrue(comparator.checkNullsOnCompare());

      comparator.attachVectors(vec, vec2);
      assertTrue(comparator.checkNullsOnCompare());
    }
  }

  @Test
  public void testCheckNullsOnCompareIsFalseWithNoNulls() {
    try (IntVector vec =
            new IntVector("nullable", FieldType.nullable(new ArrowType.Int(32, false)), allocator);
        IntVector vec2 =
            new IntVector(
                "also-nullable", FieldType.nullable(new ArrowType.Int(32, false)), allocator)) {

      // no null values
      ValueVectorDataPopulator.setVector(vec, 1, 2, 3, 4);
      ValueVectorDataPopulator.setVector(vec2, 1, 2, 3, 4);

      final VectorValueComparator<IntVector> comparator =
          DefaultVectorComparators.createDefaultComparator(vec);
      comparator.attachVector(vec);
      assertFalse(comparator.checkNullsOnCompare());

      comparator.attachVectors(vec, vec2);
      assertFalse(comparator.checkNullsOnCompare());
    }
  }

  @Test
  public void testCheckNullsOnCompareIsTrueWithEmptyVectors() {
    try (IntVector vec =
            new IntVector("nullable", FieldType.nullable(new ArrowType.Int(32, false)), allocator);
        IntVector vec2 =
            new IntVector(
                "also-nullable", FieldType.nullable(new ArrowType.Int(32, false)), allocator)) {

      final VectorValueComparator<IntVector> comparator =
          DefaultVectorComparators.createDefaultComparator(vec);
      comparator.attachVector(vec2);
      assertTrue(comparator.checkNullsOnCompare());

      comparator.attachVectors(vec, vec2);
      assertTrue(comparator.checkNullsOnCompare());
    }
  }

  @Test
  public void testVariableWidthDefaultComparators() {
    try (VarCharVector vec = new VarCharVector("test", allocator)) {
      verifyVariableWidthComparatorReturned(vec);
    }
    try (VarBinaryVector vec = new VarBinaryVector("test", allocator)) {
      verifyVariableWidthComparatorReturned(vec);
    }
    try (LargeVarCharVector vec = new LargeVarCharVector("test", allocator)) {
      verifyVariableWidthComparatorReturned(vec);
    }
    try (LargeVarBinaryVector vec = new LargeVarBinaryVector("test", allocator)) {
      verifyVariableWidthComparatorReturned(vec);
    }
  }

  private static <V extends ValueVector> void verifyVariableWidthComparatorReturned(V vec) {
    VectorValueComparator<V> comparator = DefaultVectorComparators.createDefaultComparator(vec);
    assertEquals(DefaultVectorComparators.VariableWidthComparator.class, comparator.getClass());
  }

  @Test
  public void testRepeatedDefaultComparators() {
    final FieldType type = FieldType.nullable(Types.MinorType.INT.getType());
    try (final LargeListVector vector = new LargeListVector("list", allocator, type, null)) {
      vector.addOrGetVector(FieldType.nullable(type.getType()));
      verifyRepeatedComparatorReturned(vector);
    }
  }

  private static <V extends ValueVector> void verifyRepeatedComparatorReturned(V vec) {
    VectorValueComparator<V> comparator = DefaultVectorComparators.createDefaultComparator(vec);
    assertEquals(DefaultVectorComparators.RepeatedValueComparator.class, comparator.getClass());
  }
}
