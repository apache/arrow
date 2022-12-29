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
import static org.junit.Assert.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.SmallIntVector;
import org.apache.arrow.vector.TinyIntVector;
import org.apache.arrow.vector.UInt1Vector;
import org.apache.arrow.vector.UInt2Vector;
import org.apache.arrow.vector.UInt4Vector;
import org.apache.arrow.vector.UInt8Vector;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.testing.ValueVectorDataPopulator;
import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * Test cases for {@link DefaultVectorComparators}.
 */
public class TestDefaultVectorComparator {

  private BufferAllocator allocator;

  @Before
  public void prepare() {
    allocator = new RootAllocator(1024 * 1024);
  }

  @After
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
          vec, null, (char) -2, (char) -1, (char) 0, (char) 1, (char) 2, (char) -2, null,
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
      assertTrue(comparator.compare(1, 6) == 0);
      assertTrue(comparator.compare(0, 7) == 0);
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
          vec, (short) -1, (short) 0, (short) 1, null, (short) 1, (short) 5,
          (short) (Short.MIN_VALUE + 1), Short.MAX_VALUE);

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
          vec, (byte) -1, (byte) 0, (byte) 1, null, (byte) 1, (byte) 5,
          (byte) (Byte.MIN_VALUE + 1), Byte.MAX_VALUE);

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
  public void testCheckNullsOnCompareIsFalseForNonNullableVector() {
    try (IntVector vec = new IntVector("not nullable",
            FieldType.notNullable(new ArrowType.Int(32, false)), allocator)) {

      ValueVectorDataPopulator.setVector(vec, 1, 2, 3, 4);

      final VectorValueComparator<IntVector> comparator = DefaultVectorComparators.createDefaultComparator(vec);
      comparator.attachVector(vec);

      assertFalse(comparator.checkNullsOnCompare());
    }
  }

  @Test
  public void testCheckNullsOnCompareIsTrueForNullableVector() {
    try (IntVector vec = new IntVector("nullable", FieldType.nullable(
            new ArrowType.Int(32, false)), allocator);
         IntVector vec2 = new IntVector("not-nullable", FieldType.notNullable(
                 new ArrowType.Int(32, false)), allocator)
    ) {

      ValueVectorDataPopulator.setVector(vec, 1, null, 3, 4);
      ValueVectorDataPopulator.setVector(vec2, 1, 2, 3, 4);

      final VectorValueComparator<IntVector> comparator = DefaultVectorComparators.createDefaultComparator(vec);
      comparator.attachVector(vec);
      assertTrue(comparator.checkNullsOnCompare());

      comparator.attachVectors(vec, vec2);
      assertTrue(comparator.checkNullsOnCompare());
    }
  }

  @Test
  public void testCheckNullsOnCompareIsFalseWithNoNulls() {
    try (IntVector vec = new IntVector("nullable", FieldType.nullable(
            new ArrowType.Int(32, false)), allocator);
         IntVector vec2 = new IntVector("also-nullable", FieldType.nullable(
                 new ArrowType.Int(32, false)), allocator)
    ) {

      // no null values
      ValueVectorDataPopulator.setVector(vec, 1, 2, 3, 4);
      ValueVectorDataPopulator.setVector(vec2, 1, 2, 3, 4);

      final VectorValueComparator<IntVector> comparator = DefaultVectorComparators.createDefaultComparator(vec);
      comparator.attachVector(vec);
      assertFalse(comparator.checkNullsOnCompare());

      comparator.attachVectors(vec, vec2);
      assertFalse(comparator.checkNullsOnCompare());
    }
  }

  @Test
  public void testCheckNullsOnCompareIsTrueWithEmptyVectors() {
    try (IntVector vec = new IntVector("nullable", FieldType.nullable(
            new ArrowType.Int(32, false)), allocator);
         IntVector vec2 = new IntVector("also-nullable", FieldType.nullable(
                 new ArrowType.Int(32, false)), allocator)
    ) {

      final VectorValueComparator<IntVector> comparator = DefaultVectorComparators.createDefaultComparator(vec);
      comparator.attachVector(vec2);
      assertTrue(comparator.checkNullsOnCompare());

      comparator.attachVectors(vec, vec2);
      assertTrue(comparator.checkNullsOnCompare());
    }
  }
}
