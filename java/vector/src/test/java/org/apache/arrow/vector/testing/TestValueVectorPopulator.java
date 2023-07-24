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

package org.apache.arrow.vector.testing;

import static junit.framework.TestCase.assertTrue;
import static org.apache.arrow.vector.testing.ValueVectorDataPopulator.setVector;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.BitVector;
import org.apache.arrow.vector.DateDayVector;
import org.apache.arrow.vector.DateMilliVector;
import org.apache.arrow.vector.DecimalVector;
import org.apache.arrow.vector.DurationVector;
import org.apache.arrow.vector.FixedSizeBinaryVector;
import org.apache.arrow.vector.Float4Vector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.IntervalYearVector;
import org.apache.arrow.vector.SmallIntVector;
import org.apache.arrow.vector.TimeMicroVector;
import org.apache.arrow.vector.TimeMilliVector;
import org.apache.arrow.vector.TimeNanoVector;
import org.apache.arrow.vector.TimeSecVector;
import org.apache.arrow.vector.TimeStampMicroVector;
import org.apache.arrow.vector.TimeStampMilliVector;
import org.apache.arrow.vector.TimeStampNanoVector;
import org.apache.arrow.vector.TimeStampSecVector;
import org.apache.arrow.vector.TinyIntVector;
import org.apache.arrow.vector.UInt1Vector;
import org.apache.arrow.vector.UInt2Vector;
import org.apache.arrow.vector.UInt4Vector;
import org.apache.arrow.vector.UInt8Vector;
import org.apache.arrow.vector.VarBinaryVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.compare.VectorEqualsVisitor;
import org.apache.arrow.vector.testing.ValueVectorDataPopulator;
import org.apache.arrow.vector.types.TimeUnit;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class TestValueVectorPopulator {

  private BufferAllocator allocator;

  @Before
  public void init() {
    allocator = new RootAllocator(Long.MAX_VALUE);
  }

  @After
  public void terminate() throws Exception {
    allocator.close();
  }

  @Test
  public void testPopulateBigIntVector() {
    try (final BigIntVector vector1 = new BigIntVector("vector", allocator);
         final BigIntVector vector2 = new BigIntVector("vector", allocator)) {

      vector1.allocateNew(10);
      for (int i = 0; i < 10; i++) {
        if (i % 2 == 0) {
          vector1.setNull(i);
        } else {
          vector1.set(i, i);
        }
      }
      vector1.setValueCount(10);

      setVector(vector2, null, 1L, null, 3L, null, 5L, null, 7L, null, 9L);
      assertTrue(VectorEqualsVisitor.vectorEquals(vector1, vector2));
    }
  }

  @Test
  public void testPopulateBitVector() {
    try (final BitVector vector1 = new BitVector("vector", allocator);
         final BitVector vector2 = new BitVector("vector", allocator)) {

      vector1.allocateNew(10);
      for (int i = 0; i < 10; i++) {
        if (i % 2 == 0) {
          vector1.setNull(i);
        } else {
          vector1.set(i, i > 5 ? 0 : 1);
        }
      }
      vector1.setValueCount(10);

      setVector(vector2, null, 1, null, 1, null, 1, null, 0, null, 0);
      assertTrue(VectorEqualsVisitor.vectorEquals(vector1, vector2));
    }
  }

  @Test
  public void testPopulateDateDayVector() {
    try (final DateDayVector vector1 = new DateDayVector("vector", allocator);
         final DateDayVector vector2 = new DateDayVector("vector", allocator)) {

      vector1.allocateNew(10);
      for (int i = 0; i < 10; i++) {
        if (i % 2 == 0) {
          vector1.setNull(i);
        } else {
          vector1.set(i, i * 10);
        }
      }
      vector1.setValueCount(10);

      setVector(vector2, null, 10, null, 30, null, 50, null, 70, null, 90);
      assertTrue(VectorEqualsVisitor.vectorEquals(vector1, vector2));
    }
  }

  @Test
  public void testPopulateDateMilliVector() {
    try (final DateMilliVector vector1 = new DateMilliVector("vector", allocator);
         final DateMilliVector vector2 = new DateMilliVector("vector", allocator)) {

      vector1.allocateNew(10);
      for (int i = 0; i < 10; i++) {
        if (i % 2 == 0) {
          vector1.setNull(i);
        } else {
          vector1.set(i, i * 1000);
        }
      }
      vector1.setValueCount(10);

      setVector(vector2, null, 1000L, null, 3000L, null, 5000L, null, 7000L, null, 9000L);
      assertTrue(VectorEqualsVisitor.vectorEquals(vector1, vector2));
    }
  }

  @Test
  public void testPopulateDecimalVector() {
    try (final DecimalVector vector1 = new DecimalVector("vector", allocator, 10, 3);
         final DecimalVector vector2 = new DecimalVector("vector", allocator, 10, 3)) {

      vector1.allocateNew(10);
      for (int i = 0; i < 10; i++) {
        if (i % 2 == 0) {
          vector1.setNull(i);
        } else {
          vector1.set(i, i);
        }
      }
      vector1.setValueCount(10);

      setVector(vector2, null, 1L, null, 3L, null, 5L, null, 7L, null, 9L);
      assertTrue(VectorEqualsVisitor.vectorEquals(vector1, vector2));
    }
  }

  @Test
  public void testPopulateDurationVector() {
    final FieldType fieldType = FieldType.nullable(new ArrowType.Duration(TimeUnit.SECOND));
    try (final DurationVector vector1 = new DurationVector("vector", fieldType, allocator);
         final DurationVector vector2 = new DurationVector("vector", fieldType, allocator)) {

      vector1.allocateNew(10);
      for (int i = 0; i < 10; i++) {
        if (i % 2 == 0) {
          vector1.setNull(i);
        } else {
          vector1.set(i, i);
        }
      }
      vector1.setValueCount(10);

      setVector(vector2, null, 1L, null, 3L, null, 5L, null, 7L, null, 9L);

      assertTrue(VectorEqualsVisitor.vectorEquals(vector1, vector2));
    }
  }

  @Test
  public void testPopulateFixedSizeBinaryVector() {
    try (final FixedSizeBinaryVector vector1 = new FixedSizeBinaryVector("vector", allocator, 5);
         final FixedSizeBinaryVector vector2 = new FixedSizeBinaryVector("vector", allocator, 5)) {

      vector1.allocateNew(10);
      for (int i = 0; i < 10; i++) {
        if (i % 2 == 0) {
          vector1.setNull(i);
        } else {
          vector1.set(i, ("test" + i).getBytes());
        }
      }
      vector1.setValueCount(10);

      setVector(vector2, null, "test1".getBytes(), null, "test3".getBytes(), null, "test5".getBytes(), null,
          "test7".getBytes(), null, "test9".getBytes());
      assertTrue(VectorEqualsVisitor.vectorEquals(vector1, vector2));
    }
  }

  @Test
  public void testPopulateFloat4Vector() {
    try (final Float4Vector vector1 = new Float4Vector("vector", allocator);
         final Float4Vector vector2 = new Float4Vector("vector", allocator)) {

      vector1.allocateNew(10);
      for (int i = 0; i < 10; i++) {
        if (i % 2 == 0) {
          vector1.setNull(i);
        } else {
          vector1.set(i, i);
        }
      }
      vector1.setValueCount(10);
      setVector(vector2, null, 1f, null, 3f, null, 5f, null, 7f, null, 9f);
      assertTrue(VectorEqualsVisitor.vectorEquals(vector1, vector2));
    }
  }

  @Test
  public void testPopulateFloat8Vector() {
    try (final Float8Vector vector1 = new Float8Vector("vector", allocator);
         final Float8Vector vector2 = new Float8Vector("vector", allocator)) {

      vector1.allocateNew(10);
      for (int i = 0; i < 10; i++) {
        if (i % 2 == 0) {
          vector1.setNull(i);
        } else {
          vector1.set(i, i);
        }
      }
      vector1.setValueCount(10);
      setVector(vector2, null, 1d, null, 3d, null, 5d, null, 7d, null, 9d);
      assertTrue(VectorEqualsVisitor.vectorEquals(vector1, vector2));
    }
  }

  @Test
  public void testPopulateIntVector() {
    try (final IntVector vector1 = new IntVector("vector", allocator);
         final IntVector vector2 = new IntVector("vector", allocator)) {

      vector1.allocateNew(10);
      for (int i = 0; i < 10; i++) {
        if (i % 2 == 0) {
          vector1.setNull(i);
        } else {
          vector1.set(i, i);
        }
      }
      vector1.setValueCount(10);

      ValueVectorDataPopulator.setVector(vector2, null, 1, null, 3, null, 5, null, 7, null, 9);
      assertTrue(VectorEqualsVisitor.vectorEquals(vector1, vector2));
    }
  }

  @Test
  public void testPopulateSmallIntVector() {
    try (final SmallIntVector vector1 = new SmallIntVector("vector", allocator);
         final SmallIntVector vector2 = new SmallIntVector("vector", allocator)) {

      vector1.allocateNew(10);
      for (int i = 0; i < 10; i++) {
        if (i % 2 == 0) {
          vector1.setNull(i);
        } else {
          vector1.set(i, i);
        }
      }
      vector1.setValueCount(10);

      ValueVectorDataPopulator.setVector(vector2, null, (short) 1, null, (short) 3, null, (short) 5,
          null, (short) 7, null, (short) 9);
      assertTrue(VectorEqualsVisitor.vectorEquals(vector1, vector2));
    }
  }

  @Test
  public void testPopulateIntervalDayVector() {
    try (final IntervalYearVector vector1 = new IntervalYearVector("vector", allocator);
         final IntervalYearVector vector2 = new IntervalYearVector("vector", allocator)) {

      vector1.allocateNew(10);
      for (int i = 0; i < 10; i++) {
        if (i % 2 == 0) {
          vector1.setNull(i);
        } else {
          vector1.set(i, i);
        }
      }
      vector1.setValueCount(10);

      ValueVectorDataPopulator.setVector(vector2, null, 1, null, 3, null, 5, null, 7, null, 9);
      assertTrue(VectorEqualsVisitor.vectorEquals(vector1, vector2));
    }
  }

  @Test
  public void testPopulateTimeMicroVector() {
    try (final TimeMicroVector vector1 = new TimeMicroVector("vector", allocator);
         final TimeMicroVector vector2 = new TimeMicroVector("vector", allocator)) {

      vector1.allocateNew(10);
      for (int i = 0; i < 10; i++) {
        if (i % 2 == 0) {
          vector1.setNull(i);
        } else {
          vector1.set(i, i * 10000);
        }
      }
      vector1.setValueCount(10);
      setVector(vector2, null, 10000L, null, 30000L, null, 50000L, null, 70000L, null, 90000L);
      assertTrue(VectorEqualsVisitor.vectorEquals(vector1, vector2));
    }
  }

  @Test
  public void testPopulateTimeMilliVector() {
    try (final TimeMilliVector vector1 = new TimeMilliVector("vector", allocator);
        final TimeMilliVector vector2 = new TimeMilliVector("vector", allocator)) {

      vector1.allocateNew(10);
      for (int i = 0; i < 10; i++) {
        if (i % 2 == 0) {
          vector1.setNull(i);
        } else {
          vector1.set(i, i * 100);
        }
      }
      vector1.setValueCount(10);
      setVector(vector2, null, 100, null, 300, null, 500, null, 700, null, 900);
      assertTrue(VectorEqualsVisitor.vectorEquals(vector1, vector2));
    }
  }

  @Test
  public void testPopulateTimeNanoVector() {
    try (final TimeNanoVector vector1 = new TimeNanoVector("vector", allocator);
         final TimeNanoVector vector2 = new TimeNanoVector("vector", allocator)) {

      vector1.allocateNew(10);
      for (int i = 0; i < 10; i++) {
        if (i % 2 == 0) {
          vector1.setNull(i);
        } else {
          vector1.set(i, i * 10000);
        }
      }
      vector1.setValueCount(10);
      setVector(vector2, null, 10000L, null, 30000L, null, 50000L, null, 70000L, null, 90000L);
      assertTrue(VectorEqualsVisitor.vectorEquals(vector1, vector2));
    }
  }

  @Test
  public void testPopulateTimeSecVector() {
    try (final TimeSecVector vector1 = new TimeSecVector("vector", allocator);
         final TimeSecVector vector2 = new TimeSecVector("vector", allocator)) {

      vector1.allocateNew(10);
      for (int i = 0; i < 10; i++) {
        if (i % 2 == 0) {
          vector1.setNull(i);
        } else {
          vector1.set(i, i * 100);
        }
      }
      vector1.setValueCount(10);
      setVector(vector2, null, 100, null, 300, null, 500, null, 700, null, 900);
      assertTrue(VectorEqualsVisitor.vectorEquals(vector1, vector2));
    }
  }

  @Test
  public void testPopulateTimeStampMicroVector() {
    try (final TimeStampMicroVector vector1 = new TimeStampMicroVector("vector", allocator);
        final TimeStampMicroVector vector2 = new TimeStampMicroVector("vector", allocator)) {

      vector1.allocateNew(10);
      for (int i = 0; i < 10; i++) {
        if (i % 2 == 0) {
          vector1.setNull(i);
        } else {
          vector1.set(i, i * 10000);
        }
      }
      vector1.setValueCount(10);
      setVector(vector2, null, 10000L, null, 30000L, null, 50000L, null, 70000L, null, 90000L);
      assertTrue(VectorEqualsVisitor.vectorEquals(vector1, vector2));
    }
  }

  @Test
  public void testPopulateTimeStampMilliVector() {
    try (final TimeStampMilliVector vector1 = new TimeStampMilliVector("vector", allocator);
         final TimeStampMilliVector vector2 = new TimeStampMilliVector("vector", allocator)) {

      vector1.allocateNew(10);
      for (int i = 0; i < 10; i++) {
        if (i % 2 == 0) {
          vector1.setNull(i);
        } else {
          vector1.set(i, i * 10000);
        }
      }
      vector1.setValueCount(10);
      setVector(vector2, null, 10000L, null, 30000L, null, 50000L, null, 70000L, null, 90000L);
      assertTrue(VectorEqualsVisitor.vectorEquals(vector1, vector2));
    }
  }

  @Test
  public void testPopulateTimeStampNanoVector() {
    try (final TimeStampNanoVector vector1 = new TimeStampNanoVector("vector", allocator);
        final TimeStampNanoVector vector2 = new TimeStampNanoVector("vector", allocator)) {

      vector1.allocateNew(10);
      for (int i = 0; i < 10; i++) {
        if (i % 2 == 0) {
          vector1.setNull(i);
        } else {
          vector1.set(i, i * 10000);
        }
      }
      vector1.setValueCount(10);
      setVector(vector2, null, 10000L, null, 30000L, null, 50000L, null, 70000L, null, 90000L);
      assertTrue(VectorEqualsVisitor.vectorEquals(vector1, vector2));
    }
  }

  @Test
  public void testPopulateTimeStampSecVector() {
    try (final TimeStampSecVector vector1 = new TimeStampSecVector("vector", allocator);
         final TimeStampSecVector vector2 = new TimeStampSecVector("vector", allocator)) {

      vector1.allocateNew(10);
      for (int i = 0; i < 10; i++) {
        if (i % 2 == 0) {
          vector1.setNull(i);
        } else {
          vector1.set(i, i * 100);
        }
      }
      vector1.setValueCount(10);
      setVector(vector2, null, 100L, null, 300L, null, 500L, null, 700L, null, 900L);
      assertTrue(VectorEqualsVisitor.vectorEquals(vector1, vector2));
    }
  }

  @Test
  public void testPopulateTinyIntVector() {
    try (final TinyIntVector vector1 = new TinyIntVector("vector", allocator);
         final TinyIntVector vector2 = new TinyIntVector("vector", allocator)) {

      vector1.allocateNew(10);
      for (int i = 0; i < 10; i++) {
        if (i % 2 == 0) {
          vector1.setNull(i);
        } else {
          vector1.set(i, i);
        }
      }
      vector1.setValueCount(10);
      setVector(vector2, null, (byte) 1, null, (byte) 3, null, (byte) 5, null, (byte) 7, null, (byte) 9);
      assertTrue(VectorEqualsVisitor.vectorEquals(vector1, vector2));
    }
  }

  @Test
  public void testPopulateUInt1Vector() {
    try (final UInt1Vector vector1 = new UInt1Vector("vector", allocator);
         final UInt1Vector vector2 = new UInt1Vector("vector", allocator)) {

      vector1.allocateNew(10);
      for (int i = 0; i < 10; i++) {
        if (i % 2 == 0) {
          vector1.setNull(i);
        } else {
          vector1.set(i, i);
        }
      }
      vector1.setValueCount(10);
      setVector(vector2, null, (byte) 1, null, (byte) 3, null, (byte) 5, null, (byte) 7, null, (byte) 9);
      assertTrue(VectorEqualsVisitor.vectorEquals(vector1, vector2));
    }
  }

  @Test
  public void testPopulateUInt2Vector() {
    try (final UInt2Vector vector1 = new UInt2Vector("vector", allocator);
         final UInt2Vector vector2 = new UInt2Vector("vector", allocator)) {

      vector1.allocateNew(10);
      for (int i = 0; i < 10; i++) {
        if (i % 2 == 0) {
          vector1.setNull(i);
        } else {
          vector1.set(i, i);
        }
      }
      vector1.setValueCount(10);
      setVector(vector2, null, (char) 1, null, (char) 3, null, (char) 5, null, (char) 7, null, (char) 9);
      assertTrue(VectorEqualsVisitor.vectorEquals(vector1, vector2));
    }
  }

  @Test
  public void testPopulateUInt4Vector() {
    try (final UInt4Vector vector1 = new UInt4Vector("vector", allocator);
         final UInt4Vector vector2 = new UInt4Vector("vector", allocator)) {

      vector1.allocateNew(10);
      for (int i = 0; i < 10; i++) {
        if (i % 2 == 0) {
          vector1.setNull(i);
        } else {
          vector1.set(i, i);
        }
      }
      vector1.setValueCount(10);
      setVector(vector2, null, 1, null, 3, null, 5, null, 7, null, 9);
      assertTrue(VectorEqualsVisitor.vectorEquals(vector1, vector2));
    }
  }

  @Test
  public void testPopulateUInt8Vector() {
    try (final UInt8Vector vector1 = new UInt8Vector("vector", allocator);
        final UInt8Vector vector2 = new UInt8Vector("vector", allocator)) {

      vector1.allocateNew(10);
      for (int i = 0; i < 10; i++) {
        if (i % 2 == 0) {
          vector1.setNull(i);
        } else {
          vector1.set(i, i);
        }
      }
      vector1.setValueCount(10);
      setVector(vector2, null, 1L, null, 3L, null, 5L, null, 7L, null, 9L);
      assertTrue(VectorEqualsVisitor.vectorEquals(vector1, vector2));
    }
  }

  @Test
  public void testPopulateVarBinaryVector() {
    try (final VarBinaryVector vector1 = new VarBinaryVector("vector", allocator);
         final VarBinaryVector vector2 = new VarBinaryVector("vector", allocator)) {

      vector1.allocateNew(10);
      for (int i = 0; i < 10; i++) {
        if (i % 2 == 0) {
          vector1.setNull(i);
        } else {
          vector1.set(i, ("test" + i).getBytes());
        }
      }
      vector1.setValueCount(10);

      setVector(vector2, null, "test1".getBytes(), null, "test3".getBytes(), null, "test5".getBytes(), null,
          "test7".getBytes(), null, "test9".getBytes());
      assertTrue(VectorEqualsVisitor.vectorEquals(vector1, vector2));
    }
  }

  @Test
  public void testPopulateVarCharVector() {
    try (final VarCharVector vector1 = new VarCharVector("vector", allocator);
         final VarCharVector vector2 = new VarCharVector("vector", allocator)) {

      vector1.allocateNew(10);
      for (int i = 0; i < 10; i++) {
        if (i % 2 == 0) {
          vector1.setNull(i);
        } else {
          vector1.set(i, ("test" + i).getBytes());
        }
      }
      vector1.setValueCount(10);

      setVector(vector2, null, "test1", null, "test3", null, "test5", null, "test7", null, "test9");
      assertTrue(VectorEqualsVisitor.vectorEquals(vector1, vector2));
    }
  }
}
