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

package org.apache.arrow.vector.util;

import static junit.framework.TestCase.assertEquals;
import static junit.framework.TestCase.assertTrue;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.Arrays;
import java.util.List;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.BitVector;
import org.apache.arrow.vector.Float4Vector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.LargeVarCharVector;
import org.apache.arrow.vector.ValueVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.compare.Range;
import org.apache.arrow.vector.compare.RangeEqualsVisitor;
import org.apache.arrow.vector.compare.TypeEqualsVisitor;
import org.apache.arrow.vector.complex.DenseUnionVector;
import org.apache.arrow.vector.complex.FixedSizeListVector;
import org.apache.arrow.vector.complex.LargeListVector;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.complex.StructVector;
import org.apache.arrow.vector.complex.UnionVector;
import org.apache.arrow.vector.holders.NullableBigIntHolder;
import org.apache.arrow.vector.holders.NullableFloat4Holder;
import org.apache.arrow.vector.holders.NullableIntHolder;
import org.apache.arrow.vector.testing.ValueVectorDataPopulator;
import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * Test cases for {@link VectorAppender}.
 */
public class TestVectorAppender {

  private BufferAllocator allocator;

  @Before
  public void prepare() {
    allocator = new RootAllocator(1024 * 1024);
  }

  @After
  public void shutdown() {
    allocator.close();
  }

  @Test
  public void testAppendFixedWidthVector() {
    final int length1 = 10;
    final int length2 = 5;
    try (IntVector target = new IntVector("", allocator);
         IntVector delta = new IntVector("", allocator)) {

      target.allocateNew(length1);
      delta.allocateNew(length2);

      ValueVectorDataPopulator.setVector(target, 0, 1, 2, 3, 4, 5, 6, null, 8, 9);
      ValueVectorDataPopulator.setVector(delta, null, 11, 12, 13, 14);

      VectorAppender appender = new VectorAppender(target);
      delta.accept(appender, null);

      assertEquals(length1 + length2, target.getValueCount());

      try (IntVector expected = new IntVector("expected", allocator)) {
        expected.allocateNew();
        ValueVectorDataPopulator.setVector(expected, 0, 1, 2, 3, 4, 5, 6, null, 8, 9, null, 11, 12, 13, 14);
        assertVectorsEqual(expected, target);
      }
    }
  }

  @Test
  public void testAppendBitVector() {
    final int length1 = 10;
    final int length2 = 5;
    try (BitVector target = new BitVector("", allocator);
         BitVector delta = new BitVector("", allocator)) {

      target.allocateNew(length1);
      delta.allocateNew(length2);

      ValueVectorDataPopulator.setVector(target, 0, 1, 0, 1, 0, 1, 0, null, 0, 1);
      ValueVectorDataPopulator.setVector(delta, null, 1, 1, 0, 0);

      VectorAppender appender = new VectorAppender(target);
      delta.accept(appender, null);

      assertEquals(length1 + length2, target.getValueCount());

      try (BitVector expected = new BitVector("expected", allocator)) {
        expected.allocateNew();
        ValueVectorDataPopulator.setVector(expected, 0, 1, 0, 1, 0, 1, 0, null, 0, 1, null, 1, 1, 0, 0);
        assertVectorsEqual(expected, target);
      }
    }
  }

  @Test
  public void testAppendEmptyFixedWidthVector() {
    try (IntVector target = new IntVector("", allocator);
         IntVector delta = new IntVector("", allocator)) {

      ValueVectorDataPopulator.setVector(target, 0, 1, 2, 3, 4, 5, 6, null, 8, 9);

      VectorAppender appender = new VectorAppender(target);
      delta.accept(appender, null);

      assertEquals(10, target.getValueCount());

      try (IntVector expected = new IntVector("expected", allocator)) {
        ValueVectorDataPopulator.setVector(expected, 0, 1, 2, 3, 4, 5, 6, null, 8, 9);
        assertVectorsEqual(expected, target);
      }
    }
  }

  @Test
  public void testAppendVariableWidthVector() {
    final int length1 = 10;
    final int length2 = 5;
    try (VarCharVector target = new VarCharVector("", allocator);
         VarCharVector delta = new VarCharVector("", allocator)) {

      target.allocateNew(5, length1);
      delta.allocateNew(5, length2);

      ValueVectorDataPopulator.setVector(target, "a0", "a1", "a2", "a3", null, "a5", "a6", "a7", "a8", "a9");
      ValueVectorDataPopulator.setVector(delta, "a10", "a11", "a12", "a13", null);

      VectorAppender appender = new VectorAppender(target);
      delta.accept(appender, null);

      try (VarCharVector expected = new VarCharVector("expected", allocator)) {
        expected.allocateNew();
        ValueVectorDataPopulator.setVector(expected,
            "a0", "a1", "a2", "a3", null, "a5", "a6", "a7", "a8", "a9", "a10", "a11", "a12", "a13", null);
        assertVectorsEqual(expected, target);
      }
    }
  }

  @Test
  public void testAppendEmptyVariableWidthVector() {
    try (VarCharVector target = new VarCharVector("", allocator);
         VarCharVector delta = new VarCharVector("", allocator)) {

      ValueVectorDataPopulator.setVector(target, "a0", "a1", "a2", "a3", null, "a5", "a6", "a7", "a8", "a9");

      VectorAppender appender = new VectorAppender(target);
      delta.accept(appender, null);

      try (VarCharVector expected = new VarCharVector("expected", allocator)) {
        ValueVectorDataPopulator.setVector(expected,
            "a0", "a1", "a2", "a3", null, "a5", "a6", "a7", "a8", "a9");
        assertVectorsEqual(expected, target);
      }
    }
  }

  @Test
  public void testAppendLargeVariableWidthVector() {
    final int length1 = 5;
    final int length2 = 10;
    try (LargeVarCharVector target = new LargeVarCharVector("", allocator);
         LargeVarCharVector delta = new LargeVarCharVector("", allocator)) {

      target.allocateNew(5, length1);
      delta.allocateNew(5, length2);

      ValueVectorDataPopulator.setVector(target, "a0", null, "a2", "a3", null);
      ValueVectorDataPopulator.setVector(delta, "a5", "a6", "a7", null, null, "a10", "a11", "a12", "a13", null);

      VectorAppender appender = new VectorAppender(target);
      delta.accept(appender, null);

      try (LargeVarCharVector expected = new LargeVarCharVector("expected", allocator)) {
        expected.allocateNew();
        ValueVectorDataPopulator.setVector(expected,
                "a0", null, "a2", "a3", null, "a5", "a6", "a7", null, null, "a10", "a11", "a12", "a13", null);
        assertVectorsEqual(expected, target);
      }
    }
  }

  @Test
  public void testAppendEmptyLargeVariableWidthVector() {
    try (LargeVarCharVector target = new LargeVarCharVector("", allocator);
         LargeVarCharVector delta = new LargeVarCharVector("", allocator)) {

      ValueVectorDataPopulator.setVector(target, "a0", null, "a2", "a3", null);

      VectorAppender appender = new VectorAppender(target);
      delta.accept(appender, null);

      try (LargeVarCharVector expected = new LargeVarCharVector("expected", allocator)) {
        ValueVectorDataPopulator.setVector(expected, "a0", null, "a2", "a3", null);
        assertVectorsEqual(expected, target);
      }
    }
  }

  @Test
  public void testAppendListVector() {
    final int length1 = 5;
    final int length2 = 2;
    try (ListVector target = ListVector.empty("target", allocator);
         ListVector delta = ListVector.empty("delta", allocator)) {

      target.allocateNew();
      ValueVectorDataPopulator.setVector(target,
          Arrays.asList(0, 1),
          Arrays.asList(2, 3),
          null,
          Arrays.asList(6, 7),
          Arrays.asList(8, 9));
      assertEquals(length1, target.getValueCount());

      delta.allocateNew();
      ValueVectorDataPopulator.setVector(delta,
          Arrays.asList(10, 11, 12, 13, 14),
          Arrays.asList(15, 16, 17, 18, 19));
      assertEquals(length2, delta.getValueCount());

      VectorAppender appender = new VectorAppender(target);
      delta.accept(appender, null);

      assertEquals(7, target.getValueCount());

      List<Integer> expected = Arrays.asList(0, 1);
      assertEquals(expected, target.getObject(0));

      expected = Arrays.asList(2, 3);
      assertEquals(expected, target.getObject(1));

      assertTrue(target.isNull(2));

      expected = Arrays.asList(6, 7);
      assertEquals(expected, target.getObject(3));

      expected = Arrays.asList(8, 9);
      assertEquals(expected, target.getObject(4));

      expected = Arrays.asList(10, 11, 12, 13, 14);
      assertEquals(expected, target.getObject(5));

      expected = Arrays.asList(15, 16, 17, 18, 19);
      assertEquals(expected, target.getObject(6));
    }
  }

  @Test
  public void testAppendEmptyListVector() {
    try (ListVector target = ListVector.empty("target", allocator);
         ListVector delta = ListVector.empty("delta", allocator)) {
      // populate target with data
      ValueVectorDataPopulator.setVector(target,
          Arrays.asList(0, 1),
          Arrays.asList(2, 3),
          null,
          Arrays.asList(6, 7));
      assertEquals(4, target.getValueCount());

      // leave delta vector empty and unallocated
      delta.addOrGetVector(FieldType.nullable(Types.MinorType.INT.getType()));

      VectorAppender appender = new VectorAppender(target);
      delta.accept(appender, null);

      // verify delta vector has original data
      assertEquals(4, target.getValueCount());

      List<Integer> expected = Arrays.asList(0, 1);
      assertEquals(expected, target.getObject(0));

      expected = Arrays.asList(2, 3);
      assertEquals(expected, target.getObject(1));

      assertTrue(target.isNull(2));

      expected = Arrays.asList(6, 7);
      assertEquals(expected, target.getObject(3));
    }
  }

  @Test
  public void testAppendFixedSizeListVector() {
    try (FixedSizeListVector target = FixedSizeListVector.empty("target", 5, allocator);
         FixedSizeListVector delta = FixedSizeListVector.empty("delta", 5, allocator)) {

      target.allocateNew();
      ValueVectorDataPopulator.setVector(target,
          Arrays.asList(0, 1, 2, 3, 4),
          null);
      assertEquals(2, target.getValueCount());

      delta.allocateNew();
      ValueVectorDataPopulator.setVector(delta,
          Arrays.asList(10, 11, 12, 13, 14),
          Arrays.asList(15, 16, 17, 18, 19));
      assertEquals(2, delta.getValueCount());

      VectorAppender appender = new VectorAppender(target);
      delta.accept(appender, null);

      assertEquals(4, target.getValueCount());

      assertEquals(Arrays.asList(0, 1, 2, 3, 4), target.getObject(0));
      assertTrue(target.isNull(1));
      assertEquals(Arrays.asList(10, 11, 12, 13, 14), target.getObject(2));
      assertEquals(Arrays.asList(15, 16, 17, 18, 19), target.getObject(3));
    }
  }

  @Test
  public void testAppendEmptyFixedSizeListVector() {
    try (FixedSizeListVector target = FixedSizeListVector.empty("target", 5, allocator);
         FixedSizeListVector delta = FixedSizeListVector.empty("delta", 5, allocator)) {

      ValueVectorDataPopulator.setVector(target,
          Arrays.asList(0, 1, 2, 3, 4),
          null);
      assertEquals(2, target.getValueCount());

      // leave delta vector empty and unallocated
      delta.addOrGetVector(FieldType.nullable(Types.MinorType.INT.getType()));

      VectorAppender appender = new VectorAppender(target);
      delta.accept(appender, null);

      assertEquals(2, target.getValueCount());

      assertEquals(Arrays.asList(0, 1, 2, 3, 4), target.getObject(0));
      assertTrue(target.isNull(1));
    }
  }

  @Test
  public void testAppendEmptyLargeListVector() {
    try (LargeListVector target = LargeListVector.empty("target", allocator);
         LargeListVector delta = LargeListVector.empty("delta", allocator)) {

      ValueVectorDataPopulator.setVector(target,
          Arrays.asList(0, 1, 2, 3, 4),
          null);
      assertEquals(2, target.getValueCount());

      // leave delta vector empty and unallocated
      delta.addOrGetVector(FieldType.nullable(Types.MinorType.INT.getType()));

      VectorAppender appender = new VectorAppender(target);
      delta.accept(appender, null);

      assertEquals(2, target.getValueCount());

      assertEquals(Arrays.asList(0, 1, 2, 3, 4), target.getObject(0));
      assertTrue(target.isNull(1));
    }
  }

  @Test
  public void testAppendStructVector() {
    final int length1 = 10;
    final int length2 = 5;
    try (final StructVector target = StructVector.empty("target", allocator);
         final StructVector delta = StructVector.empty("delta", allocator)) {

      IntVector targetChild1 = target.addOrGet("f0", FieldType.nullable(new ArrowType.Int(32, true)), IntVector.class);
      VarCharVector targetChild2 = target.addOrGet("f1", FieldType.nullable(new ArrowType.Utf8()), VarCharVector.class);
      targetChild1.allocateNew();
      targetChild2.allocateNew();
      ValueVectorDataPopulator.setVector(targetChild1, 0, 1, 2, 3, 4, null, 6, 7, 8, 9);
      ValueVectorDataPopulator.setVector(targetChild2, "a0", "a1", "a2", "a3", "a4", "a5", "a6", null, "a8", "a9");
      target.setValueCount(length1);

      IntVector deltaChild1 = delta.addOrGet("f0", FieldType.nullable(new ArrowType.Int(32, true)), IntVector.class);
      VarCharVector deltaChild2 = delta.addOrGet("f1", FieldType.nullable(new ArrowType.Utf8()), VarCharVector.class);
      deltaChild1.allocateNew();
      deltaChild2.allocateNew();
      ValueVectorDataPopulator.setVector(deltaChild1, 10, 11, 12, null, 14);
      ValueVectorDataPopulator.setVector(deltaChild2, "a10", "a11", "a12", "a13", "a14");
      delta.setValueCount(length2);

      VectorAppender appender = new VectorAppender(target);
      delta.accept(appender, null);

      assertEquals(length1 + length2, target.getValueCount());
      IntVector child1 = (IntVector) target.getVectorById(0);
      VarCharVector child2 = (VarCharVector) target.getVectorById(1);

      try (IntVector expected1 = new IntVector("expected1", allocator);
           VarCharVector expected2 = new VarCharVector("expected2", allocator)) {
        expected1.allocateNew();
        expected2.allocateNew();

        ValueVectorDataPopulator.setVector(expected1, 0, 1, 2, 3, 4, null, 6, 7, 8, 9, 10, 11, 12, null, 14);
        ValueVectorDataPopulator.setVector(expected2,
            "a0", "a1", "a2", "a3", "a4", "a5", "a6", null, "a8", "a9", "a10", "a11", "a12", "a13", "a14");

        assertVectorsEqual(expected1, target.getChild("f0"));
        assertVectorsEqual(expected2, target.getChild("f1"));
      }
    }
  }

  @Test
  public void testAppendEmptyStructVector() {
    try (final StructVector target = StructVector.empty("target", allocator);
         final StructVector delta = StructVector.empty("delta", allocator)) {

      IntVector targetChild1 = target.addOrGet("f0", FieldType.nullable(new ArrowType.Int(32, true)), IntVector.class);
      VarCharVector targetChild2 = target.addOrGet("f1", FieldType.nullable(new ArrowType.Utf8()), VarCharVector.class);
      ValueVectorDataPopulator.setVector(targetChild1, 0, 1, 2, 3, 4, null, 6, 7, 8, 9);
      ValueVectorDataPopulator.setVector(targetChild2, "a0", "a1", "a2", "a3", "a4", "a5", "a6", null, "a8", "a9");
      target.setValueCount(10);

      // leave delta vector fields empty and unallocated
      delta.addOrGet("f0", FieldType.nullable(new ArrowType.Int(32, true)), IntVector.class);
      delta.addOrGet("f1", FieldType.nullable(new ArrowType.Utf8()), VarCharVector.class);

      VectorAppender appender = new VectorAppender(target);
      delta.accept(appender, null);

      assertEquals(10, target.getValueCount());

      try (IntVector expected1 = new IntVector("expected1", allocator);
           VarCharVector expected2 = new VarCharVector("expected2", allocator)) {
        ValueVectorDataPopulator.setVector(expected1, 0, 1, 2, 3, 4, null, 6, 7, 8, 9);
        ValueVectorDataPopulator.setVector(expected2,
            "a0", "a1", "a2", "a3", "a4", "a5", "a6", null, "a8", "a9");

        assertVectorsEqual(expected1, target.getChild("f0"));
        assertVectorsEqual(expected2, target.getChild("f1"));
      }
    }
  }

  @Test
  public void testAppendUnionVector() {
    final int length1 = 10;
    final int length2 = 5;

    try (final UnionVector target = UnionVector.empty("target", allocator);
         final UnionVector delta = UnionVector.empty("delta", allocator)) {

      // alternating ints and big ints
      target.setType(0, Types.MinorType.INT);
      target.setType(1, Types.MinorType.BIGINT);
      target.setType(2, Types.MinorType.INT);
      target.setType(3, Types.MinorType.BIGINT);
      target.setType(4, Types.MinorType.INT);
      target.setType(5, Types.MinorType.BIGINT);
      target.setType(6, Types.MinorType.INT);
      target.setType(7, Types.MinorType.BIGINT);
      target.setType(8, Types.MinorType.INT);
      target.setType(9, Types.MinorType.BIGINT);
      target.setType(10, Types.MinorType.INT);
      target.setType(11, Types.MinorType.BIGINT);
      target.setType(12, Types.MinorType.INT);
      target.setType(13, Types.MinorType.BIGINT);
      target.setType(14, Types.MinorType.INT);
      target.setType(15, Types.MinorType.BIGINT);
      target.setType(16, Types.MinorType.INT);
      target.setType(17, Types.MinorType.BIGINT);
      target.setType(18, Types.MinorType.INT);
      target.setType(19, Types.MinorType.BIGINT);

      IntVector targetIntVec = target.getIntVector();
      targetIntVec.allocateNew();
      ValueVectorDataPopulator.setVector(
          targetIntVec,
          0, null, 1, null, 2, null, 3, null, 4, null, 5, null, 6, null, 7, null, 8, null, 9, null);
      assertEquals(length1 * 2, targetIntVec.getValueCount());

      BigIntVector targetBigIntVec = target.getBigIntVector();
      targetBigIntVec.allocateNew();
      ValueVectorDataPopulator.setVector(
          targetBigIntVec,
          null, 0L, null, 1L, null, 2L, null, 3L, null, 4L, null, 5L, null, 6L, null, 7L, null, 8L, null, 9L);
      assertEquals(length1 * 2, targetBigIntVec.getValueCount());

      target.setValueCount(length1 * 2);

      // populate the delta vector
      delta.setType(0, Types.MinorType.FLOAT4);
      delta.setType(1, Types.MinorType.FLOAT4);
      delta.setType(2, Types.MinorType.FLOAT4);
      delta.setType(3, Types.MinorType.FLOAT4);
      delta.setType(4, Types.MinorType.FLOAT4);

      Float4Vector deltaFloatVector = delta.getFloat4Vector();
      deltaFloatVector.allocateNew();
      ValueVectorDataPopulator.setVector(deltaFloatVector, 10f, 11f, 12f, 13f, 14f);
      assertEquals(length2, deltaFloatVector.getValueCount());
      delta.setValueCount(length2);

      VectorAppender appender = new VectorAppender(target);
      delta.accept(appender, null);

      assertEquals(length1 * 2 + length2, target.getValueCount());

      for (int i = 0; i < length1; i++) {
        Object intObj = target.getObject(i * 2);
        assertTrue(intObj instanceof Integer);
        assertEquals(i, ((Integer) intObj).intValue());

        Object longObj = target.getObject(i * 2 + 1);
        assertTrue(longObj instanceof Long);
        assertEquals(i, ((Long) longObj).longValue());
      }

      for (int i = 0; i < length2; i++) {
        Object floatObj = target.getObject(length1 * 2 + i);
        assertTrue(floatObj instanceof Float);
        assertEquals(i + length1, ((Float) floatObj).intValue());
      }
    }
  }

  @Test
  public void testAppendEmptyUnionVector() {
    final int length1 = 10;

    try (final UnionVector target = UnionVector.empty("target", allocator);
         final UnionVector delta = UnionVector.empty("delta", allocator)) {

      // alternating ints and big ints
      target.setType(0, Types.MinorType.INT);
      target.setType(1, Types.MinorType.BIGINT);
      target.setType(2, Types.MinorType.INT);
      target.setType(3, Types.MinorType.BIGINT);
      target.setType(4, Types.MinorType.INT);
      target.setType(5, Types.MinorType.BIGINT);
      target.setType(6, Types.MinorType.INT);
      target.setType(7, Types.MinorType.BIGINT);
      target.setType(8, Types.MinorType.INT);
      target.setType(9, Types.MinorType.BIGINT);
      target.setType(10, Types.MinorType.INT);
      target.setType(11, Types.MinorType.BIGINT);
      target.setType(12, Types.MinorType.INT);
      target.setType(13, Types.MinorType.BIGINT);
      target.setType(14, Types.MinorType.INT);
      target.setType(15, Types.MinorType.BIGINT);
      target.setType(16, Types.MinorType.INT);
      target.setType(17, Types.MinorType.BIGINT);
      target.setType(18, Types.MinorType.INT);
      target.setType(19, Types.MinorType.BIGINT);

      IntVector targetIntVec = target.getIntVector();
      ValueVectorDataPopulator.setVector(
          targetIntVec,
          0, null, 1, null, 2, null, 3, null, 4, null, 5, null, 6, null, 7, null, 8, null, 9, null);
      assertEquals(length1 * 2, targetIntVec.getValueCount());

      BigIntVector targetBigIntVec = target.getBigIntVector();
      ValueVectorDataPopulator.setVector(
          targetBigIntVec,
          null, 0L, null, 1L, null, 2L, null, 3L, null, 4L, null, 5L, null, 6L, null, 7L, null, 8L, null, 9L);
      assertEquals(length1 * 2, targetBigIntVec.getValueCount());

      target.setValueCount(length1 * 2);

      // initialize the delta vector but leave it empty and unallocated
      delta.setType(0, Types.MinorType.FLOAT4);
      delta.setType(1, Types.MinorType.FLOAT4);
      delta.setType(2, Types.MinorType.FLOAT4);
      delta.setType(3, Types.MinorType.FLOAT4);
      delta.setType(4, Types.MinorType.FLOAT4);

      VectorAppender appender = new VectorAppender(target);
      delta.accept(appender, null);

      assertEquals(length1 * 2, target.getValueCount());

      for (int i = 0; i < length1; i++) {
        Object intObj = target.getObject(i * 2);
        assertTrue(intObj instanceof Integer);
        assertEquals(i, ((Integer) intObj).intValue());

        Object longObj = target.getObject(i * 2 + 1);
        assertTrue(longObj instanceof Long);
        assertEquals(i, ((Long) longObj).longValue());
      }
    }
  }

  private DenseUnionVector getTargetVector() {
    // create a vector, and populate it with values {1, 2, null, 10L}

    final NullableIntHolder intHolder = new NullableIntHolder();
    intHolder.isSet = 1;
    final NullableBigIntHolder longHolder = new NullableBigIntHolder();
    longHolder.isSet = 1;
    final NullableFloat4Holder floatHolder = new NullableFloat4Holder();
    floatHolder.isSet = 1;
    DenseUnionVector targetVector = new DenseUnionVector("target vector", allocator, null, null);

    targetVector.allocateNew();

    while (targetVector.getValueCapacity() < 4) {
      targetVector.reAlloc();
    }

    byte intTypeId = targetVector.registerNewTypeId(Field.nullable("", Types.MinorType.INT.getType()));
    targetVector.setTypeId(0, intTypeId);
    intHolder.value = 1;
    targetVector.setSafe(0, intHolder);
    targetVector.setTypeId(1, intTypeId);
    intHolder.value = 2;
    targetVector.setSafe(1, intHolder);
    byte longTypeId = targetVector.registerNewTypeId(Field.nullable("", Types.MinorType.BIGINT.getType()));
    targetVector.setTypeId(3, longTypeId);
    longHolder.value = 10L;
    targetVector.setSafe(3, longHolder);
    targetVector.setValueCount(4);

    assertVectorValuesEqual(targetVector, new Object[]{1, 2, null, 10L});
    return targetVector;
  }

  private DenseUnionVector getDeltaVector() {
    // create a vector, and populate it with values {7, null, 8L, 9.0f}

    final NullableIntHolder intHolder = new NullableIntHolder();
    intHolder.isSet = 1;
    final NullableBigIntHolder longHolder = new NullableBigIntHolder();
    longHolder.isSet = 1;
    final NullableFloat4Holder floatHolder = new NullableFloat4Holder();
    floatHolder.isSet = 1;

    DenseUnionVector deltaVector = new DenseUnionVector("target vector", allocator, null, null);

    while (deltaVector.getValueCapacity() < 4) {
      deltaVector.reAlloc();
    }
    byte intTypeId = deltaVector.registerNewTypeId(Field.nullable("", Types.MinorType.INT.getType()));
    deltaVector.setTypeId(0, intTypeId);
    intHolder.value = 7;
    deltaVector.setSafe(0, intHolder);
    byte longTypeId = deltaVector.registerNewTypeId(Field.nullable("", Types.MinorType.BIGINT.getType()));
    deltaVector.setTypeId(2, longTypeId);
    longHolder.value = 8L;
    deltaVector.setSafe(2, longHolder);
    byte floatTypeId = deltaVector.registerNewTypeId(Field.nullable("", Types.MinorType.FLOAT4.getType()));
    deltaVector.setTypeId(3, floatTypeId);
    floatHolder.value = 9.0f;
    deltaVector.setSafe(3, floatHolder);

    deltaVector.setValueCount(4);

    assertVectorValuesEqual(deltaVector, new Object[]{7, null, 8L, 9.0f});
    return deltaVector;
  }

  @Test
  public void testAppendDenseUnionVector() {
    try (DenseUnionVector targetVector = getTargetVector();
         DenseUnionVector deltaVector = getDeltaVector()) {

      // append
      VectorAppender appender = new VectorAppender(targetVector);
      deltaVector.accept(appender, null);
      assertVectorValuesEqual(targetVector, new Object[] {1, 2, null, 10L, 7, null, 8L, 9.0f});
    }

    // test reverse append
    try (DenseUnionVector targetVector = getTargetVector();
         DenseUnionVector deltaVector = getDeltaVector()) {

      // append
      VectorAppender appender = new VectorAppender(deltaVector);
      targetVector.accept(appender, null);
      assertVectorValuesEqual(deltaVector, new Object[] {7, null, 8L, 9.0f, 1, 2, null, 10L});
    }
  }

  private DenseUnionVector getEmptyDeltaVector() {
    // create a vector, but leave it empty and uninitialized
    DenseUnionVector deltaVector = new DenseUnionVector("target vector", allocator, null, null);

    byte intTypeId = deltaVector.registerNewTypeId(Field.nullable("", Types.MinorType.INT.getType()));
    deltaVector.setTypeId(0, intTypeId);

    byte longTypeId = deltaVector.registerNewTypeId(Field.nullable("", Types.MinorType.BIGINT.getType()));
    deltaVector.setTypeId(2, longTypeId);

    byte floatTypeId = deltaVector.registerNewTypeId(Field.nullable("", Types.MinorType.FLOAT4.getType()));
    deltaVector.setTypeId(3, floatTypeId);

    return deltaVector;
  }

  @Test
  public void testAppendEmptyDenseUnionVector() {
    try (DenseUnionVector targetVector = getTargetVector();
         DenseUnionVector deltaVector = getEmptyDeltaVector()) {

      // append
      VectorAppender appender = new VectorAppender(targetVector);
      deltaVector.accept(appender, null);
      assertVectorValuesEqual(targetVector, new Object[] {1, 2, null, 10L});
    }
  }

  /**
   * Test appending dense union vectors where the child vectors do not match.
   */
  @Test
  public void testAppendDenseUnionVectorMismatch() {
    final NullableIntHolder intHolder = new NullableIntHolder();
    intHolder.isSet = 1;

    final NullableBigIntHolder longHolder = new NullableBigIntHolder();
    longHolder.isSet = 1;

    final NullableFloat4Holder floatHolder = new NullableFloat4Holder();
    floatHolder.isSet = 1;

    try (DenseUnionVector targetVector = new DenseUnionVector("target vector" , allocator, null, null);
         DenseUnionVector deltaVector = new DenseUnionVector("target vector" , allocator, null, null)) {
      targetVector.allocateNew();
      deltaVector.allocateNew();

      // populate the target vector with values {1, 2L}
      while (targetVector.getValueCapacity() < 2) {
        targetVector.reAlloc();
      }
      byte intTypeId = targetVector.registerNewTypeId(Field.nullable("", Types.MinorType.INT.getType()));
      targetVector.setTypeId(0, intTypeId);
      intHolder.value = 1;
      targetVector.setSafe(0, intHolder);
      byte longTypeId = targetVector.registerNewTypeId(Field.nullable("", Types.MinorType.BIGINT.getType()));
      targetVector.setTypeId(1, longTypeId);
      longHolder.value = 2L;
      targetVector.setSafe(1, longHolder);
      targetVector.setValueCount(2);

      assertVectorValuesEqual(targetVector, new Object[] {1, 2L});

      // populate the delta vector with values {3, 5.0f}
      while (deltaVector.getValueCapacity() < 2) {
        deltaVector.reAlloc();
      }
      intTypeId = deltaVector.registerNewTypeId(Field.nullable("", Types.MinorType.INT.getType()));
      deltaVector.setTypeId(0, intTypeId);
      intHolder.value = 3;
      deltaVector.setSafe(0, intHolder);
      byte floatTypeId = deltaVector.registerNewTypeId(Field.nullable("", Types.MinorType.FLOAT4.getType()));
      deltaVector.setTypeId(1, floatTypeId);
      floatHolder.value = 5.0f;
      deltaVector.setSafe(1, floatHolder);
      deltaVector.setValueCount(2);

      assertVectorValuesEqual(deltaVector, new Object[] {3, 5.0f});

      // append
      VectorAppender appender = new VectorAppender(targetVector);
      assertThrows(IllegalArgumentException.class,
          () -> deltaVector.accept(appender, null));
    }
  }

  @Test
  public void testAppendVectorNegative() {
    final int vectorLength = 10;
    try (IntVector target = new IntVector("", allocator);
         VarCharVector delta = new VarCharVector("", allocator)) {

      target.allocateNew(vectorLength);
      delta.allocateNew(vectorLength);

      VectorAppender appender = new VectorAppender(target);

      assertThrows(IllegalArgumentException.class,
          () -> delta.accept(appender, null));
    }
  }

  private void assertVectorValuesEqual(ValueVector vector, Object[] values) {
    assertEquals(vector.getValueCount(), values.length);
    for (int i = 0; i < values.length; i++) {
      assertEquals(vector.getObject(i), values[i]);
    }
  }

  public static void assertVectorsEqual(ValueVector vector1, ValueVector vector2) {
    assertEquals(vector1.getValueCount(), vector2.getValueCount());

    TypeEqualsVisitor typeEqualsVisitor = new TypeEqualsVisitor(vector1, false, false);
    RangeEqualsVisitor equalsVisitor =
        new RangeEqualsVisitor(vector1, vector2, (v1, v2) -> typeEqualsVisitor.equals(vector2));
    assertTrue(equalsVisitor.rangeEquals(new Range(0, 0, vector1.getValueCount())));
  }
}
