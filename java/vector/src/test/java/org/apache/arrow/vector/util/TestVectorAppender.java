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
import static org.apache.arrow.vector.complex.BaseRepeatedValueVector.OFFSET_WIDTH;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.List;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.BitVectorHelper;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.complex.FixedSizeListVector;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.complex.StructVector;
import org.apache.arrow.vector.complex.UnionVector;
import org.apache.arrow.vector.holders.NullableBigIntHolder;
import org.apache.arrow.vector.holders.NullableFloat4Holder;
import org.apache.arrow.vector.holders.NullableIntHolder;
import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.pojo.ArrowType;
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

      for (int i = 0; i < length1; i++) {
        target.set(i, i);
      }
      for (int i = 0; i < length2; i++) {
        delta.set(i, i + length1);
      }
      target.setValueCount(length1);
      delta.setValueCount(length2);

      VectorAppender appender = new VectorAppender(target);
      delta.accept(appender, null);

      assertEquals(length1 + length2, target.getValueCount());
      for (int i = 0; i < target.getValueCount(); i++) {
        assertEquals(i, target.get(i));
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

      for (int i = 0; i < length1; i++) {
        target.setSafe(i, ("a" + i).getBytes());
      }
      for (int i = 0; i < length2; i++) {
        delta.setSafe(i, ("a" + (i + length1)).getBytes());
      }
      target.setValueCount(length1);
      delta.setValueCount(length2);

      VectorAppender appender = new VectorAppender(target);
      delta.accept(appender, null);

      assertEquals(length1 + length2, target.getValueCount());
      for (int i = 0; i < target.getValueCount(); i++) {
        assertEquals("a" + i, new String(target.get(i)));
      }
    }
  }

  private ListVector createListVector(int start, int end, int step) {
    final int listLength = (end - start) / step;

    ListVector listVector = ListVector.empty("list vector", allocator);

    Types.MinorType type = Types.MinorType.INT;
    listVector.addOrGetVector(FieldType.nullable(type.getType()));

    listVector.allocateNew();

    IntVector dataVector = (IntVector) listVector.getDataVector();

    // set underlying vectors
    for (int i = 0; i < end - start; i++) {
      dataVector.set(i, i + start);
    }
    dataVector.setValueCount(end - start);

    // set offset buffer
    for (int i = 0; i < listLength; i++) {
      BitVectorHelper.setBit(listVector.getValidityBuffer(), i);
      listVector.getOffsetBuffer().setInt(i * OFFSET_WIDTH, i * step);
      listVector.getOffsetBuffer().setInt((i + 1) * OFFSET_WIDTH, (i + 1) * step);
    }
    listVector.setLastSet(listLength - 1);
    listVector.setValueCount(listLength);

    return listVector;
  }

  @Test
  public void testAppendListVector() {
    try (ListVector target = createListVector(0, 10, 2);
         ListVector delta = createListVector(10, 20, 5)) {

      VectorAppender appender = new VectorAppender(target);
      delta.accept(appender, null);

      assertEquals(7, target.getValueCount());

      int curValue = 0;
      for (int i = 0; i < 5; i++) {
        List<Integer> list = (List<Integer>) target.getObject(i);
        assertEquals(2, list.size());
        for (int j = 0; j < list.size(); j++) {
          assertEquals(curValue++, list.get(j).intValue());
        }
      }

      for (int i = 5; i < 7; i++) {
        List<Integer> list = (List<Integer>) target.getObject(i);
        assertEquals(5, list.size());
        for (int j = 0; j < list.size(); j++) {
          assertEquals(curValue++, list.get(j).intValue());
        }
      }
    }
  }

  private FixedSizeListVector createFixedSizeListVector(int start, int end, int step) {
    final int listLength = (end - start) / step;

    FixedSizeListVector listVector = FixedSizeListVector.empty("fixed size list vector", step, allocator);

    Types.MinorType type = Types.MinorType.INT;
    listVector.addOrGetVector(FieldType.nullable(type.getType()));

    listVector.allocateNew();

    IntVector dataVector = (IntVector) listVector.getDataVector();

    // set underlying vectors
    for (int i = 0; i < end - start; i++) {
      dataVector.set(i, i + start);
    }
    dataVector.setValueCount(end - start);

    listVector.setValueCount(listLength);

    return listVector;
  }

  @Test
  public void testAppendFixedSizeListVector() {
    try (ListVector target = createListVector(0, 10, 2);
         ListVector delta = createListVector(10, 20, 5)) {

      VectorAppender appender = new VectorAppender(target);
      delta.accept(appender, null);

      assertEquals(7, target.getValueCount());
      int curValue = 0;
      for (int i = 0; i < 5; i++) {
        List<Integer> list = (List<Integer>) target.getObject(i);
        assertEquals(2, list.size());
        for (int j = 0; j < list.size(); j++) {
          assertEquals(curValue++, list.get(j).intValue());
        }
      }

      for (int i = 5; i < 7; i++) {
        List<Integer> list = (List<Integer>) target.getObject(i);
        assertEquals(5, list.size());
        for (int j = 0; j < list.size(); j++) {
          assertEquals(curValue++, list.get(j).intValue());
        }
      }
    }
  }

  private StructVector createStructVector(int start, int end) {
    final StructVector vector = StructVector.empty("vector", allocator);
    IntVector child1 = vector.addOrGet("f0", FieldType.nullable(new ArrowType.Int(32, true)), IntVector.class);
    VarCharVector child2 = vector.addOrGet("f1", FieldType.nullable(new ArrowType.Utf8()), VarCharVector.class);

    child1.allocateNew();
    child2.allocateNew();

    for (int i = 0; i < end - start; i++) {
      child1.setSafe(i, start + i);
      child2.setSafe(i, ("a" + (start + i)).getBytes());
    }
    child1.setValueCount(end - start);
    child2.setLastSet(end - start - 1);
    child2.setValueCount(end - start);

    vector.setValueCount(end - start);
    return vector;
  }

  @Test
  public void testAppendStructVector() {
    final int length1 = 10;
    final int length2 = 5;
    try (final StructVector target = createStructVector(0, length1);
         final StructVector delta = createStructVector(length1, length1 + length2)) {

      VectorAppender appender = new VectorAppender(target);
      delta.accept(appender, null);

      assertEquals(length1 + length2, target.getValueCount());
      IntVector child1 = (IntVector) target.getVectorById(0);
      VarCharVector child2 = (VarCharVector) target.getVectorById(1);

      for (int i = 0; i < target.getValueCount(); i++) {
        assertEquals(i, child1.get(i));
        assertEquals("a" + i, new String(child2.get(i)));
      }
    }
  }

  private UnionVector createComplexUnionVector(int start, int end) {
    final UnionVector vector = UnionVector.empty("vector", allocator);

    final NullableIntHolder intHolder = new NullableIntHolder();
    intHolder.isSet = 1;

    final NullableBigIntHolder longHolder = new NullableBigIntHolder();
    longHolder.isSet = 1;

    for (int i = 0; i < end - start; i++) {
      vector.setType(i * 2, Types.MinorType.INT);
      intHolder.value = i + start;
      vector.setSafe(i * 2, intHolder);

      vector.setType(i * 2 + 1, Types.MinorType.BIGINT);
      longHolder.value = i + start;
      vector.setSafe(i * 2 + 1, longHolder);
    }

    vector.setValueCount((end - start) * 2);

    return vector;
  }

  private UnionVector createSingleUnionVector(int start, int end) {
    final UnionVector vector = UnionVector.empty("vector", allocator);

    final NullableFloat4Holder floatHolder = new NullableFloat4Holder();
    floatHolder.isSet = 1;

    for (int i = 0; i < end - start; i++) {
      vector.setType(i, Types.MinorType.FLOAT4);
      floatHolder.value = i + start;
      vector.setSafe(i, floatHolder);
    }

    vector.setValueCount(end - start);
    return vector;
  }

  @Test
  public void testAppendUnionVector() {
    final int length1 = 10;
    final int length2 = 5;

    try (final UnionVector target = createComplexUnionVector(0, length1);
         final UnionVector delta = createSingleUnionVector(length1, length1 + length2)) {
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
}
