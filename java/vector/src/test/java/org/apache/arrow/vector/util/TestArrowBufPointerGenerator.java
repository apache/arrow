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

import static junit.framework.TestCase.assertNull;
import static junit.framework.TestCase.assertTrue;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertFalse;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.memory.util.ArrowBufPointer;
import org.apache.arrow.memory.util.ArrowBufPointerNode;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.NullVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.complex.FixedSizeListVector;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.complex.MapVector;
import org.apache.arrow.vector.complex.StructVector;
import org.apache.arrow.vector.complex.UnionVector;
import org.apache.arrow.vector.complex.impl.UnionListWriter;
import org.apache.arrow.vector.complex.impl.UnionMapWriter;
import org.apache.arrow.vector.holders.NullableUInt4Holder;
import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * Test cases for {@link ArrowBufPointerTreeGenerator}.
 */
public class TestArrowBufPointerGenerator {
  ;

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
  public void testFixedWidthVector() {
    try (IntVector intVector = new IntVector("", allocator)) {
      intVector.allocateNew(2);
      intVector.setValueCount(2);

      intVector.set(0, 100);
      intVector.setNull(1);

      ArrowBufPointerTreeGenerator generator = new ArrowBufPointerTreeGenerator();

      ArrowBufPointerNode root = intVector.accept(generator, 0);

      assertTrue(root.isLeafNode());
      ArrowBufPointer pointer = root.getArrowBufPointer();
      assertNotNull(pointer.getBuf());
      assertEquals(0, pointer.getOffset());
      assertEquals(4, pointer.getLength());
      assertEquals(100, pointer.getBuf().getInt(pointer.getOffset()));

      root = intVector.accept(generator, 1);

      assertTrue(root.isLeafNode());
      pointer = root.getArrowBufPointer();
      assertNull(pointer.getBuf());
    }
  }

  @Test
  public void testVariableWidthVector() {
    try (VarCharVector vector = new VarCharVector("", allocator)) {
      vector.allocateNew(10, 2);
      vector.setValueCount(2);

      vector.set(0, "abcdefg".getBytes());
      vector.setNull(1);

      ArrowBufPointerTreeGenerator generator = new ArrowBufPointerTreeGenerator();

      ArrowBufPointerNode root = vector.accept(generator, 0);

      assertTrue(root.isLeafNode());
      ArrowBufPointer pointer = root.getArrowBufPointer();
      assertNotNull(pointer.getBuf());
      assertEquals(0, pointer.getOffset());
      assertEquals(7, pointer.getLength());
      byte[] actual = new byte[7];
      pointer.getBuf().getBytes(pointer.getOffset(), actual);
      assertEquals("abcdefg", new String(actual));

      root = vector.accept(generator, 1);

      assertTrue(root.isLeafNode());
      pointer = root.getArrowBufPointer();
      assertNull(pointer.getBuf());
    }
  }

  @Test
  public void testListVector() {
    try (ListVector vector = ListVector.empty("input", allocator);) {
      UnionListWriter writer = vector.getWriter();
      writer.allocate();

      // populate list vector with the following records
      // [0, 1, 2]
      // null
      writer.setPosition(0); // optional
      writer.startList();
      for (int i = 0; i < 3; i++) {
        writer.integer().writeInt(i);
      }
      writer.endList();

      writer.setPosition(2);
      writer.startList();
      writer.endList();

      writer.setValueCount(2);

      ArrowBufPointerTreeGenerator generator = new ArrowBufPointerTreeGenerator();

      ArrowBufPointerNode root = vector.accept(generator, 0);

      assertFalse(root.isLeafNode());
      ArrowBufPointerNode[] children = root.getChildNodes();
      assertEquals(3, children.length);
      for (int i = 0; i < children.length; i++) {
        assertTrue(children[i].isLeafNode());

        ArrowBufPointer pointer = children[i].getArrowBufPointer();
        assertNotNull(pointer.getBuf());
        assertEquals(i * 4, pointer.getOffset());
        assertEquals(4, pointer.getLength());
        assertEquals(i, pointer.getBuf().getInt(pointer.getOffset()));
      }

      root = vector.accept(generator, 1);

      assertTrue(root.isLeafNode());
      ArrowBufPointer pointer = root.getArrowBufPointer();
      assertNull(pointer.getBuf());
    }
  }

  @Test
  public void testFixedSizeListVector() {
    final int listElementLength = 5;
    try (FixedSizeListVector vector = FixedSizeListVector.empty("list", listElementLength, allocator)) {
      IntVector nested =
              (IntVector) vector.addOrGetVector(FieldType.nullable(Types.MinorType.INT.getType())).getVector();
      vector.allocateNew();

      // populate list vector with the following records
      // [10, 20, 30, 40, 50]
      // null
      vector.setNotNull(0);
      for (int i = 0; i < listElementLength; i++) {
        nested.set(i, (i + 1) * 10);
      }
      vector.setNull(1);
      vector.setValueCount(2);

      ArrowBufPointerTreeGenerator generator = new ArrowBufPointerTreeGenerator();

      ArrowBufPointerNode root = vector.accept(generator, 0);

      assertFalse(root.isLeafNode());
      ArrowBufPointerNode[] children = root.getChildNodes();
      assertEquals(listElementLength, children.length);
      for (int i = 0; i < children.length; i++) {
        assertTrue(children[i].isLeafNode());

        ArrowBufPointer pointer = children[i].getArrowBufPointer();
        assertNotNull(pointer.getBuf());
        assertEquals(i * 4, pointer.getOffset());
        assertEquals(4, pointer.getLength());
        assertEquals((i + 1) * 10, pointer.getBuf().getInt(pointer.getOffset()));
      }

      root = vector.accept(generator, 1);

      assertTrue(root.isLeafNode());
      ArrowBufPointer pointer = root.getArrowBufPointer();
      assertNull(pointer.getBuf());
    }
  }

  @Test
  public void testStructVector() {
    try (final StructVector vector = StructVector.empty("vector", allocator)) {
      IntVector child1 = vector.addOrGet(
              "child1", FieldType.nullable(Types.MinorType.INT.getType()), IntVector.class);
      child1.allocateNew(1);
      child1.setValueCount(1);
      BigIntVector child2 = vector.addOrGet(
              "child2", FieldType.nullable(Types.MinorType.BIGINT.getType()), BigIntVector.class);
      child2.allocateNew(1);
      child2.setValueCount(1);

      vector.setIndexDefined(0);
      child1.set(0, 10);
      child2.set(0, 20L);

      vector.setNull(1);
      vector.setValueCount(2);

      ArrowBufPointerTreeGenerator generator = new ArrowBufPointerTreeGenerator();

      ArrowBufPointerNode root = vector.accept(generator, 0);

      assertFalse(root.isLeafNode());
      ArrowBufPointerNode[] children = root.getChildNodes();
      assertEquals(2, children.length);

      assertTrue(children[0].isLeafNode());
      ArrowBufPointer pointer0 = children[0].getArrowBufPointer();
      assertNotNull(pointer0.getBuf());
      assertEquals(0, pointer0.getOffset());
      assertEquals(4, pointer0.getLength());
      assertEquals(10, pointer0.getBuf().getInt(pointer0.getOffset()));

      assertTrue(children[1].isLeafNode());
      ArrowBufPointer pointer1 = children[1].getArrowBufPointer();
      assertNotNull(pointer1.getBuf());
      assertEquals(0, pointer1.getOffset());
      assertEquals(8, pointer1.getLength());
      assertEquals(20L, pointer1.getBuf().getLong(pointer1.getOffset()));

      root = vector.accept(generator, 1);

      assertTrue(root.isLeafNode());
      ArrowBufPointer pointer = root.getArrowBufPointer();
      assertNull(pointer.getBuf());
    }
  }

  @Test
  public void testUnionVector() {
    final NullableUInt4Holder uInt4Holder = new NullableUInt4Holder();
    uInt4Holder.value = 100;
    uInt4Holder.isSet = 1;

    try (UnionVector vector = UnionVector.empty("vector", allocator)) {
      vector.allocateNew();

      // write some data
      vector.setType(0, Types.MinorType.UINT4);
      vector.setSafe(0, uInt4Holder);
      vector.setValueCount(2);

      ArrowBufPointerTreeGenerator generator = new ArrowBufPointerTreeGenerator();

      ArrowBufPointerNode root = vector.accept(generator, 0);

      assertTrue(root.isLeafNode());
      ArrowBufPointer pointer = root.getArrowBufPointer();
      assertNotNull(pointer.getBuf());
      assertEquals(0, pointer.getOffset());
      assertEquals(4, pointer.getLength());
      assertEquals(100, pointer.getBuf().getInt(pointer.getOffset()));

      root = vector.accept(generator, 1);

      assertTrue(root.isLeafNode());
      pointer = root.getArrowBufPointer();
      assertNull(pointer.getBuf());
    }
  }

  /**
   * Map vector is a case for testing nested complex vector.
   */
  @Test
  public void testMapVector() {
    int count = 5;
    try (MapVector mapVector = MapVector.empty("map", allocator, false)) {
      mapVector.allocateNew();

      UnionMapWriter mapWriter = mapVector.getWriter();
      mapWriter.startMap();
      for (int i = 0; i < count; i++) {
        mapWriter.startEntry();
        mapWriter.key().bigInt().writeBigInt(i);
        mapWriter.value().integer().writeInt(i);
        mapWriter.endEntry();
      }
      mapWriter.endMap();

      mapWriter.startMap();
      mapWriter.endMap();

      mapVector.setValueCount(2);

      ArrowBufPointerTreeGenerator generator = new ArrowBufPointerTreeGenerator();

      ArrowBufPointerNode root = mapVector.accept(generator, 0);

      assertFalse(root.isLeafNode());
      ArrowBufPointerNode[] children = root.getChildNodes();
      assertEquals(count, children.length);

      for (int i = 0; i < count; i++) {
        assertFalse(children[i].isLeafNode());
        ArrowBufPointerNode[] grandChildren = children[i].getChildNodes();
        assertEquals(2, grandChildren.length);

        assertTrue(grandChildren[0].isLeafNode());
        ArrowBufPointer keyPointer = grandChildren[0].getArrowBufPointer();
        assertNotNull(keyPointer.getBuf());
        assertEquals(i * 8, keyPointer.getOffset());
        assertEquals(8, keyPointer.getLength());
        assertEquals(i, keyPointer.getBuf().getLong(keyPointer.getOffset()));

        assertTrue(grandChildren[1].isLeafNode());
        ArrowBufPointer valuePointer = grandChildren[1].getArrowBufPointer();
        assertNotNull(valuePointer.getBuf());
        assertEquals(i * 4, valuePointer.getOffset());
        assertEquals(4, valuePointer.getLength());
        assertEquals(i, valuePointer.getBuf().getInt(valuePointer.getOffset()));
      }
    }
  }

  @Test
  public void testNullVector() {
    try (NullVector vector = new NullVector()) {
      ArrowBufPointerTreeGenerator generator = new ArrowBufPointerTreeGenerator();
      ArrowBufPointerNode node = vector.accept(generator, 0);
      assertTrue(node.isLeafNode());
      assertNull(node.getArrowBufPointer().getBuf());
    }
  }
}
