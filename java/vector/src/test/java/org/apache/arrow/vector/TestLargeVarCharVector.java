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

package org.apache.arrow.vector;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.OutOfMemoryException;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.holders.NullableLargeVarCharHolder;
import org.apache.arrow.vector.ipc.message.ArrowRecordBatch;
import org.apache.arrow.vector.testing.ValueVectorDataPopulator;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.arrow.vector.util.OversizedAllocationException;
import org.apache.arrow.vector.util.Text;
import org.apache.arrow.vector.util.TransferPair;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.jupiter.api.Assertions;

public class TestLargeVarCharVector {

  private static final byte[] STR1 = "AAAAA1".getBytes(StandardCharsets.UTF_8);
  private static final byte[] STR2 = "BBBBBBBBB2".getBytes(StandardCharsets.UTF_8);
  private static final byte[] STR3 = "CCCC3".getBytes(StandardCharsets.UTF_8);
  private static final byte[] STR4 = "DDDDDDDD4".getBytes(StandardCharsets.UTF_8);
  private static final byte[] STR5 = "EEE5".getBytes(StandardCharsets.UTF_8);
  private static final byte[] STR6 = "FFFFF6".getBytes(StandardCharsets.UTF_8);

  private BufferAllocator allocator;

  @Before
  public void prepare() {
    allocator = new RootAllocator(Integer.MAX_VALUE);
  }

  @After
  public void shutdown() {
    allocator.close();
  }

  @Test
  public void testTransfer() {
    try (BufferAllocator childAllocator1 = allocator.newChildAllocator("child1", 1000000, 1000000);
         BufferAllocator childAllocator2 = allocator.newChildAllocator("child2", 1000000, 1000000);
         LargeVarCharVector v1 = new LargeVarCharVector("v1", childAllocator1);
         LargeVarCharVector v2 = new LargeVarCharVector("v2", childAllocator2);) {
      v1.allocateNew();
      v1.setSafe(4094, "hello world".getBytes(StandardCharsets.UTF_8), 0, 11);
      v1.setValueCount(4001);

      long memoryBeforeTransfer = childAllocator1.getAllocatedMemory();

      v1.makeTransferPair(v2).transfer();

      assertEquals(0, childAllocator1.getAllocatedMemory());
      assertEquals(memoryBeforeTransfer, childAllocator2.getAllocatedMemory());
    }
  }

  @Test
  public void testCopyValueSafe() {
    try (final LargeVarCharVector largeVarCharVector = new LargeVarCharVector("myvector", allocator);
         final LargeVarCharVector newLargeVarCharVector = new LargeVarCharVector("newvector", allocator)) {
      largeVarCharVector.allocateNew(10000, 1000);

      final int valueCount = 500;
      populateLargeVarcharVector(largeVarCharVector, valueCount, null);

      final TransferPair tp = largeVarCharVector.makeTransferPair(newLargeVarCharVector);

      // new vector memory is not pre-allocated, we expect copyValueSafe work fine.
      for (int i = 0; i < valueCount; i++) {
        tp.copyValueSafe(i, i);
      }
      newLargeVarCharVector.setValueCount(valueCount);

      for (int i = 0; i < valueCount; i++) {
        final boolean expectedSet = (i % 3) == 0;
        if (expectedSet) {
          assertFalse(largeVarCharVector.isNull(i));
          assertFalse(newLargeVarCharVector.isNull(i));
          assertArrayEquals(largeVarCharVector.get(i), newLargeVarCharVector.get(i));
        } else {
          assertTrue(newLargeVarCharVector.isNull(i));
        }
      }
    }
  }

  @Test
  public void testSplitAndTransferNon() {
    try (final LargeVarCharVector largeVarCharVector = new LargeVarCharVector("myvector", allocator)) {

      largeVarCharVector.allocateNew(10000, 1000);
      final int valueCount = 500;
      populateLargeVarcharVector(largeVarCharVector, valueCount, null);

      final TransferPair tp = largeVarCharVector.getTransferPair(allocator);
      try (LargeVarCharVector newLargeVarCharVector = (LargeVarCharVector) tp.getTo()) {

        tp.splitAndTransfer(0, 0);
        assertEquals(0, newLargeVarCharVector.getValueCount());
      }
    }
  }

  @Test
  public void testSplitAndTransferAll() {
    try (final LargeVarCharVector largeVarCharVector = new LargeVarCharVector("myvector", allocator)) {

      largeVarCharVector.allocateNew(10000, 1000);
      final int valueCount = 500;
      populateLargeVarcharVector(largeVarCharVector, valueCount, null);

      final TransferPair tp = largeVarCharVector.getTransferPair(allocator);
      try (LargeVarCharVector newLargeVarCharVector = (LargeVarCharVector) tp.getTo()) {

        tp.splitAndTransfer(0, valueCount);
        assertEquals(valueCount, newLargeVarCharVector.getValueCount());
      }
    }
  }

  @Test
  public void testInvalidStartIndex() {
    try (final LargeVarCharVector largeVarCharVector = new LargeVarCharVector("myvector", allocator);
         final LargeVarCharVector newLargeVarCharVector = new LargeVarCharVector("newvector", allocator)) {

      largeVarCharVector.allocateNew(10000, 1000);
      final int valueCount = 500;
      populateLargeVarcharVector(largeVarCharVector, valueCount, null);

      final TransferPair tp = largeVarCharVector.makeTransferPair(newLargeVarCharVector);

      IllegalArgumentException e = Assertions.assertThrows(
          IllegalArgumentException.class,
          () -> tp.splitAndTransfer(valueCount, 10));

      assertEquals("Invalid startIndex: 500", e.getMessage());
    }
  }

  @Test
  public void testInvalidLength() {
    try (final LargeVarCharVector largeVarCharVector = new LargeVarCharVector("myvector", allocator);
         final LargeVarCharVector newLargeVarCharVector = new LargeVarCharVector("newvector", allocator)) {

      largeVarCharVector.allocateNew(10000, 1000);
      final int valueCount = 500;
      populateLargeVarcharVector(largeVarCharVector, valueCount, null);

      final TransferPair tp = largeVarCharVector.makeTransferPair(newLargeVarCharVector);

      IllegalArgumentException e = Assertions.assertThrows(
          IllegalArgumentException.class,
          () -> tp.splitAndTransfer(0, valueCount * 2));

      assertEquals("Invalid length: 1000", e.getMessage());
    }
  }

  @Test /* LargeVarCharVector */
  public void testSizeOfValueBuffer() {
    try (final LargeVarCharVector vector = new LargeVarCharVector("", allocator)) {
      int valueCount = 100;
      int currentSize = 0;
      vector.setInitialCapacity(valueCount);
      vector.allocateNew();
      vector.setValueCount(valueCount);
      for (int i = 0; i < valueCount; i++) {
        currentSize += i;
        vector.setSafe(i, new byte[i]);
      }

      assertEquals(currentSize, vector.sizeOfValueBuffer());
    }
  }

  @Test
  public void testSetLastSetUsage() {
    final byte[] STR1 = "AAAAA1".getBytes(StandardCharsets.UTF_8);
    final byte[] STR2 = "BBBBBBBBB2".getBytes(StandardCharsets.UTF_8);
    final byte[] STR3 = "CCCC3".getBytes(StandardCharsets.UTF_8);
    final byte[] STR4 = "DDDDDDDD4".getBytes(StandardCharsets.UTF_8);
    final byte[] STR5 = "EEE5".getBytes(StandardCharsets.UTF_8);
    final byte[] STR6 = "FFFFF6".getBytes(StandardCharsets.UTF_8);

    try (final LargeVarCharVector vector = new LargeVarCharVector("myvector", allocator)) {
      vector.allocateNew(1024 * 10, 1024);

      setBytes(0, STR1, vector);
      setBytes(1, STR2, vector);
      setBytes(2, STR3, vector);
      setBytes(3, STR4, vector);
      setBytes(4, STR5, vector);
      setBytes(5, STR6, vector);

      /* Check current lastSet */
      assertEquals(-1, vector.getLastSet());

      /* Check the vector output */
      assertArrayEquals(STR1, vector.get(0));
      assertArrayEquals(STR2, vector.get(1));
      assertArrayEquals(STR3, vector.get(2));
      assertArrayEquals(STR4, vector.get(3));
      assertArrayEquals(STR5, vector.get(4));
      assertArrayEquals(STR6, vector.get(5));

      /*
       * If we don't do setLastSe(5) before setValueCount(), then the latter will corrupt
       * the value vector by filling in all positions [0,valuecount-1] will empty byte arrays.
       * Run the test by commenting out next line and we should see incorrect vector output.
       */
      vector.setLastSet(5);
      vector.setValueCount(20);

      /* Check current lastSet */
      assertEquals(19, vector.getLastSet());

      /* Check the vector output again */
      assertArrayEquals(STR1, vector.get(0));
      assertArrayEquals(STR2, vector.get(1));
      assertArrayEquals(STR3, vector.get(2));
      assertArrayEquals(STR4, vector.get(3));
      assertArrayEquals(STR5, vector.get(4));
      assertArrayEquals(STR6, vector.get(5));
      assertEquals(0, vector.getValueLength(6));
      assertEquals(0, vector.getValueLength(7));
      assertEquals(0, vector.getValueLength(8));
      assertEquals(0, vector.getValueLength(9));
      assertEquals(0, vector.getValueLength(10));
      assertEquals(0, vector.getValueLength(11));
      assertEquals(0, vector.getValueLength(12));
      assertEquals(0, vector.getValueLength(13));
      assertEquals(0, vector.getValueLength(14));
      assertEquals(0, vector.getValueLength(15));
      assertEquals(0, vector.getValueLength(16));
      assertEquals(0, vector.getValueLength(17));
      assertEquals(0, vector.getValueLength(18));
      assertEquals(0, vector.getValueLength(19));

      /* Check offsets */
      assertEquals(0, vector.offsetBuffer.getLong(0 * BaseLargeVariableWidthVector.OFFSET_WIDTH));
      assertEquals(6, vector.offsetBuffer.getLong(1 * BaseLargeVariableWidthVector.OFFSET_WIDTH));
      assertEquals(16, vector.offsetBuffer.getLong(2 * BaseLargeVariableWidthVector.OFFSET_WIDTH));
      assertEquals(21, vector.offsetBuffer.getLong(3 * BaseLargeVariableWidthVector.OFFSET_WIDTH));
      assertEquals(30, vector.offsetBuffer.getLong(4 * BaseLargeVariableWidthVector.OFFSET_WIDTH));
      assertEquals(34, vector.offsetBuffer.getLong(5 * BaseLargeVariableWidthVector.OFFSET_WIDTH));
      assertEquals(40, vector.offsetBuffer.getLong(6 * BaseLargeVariableWidthVector.OFFSET_WIDTH));
      assertEquals(40, vector.offsetBuffer.getLong(7 * BaseLargeVariableWidthVector.OFFSET_WIDTH));
      assertEquals(40, vector.offsetBuffer.getLong(8 * BaseLargeVariableWidthVector.OFFSET_WIDTH));
      assertEquals(40, vector.offsetBuffer.getLong(9 * BaseLargeVariableWidthVector.OFFSET_WIDTH));
      assertEquals(40, vector.offsetBuffer.getLong(10 * BaseLargeVariableWidthVector.OFFSET_WIDTH));
      assertEquals(40, vector.offsetBuffer.getLong(11 * BaseLargeVariableWidthVector.OFFSET_WIDTH));
      assertEquals(40, vector.offsetBuffer.getLong(12 * BaseLargeVariableWidthVector.OFFSET_WIDTH));
      assertEquals(40, vector.offsetBuffer.getLong(13 * BaseLargeVariableWidthVector.OFFSET_WIDTH));
      assertEquals(40, vector.offsetBuffer.getLong(14 * BaseLargeVariableWidthVector.OFFSET_WIDTH));
      assertEquals(40, vector.offsetBuffer.getLong(15 * BaseLargeVariableWidthVector.OFFSET_WIDTH));
      assertEquals(40, vector.offsetBuffer.getLong(16 * BaseLargeVariableWidthVector.OFFSET_WIDTH));
      assertEquals(40, vector.offsetBuffer.getLong(17 * BaseLargeVariableWidthVector.OFFSET_WIDTH));
      assertEquals(40, vector.offsetBuffer.getLong(18 * BaseLargeVariableWidthVector.OFFSET_WIDTH));
      assertEquals(40, vector.offsetBuffer.getLong(19 * BaseLargeVariableWidthVector.OFFSET_WIDTH));
      
      vector.set(19, STR6);
      assertArrayEquals(STR6, vector.get(19));
      assertEquals(40, vector.offsetBuffer.getLong(19 * BaseLargeVariableWidthVector.OFFSET_WIDTH));
      assertEquals(46, vector.offsetBuffer.getLong(20 * BaseLargeVariableWidthVector.OFFSET_WIDTH));
    }
  }

  @Test(expected = OutOfMemoryException.class)
  public void testVectorAllocateNew() {
    try (RootAllocator smallAllocator = new RootAllocator(200);
        LargeVarCharVector vector = new LargeVarCharVector("vec", smallAllocator)) {
      vector.allocateNew();
    }
  }

  @Test(expected = OversizedAllocationException.class)
  public void testLargeVariableVectorReallocation() {
    final LargeVarCharVector vector = new LargeVarCharVector("vector", allocator);
    // edge case 1: value count = MAX_VALUE_ALLOCATION
    final long expectedAllocationInBytes = BaseValueVector.MAX_ALLOCATION_SIZE;
    final int expectedOffsetSize = 10;
    try {
      vector.allocateNew(expectedAllocationInBytes, 10);
      assertTrue(expectedOffsetSize <= vector.getValueCapacity());
      assertTrue(expectedAllocationInBytes <= vector.getDataBuffer().capacity());
      vector.reAlloc();
      assertTrue(expectedOffsetSize * 2 <= vector.getValueCapacity());
      assertTrue(expectedAllocationInBytes * 2 <= vector.getDataBuffer().capacity());
    } finally {
      vector.close();
    }

    // common: value count < MAX_VALUE_ALLOCATION
    try {
      vector.allocateNew(BaseValueVector.MAX_ALLOCATION_SIZE / 2, 0);
      vector.reAlloc(); // value allocation reaches to MAX_VALUE_ALLOCATION
      vector.reAlloc(); // this tests if it overflows
    } finally {
      vector.close();
    }
  }

  @Test
  public void testSplitAndTransfer() {
    try (final LargeVarCharVector largeVarCharVector = new LargeVarCharVector("myvector", allocator)) {
      largeVarCharVector.allocateNew(10000, 1000);

      final int valueCount = 500;
      final String[] compareArray = new String[valueCount];

      populateLargeVarcharVector(largeVarCharVector, valueCount, compareArray);

      final TransferPair tp = largeVarCharVector.getTransferPair(allocator);
      try (final LargeVarCharVector newLargeVarCharVector = (LargeVarCharVector) tp.getTo()) {
        final int[][] startLengths = {{0, 201}, {201, 0}, {201, 200}, {401, 99}};

        for (final int[] startLength : startLengths) {
          final int start = startLength[0];
          final int length = startLength[1];
          tp.splitAndTransfer(start, length);
          for (int i = 0; i < length; i++) {
            final boolean expectedSet = ((start + i) % 3) == 0;
            if (expectedSet) {
              final byte[] expectedValue = compareArray[start + i].getBytes(StandardCharsets.UTF_8);
              assertFalse(newLargeVarCharVector.isNull(i));
              assertArrayEquals(expectedValue, newLargeVarCharVector.get(i));
            } else {
              assertTrue(newLargeVarCharVector.isNull(i));
            }
          }
        }
      }
    }
  }

  @Test
  public void testReallocAfterVectorTransfer() {
    final byte[] STR1 = "AAAAA1".getBytes(StandardCharsets.UTF_8);
    final byte[] STR2 = "BBBBBBBBB2".getBytes(StandardCharsets.UTF_8);

    try (final LargeVarCharVector vector = new LargeVarCharVector("vector", allocator)) {
      /* 4096 values with 10 byte per record */
      vector.allocateNew(4096 * 10, 4096);
      int valueCapacity = vector.getValueCapacity();
      assertTrue(valueCapacity >= 4096);

      /* populate the vector */
      for (int i = 0; i < valueCapacity; i++) {
        if ((i & 1) == 1) {
          vector.set(i, STR1);
        } else {
          vector.set(i, STR2);
        }
      }

      /* Check the vector output */
      for (int i = 0; i < valueCapacity; i++) {
        if ((i & 1) == 1) {
          assertArrayEquals(STR1, vector.get(i));
        } else {
          assertArrayEquals(STR2, vector.get(i));
        }
      }

      /* trigger first realloc */
      vector.setSafe(valueCapacity, STR2, 0, STR2.length);
      assertTrue(vector.getValueCapacity() >= 2 * valueCapacity);
      while (vector.getByteCapacity() < 10 * vector.getValueCapacity()) {
        vector.reallocDataBuffer();
      }

      /* populate the remaining vector */
      for (int i = valueCapacity; i < vector.getValueCapacity(); i++) {
        if ((i & 1) == 1) {
          vector.set(i, STR1);
        } else {
          vector.set(i, STR2);
        }
      }

      /* Check the vector output */
      valueCapacity = vector.getValueCapacity();
      for (int i = 0; i < valueCapacity; i++) {
        if ((i & 1) == 1) {
          assertArrayEquals(STR1, vector.get(i));
        } else {
          assertArrayEquals(STR2, vector.get(i));
        }
      }

      /* trigger second realloc */
      vector.setSafe(valueCapacity + 10, STR2, 0, STR2.length);
      assertTrue(vector.getValueCapacity() >= 2 * valueCapacity);
      while (vector.getByteCapacity() < 10 * vector.getValueCapacity()) {
        vector.reallocDataBuffer();
      }

      /* populate the remaining vector */
      for (int i = valueCapacity; i < vector.getValueCapacity(); i++) {
        if ((i & 1) == 1) {
          vector.set(i, STR1);
        } else {
          vector.set(i, STR2);
        }
      }

      /* Check the vector output */
      valueCapacity = vector.getValueCapacity();
      for (int i = 0; i < valueCapacity; i++) {
        if ((i & 1) == 1) {
          assertArrayEquals(STR1, vector.get(i));
        } else {
          assertArrayEquals(STR2, vector.get(i));
        }
      }

      /* we are potentially working with 4x the size of vector buffer
       * that we initially started with. Now let's transfer the vector.
       */

      TransferPair transferPair = vector.getTransferPair(allocator);
      transferPair.transfer();
      try (LargeVarCharVector toVector = (LargeVarCharVector) transferPair.getTo()) {
        valueCapacity = toVector.getValueCapacity();

        for (int i = 0; i < valueCapacity; i++) {
          if ((i & 1) == 1) {
            assertArrayEquals(STR1, toVector.get(i));
          } else {
            assertArrayEquals(STR2, toVector.get(i));
          }
        }
      }
    }
  }

  @Test
  public void testVectorLoadUnload() {
    try (final LargeVarCharVector vector1 = new LargeVarCharVector("myvector", allocator)) {

      ValueVectorDataPopulator.setVector(vector1, STR1, STR2, STR3, STR4, STR5, STR6);

      assertEquals(5, vector1.getLastSet());
      vector1.setValueCount(15);
      assertEquals(14, vector1.getLastSet());

      /* Check the vector output */
      assertArrayEquals(STR1, vector1.get(0));
      assertArrayEquals(STR2, vector1.get(1));
      assertArrayEquals(STR3, vector1.get(2));
      assertArrayEquals(STR4, vector1.get(3));
      assertArrayEquals(STR5, vector1.get(4));
      assertArrayEquals(STR6, vector1.get(5));

      Field field = vector1.getField();
      String fieldName = field.getName();

      List<Field> fields = new ArrayList<>();
      List<FieldVector> fieldVectors = new ArrayList<>();

      fields.add(field);
      fieldVectors.add(vector1);

      Schema schema = new Schema(fields);

      VectorSchemaRoot schemaRoot1 = new VectorSchemaRoot(schema, fieldVectors, vector1.getValueCount());
      VectorUnloader vectorUnloader = new VectorUnloader(schemaRoot1);

      try (
          ArrowRecordBatch recordBatch = vectorUnloader.getRecordBatch();
          VectorSchemaRoot schemaRoot2 = VectorSchemaRoot.create(schema, allocator);
      ) {

        VectorLoader vectorLoader = new VectorLoader(schemaRoot2);
        vectorLoader.load(recordBatch);

        LargeVarCharVector vector2 = (LargeVarCharVector) schemaRoot2.getVector(fieldName);
        /*
         * lastSet would have internally been set by VectorLoader.load() when it invokes
         * loadFieldBuffers.
         */
        assertEquals(14, vector2.getLastSet());
        vector2.setValueCount(25);
        assertEquals(24, vector2.getLastSet());

        /* Check the vector output */
        assertArrayEquals(STR1, vector2.get(0));
        assertArrayEquals(STR2, vector2.get(1));
        assertArrayEquals(STR3, vector2.get(2));
        assertArrayEquals(STR4, vector2.get(3));
        assertArrayEquals(STR5, vector2.get(4));
        assertArrayEquals(STR6, vector2.get(5));
      }
    }
  }

  @Test
  public void testFillEmptiesUsage() {
    try (final LargeVarCharVector vector = new LargeVarCharVector("myvector", allocator)) {

      vector.allocateNew(1024 * 10, 1024);

      setBytes(0, STR1, vector);
      setBytes(1, STR2, vector);
      setBytes(2, STR3, vector);
      setBytes(3, STR4, vector);
      setBytes(4, STR5, vector);
      setBytes(5, STR6, vector);

      /* Check current lastSet */
      assertEquals(-1, vector.getLastSet());

      /* Check the vector output */
      assertArrayEquals(STR1, vector.get(0));
      assertArrayEquals(STR2, vector.get(1));
      assertArrayEquals(STR3, vector.get(2));
      assertArrayEquals(STR4, vector.get(3));
      assertArrayEquals(STR5, vector.get(4));
      assertArrayEquals(STR6, vector.get(5));

      vector.setLastSet(5);
      /* fill empty byte arrays from index [6, 9] */
      vector.fillEmpties(10);

      /* Check current lastSet */
      assertEquals(9, vector.getLastSet());

      /* Check the vector output */
      assertArrayEquals(STR1, vector.get(0));
      assertArrayEquals(STR2, vector.get(1));
      assertArrayEquals(STR3, vector.get(2));
      assertArrayEquals(STR4, vector.get(3));
      assertArrayEquals(STR5, vector.get(4));
      assertArrayEquals(STR6, vector.get(5));
      assertEquals(0, vector.getValueLength(6));
      assertEquals(0, vector.getValueLength(7));
      assertEquals(0, vector.getValueLength(8));
      assertEquals(0, vector.getValueLength(9));

      setBytes(10, STR1, vector);
      setBytes(11, STR2, vector);

      vector.setLastSet(11);
      /* fill empty byte arrays from index [12, 14] */
      vector.setValueCount(15);

      /* Check current lastSet */
      assertEquals(14, vector.getLastSet());

      /* Check the vector output */
      assertArrayEquals(STR1, vector.get(0));
      assertArrayEquals(STR2, vector.get(1));
      assertArrayEquals(STR3, vector.get(2));
      assertArrayEquals(STR4, vector.get(3));
      assertArrayEquals(STR5, vector.get(4));
      assertArrayEquals(STR6, vector.get(5));
      assertEquals(0, vector.getValueLength(6));
      assertEquals(0, vector.getValueLength(7));
      assertEquals(0, vector.getValueLength(8));
      assertEquals(0, vector.getValueLength(9));
      assertArrayEquals(STR1, vector.get(10));
      assertArrayEquals(STR2, vector.get(11));
      assertEquals(0, vector.getValueLength(12));
      assertEquals(0, vector.getValueLength(13));
      assertEquals(0, vector.getValueLength(14));

      /* Check offsets */
      assertEquals(0,
          vector.offsetBuffer.getLong(0 * BaseLargeVariableWidthVector.OFFSET_WIDTH));
      assertEquals(6,
          vector.offsetBuffer.getLong(1 * BaseLargeVariableWidthVector.OFFSET_WIDTH));
      assertEquals(16,
          vector.offsetBuffer.getLong(2 * BaseLargeVariableWidthVector.OFFSET_WIDTH));
      assertEquals(21,
          vector.offsetBuffer.getLong(3 * BaseLargeVariableWidthVector.OFFSET_WIDTH));
      assertEquals(30,
          vector.offsetBuffer.getLong(4 * BaseLargeVariableWidthVector.OFFSET_WIDTH));
      assertEquals(34,
          vector.offsetBuffer.getLong(5 * BaseLargeVariableWidthVector.OFFSET_WIDTH));

      assertEquals(40,
          vector.offsetBuffer.getLong(6 * BaseLargeVariableWidthVector.OFFSET_WIDTH));
      assertEquals(40,
          vector.offsetBuffer.getLong(7 * BaseLargeVariableWidthVector.OFFSET_WIDTH));
      assertEquals(40,
          vector.offsetBuffer.getLong(8 * BaseLargeVariableWidthVector.OFFSET_WIDTH));
      assertEquals(40,
          vector.offsetBuffer.getLong(9 * BaseLargeVariableWidthVector.OFFSET_WIDTH));
      assertEquals(40,
          vector.offsetBuffer.getLong(10 * BaseLargeVariableWidthVector.OFFSET_WIDTH));

      assertEquals(46,
          vector.offsetBuffer.getLong(11 * BaseLargeVariableWidthVector.OFFSET_WIDTH));
      assertEquals(56,
          vector.offsetBuffer.getLong(12 * BaseLargeVariableWidthVector.OFFSET_WIDTH));

      assertEquals(56,
          vector.offsetBuffer.getLong(13 * BaseLargeVariableWidthVector.OFFSET_WIDTH));
      assertEquals(56,
          vector.offsetBuffer.getLong(14 * BaseLargeVariableWidthVector.OFFSET_WIDTH));
      assertEquals(56,
          vector.offsetBuffer.getLong(15 * BaseLargeVariableWidthVector.OFFSET_WIDTH));
    }
  }

  @Test
  public void testGetBufferAddress1() {
    try (final LargeVarCharVector vector = new LargeVarCharVector("myvector", allocator)) {

      ValueVectorDataPopulator.setVector(vector, STR1, STR2, STR3, STR4, STR5, STR6);
      vector.setValueCount(15);

      /* check the vector output */
      assertArrayEquals(STR1, vector.get(0));
      assertArrayEquals(STR2, vector.get(1));
      assertArrayEquals(STR3, vector.get(2));
      assertArrayEquals(STR4, vector.get(3));
      assertArrayEquals(STR5, vector.get(4));
      assertArrayEquals(STR6, vector.get(5));

      List<ArrowBuf> buffers = vector.getFieldBuffers();
      long bitAddress = vector.getValidityBufferAddress();
      long offsetAddress = vector.getOffsetBufferAddress();
      long dataAddress = vector.getDataBufferAddress();

      assertEquals(3, buffers.size());
      assertEquals(bitAddress, buffers.get(0).memoryAddress());
      assertEquals(offsetAddress, buffers.get(1).memoryAddress());
      assertEquals(dataAddress, buffers.get(2).memoryAddress());
    }
  }

  @Test
  public void testSetNullableLargeVarCharHolder() {
    try (LargeVarCharVector vector = new LargeVarCharVector("", allocator)) {
      vector.allocateNew(100, 10);

      NullableLargeVarCharHolder nullHolder = new NullableLargeVarCharHolder();
      nullHolder.isSet = 0;

      NullableLargeVarCharHolder stringHolder = new NullableLargeVarCharHolder();
      stringHolder.isSet = 1;

      String str = "hello";
      ArrowBuf buf = allocator.buffer(16);
      buf.setBytes(0, str.getBytes(StandardCharsets.UTF_8));

      stringHolder.start = 0;
      stringHolder.end = str.length();
      stringHolder.buffer = buf;

      vector.set(0, nullHolder);
      vector.set(1, stringHolder);

      // verify results
      assertTrue(vector.isNull(0));
      assertEquals(str, new String(Objects.requireNonNull(vector.get(1)), StandardCharsets.UTF_8));

      buf.close();
    }
  }

  @Test
  public void testSetNullableLargeVarCharHolderSafe() {
    try (LargeVarCharVector vector = new LargeVarCharVector("", allocator)) {
      vector.allocateNew(5, 1);

      NullableLargeVarCharHolder nullHolder = new NullableLargeVarCharHolder();
      nullHolder.isSet = 0;

      NullableLargeVarCharHolder stringHolder = new NullableLargeVarCharHolder();
      stringHolder.isSet = 1;

      String str = "hello world";
      ArrowBuf buf = allocator.buffer(16);
      buf.setBytes(0, str.getBytes(StandardCharsets.UTF_8));

      stringHolder.start = 0;
      stringHolder.end = str.length();
      stringHolder.buffer = buf;

      vector.setSafe(0, stringHolder);
      vector.setSafe(1, nullHolder);

      // verify results
      assertEquals(str, new String(Objects.requireNonNull(vector.get(0)), StandardCharsets.UTF_8));
      assertTrue(vector.isNull(1));

      buf.close();
    }
  }

  @Test
  public void testGetNullFromLargeVariableWidthVector() {
    try (final LargeVarCharVector largeVarCharVector = new LargeVarCharVector("largevarcharvec", allocator);
         final LargeVarBinaryVector largeVarBinaryVector = new LargeVarBinaryVector("largevarbinary", allocator)) {
      largeVarCharVector.allocateNew(10, 1);
      largeVarBinaryVector.allocateNew(10, 1);

      largeVarCharVector.setNull(0);
      largeVarBinaryVector.setNull(0);

      assertNull(largeVarCharVector.get(0));
      assertNull(largeVarBinaryVector.get(0));
    }
  }
  
  @Test
  public void testLargeVariableWidthVectorNullHashCode() {
    try (LargeVarCharVector largeVarChVec = new LargeVarCharVector("large var char vector", allocator)) {
      largeVarChVec.allocateNew(100, 1);
      largeVarChVec.setValueCount(1);

      largeVarChVec.set(0, "abc".getBytes(StandardCharsets.UTF_8));
      largeVarChVec.setNull(0);

      assertEquals(0, largeVarChVec.hashCode(0));
    }
  }

  @Test
  public void testUnloadLargeVariableWidthVector() {
    try (final LargeVarCharVector largeVarCharVector = new LargeVarCharVector("var char", allocator)) {
      largeVarCharVector.allocateNew(5, 2);
      largeVarCharVector.setValueCount(2);

      largeVarCharVector.set(0, "abcd".getBytes(StandardCharsets.UTF_8));

      List<ArrowBuf> bufs = largeVarCharVector.getFieldBuffers();
      assertEquals(3, bufs.size());

      ArrowBuf offsetBuf = bufs.get(1);
      ArrowBuf dataBuf = bufs.get(2);

      assertEquals(24, offsetBuf.writerIndex());
      assertEquals(4, offsetBuf.getLong(8));
      assertEquals(4, offsetBuf.getLong(16));

      assertEquals(4, dataBuf.writerIndex());
    }
  }

  @Test
  public void testNullableType() {
    try (final LargeVarCharVector vector = new LargeVarCharVector("", allocator)) {
      vector.setInitialCapacity(512);
      vector.allocateNew();

      assertTrue(vector.getValueCapacity() >= 512);
      int initialCapacity = vector.getValueCapacity();

      try {
        vector.set(initialCapacity, "foo".getBytes(StandardCharsets.UTF_8));
        Assert.fail("Expected out of bounds exception");
      } catch (Exception e) {
        // ok
      }

      vector.reAlloc();
      assertTrue(vector.getValueCapacity() >= 2 * initialCapacity);

      vector.set(initialCapacity, "foo".getBytes(StandardCharsets.UTF_8));
      assertEquals("foo", new String(vector.get(initialCapacity), StandardCharsets.UTF_8));
    }
  }

  @Test
  public void testGetTextRepeatedly() {
    try (final LargeVarCharVector vector = new LargeVarCharVector("myvector", allocator)) {

      ValueVectorDataPopulator.setVector(vector, STR1, STR2);
      vector.setValueCount(2);

      /* check the vector output */
      Text text = new Text();
      vector.read(0, text);
      byte[] result = new byte[(int) text.getLength()];
      System.arraycopy(text.getBytes(), 0, result, 0, (int) text.getLength());
      assertArrayEquals(STR1, result);
      vector.read(1, text);
      result = new byte[(int) text.getLength()];
      System.arraycopy(text.getBytes(), 0, result, 0, (int) text.getLength());
      assertArrayEquals(STR2, text.getBytes());
    }
  }

  @Test
  public void testGetTransferPairWithField() {
    try (BufferAllocator childAllocator1 = allocator.newChildAllocator("child1", 1000000, 1000000);
        LargeVarCharVector v1 = new LargeVarCharVector("v1", childAllocator1)) {
      v1.allocateNew();
      v1.setSafe(4094, "hello world".getBytes(StandardCharsets.UTF_8), 0, 11);
      v1.setValueCount(4001);

      TransferPair tp = v1.getTransferPair(v1.getField(), allocator);
      tp.transfer();
      LargeVarCharVector v2 = (LargeVarCharVector) tp.getTo();
      assertSame(v1.getField(), v2.getField());
      v2.clear();
    }
  }

  private void populateLargeVarcharVector(final LargeVarCharVector vector, int valueCount, String[] values) {
    for (int i = 0; i < valueCount; i += 3) {
      final String s = String.format("%010d", i);
      vector.set(i, s.getBytes(StandardCharsets.UTF_8));
      if (values != null) {
        values[i] = s;
      }
    }
    vector.setValueCount(valueCount);
  }

  public static void setBytes(int index, byte[] bytes, LargeVarCharVector vector) {
    final long currentOffset = vector.offsetBuffer.getLong((long) index * BaseLargeVariableWidthVector.OFFSET_WIDTH);

    BitVectorHelper.setBit(vector.validityBuffer, index);
    vector.offsetBuffer.setLong(
        (long) (index + 1) * BaseLargeVariableWidthVector.OFFSET_WIDTH, currentOffset + bytes.length);
    vector.valueBuffer.setBytes(currentOffset, bytes, 0, bytes.length);
  }
}
