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
import static org.junit.Assert.assertTrue;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.util.TransferPair;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.jupiter.api.Assertions;

public class TestSplitAndTransfer {
  private BufferAllocator allocator;

  @Before
  public void init() {
    allocator = new RootAllocator(Long.MAX_VALUE);
  }
  
  @After
  public void terminate() throws Exception {
    allocator.close();
  }

  private void populateVarcharVector(final VarCharVector vector, int valueCount, String[] compareArray) {
    for (int i = 0; i < valueCount; i += 3) {
      final String s = String.format("%010d", i);
      vector.set(i, s.getBytes());
      if (compareArray != null) {
        compareArray[i] = s;
      }
    }
    vector.setValueCount(valueCount);
  }
  
  @Test /* VarCharVector */
  public void test() throws Exception {
    try (final VarCharVector varCharVector = new VarCharVector("myvector", allocator)) {
      varCharVector.allocateNew(10000, 1000);
  
      final int valueCount = 500;
      final String[] compareArray = new String[valueCount];
  
      populateVarcharVector(varCharVector, valueCount, compareArray);
  
      final TransferPair tp = varCharVector.getTransferPair(allocator);
      final VarCharVector newVarCharVector = (VarCharVector) tp.getTo();
      final int[][] startLengths = {{0, 201}, {201, 0}, {201, 200}, {401, 99}};
  
      for (final int[] startLength : startLengths) {
        final int start = startLength[0];
        final int length = startLength[1];
        tp.splitAndTransfer(start, length);
        for (int i = 0; i < length; i++) {
          final boolean expectedSet = ((start + i) % 3) == 0;
          if (expectedSet) {
            final byte[] expectedValue = compareArray[start + i].getBytes();
            assertFalse(newVarCharVector.isNull(i));
            assertArrayEquals(expectedValue, newVarCharVector.get(i));
          } else {
            assertTrue(newVarCharVector.isNull(i));
          }
        }
        newVarCharVector.clear();
      }
    }
  }

  @Test
  public void testMemoryConstrainedTransfer() {
    try (final VarCharVector varCharVector = new VarCharVector("myvector", allocator)) {
      allocator.setLimit(32768); /* set limit of 32KB */

      varCharVector.allocateNew(10000, 1000);

      final int valueCount = 1000;

      populateVarcharVector(varCharVector, valueCount, null);

      final TransferPair tp = varCharVector.getTransferPair(allocator);
      final VarCharVector newVarCharVector = (VarCharVector) tp.getTo();
      final int[][] startLengths = {{0, 700}, {700, 299}};

      for (final int[] startLength : startLengths) {
        final int start = startLength[0];
        final int length = startLength[1];
        tp.splitAndTransfer(start, length);
        newVarCharVector.clear();
      }
    }
  }

  @Test
  public void testTransfer() {
    try (final VarCharVector varCharVector = new VarCharVector("myvector", allocator)) {
      varCharVector.allocateNew(10000, 1000);

      final int valueCount = 500;
      final String[] compareArray = new String[valueCount];
      populateVarcharVector(varCharVector, valueCount, compareArray);

      final TransferPair tp = varCharVector.getTransferPair(allocator);
      final VarCharVector newVarCharVector = (VarCharVector) tp.getTo();
      tp.transfer();

      assertEquals(0, varCharVector.valueCount);
      assertEquals(valueCount, newVarCharVector.valueCount);

      for (int i = 0; i < valueCount; i++) {
        final boolean expectedSet = (i % 3) == 0;
        if (expectedSet) {
          final byte[] expectedValue = compareArray[i].getBytes();
          assertFalse(newVarCharVector.isNull(i));
          assertArrayEquals(expectedValue, newVarCharVector.get(i));
        } else {
          assertTrue(newVarCharVector.isNull(i));
        }
      }

      newVarCharVector.clear();
    }
  }

  @Test
  public void testCopyValueSafe() {
    try (final VarCharVector varCharVector = new VarCharVector("myvector", allocator);
         final VarCharVector newVarCharVector = new VarCharVector("newvector", allocator)) {
      varCharVector.allocateNew(10000, 1000);

      final int valueCount = 500;
      populateVarcharVector(varCharVector, valueCount, null);

      final TransferPair tp = varCharVector.makeTransferPair(newVarCharVector);

      // new vector memory is not pre-allocated, we expect copyValueSafe work fine.
      for (int i = 0; i < valueCount; i++) {
        tp.copyValueSafe(i, i);
      }
      newVarCharVector.setValueCount(valueCount);

      for (int i = 0; i < valueCount; i++) {
        final boolean expectedSet = (i % 3) == 0;
        if (expectedSet) {
          assertFalse(varCharVector.isNull(i));
          assertFalse(newVarCharVector.isNull(i));
          assertArrayEquals(varCharVector.get(i), newVarCharVector.get(i));
        } else {
          assertTrue(newVarCharVector.isNull(i));
        }
      }
    }
  }

  @Test
  public void testSplitAndTransferNon() {
    try (final VarCharVector varCharVector = new VarCharVector("myvector", allocator)) {

      varCharVector.allocateNew(10000, 1000);
      final int valueCount = 500;
      populateVarcharVector(varCharVector, valueCount, null);

      final TransferPair tp = varCharVector.getTransferPair(allocator);
      VarCharVector newVarCharVector = (VarCharVector) tp.getTo();

      tp.splitAndTransfer(0, 0);
      assertEquals(0, newVarCharVector.getValueCount());

      newVarCharVector.clear();
    }
  }

  @Test
  public void testSplitAndTransferAll() {
    try (final VarCharVector varCharVector = new VarCharVector("myvector", allocator)) {

      varCharVector.allocateNew(10000, 1000);
      final int valueCount = 500;
      populateVarcharVector(varCharVector, valueCount, null);

      final TransferPair tp = varCharVector.getTransferPair(allocator);
      VarCharVector newVarCharVector = (VarCharVector) tp.getTo();

      tp.splitAndTransfer(0, valueCount);
      assertEquals(valueCount, newVarCharVector.getValueCount());

      newVarCharVector.clear();
    }
  }

  @Test
  public void testInvalidStartIndex() {
    try (final VarCharVector varCharVector = new VarCharVector("myvector", allocator);
        final VarCharVector newVarCharVector = new VarCharVector("newvector", allocator)) {

      varCharVector.allocateNew(10000, 1000);
      final int valueCount = 500;
      populateVarcharVector(varCharVector, valueCount, null);

      final TransferPair tp = varCharVector.makeTransferPair(newVarCharVector);

      IllegalArgumentException e = Assertions.assertThrows(
          IllegalArgumentException.class,
          () -> tp.splitAndTransfer(valueCount, 10));

      assertEquals("Invalid startIndex: 500", e.getMessage());

      newVarCharVector.clear();
    }
  }

  @Test
  public void testInvalidLength() {
    try (final VarCharVector varCharVector = new VarCharVector("myvector", allocator);
        final VarCharVector newVarCharVector = new VarCharVector("newvector", allocator)) {

      varCharVector.allocateNew(10000, 1000);
      final int valueCount = 500;
      populateVarcharVector(varCharVector, valueCount, null);

      final TransferPair tp = varCharVector.makeTransferPair(newVarCharVector);

      IllegalArgumentException e = Assertions.assertThrows(
          IllegalArgumentException.class,
          () -> tp.splitAndTransfer(0, valueCount * 2));

      assertEquals("Invalid length: 1000", e.getMessage());

      newVarCharVector.clear();
    }
  }
}
