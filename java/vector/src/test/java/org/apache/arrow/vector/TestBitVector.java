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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.stream.IntStream;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.memory.util.hash.MurmurHasher;
import org.apache.arrow.vector.testing.ValueVectorDataPopulator;
import org.apache.arrow.vector.util.TransferPair;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class TestBitVector {
  private static final String EMPTY_SCHEMA_PATH = "";

  private BufferAllocator allocator;

  @BeforeEach
  public void init() {
    allocator = new RootAllocator(Long.MAX_VALUE);
  }

  @AfterEach
  public void terminate() throws Exception {
    allocator.close();
  }

  @Test
  public void testBitVectorCopyFromSafe() {
    final int size = 20;
    try (final BitVector src = new BitVector(EMPTY_SCHEMA_PATH, allocator);
        final BitVector dst = new BitVector(EMPTY_SCHEMA_PATH, allocator)) {
      src.allocateNew(size);
      dst.allocateNew(10);

      for (int i = 0; i < size; i++) {
        src.set(i, i % 2);
      }
      src.setValueCount(size);

      for (int i = 0; i < size; i++) {
        dst.copyFromSafe(i, i, src);
      }
      dst.setValueCount(size);

      for (int i = 0; i < size; i++) {
        assertEquals(src.getObject(i), dst.getObject(i));
      }
    }
  }

  @Test
  public void testSplitAndTransfer() throws Exception {

    try (final BitVector sourceVector = new BitVector("bitvector", allocator)) {

      sourceVector.allocateNew(40);

      /* populate the bitvector -- 010101010101010101010101..... */
      for (int i = 0; i < 40; i++) {
        if ((i & 1) == 1) {
          sourceVector.set(i, 1);
        } else {
          sourceVector.set(i, 0);
        }
      }

      sourceVector.setValueCount(40);

      /* check the vector output */
      for (int i = 0; i < 40; i++) {
        int result = sourceVector.get(i);
        if ((i & 1) == 1) {
          assertEquals(Integer.toString(1), Integer.toString(result));
        } else {
          assertEquals(Integer.toString(0), Integer.toString(result));
        }
      }

      try (final BitVector toVector = new BitVector("toVector", allocator)) {
        final TransferPair transferPair = sourceVector.makeTransferPair(toVector);

        /*
         * form test cases such that we cover:
         *
         * (1) the start index is exactly where a particular byte starts in the source bit vector
         * (2) the start index is randomly positioned within a byte in the source bit vector
         *    (2.1) the length is a multiple of 8
         *    (2.2) the length is not a multiple of 8
         */
        final int[][] transferLengths = {{0, 8}, {8, 10}, {18, 0}, {18, 8}, {26, 0}, {26, 14}};

        for (final int[] transferLength : transferLengths) {
          final int start = transferLength[0];
          final int length = transferLength[1];

          transferPair.splitAndTransfer(start, length);

          /* check the toVector output after doing splitAndTransfer */
          for (int i = 0; i < length; i++) {
            int actual = toVector.get(i);
            int expected = sourceVector.get(start + i);
            assertEquals(
                expected,
                actual,
                "different data values not expected --> sourceVector index: "
                    + (start + i)
                    + " toVector index: "
                    + i);
          }
        }
      }
    }
  }

  @Test
  public void testSplitAndTransfer1() throws Exception {

    try (final BitVector sourceVector = new BitVector("bitvector", allocator)) {

      sourceVector.allocateNew(8190);

      /* populate the bitvector */
      for (int i = 0; i < 8190; i++) {
        sourceVector.set(i, 1);
      }

      sourceVector.setValueCount(8190);

      /* check the vector output */
      for (int i = 0; i < 8190; i++) {
        int result = sourceVector.get(i);
        assertEquals(Integer.toString(1), Integer.toString(result));
      }

      try (final BitVector toVector = new BitVector("toVector", allocator)) {
        final TransferPair transferPair = sourceVector.makeTransferPair(toVector);

        final int[][] transferLengths = {{0, 4095}, {4095, 4095}};

        for (final int[] transferLength : transferLengths) {
          final int start = transferLength[0];
          final int length = transferLength[1];

          transferPair.splitAndTransfer(start, length);

          /* check the toVector output after doing splitAndTransfer */
          for (int i = 0; i < length; i++) {
            int actual = toVector.get(i);
            int expected = sourceVector.get(start + i);
            assertEquals(
                expected,
                actual,
                "different data values not expected --> sourceVector index: "
                    + (start + i)
                    + " toVector index: "
                    + i);
          }
        }
      }
    }
  }

  @Test
  public void testSplitAndTransfer2() throws Exception {

    try (final BitVector sourceVector = new BitVector("bitvector", allocator)) {

      sourceVector.allocateNew(32);

      /* populate the bitvector */
      for (int i = 0; i < 32; i++) {
        if ((i & 1) == 1) {
          sourceVector.set(i, 1);
        } else {
          sourceVector.set(i, 0);
        }
      }

      sourceVector.setValueCount(32);

      /* check the vector output */
      for (int i = 0; i < 32; i++) {
        int result = sourceVector.get(i);
        if ((i & 1) == 1) {
          assertEquals(Integer.toString(1), Integer.toString(result));
        } else {
          assertEquals(Integer.toString(0), Integer.toString(result));
        }
      }

      try (final BitVector toVector = new BitVector("toVector", allocator)) {
        final TransferPair transferPair = sourceVector.makeTransferPair(toVector);

        final int[][] transferLengths = {
          {5, 22}, {5, 24}, {5, 25}, {5, 27}, {0, 31}, {5, 7}, {2, 3}
        };

        for (final int[] transferLength : transferLengths) {
          final int start = transferLength[0];
          final int length = transferLength[1];

          transferPair.splitAndTransfer(start, length);

          /* check the toVector output after doing splitAndTransfer */
          for (int i = 0; i < length; i++) {
            int actual = toVector.get(i);
            int expected = sourceVector.get(start + i);
            assertEquals(
                expected,
                actual,
                "different data values not expected --> sourceVector index: "
                    + (start + i)
                    + " toVector index: "
                    + i);
          }
        }
      }
    }
  }

  @Test
  public void testReallocAfterVectorTransfer1() {
    try (final BitVector vector = new BitVector(EMPTY_SCHEMA_PATH, allocator)) {
      vector.allocateNew(4096);
      int valueCapacity = vector.getValueCapacity();
      assertEquals(4096, valueCapacity);

      for (int i = 0; i < valueCapacity; i++) {
        if ((i & 1) == 1) {
          vector.setToOne(i);
        }
      }

      for (int i = 0; i < valueCapacity; i++) {
        if ((i & 1) == 1) {
          assertEquals(1, vector.get(i), "unexpected cleared bit at index: " + i);
        } else {
          assertTrue(vector.isNull(i), "unexpected set bit at index: " + i);
        }
      }

      /* trigger first realloc */
      vector.setSafeToOne(valueCapacity);
      assertEquals(valueCapacity * 2, vector.getValueCapacity());

      for (int i = valueCapacity; i < valueCapacity * 2; i++) {
        if ((i & 1) == 1) {
          vector.setToOne(i);
        }
      }

      for (int i = 0; i < valueCapacity * 2; i++) {
        if (((i & 1) == 1) || (i == valueCapacity)) {
          assertEquals(1, vector.get(i), "unexpected cleared bit at index: " + i);
        } else {
          assertTrue(vector.isNull(i), "unexpected set bit at index: " + i);
        }
      }

      /* trigger second realloc */
      vector.setSafeToOne(valueCapacity * 2);
      assertEquals(valueCapacity * 4, vector.getValueCapacity());

      for (int i = valueCapacity * 2; i < valueCapacity * 4; i++) {
        if ((i & 1) == 1) {
          vector.setToOne(i);
        }
      }

      for (int i = 0; i < valueCapacity * 4; i++) {
        if (((i & 1) == 1) || (i == valueCapacity) || (i == valueCapacity * 2)) {
          assertEquals(1, vector.get(i), "unexpected cleared bit at index: " + i);
        } else {
          assertTrue(vector.isNull(i), "unexpected set bit at index: " + i);
        }
      }

      /* now transfer the vector */
      TransferPair transferPair = vector.getTransferPair(allocator);
      transferPair.transfer();
      final BitVector toVector = (BitVector) transferPair.getTo();

      assertEquals(valueCapacity * 4, toVector.getValueCapacity());

      /* realloc the toVector */
      toVector.setSafeToOne(valueCapacity * 4);

      for (int i = 0; i < toVector.getValueCapacity(); i++) {
        if (i <= valueCapacity * 4) {
          if (((i & 1) == 1)
              || (i == valueCapacity)
              || (i == valueCapacity * 2)
              || (i == valueCapacity * 4)) {
            assertEquals(1, toVector.get(i), "unexpected cleared bit at index: " + i);
          } else {
            assertTrue(toVector.isNull(i), "unexpected set bit at index: " + i);
          }
        } else {
          assertTrue(toVector.isNull(i), "unexpected set bit at index: " + i);
        }
      }

      toVector.close();
    }
  }

  @Test
  public void testReallocAfterVectorTransfer2() {
    try (final BitVector vector = new BitVector(EMPTY_SCHEMA_PATH, allocator)) {
      vector.allocateNew(4096);
      int valueCapacity = vector.getValueCapacity();
      assertEquals(4096, valueCapacity);

      for (int i = 0; i < valueCapacity; i++) {
        if ((i & 1) == 1) {
          vector.set(i, 1);
        }
      }

      for (int i = 0; i < valueCapacity; i++) {
        if ((i & 1) == 1) {
          assertFalse(vector.isNull(i), "unexpected cleared bit at index: " + i);
        } else {
          assertTrue(vector.isNull(i), "unexpected set bit at index: " + i);
        }
      }

      /* trigger first realloc */
      vector.setSafe(valueCapacity, 1, 1);
      assertEquals(valueCapacity * 2, vector.getValueCapacity());

      for (int i = valueCapacity; i < valueCapacity * 2; i++) {
        if ((i & 1) == 1) {
          vector.set(i, 1);
        }
      }

      for (int i = 0; i < valueCapacity * 2; i++) {
        if (((i & 1) == 1) || (i == valueCapacity)) {
          assertFalse(vector.isNull(i), "unexpected cleared bit at index: " + i);
        } else {
          assertTrue(vector.isNull(i), "unexpected set bit at index: " + i);
        }
      }

      /* trigger second realloc */
      vector.setSafe(valueCapacity * 2, 1, 1);
      assertEquals(valueCapacity * 4, vector.getValueCapacity());

      for (int i = valueCapacity * 2; i < valueCapacity * 4; i++) {
        if ((i & 1) == 1) {
          vector.set(i, 1);
        }
      }

      for (int i = 0; i < valueCapacity * 4; i++) {
        if (((i & 1) == 1) || (i == valueCapacity) || (i == valueCapacity * 2)) {
          assertFalse(vector.isNull(i), "unexpected cleared bit at index: " + i);
        } else {
          assertTrue(vector.isNull(i), "unexpected set bit at index: " + i);
        }
      }

      /* now transfer the vector */
      TransferPair transferPair = vector.getTransferPair(allocator);
      transferPair.transfer();
      final BitVector toVector = (BitVector) transferPair.getTo();

      assertEquals(valueCapacity * 4, toVector.getValueCapacity());

      /* realloc the toVector */
      toVector.setSafe(valueCapacity * 4, 1, 1);

      for (int i = 0; i < toVector.getValueCapacity(); i++) {
        if (i <= valueCapacity * 4) {
          if (((i & 1) == 1)
              || (i == valueCapacity)
              || (i == valueCapacity * 2)
              || (i == valueCapacity * 4)) {
            assertFalse(toVector.isNull(i), "unexpected cleared bit at index: " + i);
          } else {
            assertTrue(toVector.isNull(i), "unexpected set bit at index: " + i);
          }
        } else {
          assertTrue(toVector.isNull(i), "unexpected set bit at index: " + i);
        }
      }

      toVector.close();
    }
  }

  @Test
  public void testBitVector() {
    // Create a new value vector for 1024 integers
    try (final BitVector vector = new BitVector(EMPTY_SCHEMA_PATH, allocator)) {
      vector.allocateNew(1024);
      vector.setValueCount(1024);

      // Put and set a few values
      vector.set(0, 1);
      vector.set(1, 0);
      vector.set(100, 0);
      vector.set(1022, 1);

      vector.setValueCount(1024);

      assertEquals(1, vector.get(0));
      assertEquals(0, vector.get(1));
      assertEquals(0, vector.get(100));
      assertEquals(1, vector.get(1022));

      assertEquals(1020, vector.getNullCount());

      // test setting the same value twice
      vector.set(0, 1);
      vector.set(0, 1);
      vector.set(1, 0);
      vector.set(1, 0);
      assertEquals(1, vector.get(0));
      assertEquals(0, vector.get(1));

      // test toggling the values
      vector.set(0, 0);
      vector.set(1, 1);
      assertEquals(0, vector.get(0));
      assertEquals(1, vector.get(1));

      // should not change
      assertEquals(1020, vector.getNullCount());

      // Ensure null value
      assertTrue(vector.isNull(3));

      // unset the previously set bits
      vector.setNull(0);
      vector.setNull(1);
      vector.setNull(100);
      vector.setNull(1022);
      // this should set all the array to 0
      assertEquals(1024, vector.getNullCount());

      // set all the array to 1
      for (int i = 0; i < 1024; ++i) {
        assertEquals(1024 - i, vector.getNullCount());
        vector.set(i, 1);
      }

      assertEquals(0, vector.getNullCount());

      vector.allocateNew(1015);
      vector.setValueCount(1015);

      // ensure it has been zeroed
      assertEquals(1015, vector.getNullCount());

      vector.set(0, 1);
      vector.set(1014, 1); // ensure that the last item of the last byte is allocated

      assertEquals(1013, vector.getNullCount());

      vector.zeroVector();
      assertEquals(1015, vector.getNullCount());

      // set all the array to 1
      for (int i = 0; i < 1015; ++i) {
        assertEquals(1015 - i, vector.getNullCount());
        vector.set(i, 1);
      }

      assertEquals(0, vector.getNullCount());
    }
  }

  @Test
  public void testBitVectorRangeSetAllOnes() {
    validateRange(1000, 0, 1000);
    validateRange(1000, 0, 1);
    validateRange(1000, 1, 2);
    validateRange(1000, 5, 6);
    validateRange(1000, 5, 10);
    validateRange(1000, 5, 150);
    validateRange(1000, 5, 27);
    for (int i = 0; i < 8; i++) {
      for (int j = 0; j < 8; j++) {
        validateRange(1000, 10 + i, 27 + j);
        validateRange(1000, i, j);
      }
    }
  }

  private void validateRange(int length, int start, int count) {
    String desc = "[" + start + ", " + (start + count) + ") ";
    try (BitVector bitVector = new BitVector("bits", allocator)) {
      bitVector.reset();
      bitVector.allocateNew(length);
      bitVector.setRangeToOne(start, count);
      for (int i = 0; i < start; i++) {
        assertTrue(bitVector.isNull(i), desc + i);
      }
      for (int i = start; i < start + count; i++) {
        assertEquals(1, bitVector.get(i), desc + i);
      }
      for (int i = start + count; i < length; i++) {
        assertTrue(bitVector.isNull(i), desc + i);
      }
    }
  }

  @Test
  public void testBitVectorHashCode() {
    final int size = 6;
    try (final BitVector vector = new BitVector(EMPTY_SCHEMA_PATH, allocator)) {
      ValueVectorDataPopulator.setVector(vector, 0, 1, null, 0, 1, null);

      int[] hashCodes = new int[size];
      IntStream.range(0, size).forEach(i -> hashCodes[i] = vector.hashCode(i));

      assertTrue(hashCodes[0] == hashCodes[3]);
      assertTrue(hashCodes[1] == hashCodes[4]);
      assertTrue(hashCodes[2] == hashCodes[5]);

      assertFalse(hashCodes[0] == hashCodes[1]);
      assertFalse(hashCodes[0] == hashCodes[2]);
      assertFalse(hashCodes[1] == hashCodes[2]);

      MurmurHasher hasher = new MurmurHasher();

      IntStream.range(0, size).forEach(i -> hashCodes[i] = vector.hashCode(i, hasher));

      assertTrue(hashCodes[0] == hashCodes[3]);
      assertTrue(hashCodes[1] == hashCodes[4]);
      assertTrue(hashCodes[2] == hashCodes[5]);

      assertFalse(hashCodes[0] == hashCodes[1]);
      assertFalse(hashCodes[0] == hashCodes[2]);
      assertFalse(hashCodes[1] == hashCodes[2]);
    }
  }

  @Test
  public void testGetTransferPairWithField() {
    final BitVector fromVector = new BitVector(EMPTY_SCHEMA_PATH, allocator);
    final TransferPair transferPair = fromVector.getTransferPair(fromVector.getField(), allocator);
    final BitVector toVector = (BitVector) transferPair.getTo();
    // Field inside a new vector created by reusing a field should be the same in memory as the
    // original field.
    assertSame(fromVector.getField(), toVector.getField());
  }
}
