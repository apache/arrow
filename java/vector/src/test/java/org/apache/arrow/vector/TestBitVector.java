/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.arrow.vector;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.util.TransferPair;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class TestBitVector {
  private final static String EMPTY_SCHEMA_PATH = "";

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
  public void testBitVectorCopyFromSafe() {
    final int size = 20;
    try (final BitVector src = new BitVector(EMPTY_SCHEMA_PATH, allocator);
         final BitVector dst = new BitVector(EMPTY_SCHEMA_PATH, allocator)) {
      src.allocateNew(size);
      dst.allocateNew(10);

      for (int i = 0; i < size; i++) {
        src.getMutator().set(i, i % 2);
      }
      src.getMutator().setValueCount(size);

      for (int i = 0; i < size; i++) {
        dst.copyFromSafe(i, i, src);
      }
      dst.getMutator().setValueCount(size);

      for (int i = 0; i < size; i++) {
        assertEquals(src.getAccessor().getObject(i), dst.getAccessor().getObject(i));
      }
    }
  }

  @Test
  public void testSplitAndTransfer() throws Exception {

    try (final BitVector sourceVector = new BitVector("bitvector", allocator)) {
      final BitVector.Mutator sourceMutator = sourceVector.getMutator();
      final BitVector.Accessor sourceAccessor = sourceVector.getAccessor();

      sourceVector.allocateNew(40);

      /* populate the bitvector -- 010101010101010101010101..... */
      for (int i = 0; i < 40; i++) {
        if ((i & 1) == 1) {
          sourceMutator.set(i, 1);
        } else {
          sourceMutator.set(i, 0);
        }
      }

      sourceMutator.setValueCount(40);

      /* check the vector output */
      for (int i = 0; i < 40; i++) {
        int result = sourceAccessor.get(i);
        if ((i & 1) == 1) {
          assertEquals(Integer.toString(1), Integer.toString(result));
        } else {
          assertEquals(Integer.toString(0), Integer.toString(result));
        }
      }

      final TransferPair transferPair = sourceVector.getTransferPair(allocator);
      final BitVector toVector = (BitVector) transferPair.getTo();
      final BitVector.Accessor toAccessor = toVector.getAccessor();
      final BitVector.Mutator toMutator = toVector.getMutator();

      /*
       * form test cases such that we cover:
       *
       * (1) the start index is exactly where a particular byte starts in the source bit vector
       * (2) the start index is randomly positioned within a byte in the source bit vector
       *    (2.1) the length is a multiple of 8
       *    (2.2) the length is not a multiple of 8
       */
      final int[][] transferLengths = {{0, 8},     /* (1) */
          {8, 10},    /* (1) */
          {18, 0},    /* zero length scenario */
          {18, 8},    /* (2.1) */
          {26, 0},    /* zero length scenario */
          {26, 14}    /* (2.2) */
      };

      for (final int[] transferLength : transferLengths) {
        final int start = transferLength[0];
        final int length = transferLength[1];

        transferPair.splitAndTransfer(start, length);

        /* check the toVector output after doing splitAndTransfer */
        for (int i = 0; i < length; i++) {
          int result = toAccessor.get(i);
          if ((i & 1) == 1) {
            assertEquals(Integer.toString(1), Integer.toString(result));
          } else {
            assertEquals(Integer.toString(0), Integer.toString(result));
          }
        }

        toVector.clear();
      }

      sourceVector.close();
    }
  }

  @Test
  public void testBitVector() {
    // Create a new value vector for 1024 integers
    try (final BitVector vector = new BitVector(EMPTY_SCHEMA_PATH, allocator)) {
      final BitVector.Mutator m = vector.getMutator();
      vector.allocateNew(1024);
      m.setValueCount(1024);

      // Put and set a few values
      m.set(0, 1);
      m.set(1, 0);
      m.set(100, 0);
      m.set(1022, 1);

      m.setValueCount(1024);

      final BitVector.Accessor accessor = vector.getAccessor();
      assertEquals(1, accessor.get(0));
      assertEquals(0, accessor.get(1));
      assertEquals(0, accessor.get(100));
      assertEquals(1, accessor.get(1022));

      assertEquals(1022, accessor.getNullCount());

      // test setting the same value twice
      m.set(0, 1);
      m.set(0, 1);
      m.set(1, 0);
      m.set(1, 0);
      assertEquals(1, accessor.get(0));
      assertEquals(0, accessor.get(1));

      // test toggling the values
      m.set(0, 0);
      m.set(1, 1);
      assertEquals(0, accessor.get(0));
      assertEquals(1, accessor.get(1));

      // should not change
      assertEquals(1022, accessor.getNullCount());

      // Ensure unallocated space returns 0
      assertEquals(0, accessor.get(3));

      // unset the previously set bits
      m.set(1, 0);
      m.set(1022, 0);
      // this should set all the array to 0
      assertEquals(1024, accessor.getNullCount());

      // set all the array to 1
      for (int i = 0; i < 1024; ++i) {
        assertEquals(1024 - i, accessor.getNullCount());
        m.set(i, 1);
      }

      assertEquals(0, accessor.getNullCount());

      vector.allocateNew(1015);
      m.setValueCount(1015);

      // ensure it has been zeroed
      assertEquals(1015, accessor.getNullCount());

      m.set(0, 1);
      m.set(1014, 1); // ensure that the last item of the last byte is allocated

      assertEquals(1013, accessor.getNullCount());

      vector.zeroVector();
      assertEquals(1015, accessor.getNullCount());

      // set all the array to 1
      for (int i = 0; i < 1015; ++i) {
        assertEquals(1015 - i, accessor.getNullCount());
        m.set(i, 1);
      }

      assertEquals(0, accessor.getNullCount());
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
      bitVector.getMutator().setRangeToOne(start, count);
      for (int i = 0; i < start; i++) {
        Assert.assertEquals(desc + i, 0, bitVector.getAccessor().get(i));
      }
      for (int i = start; i < start + count; i++) {
        Assert.assertEquals(desc + i, 1, bitVector.getAccessor().get(i));
      }
      for (int i = start + count; i < length; i++) {
        Assert.assertEquals(desc + i, 0, bitVector.getAccessor().get(i));
      }
    }
  }
}
