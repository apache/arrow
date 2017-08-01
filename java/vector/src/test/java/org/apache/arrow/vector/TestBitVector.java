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

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.util.TransferPair;
import org.junit.After;
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
      for(int i = 0; i < 40; i++) {
        if((i & 1) ==  1) {
          sourceMutator.set(i, 1);
        }
        else {
          sourceMutator.set(i, 0);
        }
      }

      sourceMutator.setValueCount(40);

      /* check the vector output */
      for(int i = 0; i < 40; i++) {
        int result = sourceAccessor.get(i);
        if((i & 1) ==  1) {
          assertEquals(Integer.toString(1), Integer.toString(result));
        }
        else {
          assertEquals(Integer.toString(0), Integer.toString(result));
        }
      }

      final TransferPair transferPair = sourceVector.getTransferPair(allocator);
      final BitVector toVector = (BitVector)transferPair.getTo();
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
      final int[][] transferLengths = {  {0, 8},     /* (1) */
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
          if((i & 1) == 1) {
            assertEquals(Integer.toString(1), Integer.toString(result));
          }
          else {
            assertEquals(Integer.toString(0), Integer.toString(result));
          }
        }

        toVector.clear();
      }

      sourceVector.close();
    }
  }
}
