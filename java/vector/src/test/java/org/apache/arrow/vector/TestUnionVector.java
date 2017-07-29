/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.arrow.vector;

import static org.junit.Assert.assertEquals;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.complex.UnionVector;
import org.apache.arrow.vector.holders.NullableBitHolder;
import org.apache.arrow.vector.holders.NullableIntHolder;
import org.apache.arrow.vector.holders.NullableUInt4Holder;
import org.apache.arrow.vector.holders.NullableFloat4Holder;
import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.Types.MinorType;
import org.apache.arrow.vector.util.TransferPair;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class TestUnionVector {
  private final static String EMPTY_SCHEMA_PATH = "";

  private BufferAllocator allocator;

  @Before
  public void init() {
    allocator = new DirtyRootAllocator(Long.MAX_VALUE, (byte) 100);
  }

  @After
  public void terminate() throws Exception {
    allocator.close();
  }

  @Test
  public void testUnionVector() throws Exception {

    final NullableUInt4Holder uInt4Holder = new NullableUInt4Holder();
    uInt4Holder.value = 100;
    uInt4Holder.isSet = 1;

    try (UnionVector unionVector = new UnionVector(EMPTY_SCHEMA_PATH, allocator, null)) {
      unionVector.allocateNew();

      // write some data
      final UnionVector.Mutator mutator = unionVector.getMutator();
      mutator.setType(0, Types.MinorType.UINT4);
      mutator.setSafe(0, uInt4Holder);
      mutator.setType(2, Types.MinorType.UINT4);
      mutator.setSafe(2, uInt4Holder);
      mutator.setValueCount(4);

      // check that what we wrote is correct
      final UnionVector.Accessor accessor = unionVector.getAccessor();
      assertEquals(4, accessor.getValueCount());

      assertEquals(false, accessor.isNull(0));
      assertEquals(100, accessor.getObject(0));

      assertEquals(true, accessor.isNull(1));

      assertEquals(false, accessor.isNull(2));
      assertEquals(100, accessor.getObject(2));

      assertEquals(true, accessor.isNull(3));
    }
  }

  @Test
  public void testTransfer() throws Exception {
    try (UnionVector srcVector = new UnionVector(EMPTY_SCHEMA_PATH, allocator, null)) {
      srcVector.allocateNew();

      // write some data
      final UnionVector.Mutator mutator = srcVector.getMutator();
      mutator.setType(0, MinorType.INT);
      mutator.setSafe(0, newIntHolder(5));
      mutator.setType(1, MinorType.BIT);
      mutator.setSafe(1, newBitHolder(false));
      mutator.setType(3, MinorType.INT);
      mutator.setSafe(3, newIntHolder(10));
      mutator.setType(5, MinorType.BIT);
      mutator.setSafe(5, newBitHolder(false));
      mutator.setValueCount(6);

      try(UnionVector destVector = new UnionVector(EMPTY_SCHEMA_PATH, allocator, null)) {
        TransferPair pair = srcVector.makeTransferPair(destVector);

        // Creating the transfer should transfer the type of the field at least.
        assertEquals(srcVector.getField(), destVector.getField());

        // transfer
        pair.transfer();

        assertEquals(srcVector.getField(), destVector.getField());

        // now check the values are transferred
        assertEquals(srcVector.getAccessor().getValueCount(), destVector.getAccessor().getValueCount());
        for(int i=0; i<srcVector.getAccessor().getValueCount(); i++) {
          assertEquals("Different values at index " + i, srcVector.getAccessor().get(i), destVector.getAccessor().get(i));
        }
      }
    }
  }

  @Test
  public void testSplitAndTransfer() throws Exception {
    try (UnionVector sourceVector = new UnionVector(EMPTY_SCHEMA_PATH, allocator, null)) {
      final UnionVector.Mutator sourceMutator = sourceVector.getMutator();
      final UnionVector.Accessor sourceAccessor = sourceVector.getAccessor();

      sourceVector.allocateNew();

      /* populate the UnionVector */
      sourceMutator.setType(0, MinorType.INT);
      sourceMutator.setSafe(0, newIntHolder(5));
      sourceMutator.setType(1, MinorType.INT);
      sourceMutator.setSafe(1, newIntHolder(10));
      sourceMutator.setType(2, MinorType.INT);
      sourceMutator.setSafe(2, newIntHolder(15));
      sourceMutator.setType(3, MinorType.INT);
      sourceMutator.setSafe(3, newIntHolder(20));
      sourceMutator.setType(4, MinorType.INT);
      sourceMutator.setSafe(4, newIntHolder(25));
      sourceMutator.setType(5, MinorType.INT);
      sourceMutator.setSafe(5, newIntHolder(30));
      sourceMutator.setType(6, MinorType.INT);
      sourceMutator.setSafe(6, newIntHolder(35));
      sourceMutator.setType(7, MinorType.INT);
      sourceMutator.setSafe(7, newIntHolder(40));
      sourceMutator.setType(8, MinorType.INT);
      sourceMutator.setSafe(8, newIntHolder(45));
      sourceMutator.setType(9, MinorType.INT);
      sourceMutator.setSafe(9, newIntHolder(50));
      sourceMutator.setValueCount(10);

      /* check the vector output */
      assertEquals(10, sourceAccessor.getValueCount());
      assertEquals(false, sourceAccessor.isNull(0));
      assertEquals(5, sourceAccessor.getObject(0));
      assertEquals(false, sourceAccessor.isNull(1));
      assertEquals(10, sourceAccessor.getObject(1));
      assertEquals(false, sourceAccessor.isNull(2));
      assertEquals(15, sourceAccessor.getObject(2));
      assertEquals(false, sourceAccessor.isNull(3));
      assertEquals(20, sourceAccessor.getObject(3));
      assertEquals(false, sourceAccessor.isNull(4));
      assertEquals(25, sourceAccessor.getObject(4));
      assertEquals(false, sourceAccessor.isNull(5));
      assertEquals(30, sourceAccessor.getObject(5));
      assertEquals(false, sourceAccessor.isNull(6));
      assertEquals(35, sourceAccessor.getObject(6));
      assertEquals(false, sourceAccessor.isNull(7));
      assertEquals(40, sourceAccessor.getObject(7));
      assertEquals(false, sourceAccessor.isNull(8));
      assertEquals(45, sourceAccessor.getObject(8));
      assertEquals(false, sourceAccessor.isNull(9));
      assertEquals(50, sourceAccessor.getObject(9));

      try(UnionVector toVector = new UnionVector(EMPTY_SCHEMA_PATH, allocator, null)) {

        final TransferPair transferPair = sourceVector.makeTransferPair(toVector);
        final UnionVector.Accessor toAccessor = toVector.getAccessor();

        final int[][] transferLengths = { {0, 3},
                                          {3, 1},
                                          {4, 2},
                                          {6, 1},
                                          {7, 1},
                                          {8, 2}
                                        };

        for (final int[] transferLength : transferLengths) {
          final int start = transferLength[0];
          final int length = transferLength[1];

          transferPair.splitAndTransfer(start, length);

          /* check the toVector output after doing the splitAndTransfer */
          for (int i = 0; i < length; i++) {
            assertEquals("Different data at indexes: " + (start + i) + "and " + i, sourceAccessor.getObject(start + i),
                         toAccessor.getObject(i));
          }
        }
      }
    }
  }

  @Test
  public void testSplitAndTransferWithMixedVectors() throws Exception {
    try (UnionVector sourceVector = new UnionVector(EMPTY_SCHEMA_PATH, allocator, null)) {
      final UnionVector.Mutator sourceMutator = sourceVector.getMutator();
      final UnionVector.Accessor sourceAccessor = sourceVector.getAccessor();

      sourceVector.allocateNew();

      /* populate the UnionVector */
      sourceMutator.setType(0, MinorType.INT);
      sourceMutator.setSafe(0, newIntHolder(5));

      sourceMutator.setType(1, MinorType.FLOAT4);
      sourceMutator.setSafe(1, newFloat4Holder(5.5f));

      sourceMutator.setType(2, MinorType.INT);
      sourceMutator.setSafe(2, newIntHolder(10));

      sourceMutator.setType(3, MinorType.FLOAT4);
      sourceMutator.setSafe(3, newFloat4Holder(10.5f));

      sourceMutator.setType(4, MinorType.INT);
      sourceMutator.setSafe(4, newIntHolder(15));

      sourceMutator.setType(5, MinorType.FLOAT4);
      sourceMutator.setSafe(5, newFloat4Holder(15.5f));

      sourceMutator.setType(6, MinorType.INT);
      sourceMutator.setSafe(6, newIntHolder(20));

      sourceMutator.setType(7, MinorType.FLOAT4);
      sourceMutator.setSafe(7, newFloat4Holder(20.5f));

      sourceMutator.setType(8, MinorType.INT);
      sourceMutator.setSafe(8, newIntHolder(30));

      sourceMutator.setType(9, MinorType.FLOAT4);
      sourceMutator.setSafe(9, newFloat4Holder(30.5f));
      sourceMutator.setValueCount(10);

      /* check the vector output */
      assertEquals(10, sourceAccessor.getValueCount());
      assertEquals(false, sourceAccessor.isNull(0));
      assertEquals(5, sourceAccessor.getObject(0));
      assertEquals(false, sourceAccessor.isNull(1));
      assertEquals(5.5f, sourceAccessor.getObject(1));
      assertEquals(false, sourceAccessor.isNull(2));
      assertEquals(10, sourceAccessor.getObject(2));
      assertEquals(false, sourceAccessor.isNull(3));
      assertEquals(10.5f, sourceAccessor.getObject(3));
      assertEquals(false, sourceAccessor.isNull(4));
      assertEquals(15, sourceAccessor.getObject(4));
      assertEquals(false, sourceAccessor.isNull(5));
      assertEquals(15.5f, sourceAccessor.getObject(5));
      assertEquals(false, sourceAccessor.isNull(6));
      assertEquals(20, sourceAccessor.getObject(6));
      assertEquals(false, sourceAccessor.isNull(7));
      assertEquals(20.5f, sourceAccessor.getObject(7));
      assertEquals(false, sourceAccessor.isNull(8));
      assertEquals(30, sourceAccessor.getObject(8));
      assertEquals(false, sourceAccessor.isNull(9));
      assertEquals(30.5f, sourceAccessor.getObject(9));

      try(UnionVector toVector = new UnionVector(EMPTY_SCHEMA_PATH, allocator, null)) {

        final TransferPair transferPair = sourceVector.makeTransferPair(toVector);
        final UnionVector.Accessor toAccessor = toVector.getAccessor();

        final int[][] transferLengths = { {0, 2},
                                          {2, 1},
                                          {3, 2},
                                          {5, 3},
                                          {8, 2}
                                        };

        for (final int[] transferLength : transferLengths) {
          final int start = transferLength[0];
          final int length = transferLength[1];

          transferPair.splitAndTransfer(start, length);

          /* check the toVector output after doing the splitAndTransfer */
          for (int i = 0; i < length; i++) {
            assertEquals("Different values at index: " + i, sourceAccessor.getObject(start + i), toAccessor.getObject(i));
          }
        }
      }
    }
  }

  private static NullableIntHolder newIntHolder(int value) {
    final NullableIntHolder holder = new NullableIntHolder();
    holder.isSet = 1;
    holder.value = value;
    return holder;
  }

  private static NullableBitHolder newBitHolder(boolean value) {
    final NullableBitHolder holder = new NullableBitHolder();
    holder.isSet = 1;
    holder.value = value ? 1 : 0;
    return holder;
  }

  private static NullableFloat4Holder newFloat4Holder(float value) {
    final NullableFloat4Holder holder = new NullableFloat4Holder();
    holder.isSet = 1;
    holder.value = value;
    return holder;
  }
}
