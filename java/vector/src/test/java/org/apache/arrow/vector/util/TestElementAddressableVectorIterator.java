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
import static org.junit.Assert.assertEquals;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.memory.util.ArrowBufPointer;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VarCharVector;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * Test cases for {@link ElementAddressableVectorIterator}.
 */
public class TestElementAddressableVectorIterator {

  private final int VECTOR_LENGTH = 100;

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
  public void testIterateIntVector() {
    try (IntVector intVector = new IntVector("", allocator)) {
      intVector.allocateNew(VECTOR_LENGTH);
      intVector.setValueCount(VECTOR_LENGTH);

      // prepare data in sorted order
      for (int i = 0; i < VECTOR_LENGTH; i++) {
        if (i == 0) {
          intVector.setNull(i);
        } else {
          intVector.set(i, i);
        }
      }

      // iterate
      ElementAddressableVectorIterator<IntVector> it = new ElementAddressableVectorIterator<>(intVector);
      int index = 0;
      while (it.hasNext()) {
        ArrowBufPointer pt;

        if (index % 2 == 0) {
          // use populated pointer.
          pt = new ArrowBufPointer();
          it.next(pt);
        } else {
          // use iterator inner pointer
          pt = it.next();
        }
        if (index == 0) {
          assertNull(pt.getBuf());
        } else {
          assertEquals(index, pt.getBuf().getInt(pt.getOffset()));
        }
        index += 1;
      }
    }
  }

  @Test
  public void testIterateVarCharVector() {
    try (VarCharVector strVector = new VarCharVector("", allocator)) {
      strVector.allocateNew(VECTOR_LENGTH * 10, VECTOR_LENGTH);
      strVector.setValueCount(VECTOR_LENGTH);

      // prepare data in sorted order
      for (int i = 0; i < VECTOR_LENGTH; i++) {
        if (i == 0) {
          strVector.setNull(i);
        } else {
          strVector.set(i, String.valueOf(i).getBytes());
        }
      }

      // iterate
      ElementAddressableVectorIterator<VarCharVector> it = new ElementAddressableVectorIterator<>(strVector);
      int index = 0;
      while (it.hasNext()) {
        ArrowBufPointer pt;

        if (index % 2 == 0) {
          // use populated pointer.
          pt = new ArrowBufPointer();
          it.next(pt);
        } else {
          // use iterator inner pointer
          pt = it.next();
        }

        if (index == 0) {
          assertNull(pt.getBuf());
        } else {
          String expected = String.valueOf(index);
          byte[] actual = new byte[expected.length()];
          assertEquals(expected.length(), pt.getLength());

          pt.getBuf().getBytes(pt.getOffset(), actual);
          assertEquals(expected, new String(actual));
        }
        index += 1;
      }
    }
  }
}
