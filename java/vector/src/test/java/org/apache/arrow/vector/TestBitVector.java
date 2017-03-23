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
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class TestBitVector {
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

}
