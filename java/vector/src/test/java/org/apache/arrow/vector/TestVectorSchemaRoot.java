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

import static org.junit.Assert.assertEquals;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class TestVectorSchemaRoot {
  private BufferAllocator allocator;

  @Before
  public void init() {
    allocator = new RootAllocator(Long.MAX_VALUE);
  }

  @After
  public void terminate() {
    allocator.close();
  }

  @Test
  public void testResetRowCount() {
    final int size = 20;
    try (final BitVector vec1 = new BitVector("bit", allocator);
         final IntVector vec2 = new IntVector("int", allocator)) {
      VectorSchemaRoot vsr = VectorSchemaRoot.of(vec1, vec2);

      vsr.allocateNew();
      assertEquals(vsr.getRowCount(), 0);

      for (int i = 0; i < size; i++) {
        vec1.setSafe(i, i % 2);
        vec2.setSafe(i, i);
      }
      vsr.setRowCount(size);
      checkCount(vec1, vec2, vsr, size);

      vsr.allocateNew();
      checkCount(vec1, vec2, vsr, 0);

      for (int i = 0; i < size; i++) {
        vec1.setSafe(i, i % 2);
        vec2.setSafe(i, i);
      }
      vsr.setRowCount(size);
      checkCount(vec1, vec2, vsr, size);

      vsr.clear();
      checkCount(vec1, vec2, vsr, 0);
    }
  }

  private void checkCount(BitVector vec1, IntVector vec2, VectorSchemaRoot vsr, int count) {
    assertEquals(vec1.getValueCount(), count);
    assertEquals(vec2.getValueCount(), count);
    assertEquals(vsr.getRowCount(), count);
  }
}
