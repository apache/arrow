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

import static junit.framework.TestCase.assertEquals;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.testing.ValueVectorDataPopulator;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * Test cases for {@link VectorBatchAppender}.
 */
public class TestVectorBatchAppender {

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
  public void testBatchAppendIntVector() {
    final int length1 = 10;
    final int length2 = 5;
    final int length3 = 7;
    try (IntVector target = new IntVector("", allocator);
         IntVector delta1 = new IntVector("", allocator);
         IntVector delta2 = new IntVector("", allocator)) {

      target.allocateNew(length1);
      delta1.allocateNew(length2);
      delta2.allocateNew(length3);

      ValueVectorDataPopulator.setVector(target, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9);
      ValueVectorDataPopulator.setVector(delta1, 10, 11, 12, 13, 14);
      ValueVectorDataPopulator.setVector(delta2, 15, 16, 17, 18, 19, 20, 21);

      VectorBatchAppender.batchAppend(target, delta1, delta2);

      assertEquals(length1 + length2 + length3, target.getValueCount());
      for (int i = 0; i < target.getValueCount(); i++) {
        assertEquals(i, target.get(i));
      }
    }
  }
}
