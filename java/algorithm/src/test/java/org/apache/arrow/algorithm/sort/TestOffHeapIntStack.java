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

package org.apache.arrow.algorithm.sort;

import static junit.framework.TestCase.assertEquals;
import static junit.framework.TestCase.assertTrue;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * Test cases for {@link OffHeapIntStack}.
 */
public class TestOffHeapIntStack {

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
  public void testPushPop() {
    try (OffHeapIntStack stack = new OffHeapIntStack(allocator)) {
      assertTrue(stack.isEmpty());

      final int elemCount = 500;
      for (int i = 0; i < elemCount; i++) {
        stack.push(i);
        assertEquals(i, stack.getTop());
      }

      assertEquals(elemCount, stack.getCount());

      for (int i = 0; i < elemCount; i++) {
        assertEquals(elemCount - i - 1, stack.getTop());
        assertEquals(elemCount - i - 1, stack.pop());
      }

      assertTrue(stack.isEmpty());
    }
  }
}
