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

package org.apache.arrow.memory.util;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.apache.arrow.memory.BufferAllocator;
import org.junit.Before;
import org.junit.Test;

/**
 * Test cases for {@link GlobalAllocator}.
 */
public class GlobalAllocatorTest {
  @Before
  public void init() {
    GlobalAllocator.checkGlobalCleanUpResources();
  }

  @Test
  public void testGlobalAllocator() {
    assertGlobalAllocatorEmpty();
    try (BufferAllocator allocator = GlobalAllocator.getChildAllocator()) {
      GlobalAllocator.verifyStateOfAllocator();
      assertEquals(0, allocator.getAllocatedMemory());
      assertTrue(allocator.getName().contains("ChildAllocator-"));
      assertTrue(GlobalAllocator.hasActiveAllocators());
    }
    assertGlobalAllocatorEmpty();
  }

  @Test
  public void testGlobalAllocatorHasActiveAllocators() {
    BufferAllocator allocator = GlobalAllocator.getChildAllocator();
    assertTrue(GlobalAllocator.hasActiveAllocators());
    IllegalStateException e = assertThrows(IllegalStateException.class,
        () -> GlobalAllocator.checkGlobalCleanUpResources());
    assertTrue(e.getMessage().contains("Cannot continue with active allocators:"));
    allocator.close();
  }

  private void assertGlobalAllocatorEmpty() {
    GlobalAllocator.verifyStateOfAllocator();
    assertFalse(GlobalAllocator.hasActiveAllocators());
    assertEquals(0, GlobalAllocator.getAllChildAllocatedMemory());
  }
}
