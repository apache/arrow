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

package org.apache.arrow.memory.util.test;

import static org.junit.Assert.assertNotNull;

import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.memory.BufferAllocator;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

public class TestGlobalAllocatorTestExtension {

  @Test
  @ExtendWith(GlobalAllocatorTestExtension.class)
  public void testExampleScenario() {
    try (
        final BufferAllocator rootAllocator = GlobalAllocator.getChildAllocator();
        final ArrowBuf arrowBuf = rootAllocator.buffer(512)
    ) {
      assertNotNull("allocation failed", arrowBuf);
    }
  }

  @Test
  @ExtendWith(GlobalAllocatorTestExtension.class)
  public void testMemoryLeakScenario() {
    BufferAllocator allocator = GlobalAllocator.getChildAllocator();
    Assertions.assertTrue(GlobalAllocator.hasActiveAllocators());
    IllegalStateException e = Assertions.assertThrows(IllegalStateException.class,
        () -> GlobalAllocator.checkGlobalCleanUpResources());
    Assertions.assertTrue(e.getMessage().startsWith(
        "Cannot continue with active allocators: GlobalAllocator-Child-org.apache.arrow.memory.util.test." +
            "TestGlobalAllocatorTestExtension-testMemoryLeakScenario#"));
    allocator.close();
  }
}
