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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.charset.StandardCharsets;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.ReferenceManager;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.util.CallBack;
import org.junit.jupiter.api.Test;

public class TestBufferOwnershipTransfer {

  @Test
  public void testTransferFixedWidth() {
    BufferAllocator allocator = new RootAllocator(Integer.MAX_VALUE);
    BufferAllocator childAllocator1 = allocator.newChildAllocator("child1", 100000, 100000);
    BufferAllocator childAllocator2 = allocator.newChildAllocator("child2", 100000, 100000);

    IntVector v1 = new IntVector("v1", childAllocator1);
    v1.allocateNew();
    v1.setValueCount(4095);
    long totalAllocatedMemory = childAllocator1.getAllocatedMemory();

    IntVector v2 = new IntVector("v2", childAllocator2);

    v1.makeTransferPair(v2).transfer();

    assertEquals(0, childAllocator1.getAllocatedMemory());
    assertEquals(totalAllocatedMemory, childAllocator2.getAllocatedMemory());

    v1.close();
    v2.close();
    childAllocator1.close();
    childAllocator2.close();
    allocator.close();
  }

  @Test
  public void testTransferVariableWidth() {
    BufferAllocator allocator = new RootAllocator(Integer.MAX_VALUE);
    BufferAllocator childAllocator1 = allocator.newChildAllocator("child1", 100000, 100000);
    BufferAllocator childAllocator2 = allocator.newChildAllocator("child2", 100000, 100000);

    VarCharVector v1 = new VarCharVector("v1", childAllocator1);
    v1.allocateNew();
    v1.setSafe(4094, "hello world".getBytes(StandardCharsets.UTF_8), 0, 11);
    v1.setValueCount(4001);

    VarCharVector v2 = new VarCharVector("v2", childAllocator2);
    long memoryBeforeTransfer = childAllocator1.getAllocatedMemory();

    v1.makeTransferPair(v2).transfer();

    assertEquals(0, childAllocator1.getAllocatedMemory());
    assertEquals(memoryBeforeTransfer, childAllocator2.getAllocatedMemory());

    v1.close();
    v2.close();
    childAllocator1.close();
    childAllocator2.close();
    allocator.close();
  }

  private static class Pointer<T> {
    T value;
  }

  private static CallBack newTriggerCallback(final Pointer<Boolean> trigger) {
    trigger.value = false;
    return new CallBack() {
      @Override
      public void doWork() {
        trigger.value = true;
      }
    };
  }

  @Test
  public void emptyListTransferShouldNotTriggerSchemaChange() {
    final BufferAllocator allocator = new RootAllocator(Integer.MAX_VALUE);

    final Pointer<Boolean> trigger1 = new Pointer<>();
    final Pointer<Boolean> trigger2 = new Pointer<>();
    final ListVector v1 =
        new ListVector(
            "v1",
            allocator,
            FieldType.nullable(ArrowType.Null.INSTANCE),
            newTriggerCallback(trigger1));
    final ListVector v2 =
        new ListVector(
            "v2",
            allocator,
            FieldType.nullable(ArrowType.Null.INSTANCE),
            newTriggerCallback(trigger2));

    try {
      // since we are working with empty vectors, their internal
      // buffers will be allocator.EMPTY which use
      // ReferenceManager.NO_OP instance and transfer() is not
      // supported
      v1.makeTransferPair(v2).transfer();
    } catch (Exception e) {
      assertTrue(e instanceof UnsupportedOperationException);
      assertTrue(e.getMessage().contains(ReferenceManager.NO_OP_ERROR_MESSAGE));
    }

    assertFalse(trigger1.value);
    assertFalse(trigger2.value);

    v1.close();
    v2.close();
    allocator.close();
  }
}
