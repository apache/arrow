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
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.util.CallBack;
import org.junit.Test;

public class TestBufferOwnershipTransfer {

  @Test
  public void testTransferFixedWidth() {
    BufferAllocator allocator = new RootAllocator(Integer.MAX_VALUE);
    BufferAllocator childAllocator1 = allocator.newChildAllocator("child1", 100000, 100000);
    BufferAllocator childAllocator2 = allocator.newChildAllocator("child2", 100000, 100000);

    NullableIntVector v1 = new NullableIntVector("v1", childAllocator1);
    v1.allocateNew();
    v1.getMutator().setValueCount(4095);

    NullableIntVector v2 = new NullableIntVector("v2", childAllocator2);

    v1.makeTransferPair(v2).transfer();

    assertEquals(0, childAllocator1.getAllocatedMemory());
    assertEquals(5 * 4096, childAllocator2.getAllocatedMemory());
  }

  @Test
  public void testTransferVariableidth() {
    BufferAllocator allocator = new RootAllocator(Integer.MAX_VALUE);
    BufferAllocator childAllocator1 = allocator.newChildAllocator("child1", 100000, 100000);
    BufferAllocator childAllocator2 = allocator.newChildAllocator("child2", 100000, 100000);

    NullableVarCharVector v1 = new NullableVarCharVector("v1", childAllocator1);
    v1.allocateNew();
    v1.getMutator().setSafe(4094, "hello world".getBytes(), 0, 11);
    v1.getMutator().setValueCount(4001);

    NullableVarCharVector v2 = new NullableVarCharVector("v2", childAllocator2);

    v1.makeTransferPair(v2).transfer();

    assertEquals(0, childAllocator1.getAllocatedMemory());
    int expected = 8*4096 + 4*4096 + 4096;
    assertEquals(expected, childAllocator2.getAllocatedMemory());
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
    final ListVector v1 = new ListVector("v1", allocator,
            FieldType.nullable(ArrowType.Null.INSTANCE),
            newTriggerCallback(trigger1));
    final ListVector v2 = new ListVector("v2", allocator,
            FieldType.nullable(ArrowType.Null.INSTANCE),
            newTriggerCallback(trigger2));

    v1.makeTransferPair(v2).transfer();

    assertFalse(trigger1.value);
    assertFalse(trigger2.value);
  }
}
