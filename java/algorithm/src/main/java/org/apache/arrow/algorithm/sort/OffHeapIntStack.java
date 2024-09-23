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

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.IntVector;

/** An off heap implementation of stack with int elements. */
class OffHeapIntStack implements AutoCloseable {

  private static final int INIT_SIZE = 128;

  private IntVector intVector;

  private int top = 0;

  public OffHeapIntStack(BufferAllocator allocator) {
    intVector = new IntVector("int stack inner vector", allocator);
    intVector.allocateNew(INIT_SIZE);
    intVector.setValueCount(INIT_SIZE);
  }

  public void push(int value) {
    if (top == intVector.getValueCount()) {
      int targetCapacity = intVector.getValueCount() * 2;
      while (intVector.getValueCapacity() < targetCapacity) {
        intVector.reAlloc();
      }
      intVector.setValueCount(targetCapacity);
    }

    intVector.set(top++, value);
  }

  public int pop() {
    return intVector.get(--top);
  }

  public int getTop() {
    return intVector.get(top - 1);
  }

  public boolean isEmpty() {
    return top == 0;
  }

  public int getCount() {
    return top;
  }

  @Override
  public void close() {
    intVector.close();
  }
}
