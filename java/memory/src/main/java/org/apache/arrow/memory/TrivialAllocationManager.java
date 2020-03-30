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

package org.apache.arrow.memory;

import java.lang.reflect.Field;

import org.apache.arrow.memory.util.LargeMemoryUtil;

import sun.misc.Unsafe;

/**
 * trivial allocation manager for the case when no other allocation manager exists.
 */
public class TrivialAllocationManager extends AllocationManager {

  public static final Factory FACTORY = new Factory();

  private final Unsafe unsafe = getUnsafe();
  private final long address;
  private final int requestedSize;

  protected TrivialAllocationManager(BaseAllocator accountingAllocator, int requestedSize) {
    super(accountingAllocator);
    this.requestedSize = requestedSize;
    address = unsafe.allocateMemory(requestedSize);
  }

  @Override
  protected long memoryAddress() {
    return address;
  }

  @Override
  protected void release0() {
    unsafe.setMemory(address, requestedSize, (byte) 0);
    unsafe.freeMemory(address);
  }

  @Override
  public long getSize() {
    return requestedSize;
  }

  private Unsafe getUnsafe() {
    Field f = null;
    try {
      f = Unsafe.class.getDeclaredField("theUnsafe");
      f.setAccessible(true);
      return (Unsafe) f.get(null);
    } catch (NoSuchFieldException | IllegalAccessException e) {
      throw new RuntimeException(e);
    } finally {
      if (f != null) {
        f.setAccessible(false);
      }
    }
  }

  /**
   * Factory for creating {@link TrivialAllocationManager}.
   */
  public static class Factory implements AllocationManager.Factory {

    @Override
    public AllocationManager create(BaseAllocator accountingAllocator, long size) {
      return new TrivialAllocationManager(accountingAllocator, LargeMemoryUtil.checkedCastToInt(size));
    }
  }
}
