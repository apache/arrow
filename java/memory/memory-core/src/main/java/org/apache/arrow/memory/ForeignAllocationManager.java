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

/**
 * An allocation of memory that does not come from a BufferAllocator, but rather an outside source (like JNI).
 */
public abstract class ForeignAllocationManager extends AllocationManager {
  private final long memoryAddress;
  private final long size;

  /**
   * Create a new AllocationManager representing an imported buffer.
   *
   * @param allocator The allocator to associate with.
   * @param size The buffer size.
   * @param memoryAddress The buffer address.
   */
  protected ForeignAllocationManager(BufferAllocator allocator, long size, long memoryAddress) {
    super(allocator);
    this.memoryAddress = memoryAddress;
    this.size = size;
  }

  @Override
  public long getSize() {
    return size;
  }

  @Override
  protected long memoryAddress() {
    return memoryAddress;
  }
}
