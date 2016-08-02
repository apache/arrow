/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.arrow.vector;

import org.apache.arrow.memory.AllocationReservation;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.BufferManager;

import io.netty.buffer.ArrowBuf;
import io.netty.buffer.ByteBufAllocator;

/**
 * Wrapper around a buffer delegate that populates any allocated buffer with a constant
 * value. Useful for testing if value vectors are properly resetting their buffers.
 */
public class DirtyBufferAllocator implements BufferAllocator {

  private final BufferAllocator delegate;
  private final byte fillValue;

  DirtyBufferAllocator(final BufferAllocator delegate, final byte fillValue) {
    this.delegate = delegate;
    this.fillValue = fillValue;
  }

  @Override
  public ArrowBuf buffer(int size) {
    return buffer(size, null);
  }

  @Override
  public ArrowBuf buffer(int size, BufferManager manager) {
    ArrowBuf buffer = delegate.buffer(size, manager);
    // contaminate the buffer
    for (int i = 0; i < buffer.capacity(); i++) {
      buffer.setByte(i, fillValue);
    }

    return buffer;
  }

  @Override
  public ByteBufAllocator getAsByteBufAllocator() {
    return delegate.getAsByteBufAllocator();
  }

  @Override
  public BufferAllocator newChildAllocator(String name, long initReservation, long maxAllocation) {
    return delegate.newChildAllocator(name, initReservation, maxAllocation);
  }

  @Override
  public void close() {
    delegate.close();
  }

  @Override
  public long getAllocatedMemory() {
    return delegate.getAllocatedMemory();
  }

  @Override
  public void setLimit(long newLimit) {
    delegate.setLimit(newLimit);
  }

  @Override
  public long getLimit() {
    return delegate.getLimit();
  }

  @Override
  public long getPeakMemoryAllocation() {
    return delegate.getPeakMemoryAllocation();
  }

  @Override
  public AllocationReservation newReservation() {
    return delegate.newReservation();
  }

  @Override
  public ArrowBuf getEmpty() {
    return delegate.getEmpty();
  }

  @Override
  public String getName() {
    return delegate.getName();
  }

  @Override
  public boolean isOverLimit() {
    return delegate.isOverLimit();
  }

  @Override
  public String toVerboseString() {
    return delegate.toVerboseString();
  }

  @Override
  public void assertOpen() {
    delegate.assertOpen();
  }}
