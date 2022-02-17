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

package org.apache.arrow.c;

import java.util.concurrent.atomic.AtomicInteger;

import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.OwnershipTransferResult;
import org.apache.arrow.memory.ReferenceManager;
import org.apache.arrow.util.Preconditions;

/**
 * A ReferenceManager implementation that holds a
 * {@link org.apache.arrow.c.BaseStruct}.
 * <p>
 * A reference count is maintained and once it reaches zero the struct is
 * released (as per the C data interface specification) and closed.
 */
final class CDataReferenceManager implements ReferenceManager {
  private final AtomicInteger bufRefCnt = new AtomicInteger(0);

  private final BaseStruct struct;

  CDataReferenceManager(BaseStruct struct) {
    this.struct = struct;
  }

  @Override
  public int getRefCount() {
    return bufRefCnt.get();
  }

  @Override
  public boolean release() {
    return release(1);
  }

  /**
   * Increment the reference count without any safety checks.
   */
  void increment() {
    bufRefCnt.incrementAndGet();
  }

  @Override
  public boolean release(int decrement) {
    Preconditions.checkState(decrement >= 1, "ref count decrement should be greater than or equal to 1");
    // decrement the ref count
    final int refCnt = bufRefCnt.addAndGet(-decrement);
    // the new ref count should be >= 0
    Preconditions.checkState(refCnt >= 0, "ref count has gone negative");
    if (refCnt == 0) {
      // refcount of this reference manager has dropped to 0
      // release the underlying memory
      struct.release();
      struct.close();
    }
    return refCnt == 0;
  }

  @Override
  public void retain() {
    retain(1);
  }

  @Override
  public void retain(int increment) {
    Preconditions.checkArgument(increment > 0, "retain(%s) argument is not positive", increment);
    final int originalReferenceCount = bufRefCnt.getAndAdd(increment);
    Preconditions.checkState(originalReferenceCount > 0, "retain called but memory was already released");
  }

  @Override
  public ArrowBuf retain(ArrowBuf srcBuffer, BufferAllocator targetAllocator) {
    retain();

    ArrowBuf targetArrowBuf = this.deriveBuffer(srcBuffer, 0, srcBuffer.capacity());
    targetArrowBuf.readerIndex(srcBuffer.readerIndex());
    targetArrowBuf.writerIndex(srcBuffer.writerIndex());
    return targetArrowBuf;
  }

  @Override
  public ArrowBuf deriveBuffer(ArrowBuf sourceBuffer, long index, long length) {
    final long derivedBufferAddress = sourceBuffer.memoryAddress() + index;
    return new ArrowBuf(this, null, length, derivedBufferAddress);
  }

  @Override
  public OwnershipTransferResult transferOwnership(ArrowBuf sourceBuffer, BufferAllocator targetAllocator) {
    throw new UnsupportedOperationException();
  }

  @Override
  public BufferAllocator getAllocator() {
    return null;
  }

  @Override
  public long getSize() {
    return 0L;
  }

  @Override
  public long getAccountedSize() {
    return 0L;
  }
}
