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

package org.apache.arrow.adapter.orc;

import java.util.concurrent.atomic.AtomicInteger;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.OwnershipTransferResult;
import org.apache.arrow.memory.ReferenceManager;
import org.apache.arrow.util.Preconditions;

import io.netty.buffer.ArrowBuf;

public class OrcReferenceManager implements ReferenceManager {
  private final AtomicInteger bufRefCnt = new AtomicInteger(0);

  private OrcMemoryJniWrapper memory;

  OrcReferenceManager(OrcMemoryJniWrapper memory) {
    this.memory = memory;
  }

  /**
   * Return the reference count.
   *
   * @return reference count
   */
  @Override
  public int getRefCount() {
    return bufRefCnt.get();
  }

  /**
   * Decrement this reference manager's reference count by 1 for the associated underlying
   * memory. If the reference count drops to 0, it implies that ArrowBufs managed by this
   * reference manager no longer need access to the underlying memory
   *
   * @return true if ref count has dropped to 0, false otherwise
   */
  @Override
  public boolean release() {
    return release(1);
  }

  /**
   * Decrement this reference manager's reference count for the associated underlying
   * memory. If the reference count drops to 0, it implies that ArrowBufs managed by this
   * reference manager no longer need access to the underlying memory
   *
   * @param decrement the count to decrease the reference count by
   * @return the new reference count
   */
  @Override
  public boolean release(int decrement) {
    Preconditions.checkState(decrement >= 1,
            "ref count decrement should be greater than or equal to 1");
    // decrement the ref count
    final int refCnt;
    synchronized (this) {
      refCnt = bufRefCnt.addAndGet(-decrement);
      if (refCnt == 0) {
        // refcount of this reference manager has dropped to 0
        // release the underlying memory
        memory.release();
      }
    }
    // the new ref count should be >= 0
    Preconditions.checkState(refCnt >= 0, "RefCnt has gone negative");
    return refCnt == 0;
  }

  /**
   * Increment this reference manager's reference count by 1 for the associated underlying
   * memory.
   */
  @Override
  public void retain() {
    retain(1);
  }

  /**
   * Increment this reference manager's reference count by a given amount for the
   * associated underlying memory.
   *
   * @param increment the count to increase the reference count by
   */
  @Override
  public void retain(int increment) {
    Preconditions.checkArgument(increment > 0, "retain(%d) argument is not positive", increment);
    final int originalReferenceCount = bufRefCnt.getAndAdd(increment);
    Preconditions.checkArgument(originalReferenceCount > 0);
  }

  /**
   * Create a new ArrowBuf that is associated with an alternative allocator for the purposes of
   * memory ownership and accounting. This has no impact on the reference counting for the current
   * ArrowBuf except in the situation where the passed in Allocator is the same as the current buffer.
   * This operation has no impact on the reference count of this ArrowBuf. The newly created
   * ArrowBuf with either have a reference count of 1 (in the case that this is the first time this
   * memory is being associated with the target allocator or in other words allocation manager currently
   * doesn't hold a mapping for the target allocator) or the current value of the reference count for
   * the target allocator-reference manager combination + 1 in the case that the provided allocator
   * already had an association to this underlying memory.
   *
   * @param srcBuffer       source ArrowBuf
   * @param targetAllocator The target allocator to create an association with.
   * @return A new ArrowBuf which shares the same underlying memory as this ArrowBuf.
   */
  @Override
  public ArrowBuf retain(ArrowBuf srcBuffer, BufferAllocator targetAllocator) {
    retain();
    return srcBuffer;
  }

  /**
   * Derive a new ArrowBuf from a given source ArrowBuf. The new derived
   * ArrowBuf will share the same reference count as rest of the ArrowBufs
   * associated with this reference manager.
   *
   * @param sourceBuffer source ArrowBuf
   * @param index        index (relative to source ArrowBuf) new ArrowBuf should be derived from
   * @param length       length (bytes) of data in underlying memory that derived buffer will
   *                     have access to in underlying memory
   * @return derived buffer
   */
  @Override
  public ArrowBuf deriveBuffer(ArrowBuf sourceBuffer, int index, int length) {
    final long derivedBufferAddress = sourceBuffer.memoryAddress() + index;

    // create new ArrowBuf
    final ArrowBuf derivedBuf = new ArrowBuf(
            this,
            null,
            length, // length (in bytes) in the underlying memory chunk for this new ArrowBuf
            derivedBufferAddress, // starting byte address in the underlying memory for this new ArrowBuf,
            false);

    return derivedBuf;
  }

  /**
   * Transfer the memory accounting ownership of this ArrowBuf to another allocator.
   * This will generate a new ArrowBuf that carries an association with the underlying memory
   * for the given ArrowBuf
   *
   * @param sourceBuffer    source ArrowBuf
   * @param targetAllocator The target allocator to create an association with
   * @return {@link OwnershipTransferResult} with info on transfer result and new buffer
   */
  @Override
  public OwnershipTransferResult transferOwnership(ArrowBuf sourceBuffer, BufferAllocator targetAllocator) {
    retain();
    return new OwnershipTransferResult() {
      @Override
      public boolean getAllocationFit() {
        return false;
      }

      @Override
      public ArrowBuf getTransferredBuffer() {
        return sourceBuffer;
      }
    };
  }

  /**
   * Get the buffer allocator associated with this reference manager.
   *
   * @return buffer allocator.
   */
  @Override
  public BufferAllocator getAllocator() {
    return null;
  }

  /**
   * Total size (in bytes) of memory underlying this reference manager.
   *
   * @return Size (in bytes) of the memory chunk.
   */
  @Override
  public int getSize() {
    return (int)memory.getSize();
  }

  /**
   * Get the total accounted size (in bytes).
   *
   * @return accounted size.
   */
  @Override
  public int getAccountedSize() {
    return 0;
  }
}
