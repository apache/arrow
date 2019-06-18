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


import io.netty.buffer.ArrowBuf;

/**
 * Reference Manager manages one or more ArrowBufs that share the
 * reference count for the underlying memory chunk.
 */
public interface ReferenceManager {

  /**
   * Return the reference count.
   * @return reference count
   */
  int getRefCount();

  /**
   * Decrement this reference manager's reference count by 1 for the associated underlying
   * memory. If the reference count drops to 0, it implies that ArrowBufs managed by this
   * reference manager no longer need access to the underlying memory
   * @return true if ref count has dropped to 0, false otherwise
   */
  boolean release();

  /**
   * Decrement this reference manager's reference count for the associated underlying
   * memory. If the reference count drops to 0, it implies that ArrowBufs managed by this
   * reference manager no longer need access to the underlying memory
   * @param decrement the count to decrease the reference count by
   * @return the new reference count
   */
  boolean release(int decrement);

  /**
   * Increment this reference manager's reference count by 1 for the associated underlying
   * memory.
   */
  void retain();

  /**
   * Increment this reference manager's reference count by a given amount for the
   * associated underlying memory.
   * @param increment the count to increase the reference count by
   */
  void retain(int increment);

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
   * @param srcBuffer source ArrowBuf
   * @param targetAllocator The target allocator to create an association with.
   * @return A new ArrowBuf which shares the same underlying memory as this ArrowBuf.
   */
  ArrowBuf retain(ArrowBuf srcBuffer, BufferAllocator targetAllocator);

  /**
   * Derive a new ArrowBuf from a given source ArrowBuf. The new derived
   * ArrowBuf will share the same reference count as rest of the ArrowBufs
   * associated with this reference manager.
   * @param sourceBuffer source ArrowBuf
   * @param index index (relative to source ArrowBuf) new ArrowBuf should be derived from
   * @param length length (bytes) of data in underlying memory that derived buffer will
   *               have access to in underlying memory
   * @return derived buffer
   */
  ArrowBuf deriveBuffer(ArrowBuf sourceBuffer, int index, int length);

  /**
   * Transfer the memory accounting ownership of this ArrowBuf to another allocator.
   * This will generate a new ArrowBuf that carries an association with the underlying memory
   * for the given ArrowBuf
   * @param sourceBuffer source ArrowBuf
   * @param targetAllocator The target allocator to create an association with
   * @return {@link OwnershipTransferResult} with info on transfer result and new buffer
   */
  OwnershipTransferResult transferOwnership(ArrowBuf sourceBuffer, BufferAllocator targetAllocator);

  /**
   * Get the buffer allocator associated with this reference manager
   * @return buffer allocator.
   */
  BufferAllocator getAllocator();

  /**
   * Total size (in bytes) of memory underlying this reference manager.
   * @return Size (in bytes) of the memory chunk.
   */
  int getSize();

  /**
   * Get the total accounted size (in bytes).
   * @return accounted size.
   */
  int getAccountedSize();

  String NO_OP_ERROR_MESSAGE = "Operation not supported on NO_OP Reference Manager";

  // currently used for empty ArrowBufs
  ReferenceManager NO_OP = new ReferenceManager() {
    @Override
    public int getRefCount() {
      return 1;
    }

    @Override
    public boolean release() {
      return false;
    }

    @Override
    public boolean release(int decrement) {
      return false;
    }

    @Override
    public void retain() { }

    @Override
    public void retain(int increment) { }

    @Override
    public ArrowBuf retain(ArrowBuf srcBuffer, BufferAllocator targetAllocator) {
      return srcBuffer;
    }

    @Override
    public ArrowBuf deriveBuffer(ArrowBuf sourceBuffer, int index, int length) {
      return sourceBuffer;
    }

    @Override
    public OwnershipTransferResult transferOwnership(ArrowBuf sourceBuffer, BufferAllocator targetAllocator) {
      return new OwnershipTransferNOOP(sourceBuffer);
    }

    @Override
    public BufferAllocator getAllocator() {
      return new RootAllocator(0);
    }

    @Override
    public int getSize() {
      return 0;
    }

    @Override
    public int getAccountedSize() {
      return 0;
    }

  };
}
