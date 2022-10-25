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

import static org.apache.arrow.c.NativeUtil.NULL;
import static org.apache.arrow.util.Preconditions.checkNotNull;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

import org.apache.arrow.c.jni.JniWrapper;
import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.ReferenceManager;
import org.apache.arrow.memory.util.MemoryUtil;
import org.apache.arrow.util.VisibleForTesting;

/**
 * C Data Interface ArrowArray.
 * <p>
 * Represents a wrapper for the following C structure:
 * 
 * <pre>
 * struct ArrowArray {
 *     // Array data description
 *     int64_t length;
 *     int64_t null_count;
 *     int64_t offset;
 *     int64_t n_buffers;
 *     int64_t n_children;
 *     const void** buffers;
 *     struct ArrowArray** children;
 *     struct ArrowArray* dictionary;
 * 
 *     // Release callback
 *     void (*release)(struct ArrowArray*);
 *     // Opaque producer-specific data
 *     void* private_data;
 * };
 * </pre>
 */
public class ArrowArray implements BaseStruct {
  private static final int SIZE_OF = 80;
  private static final int INDEX_RELEASE_CALLBACK = 64;

  private ArrowBuf data;

  /**
   * Snapshot of the ArrowArray raw data.
   */
  public static class Snapshot {
    public long length;
    public long null_count;
    public long offset;
    public long n_buffers;
    public long n_children;
    public long buffers;
    public long children;
    public long dictionary;
    public long release;
    public long private_data;

    /**
     * Initialize empty ArrowArray snapshot.
     */
    public Snapshot() {
      length = NULL;
      null_count = NULL;
      offset = NULL;
      n_buffers = NULL;
      n_children = NULL;
      buffers = NULL;
      children = NULL;
      dictionary = NULL;
      release = NULL;
      private_data = NULL;
    }
  }

  /**
   * Create ArrowArray from an existing memory address.
   * <p>
   * The resulting ArrowArray does not own the memory.
   * 
   * @param memoryAddress Memory address to wrap
   * @return A new ArrowArray instance
   */
  public static ArrowArray wrap(long memoryAddress) {
    return new ArrowArray(new ArrowBuf(ReferenceManager.NO_OP, null, ArrowArray.SIZE_OF, memoryAddress));
  }

  /**
   * Create ArrowArray by allocating memory.
   * <p>
   * The resulting ArrowArray owns the memory.
   * 
   * @param allocator Allocator for memory allocations
   * @return A new ArrowArray instance
   */
  public static ArrowArray allocateNew(BufferAllocator allocator) {
    ArrowArray array = new ArrowArray(allocator.buffer(ArrowArray.SIZE_OF));
    array.markReleased();
    return array;
  }

  ArrowArray(ArrowBuf data) {
    checkNotNull(data, "ArrowArray initialized with a null buffer");
    this.data = data;
  }

  /**
   * Mark the array as released.
   */
  public void markReleased() {
    directBuffer().putLong(INDEX_RELEASE_CALLBACK, NULL);
  }

  @Override
  public long memoryAddress() {
    checkNotNull(data, "ArrowArray is already closed");
    return data.memoryAddress();
  }

  @Override
  public void release() {
    long address = memoryAddress();
    JniWrapper.get().releaseArray(address);
  }

  @Override
  public void close() {
    if (data != null) {
      data.close();
      data = null;
    }
  }

  @VisibleForTesting
  boolean isClosed() {
    return data == null;
  }

  private ByteBuffer directBuffer() {
    return MemoryUtil.directBuffer(memoryAddress(), ArrowArray.SIZE_OF).order(ByteOrder.nativeOrder());
  }

  /**
   * Take a snapshot of the ArrowArray raw values.
   * 
   * @return snapshot
   */
  public Snapshot snapshot() {
    ByteBuffer data = directBuffer();
    Snapshot snapshot = new Snapshot();
    snapshot.length = data.getLong();
    snapshot.null_count = data.getLong();
    snapshot.offset = data.getLong();
    snapshot.n_buffers = data.getLong();
    snapshot.n_children = data.getLong();
    snapshot.buffers = data.getLong();
    snapshot.children = data.getLong();
    snapshot.dictionary = data.getLong();
    snapshot.release = data.getLong();
    snapshot.private_data = data.getLong();
    return snapshot;
  }

  /**
   * Write values from Snapshot to the underlying ArrowArray memory buffer.
   */
  public void save(Snapshot snapshot) {
    directBuffer().putLong(snapshot.length).putLong(snapshot.null_count).putLong(snapshot.offset)
        .putLong(snapshot.n_buffers).putLong(snapshot.n_children).putLong(snapshot.buffers).putLong(snapshot.children)
        .putLong(snapshot.dictionary).putLong(snapshot.release).putLong(snapshot.private_data);
  }
}
