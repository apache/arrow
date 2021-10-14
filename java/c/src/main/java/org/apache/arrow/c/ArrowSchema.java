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

/**
 * C Data Interface ArrowSchema.
 * <p>
 * Represents a wrapper for the following C structure:
 * 
 * <pre>
 * struct ArrowSchema {
 *     // Array type description
 *     const char* format;
 *     const char* name;
 *     const char* metadata;
 *     int64_t flags;
 *     int64_t n_children;
 *     struct ArrowSchema** children;
 *     struct ArrowSchema* dictionary;
 *      
 *     // Release callback
 *     void (*release)(struct ArrowSchema*);
 *     // Opaque producer-specific data
 *     void* private_data; 
 * };
 * </pre>
 */
public class ArrowSchema implements BaseStruct {
  private static final int SIZE_OF = 72;

  private ArrowBuf data;

  /**
   * Snapshot of the ArrowSchema raw data.
   */
  public static class Snapshot {
    public long format;
    public long name;
    public long metadata;
    public long flags;
    public long n_children;
    public long children;
    public long dictionary;
    public long release;
    public long private_data;

    /**
     * Initialize empty ArrowSchema snapshot.
     */
    public Snapshot() {
      format = NULL;
      name = NULL;
      metadata = NULL;
      flags = NULL;
      n_children = NULL;
      children = NULL;
      dictionary = NULL;
      release = NULL;
      private_data = NULL;
    }
  }

  /**
   * Create ArrowSchema from an existing memory address.
   * <p>
   * The resulting ArrowSchema does not own the memory.
   * 
   * @param memoryAddress Memory address to wrap
   * @return A new ArrowSchema instance
   */
  public static ArrowSchema wrap(long memoryAddress) {
    return new ArrowSchema(new ArrowBuf(ReferenceManager.NO_OP, null, ArrowSchema.SIZE_OF, memoryAddress));
  }

  /**
   * Create ArrowSchema by allocating memory.
   * <p>
   * The resulting ArrowSchema owns the memory.
   * 
   * @param allocator Allocator for memory allocations
   * @return A new ArrowSchema instance
   */
  public static ArrowSchema allocateNew(BufferAllocator allocator) {
    return new ArrowSchema(allocator.buffer(ArrowSchema.SIZE_OF));
  }

  ArrowSchema(ArrowBuf data) {
    checkNotNull(data, "ArrowSchema initialized with a null buffer");
    this.data = data;
  }

  @Override
  public long memoryAddress() {
    checkNotNull(data, "ArrowSchema is already closed");
    return data.memoryAddress();
  }

  @Override
  public void release() {
    long address = memoryAddress();
    JniWrapper.get().releaseSchema(address);
  }

  @Override
  public void close() {
    if (data != null) {
      data.close();
      data = null;
    }
  }

  private ByteBuffer directBuffer() {
    return MemoryUtil.directBuffer(memoryAddress(), ArrowSchema.SIZE_OF).order(ByteOrder.nativeOrder());
  }

  /**
   * Take a snapshot of the ArrowSchema raw values.
   * 
   * @return snapshot
   */
  public Snapshot snapshot() {
    ByteBuffer data = directBuffer();
    Snapshot snapshot = new Snapshot();
    snapshot.format = data.getLong();
    snapshot.name = data.getLong();
    snapshot.metadata = data.getLong();
    snapshot.flags = data.getLong();
    snapshot.n_children = data.getLong();
    snapshot.children = data.getLong();
    snapshot.dictionary = data.getLong();
    snapshot.release = data.getLong();
    snapshot.private_data = data.getLong();
    return snapshot;
  }

  /**
   * Write values from Snapshot to the underlying ArrowSchema memory buffer.
   */
  public void save(Snapshot snapshot) {
    directBuffer().putLong(snapshot.format).putLong(snapshot.name).putLong(snapshot.metadata).putLong(snapshot.flags)
        .putLong(snapshot.n_children).putLong(snapshot.children).putLong(snapshot.dictionary).putLong(snapshot.release)
        .putLong(snapshot.private_data);
  }
}
