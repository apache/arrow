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

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

import org.apache.arrow.c.jni.CDataJniException;
import org.apache.arrow.c.jni.JniWrapper;
import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.ReferenceManager;
import org.apache.arrow.memory.util.MemoryUtil;

/**
 * C Stream Interface ArrowArrayStream.
 * <p>
 * Represents a wrapper for the following C structure:
 *
 * <pre>
 * struct ArrowArrayStream {
 *   int (*get_schema)(struct ArrowArrayStream*, struct ArrowSchema* out);
 *   int (*get_next)(struct ArrowArrayStream*, struct ArrowArray* out);
 *   const char* (*get_last_error)(struct ArrowArrayStream*);
 *   void (*release)(struct ArrowArrayStream*);
 *   void* private_data;
 * };
 * </pre>
 */
public class ArrowArrayStream implements BaseStruct {
  private static final int SIZE_OF = 40;
  private static final int INDEX_RELEASE_CALLBACK = 24;

  private ArrowBuf data;

  /**
   * Snapshot of the ArrowArrayStream raw data.
   */
  public static class Snapshot {
    public long get_schema;
    public long get_next;
    public long get_last_error;
    public long release;
    public long private_data;

    /**
     * Initialize empty ArrowArray snapshot.
     */
    public Snapshot() {
      get_schema = NULL;
      get_next = NULL;
      get_last_error = NULL;
      release = NULL;
      private_data = NULL;
    }
  }

  /**
   * Create ArrowArrayStream from an existing memory address.
   * <p>
   * The resulting ArrowArrayStream does not own the memory.
   *
   * @param memoryAddress Memory address to wrap
   * @return A new ArrowArrayStream instance
   */
  public static ArrowArrayStream wrap(long memoryAddress) {
    return new ArrowArrayStream(new ArrowBuf(ReferenceManager.NO_OP, null, ArrowArrayStream.SIZE_OF, memoryAddress));
  }

  /**
   * Create ArrowArrayStream by allocating memory.
   * <p>
   * The resulting ArrowArrayStream owns the memory.
   *
   * @param allocator Allocator for memory allocations
   * @return A new ArrowArrayStream instance
   */
  public static ArrowArrayStream allocateNew(BufferAllocator allocator) {
    ArrowArrayStream array = new ArrowArrayStream(allocator.buffer(ArrowArrayStream.SIZE_OF));
    array.markReleased();
    return array;
  }

  ArrowArrayStream(ArrowBuf data) {
    checkNotNull(data, "ArrowArrayStream initialized with a null buffer");
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
    checkNotNull(data, "ArrowArrayStream is already closed");
    return data.memoryAddress();
  }

  @Override
  public void release() {
    long address = memoryAddress();
    JniWrapper.get().releaseArrayStream(address);
  }

  /**
   * Get the schema of the stream.
   * @param schema The ArrowSchema struct to output to
   * @throws IOException if the stream returns an error
   */
  public void getSchema(ArrowSchema schema) throws IOException {
    long address = memoryAddress();
    try {
      JniWrapper.get().getSchemaArrayStream(address, schema.memoryAddress());
    } catch (CDataJniException e) {
      throw new IOException("[errno " + e.getErrno() + "] " + e.getMessage());
    }
  }

  /**
   * Get the next batch in the stream.
   * @param array The ArrowArray struct to output to
   * @throws IOException if the stream returns an error
   */
  public void getNext(ArrowArray array) throws IOException {
    long address = memoryAddress();
    try {
      JniWrapper.get().getNextArrayStream(address, array.memoryAddress());
    } catch (CDataJniException e) {
      throw new IOException("[errno " + e.getErrno() + "] " + e.getMessage());
    }
  }

  @Override
  public void close() {
    if (data != null) {
      data.close();
      data = null;
    }
  }

  private ByteBuffer directBuffer() {
    return MemoryUtil.directBuffer(memoryAddress(), ArrowArrayStream.SIZE_OF).order(ByteOrder.nativeOrder());
  }

  /**
   * Take a snapshot of the ArrowArrayStream raw values.
   *
   * @return snapshot
   */
  public ArrowArrayStream.Snapshot snapshot() {
    ByteBuffer data = directBuffer();
    ArrowArrayStream.Snapshot snapshot = new ArrowArrayStream.Snapshot();
    snapshot.get_schema = data.getLong();
    snapshot.get_next = data.getLong();
    snapshot.get_last_error = data.getLong();
    snapshot.release = data.getLong();
    snapshot.private_data = data.getLong();
    return snapshot;
  }

  /**
   * Write values from Snapshot to the underlying ArrowArrayStream memory buffer.
   */
  public void save(ArrowArrayStream.Snapshot snapshot) {
    directBuffer()
        .putLong(snapshot.get_schema)
        .putLong(snapshot.get_next)
        .putLong(snapshot.get_last_error)
        .putLong(snapshot.release)
        .putLong(snapshot.private_data);
  }
}
