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

package org.apache.arrow.dataset.jni;

/**
 * C++ memory pool(arrow::MemoryPool)'s Java mapped instance.
 */
public class NativeMemoryPool implements AutoCloseable {
  private final long nativeInstanceId;

  private NativeMemoryPool(long nativeInstanceId) {
    this.nativeInstanceId = nativeInstanceId;
  }

  /**
   * Get the default memory pool. This will return arrow::default_memory_pool() directly.
   */
  public static NativeMemoryPool getDefault() {
    JniLoader.get().ensureLoaded();
    return new NativeMemoryPool(getDefaultMemoryPool());
  }

  /**
   * Create a listenable memory pool (see also: arrow::ReservationListenableMemoryPool) with
   * a specific listener. All buffers created from the memory pool should take enough reservation
   * from the listener in advance.
   */
  public static NativeMemoryPool createListenable(ReservationListener listener) {
    JniLoader.get().ensureLoaded();
    return new NativeMemoryPool(createListenableMemoryPool(listener));
  }

  /**
   * Return native instance ID of this memory pool.
   */
  public long getNativeInstanceId() {
    return nativeInstanceId;
  }

  /**
   * Get current allocated bytes.
   */
  public long getBytesAllocated() {
    return bytesAllocated(nativeInstanceId);
  }

  @Override
  public void close() throws Exception {
    releaseMemoryPool(nativeInstanceId);
  }

  private static native long getDefaultMemoryPool();

  private static native long createListenableMemoryPool(ReservationListener listener);

  private static native void releaseMemoryPool(long id);

  private static native long bytesAllocated(long id);
}
