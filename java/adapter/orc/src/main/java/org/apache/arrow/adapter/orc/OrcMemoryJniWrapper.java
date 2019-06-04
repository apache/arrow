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

/**
 * Wrapper for orc memory allocated by native code.
 */
class OrcMemoryJniWrapper implements AutoCloseable {

  private final long nativeInstanceId;

  private final long memoryAddress;

  private final long size;

  private final long capacity;

  /**
   * Construct a new instance.
   * @param nativeInstanceId unique id of the underlying memory.
   * @param memoryAddress starting memory address of the the underlying memory.
   * @param size size of the valid data.
   * @param capacity allocated memory size.
   */
  OrcMemoryJniWrapper(long nativeInstanceId, long memoryAddress, long size, long capacity) {
    this.nativeInstanceId = nativeInstanceId;
    this.memoryAddress = memoryAddress;
    this.size = size;
    this.capacity = capacity;
  }

  /**
   * Return the size of underlying chunk of memory that has valid data.
   * @return valid data size
   */
  long getSize() {
    return size;
  }

  /**
   * Return the size of underlying chunk of memory managed by this OrcMemoryJniWrapper.
   * @return underlying memory size
   */
  long getCapacity() {
    return capacity;
  }

  /**
   * Return the memory address of underlying chunk of memory.
   * @return memory address
   */
  long getMemoryAddress() {
    return memoryAddress;
  }

  @Override
  public void close() {
    release(nativeInstanceId);
  }

  private native void release(long id);
}
