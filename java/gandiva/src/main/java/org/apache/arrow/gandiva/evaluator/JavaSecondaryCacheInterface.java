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

package org.apache.arrow.gandiva.evaluator;

/**
 * Acts as an interface to the secondary cache class that uses a callback
 * mechanism from gandiva.
 */
public interface JavaSecondaryCacheInterface {
  /**
   * Resultant buffer of the get call to the secondary cache.
   */
  class BufferResult {
    public long address;
    public long size;

    public BufferResult(long address, long size) {
      this.address = address;
      this.size = size;
    }

  }

  /**
   * Get from the secondary cache using the key buffer.
   * @param keyBufAddr address of the key buffer
   * @param keyBufSize size of the key buffer
   * @return
   */
  BufferResult get(long keyBufAddr, long keyBufSize);

  /**
   * Set the secondary cache with the value in the buffer.
   * @param keyBufAddr address of the key buffer
   * @param keyBufSize size of the key buffer
   * @param valueBufAddr address of the value buffer
   * @param valueBufSize size of the value buffer
   */
  void set(long keyBufAddr, long keyBufSize, long valueBufAddr, long valueBufSize);

  /**
   * release the buffer associated with the given address.
   */
  void releaseBufferResult(long address);

}

