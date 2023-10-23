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

/**
 * A lightweight, automatically expanding container for holding byte data.
 * @param <T> The type of the underlying buffer.
 */
public interface ReusableBuffer<T> {
  /**
   * Get the number of valid bytes in the data.
   *
   * @return the number of valid bytes in the data
   */
  long getLength();

  /**
   * Get the buffer backing this ReusableBuffer.
   */
  T getBuffer();

  /**
   * Set the buffer to the contents of the given ArrowBuf.
   * The internal buffer must resize if it cannot fit the contents
   * of the data.
   *
   * @param srcBytes  the data to copy from
   * @param start     the first position of the new data
   * @param len       the number of bytes of the new data
   */
  void set(ArrowBuf srcBytes, long start, long len);
}
