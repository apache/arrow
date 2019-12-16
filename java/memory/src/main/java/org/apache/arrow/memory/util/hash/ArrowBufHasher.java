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

package org.apache.arrow.memory.util.hash;

import io.netty.buffer.ArrowBuf;

/**
 * Utility for calculating the hash code for a consecutive memory region.
 * This class provides the basic framework for efficiently calculating the hash code.
 * <p>
 *   A default light-weight implementation is given in {@link SimpleHasher}.
 * </p>
 */
public interface ArrowBufHasher {

  /**
   * Calculates the hash code for a memory region.
   * @param address start address of the memory region.
   * @param length length of the memory region.
   * @return the hash code.
   */
  int hashCode(long address, int length);

  /**
   * Calculates the hash code for a memory region.
   * @param buf the buffer for the memory region.
   * @param offset offset within the buffer for the memory region.
   * @param length length of the memory region.
   * @return the hash code.
   */
  int hashCode(ArrowBuf buf, int offset, int length);
}
