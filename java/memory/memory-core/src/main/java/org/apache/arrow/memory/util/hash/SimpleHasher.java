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

import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.memory.util.MemoryUtil;
import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * A simple hasher that calculates the hash code of integers as is, and does not perform any
 * finalization. So the computation is extremely efficient.
 *
 * <p>This algorithm only provides the most basic semantics for the hash code. That is, if two
 * objects are equal, they must have equal hash code. However, the quality of the produced hash code
 * may not be good. In other words, the generated hash codes are far from being uniformly
 * distributed in the universe.
 *
 * <p>Therefore, this algorithm is suitable only for scenarios where the most basic semantics of the
 * hash code is required (e.g. in scenarios that require fast and proactive data pruning)
 *
 * <p>An object of this class is stateless, so it can be shared between threads.
 */
public class SimpleHasher implements ArrowBufHasher {

  public static SimpleHasher INSTANCE = new SimpleHasher();

  protected SimpleHasher() {}

  /**
   * Calculates the hash code for a memory region.
   *
   * @param address start address of the memory region.
   * @param length length of the memory region.
   * @return the hash code.
   */
  @Override
  public int hashCode(long address, long length) {
    int hashValue = 0;
    int index = 0;
    while (index + 8 <= length) {
      long longValue = MemoryUtil.UNSAFE.getLong(address + index);
      int longHash = getLongHashCode(longValue);
      hashValue = combineHashCode(hashValue, longHash);
      index += 8;
    }

    if (index + 4 <= length) {
      int intValue = MemoryUtil.UNSAFE.getInt(address + index);
      int intHash = intValue;
      hashValue = combineHashCode(hashValue, intHash);
      index += 4;
    }

    while (index < length) {
      byte byteValue = MemoryUtil.UNSAFE.getByte(address + index);
      int byteHash = byteValue;
      hashValue = combineHashCode(hashValue, byteHash);
      index += 1;
    }

    return finalizeHashCode(hashValue);
  }

  /**
   * Calculates the hash code for a memory region.
   *
   * @param buf the buffer for the memory region.
   * @param offset offset within the buffer for the memory region.
   * @param length length of the memory region.
   * @return the hash code.
   */
  @Override
  public int hashCode(ArrowBuf buf, long offset, long length) {
    buf.checkBytes(offset, offset + length);
    return hashCode(buf.memoryAddress() + offset, length);
  }

  protected int combineHashCode(int currentHashCode, int newHashCode) {
    return currentHashCode * 37 + newHashCode;
  }

  protected int getLongHashCode(long longValue) {
    return Long.hashCode(longValue);
  }

  protected int finalizeHashCode(int hashCode) {
    return hashCode;
  }

  @Override
  public int hashCode() {
    return 123;
  }

  @Override
  public boolean equals(@Nullable Object obj) {
    return obj != null && (obj instanceof SimpleHasher);
  }
}
