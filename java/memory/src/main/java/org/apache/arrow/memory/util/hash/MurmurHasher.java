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

import static io.netty.util.internal.PlatformDependent.getByte;
import static io.netty.util.internal.PlatformDependent.getInt;

import io.netty.buffer.ArrowBuf;

/**
 * Implementation of the Murmur hashing algorithm.
 * Details of the algorithm can be found in
 * https://en.wikipedia.org/wiki/MurmurHash
 * <p>
 *   Murmur hashing is computationally expensive, as it involves several
 *   integer multiplications. However, the produced hash codes have
 *   good quality in the sense that they are uniformly distributed in the universe.
 * </p>
 * <p>
 *   Therefore, this algorithm is suitable for scenarios where uniform hashing
 *   is desired (e.g. in an open addressing hash table/hash set).
 * </p>
 */
public class MurmurHasher implements ArrowBufHasher {

  private final int seed;

  /**
   * Creates a default Murmur hasher, with seed 0.
   */
  public MurmurHasher() {
    this(0);
  }

  /**
   * Creates a Murmur hasher.
   * @param seed the seed for the hasher.
   */
  public MurmurHasher(int seed) {
    this.seed = seed;
  }

  @Override
  public int hashCode(long address, int length) {
    return hashCode(address, length, seed);
  }

  @Override
  public int hashCode(ArrowBuf buf, int offset, int length) {
    buf.checkBytes(offset, offset + length);
    return hashCode(buf.memoryAddress() + offset, length);
  }

  /**
   * Calculates the hash code for a memory region.
   * @param buf the buffer for the memory region.
   * @param offset offset within the buffer for the memory region.
   * @param length length of the memory region.
   * @param seed the seed.
   * @return the hash code.
   */
  public static int hashCode(ArrowBuf buf, int offset, int length, int seed) {
    buf.checkBytes(offset, offset + length);
    return hashCode(buf.memoryAddress() + offset, length, seed);
  }

  /**
   * Calculates the hash code for a memory region.
   * @param address start address of the memory region.
   * @param length length of the memory region.
   * @param seed the seed.
   * @return the hash code.
   */
  public static int hashCode(long address, int length, int seed) {
    int index = 0;
    int hash = seed;
    while (index + 4 <= length) {
      int intValue = getInt(address + index);
      hash = combineHashCode(hash, intValue);
      index += 4;
    }

    if (index < length) {
      // process remaining data as a integer in little endian
      int intValue = 0;
      for (int i = index - 1; i >= index; i--) {
        intValue <<= 8;
        intValue |= (getByte(address + i) & 0x000000ff);
        index += 1;
      }
      hash = combineHashCode(hash, intValue);
    }
    return finalizeHashCode(hash, length);
  }

  /**
   * Combine the current hash code and a new int value to calculate
   * a new hash code.
   * @param currentHashCode the current hash code.
   * @param intValue the new int value.
   * @return the new hah code.
   */
  public static int combineHashCode(int currentHashCode, int intValue) {
    int c1 = 0xcc9e2d51;
    int c2 = 0x1b873593;
    int r1 = 15;
    int r2 = 13;
    int m = 5;
    int n = 0xe6546b64;

    int k = intValue;
    k = k * c1;
    k = rotateLeft(k, r1);
    k = k * c2;

    int hash = currentHashCode;
    hash = hash ^ k;
    hash = rotateLeft(hash, r2);
    hash = hash * m + n;

    return hash;
  }

  /**
   * Finalizing the hash code.
   * @param hashCode the current hash code.
   * @param length the length of the memory region.
   * @return the finalized hash code.
   */
  public static int finalizeHashCode(int hashCode, int length) {
    hashCode = hashCode ^ length;

    hashCode = hashCode ^ (hashCode >>> 16);
    hashCode = hashCode * 0x85ebca6b;
    hashCode = hashCode ^ (hashCode >>> 13);
    hashCode = hashCode * 0xc2b2ae35;
    hashCode = hashCode ^ (hashCode >>> 16);

    return hashCode;
  }

  private static int rotateLeft(int value, int count) {
    return (value << count) | (value >>> (32 - count));
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    MurmurHasher that = (MurmurHasher) o;
    return seed == that.seed;
  }

  @Override
  public int hashCode() {
    return seed;
  }
}
