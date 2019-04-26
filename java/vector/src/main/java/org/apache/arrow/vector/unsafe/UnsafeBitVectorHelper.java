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

package org.apache.arrow.vector.unsafe;

import io.netty.buffer.ArrowBuf;
import io.netty.util.internal.PlatformDependent;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.BitVectorHelper;
import org.apache.arrow.vector.ipc.message.ArrowFieldNode;

/**
 * Helper class for performing generic operations on a bit vector buffer.
 * Compared with org.apache.arrow.BitVectorHelp, this implementation tries the best to avoid checks..
 */
public class UnsafeBitVectorHelper {
  /**
   * Set the bit at provided index to 1.
   *
   * @param validityBuffer validity buffer of the vector
   * @param index index to be set
   */
  public static void setValidityBitToOne(ArrowBuf validityBuffer, int index) {
    final int byteIndex = BitVectorHelper.byteIndex(index);
    final int bitIndex = BitVectorHelper.bitIndex(index);
    byte currentByte = PlatformDependent.getByte(validityBuffer.memoryAddress() + byteIndex);
    final byte bitMask = (byte) (1L << bitIndex);
    currentByte |= bitMask;
    PlatformDependent.putByte(validityBuffer.memoryAddress() + byteIndex, currentByte);
  }

  /**
   * Set the bit at a given index to provided value (1 or 0).
   *
   * @param validityBuffer validity buffer of the vector
   * @param index index to be set
   * @param value value to set
   */
  public static void setValidityBit(ArrowBuf validityBuffer, int index, int value) {
    final int byteIndex = BitVectorHelper.byteIndex(index);
    final int bitIndex = BitVectorHelper.bitIndex(index);
    byte currentByte = PlatformDependent.getByte(validityBuffer.memoryAddress() + byteIndex);
    final byte bitMask = (byte) (1L << bitIndex);
    if (value != 0) {
      currentByte |= bitMask;
    } else {
      currentByte -= (bitMask & currentByte);
    }
    PlatformDependent.putByte(validityBuffer.memoryAddress() + byteIndex, currentByte);
  }
}
