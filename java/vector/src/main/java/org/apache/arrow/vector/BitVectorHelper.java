/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.arrow.vector;

import io.netty.buffer.ArrowBuf;

class BitVectorHelper {

   /**
    * Get the index of byte corresponding to bit index in validity buffer
    */
   protected static int byteIndex(int absoluteBitIndex) {
      return absoluteBitIndex >> 3;
   }

   /**
    * Get the relative index of bit within the byte in validity buffer
    */
   private static int bitIndex(int absoluteBitIndex) {
      return absoluteBitIndex & 7;
   }

   protected static void setValidityBitToOne(ArrowBuf validityBuffer, int index) {
      final int byteIndex = byteIndex(index);
      final int bitIndex = bitIndex(index);
      byte currentByte = validityBuffer.getByte(byteIndex);
      final byte bitMask = (byte) (1L << bitIndex);
      currentByte |= bitMask;
      validityBuffer.setByte(byteIndex, currentByte);
   }

   protected static void setValidityBit(ArrowBuf validityBuffer, int index, int value) {
      final int byteIndex = byteIndex(index);
      final int bitIndex = bitIndex(index);
      byte currentByte = validityBuffer.getByte(byteIndex);
      final byte bitMask = (byte) (1L << bitIndex);
      if (value != 0) {
         currentByte |= bitMask;
      } else {
         currentByte -= (bitMask & currentByte);
      }
      validityBuffer.setByte(byteIndex, currentByte);
   }
}
