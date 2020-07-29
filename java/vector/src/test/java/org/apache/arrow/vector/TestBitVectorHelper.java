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

package org.apache.arrow.vector;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.jupiter.api.Assertions.assertFalse;

import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.junit.Test;

import io.netty.util.internal.PlatformDependent;

public class TestBitVectorHelper {
  @Test
  public void testGetNullCount() throws Exception {
    try (BufferAllocator root = new RootAllocator()) {
      // test case 1, 1 null value for 0b110
      ArrowBuf validityBuffer = root.buffer(3);
      // we set validity buffer to be 0b10110, but only have 3 items with 1st item is null
      validityBuffer.setByte(0, 0b10110);

      // we will only consider 0b110 here, since we only 3 items and only one is null
      int count = BitVectorHelper.getNullCount(validityBuffer, 3);
      assertEquals(count, 1);
      validityBuffer.close();

      // test case 2, no null value for 0xFF
      validityBuffer = root.buffer(8);
      validityBuffer.setByte(0, 0xFF);

      count = BitVectorHelper.getNullCount(validityBuffer, 8);
      assertEquals(count, 0);
      validityBuffer.close();

      // test case 3, 1 null value for 0x7F
      validityBuffer = root.buffer(8);
      validityBuffer.setByte(0, 0x7F);

      count = BitVectorHelper.getNullCount(validityBuffer, 8);
      assertEquals(count, 1);
      validityBuffer.close();

      // test case 4, validity buffer has multiple bytes, 11 items
      validityBuffer = root.buffer(11);
      validityBuffer.setByte(0, 0b10101010);
      validityBuffer.setByte(1, 0b01010101);

      count = BitVectorHelper.getNullCount(validityBuffer, 11);
      assertEquals(count, 5);
      validityBuffer.close();
    }
  }

  @Test
  public void testAllBitsNull() {
    final int bufferLength = 32 * 1024;
    try (RootAllocator allocator = new RootAllocator(bufferLength);
        ArrowBuf validityBuffer = allocator.buffer(bufferLength)) {

      validityBuffer.setZero(0, bufferLength);
      int bitLength = 1024;
      assertTrue(BitVectorHelper.checkAllBitsEqualTo(validityBuffer, bitLength, false));

      bitLength = 1027;
      assertTrue(BitVectorHelper.checkAllBitsEqualTo(validityBuffer, bitLength, false));

      validityBuffer.setZero(0, bufferLength);
      bitLength = 1025;
      BitVectorHelper.setBit(validityBuffer, 12);
      assertFalse(BitVectorHelper.checkAllBitsEqualTo(validityBuffer, bitLength, false));

      validityBuffer.setZero(0, bufferLength);
      bitLength = 1025;
      BitVectorHelper.setBit(validityBuffer, 1024);
      assertFalse(BitVectorHelper.checkAllBitsEqualTo(validityBuffer, bitLength, false));

      validityBuffer.setZero(0, bufferLength);
      bitLength = 1026;
      BitVectorHelper.setBit(validityBuffer, 1024);
      assertFalse(BitVectorHelper.checkAllBitsEqualTo(validityBuffer, bitLength, false));

      validityBuffer.setZero(0, bufferLength);
      bitLength = 1027;
      BitVectorHelper.setBit(validityBuffer, 1025);
      assertFalse(BitVectorHelper.checkAllBitsEqualTo(validityBuffer, bitLength, false));

      validityBuffer.setZero(0, bufferLength);
      bitLength = 1031;
      BitVectorHelper.setBit(validityBuffer, 1029);
      BitVectorHelper.setBit(validityBuffer, 1030);
      assertFalse(BitVectorHelper.checkAllBitsEqualTo(validityBuffer, bitLength, false));
    }
  }

  @Test
  public void testAllBitsSet() {
    final int bufferLength = 32 * 1024;
    try (RootAllocator allocator = new RootAllocator(bufferLength);
         ArrowBuf validityBuffer = allocator.buffer(bufferLength)) {

      PlatformDependent.setMemory(validityBuffer.memoryAddress(), bufferLength, (byte) -1);
      int bitLength = 1024;
      assertTrue(BitVectorHelper.checkAllBitsEqualTo(validityBuffer, bitLength, true));

      bitLength = 1028;
      assertTrue(BitVectorHelper.checkAllBitsEqualTo(validityBuffer, bitLength, true));

      PlatformDependent.setMemory(validityBuffer.memoryAddress(), bufferLength, (byte) -1);
      bitLength = 1025;
      BitVectorHelper.unsetBit(validityBuffer, 12);
      assertFalse(BitVectorHelper.checkAllBitsEqualTo(validityBuffer, bitLength, true));

      PlatformDependent.setMemory(validityBuffer.memoryAddress(), bufferLength, (byte) -1);
      bitLength = 1025;
      BitVectorHelper.unsetBit(validityBuffer, 1024);
      assertFalse(BitVectorHelper.checkAllBitsEqualTo(validityBuffer, bitLength, true));

      PlatformDependent.setMemory(validityBuffer.memoryAddress(), bufferLength, (byte) -1);
      bitLength = 1026;
      BitVectorHelper.unsetBit(validityBuffer, 1024);
      assertFalse(BitVectorHelper.checkAllBitsEqualTo(validityBuffer, bitLength, true));

      PlatformDependent.setMemory(validityBuffer.memoryAddress(), bufferLength, (byte) -1);
      bitLength = 1027;
      BitVectorHelper.unsetBit(validityBuffer, 1025);
      assertFalse(BitVectorHelper.checkAllBitsEqualTo(validityBuffer, bitLength, true));

      PlatformDependent.setMemory(validityBuffer.memoryAddress(), bufferLength, (byte) -1);
      bitLength = 1031;
      BitVectorHelper.unsetBit(validityBuffer, 1029);
      BitVectorHelper.unsetBit(validityBuffer, 1030);
      assertFalse(BitVectorHelper.checkAllBitsEqualTo(validityBuffer, bitLength, true));
    }
  }

  @Test
  public void testConcatBits() {
    try (RootAllocator allocator = new RootAllocator(1024 * 1024)) {
      try (ArrowBuf buf1 = allocator.buffer(1024);
           ArrowBuf buf2 = allocator.buffer(1024);
           ArrowBuf output = allocator.buffer(1024)) {

        buf1.setZero(0, buf1.capacity());
        buf2.setZero(0, buf2.capacity());

        final int maxCount = 100;
        for (int i = 0; i < maxCount; i++) {
          if (i % 3 == 0) {
            BitVectorHelper.setValidityBitToOne(buf1, i);
            BitVectorHelper.setValidityBitToOne(buf2, i);
          }
        }

        // test the case where the number of bits for both sets are multiples of 8.
        concatAndVerify(buf1, 40, buf2, 48, output);

        // only the number of bits in the first set is a multiple of 8
        concatAndVerify(buf1, 32, buf2, 47, output);

        // only the number of bits in the second set is a multiple of 8
        concatAndVerify(buf1, 31, buf2, 48, output);

        // neither set has a size that is a multiple of 8
        concatAndVerify(buf1, 27, buf2, 52, output);

        // the remaining bits in the second set is spread in two bytes
        concatAndVerify(buf1, 31, buf2, 55, output);
      }
    }
  }

  @Test
  public void testConcatBitsInPlace() {
    try (RootAllocator allocator = new RootAllocator(1024 * 1024)) {
      try (ArrowBuf buf1 = allocator.buffer(1024);
           ArrowBuf buf2 = allocator.buffer(1024)) {

        buf1.setZero(0, buf1.capacity());
        buf2.setZero(0, buf2.capacity());

        final int maxCount = 100;
        for (int i = 0; i < maxCount; i++) {
          if (i % 3 == 0) {
            BitVectorHelper.setValidityBitToOne(buf1, i);
            BitVectorHelper.setValidityBitToOne(buf2, i);
          }
        }

        // test the case where the number of bits for both sets are multiples of 8.
        concatAndVerify(buf1, 40, buf2, 48, buf1);

        // only the number of bits in the first set is a multiple of 8
        concatAndVerify(buf1, 32, buf2, 47, buf1);

        // only the number of bits in the second set is a multiple of 8
        concatAndVerify(buf1, 31, buf2, 48, buf1);

        // neither set has a size that is a multiple of 8
        concatAndVerify(buf1, 27, buf2, 52, buf1);

        // the remaining bits in the second set is spread in two bytes
        concatAndVerify(buf1, 31, buf2, 55, buf1);
      }
    }
  }

  private void concatAndVerify(ArrowBuf buf1, int count1, ArrowBuf buf2, int count2, ArrowBuf output) {
    BitVectorHelper.concatBits(buf1, count1, buf2, count2, output);
    int outputIdx = 0;
    for (int i = 0; i < count1; i++, outputIdx++) {
      assertEquals(BitVectorHelper.get(output, outputIdx), BitVectorHelper.get(buf1, i));
    }
    for (int i = 0; i < count2; i++, outputIdx++) {
      assertEquals(BitVectorHelper.get(output, outputIdx), BitVectorHelper.get(buf2, i));
    }
  }
}
