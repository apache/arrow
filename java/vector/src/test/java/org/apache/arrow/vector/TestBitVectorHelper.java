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

import org.apache.arrow.memory.ReferenceManager;
import org.apache.arrow.memory.RootAllocator;
import org.junit.Test;

import io.netty.buffer.ArrowBuf;
import io.netty.buffer.PooledByteBufAllocatorL;
import io.netty.util.internal.PlatformDependent;

public class TestBitVectorHelper {
  @Test
  public void testGetNullCount() throws Exception {
    // test case 1, 1 null value for 0b110
    ArrowBuf validityBuffer = new ArrowBuf(
        ReferenceManager.NO_OP, null,3,  new PooledByteBufAllocatorL().empty.memoryAddress(), true);
    // we set validity buffer to be 0b10110, but only have 3 items with 1st item is null
    validityBuffer.setByte(0, 0b10110);

    // we will only consider 0b110 here, since we only 3 items and only one is null
    int count = BitVectorHelper.getNullCount(validityBuffer, 3);
    assertEquals(count, 1);

    // test case 2, no null value for 0xFF
    validityBuffer = new ArrowBuf(
        ReferenceManager.NO_OP,  null,8, new PooledByteBufAllocatorL().empty.memoryAddress(), true);
    validityBuffer.setByte(0, 0xFF);

    count = BitVectorHelper.getNullCount(validityBuffer, 8);
    assertEquals(count, 0);

    // test case 3, 1 null value for 0x7F
    validityBuffer = new ArrowBuf(
        ReferenceManager.NO_OP, null, 8, new PooledByteBufAllocatorL().empty.memoryAddress(), true);
    validityBuffer.setByte(0, 0x7F);

    count = BitVectorHelper.getNullCount(validityBuffer, 8);
    assertEquals(count, 1);

    // test case 4, validity buffer has multiple bytes, 11 items
    validityBuffer = new ArrowBuf(
        ReferenceManager.NO_OP, null,11, new PooledByteBufAllocatorL().empty.memoryAddress(), true);
    validityBuffer.setByte(0, 0b10101010);
    validityBuffer.setByte(1, 0b01010101);

    count = BitVectorHelper.getNullCount(validityBuffer, 11);
    assertEquals(count, 5);
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
      BitVectorHelper.setValidityBit(validityBuffer, 12, 1);
      assertFalse(BitVectorHelper.checkAllBitsEqualTo(validityBuffer, bitLength, false));

      validityBuffer.setZero(0, bufferLength);
      bitLength = 1025;
      BitVectorHelper.setValidityBit(validityBuffer, 1024, 1);
      assertFalse(BitVectorHelper.checkAllBitsEqualTo(validityBuffer, bitLength, false));

      validityBuffer.setZero(0, bufferLength);
      bitLength = 1026;
      BitVectorHelper.setValidityBit(validityBuffer, 1024, 1);
      assertFalse(BitVectorHelper.checkAllBitsEqualTo(validityBuffer, bitLength, false));

      validityBuffer.setZero(0, bufferLength);
      bitLength = 1027;
      BitVectorHelper.setValidityBit(validityBuffer, 1025, 1);
      assertFalse(BitVectorHelper.checkAllBitsEqualTo(validityBuffer, bitLength, false));

      validityBuffer.setZero(0, bufferLength);
      bitLength = 1031;
      BitVectorHelper.setValidityBit(validityBuffer, 1029, 1);
      BitVectorHelper.setValidityBit(validityBuffer, 1030, 1);
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
      BitVectorHelper.setValidityBit(validityBuffer, 12, 0);
      assertFalse(BitVectorHelper.checkAllBitsEqualTo(validityBuffer, bitLength, true));

      PlatformDependent.setMemory(validityBuffer.memoryAddress(), bufferLength, (byte) -1);
      bitLength = 1025;
      BitVectorHelper.setValidityBit(validityBuffer, 1024, 0);
      assertFalse(BitVectorHelper.checkAllBitsEqualTo(validityBuffer, bitLength, true));

      PlatformDependent.setMemory(validityBuffer.memoryAddress(), bufferLength, (byte) -1);
      bitLength = 1026;
      BitVectorHelper.setValidityBit(validityBuffer, 1024, 0);
      assertFalse(BitVectorHelper.checkAllBitsEqualTo(validityBuffer, bitLength, true));

      PlatformDependent.setMemory(validityBuffer.memoryAddress(), bufferLength, (byte) -1);
      bitLength = 1027;
      BitVectorHelper.setValidityBit(validityBuffer, 1025, 0);
      assertFalse(BitVectorHelper.checkAllBitsEqualTo(validityBuffer, bitLength, true));

      PlatformDependent.setMemory(validityBuffer.memoryAddress(), bufferLength, (byte) -1);
      bitLength = 1031;
      BitVectorHelper.setValidityBit(validityBuffer, 1029, 0);
      BitVectorHelper.setValidityBit(validityBuffer, 1030, 0);
      assertFalse(BitVectorHelper.checkAllBitsEqualTo(validityBuffer, bitLength, true));
    }
  }
}
