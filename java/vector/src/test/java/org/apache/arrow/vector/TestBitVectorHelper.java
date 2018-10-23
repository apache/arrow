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

import org.junit.Test;

import io.netty.buffer.ArrowBuf;
import io.netty.buffer.PooledByteBufAllocatorL;

public class TestBitVectorHelper {
  @Test
  public void testGetNullCount() throws Exception {
    // test case 1, 1 null value for 0b110
    ArrowBuf validityBuffer = new ArrowBuf(
        null, null, new PooledByteBufAllocatorL().empty,
        null, null, 0, 3, true);
    // we set validity buffer to be 0b10110, but only have 3 items with 1st item is null
    validityBuffer.setByte(0, 0b10110);

    // we will only consider 0b110 here, since we only 3 items and only one is null
    int count = BitVectorHelper.getNullCount(validityBuffer, 3);
    assertEquals(count, 1);

    // test case 2, no null value for 0xFF
    validityBuffer = new ArrowBuf(
        null, null, new PooledByteBufAllocatorL().empty,
        null, null, 0, 8, true);
    validityBuffer.setByte(0, 0xFF);

    count = BitVectorHelper.getNullCount(validityBuffer, 8);
    assertEquals(count, 0);

    // test case 3, 1 null value for 0x7F
    validityBuffer = new ArrowBuf(
        null, null, new PooledByteBufAllocatorL().empty,
        null, null, 0, 8, true);
    validityBuffer.setByte(0, 0x7F);

    count = BitVectorHelper.getNullCount(validityBuffer, 8);
    assertEquals(count, 1);

    // test case 4, validity buffer has multiple bytes, 11 items
    validityBuffer = new ArrowBuf(
        null, null, new PooledByteBufAllocatorL().empty,
        null, null, 0, 11, true);
    validityBuffer.setByte(0, 0b10101010);
    validityBuffer.setByte(1, 0b01010101);

    count = BitVectorHelper.getNullCount(validityBuffer, 11);
    assertEquals(count, 5);
  }
}
