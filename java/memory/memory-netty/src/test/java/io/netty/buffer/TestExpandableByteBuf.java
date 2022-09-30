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

package io.netty.buffer;

import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.junit.Assert;
import org.junit.Test;

public class TestExpandableByteBuf {

  @Test
  public void testCapacity() {
    try (BufferAllocator allocator = new RootAllocator(128);
         ArrowBuf buf = allocator.buffer(20);
    ) {
      NettyArrowBuf srcByteBuf = NettyArrowBuf.unwrapBuffer(buf);
      ExpandableByteBuf expandableByteBuf = new ExpandableByteBuf(srcByteBuf, allocator);
      ByteBuf newByteBuf = expandableByteBuf.capacity(31);
      int capacity = newByteBuf.capacity();
      Assert.assertEquals(32, capacity);
    }
  }

  @Test
  public void testCapacity1() {
    try (BufferAllocator allocator = new RootAllocator(128);
         ArrowBuf buf = allocator.buffer(20);
    ) {
      NettyArrowBuf srcByteBuf = NettyArrowBuf.unwrapBuffer(buf);
      ExpandableByteBuf expandableByteBuf = new ExpandableByteBuf(srcByteBuf, allocator);
      ByteBuf newByteBuf = expandableByteBuf.capacity(32);
      int capacity = newByteBuf.capacity();
      Assert.assertEquals(32, capacity);
    }
  }

  @Test
  public void testSetAndGetIntValues() {
    try (BufferAllocator allocator = new RootAllocator(128);
         ArrowBuf buf = allocator.buffer(20);
    ) {
      NettyArrowBuf srcByteBuf = NettyArrowBuf.unwrapBuffer(buf);
      ExpandableByteBuf expandableByteBuf = new ExpandableByteBuf(srcByteBuf, allocator);
      int [] intVals = new int[] {Integer.MIN_VALUE, Short.MIN_VALUE - 1, Short.MIN_VALUE, 0 ,
        Short.MAX_VALUE , Short.MAX_VALUE + 1, Integer.MAX_VALUE};
      for (int intValue :intVals) {
        expandableByteBuf.setInt(0, intValue);
        Assert.assertEquals(expandableByteBuf.getInt(0), intValue);
        Assert.assertEquals(expandableByteBuf.getIntLE(0), Integer.reverseBytes(intValue));
      }
    }
  }

  @Test
  public void testSetAndGetLongValues() {
    try (BufferAllocator allocator = new RootAllocator(128);
         ArrowBuf buf = allocator.buffer(20);
    ) {
      NettyArrowBuf srcByteBuf = NettyArrowBuf.unwrapBuffer(buf);
      ExpandableByteBuf expandableByteBuf = new ExpandableByteBuf(srcByteBuf, allocator);
      long [] longVals = new long[] {Long.MIN_VALUE, 0 , Long.MAX_VALUE};
      for (long longValue :longVals) {
        expandableByteBuf.setLong(0, longValue);
        Assert.assertEquals(expandableByteBuf.getLong(0), longValue);
        Assert.assertEquals(expandableByteBuf.getLongLE(0), Long.reverseBytes(longValue));
      }
    }
  }

  @Test
  public void testSetAndGetShortValues() {
    try (BufferAllocator allocator = new RootAllocator(128);
         ArrowBuf buf = allocator.buffer(20);
    ) {
      NettyArrowBuf srcByteBuf = NettyArrowBuf.unwrapBuffer(buf);
      ExpandableByteBuf expandableByteBuf = new ExpandableByteBuf(srcByteBuf, allocator);
      short [] shortVals = new short[] {Short.MIN_VALUE, 0 , Short.MAX_VALUE};
      for (short shortValue :shortVals) {
        expandableByteBuf.setShort(0, shortValue);
        Assert.assertEquals(expandableByteBuf.getShort(0), shortValue);
        Assert.assertEquals(expandableByteBuf.getShortLE(0), Short.reverseBytes(shortValue));
      }
    }
  }

  @Test
  public void testSetAndGetByteValues() {
    try (BufferAllocator allocator = new RootAllocator(128);
         ArrowBuf buf = allocator.buffer(20);
    ) {
      NettyArrowBuf srcByteBuf = NettyArrowBuf.unwrapBuffer(buf);
      ExpandableByteBuf expandableByteBuf = new ExpandableByteBuf(srcByteBuf, allocator);
      byte [] byteVals = new byte[] {Byte.MIN_VALUE, 0 , Byte.MAX_VALUE};
      for (short byteValue :byteVals) {
        expandableByteBuf.setByte(0, byteValue);
        Assert.assertEquals(expandableByteBuf.getByte(0), byteValue);
      }
    }
  }
}
