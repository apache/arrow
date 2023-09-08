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

import java.nio.ByteBuffer;

import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.memory.ArrowByteBufAllocator;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.junit.Assert;
import org.junit.Test;

public class TestNettyArrowBuf {

  @Test
  public void testSliceWithoutArgs() {
    try (BufferAllocator allocator = new RootAllocator(128);
         ArrowBuf buf = allocator.buffer(20);
    ) {
      NettyArrowBuf nettyBuf = NettyArrowBuf.unwrapBuffer(buf);
      nettyBuf.writerIndex(20);
      nettyBuf.readerIndex(10);
      NettyArrowBuf slicedBuffer = nettyBuf.slice();
      int readableBytes = slicedBuffer.readableBytes();
      Assert.assertEquals(10, readableBytes);
    }
  }

  @Test
  public void testNioBuffer() {
    try (BufferAllocator allocator = new RootAllocator(128);
         ArrowBuf buf = allocator.buffer(20);
    ) {
      NettyArrowBuf nettyBuf = NettyArrowBuf.unwrapBuffer(buf);
      ByteBuffer byteBuffer = nettyBuf.nioBuffer(4, 6);
      // Nio Buffers should always be 0 indexed
      Assert.assertEquals(0, byteBuffer.position());
      Assert.assertEquals(6, byteBuffer.limit());
      // Underlying buffer has size 32 excluding 4 should have capacity of 28.
      Assert.assertEquals(28, byteBuffer.capacity());

    }
  }

  @Test
  public void testInternalNioBuffer() {
    try (BufferAllocator allocator = new RootAllocator(128);
         ArrowBuf buf = allocator.buffer(20);
    ) {
      NettyArrowBuf nettyBuf = NettyArrowBuf.unwrapBuffer(buf);
      ByteBuffer byteBuffer = nettyBuf.internalNioBuffer(4, 6);
      Assert.assertEquals(0, byteBuffer.position());
      Assert.assertEquals(6, byteBuffer.limit());
      // Underlying buffer has size 32 excluding 4 should have capacity of 28.
      Assert.assertEquals(28, byteBuffer.capacity());

    }
  }

  @Test
  public void testSetLEValues() {
    try (BufferAllocator allocator = new RootAllocator(128);
         ArrowBuf buf = allocator.buffer(20);
    ) {
      NettyArrowBuf nettyBuf = NettyArrowBuf.unwrapBuffer(buf);
      int [] intVals = new int[] {Integer.MIN_VALUE, Short.MIN_VALUE - 1, Short.MIN_VALUE, 0 ,
        Short.MAX_VALUE , Short.MAX_VALUE + 1, Integer.MAX_VALUE};
      for (int intValue :intVals ) {
        nettyBuf._setInt(0, intValue);
        Assert.assertEquals(nettyBuf._getIntLE(0), Integer.reverseBytes(intValue));
      }

      long [] longVals = new long[] {Long.MIN_VALUE, 0 , Long.MAX_VALUE};
      for (long longValue :longVals ) {
        nettyBuf._setLong(0, longValue);
        Assert.assertEquals(nettyBuf._getLongLE(0), Long.reverseBytes(longValue));
      }

      short [] shortVals = new short[] {Short.MIN_VALUE, 0 , Short.MAX_VALUE};
      for (short shortValue :shortVals ) {
        nettyBuf._setShort(0, shortValue);
        Assert.assertEquals(nettyBuf._getShortLE(0), Short.reverseBytes(shortValue));
      }
    }
  }

  @Test
  public void testSetCompositeBuffer() {
    try (BufferAllocator allocator = new RootAllocator(128);
         ArrowBuf buf = allocator.buffer(20);
         NettyArrowBuf buf2 = NettyArrowBuf.unwrapBuffer(allocator.buffer(20));
    ) {
      CompositeByteBuf byteBufs = new CompositeByteBuf(new ArrowByteBufAllocator(allocator),
              true, 1);
      int expected = 4;
      buf2.setInt(0, expected);
      buf2.writerIndex(4);
      byteBufs.addComponent(true, buf2);
      NettyArrowBuf.unwrapBuffer(buf).setBytes(0, byteBufs, 4);
      int actual = buf.getInt(0);
      Assert.assertEquals(expected, actual);
    }
  }

  @Test
  public void testGetCompositeBuffer() {
    try (BufferAllocator allocator = new RootAllocator(128);
         ArrowBuf buf = allocator.buffer(20);
    ) {
      CompositeByteBuf byteBufs = new CompositeByteBuf(new ArrowByteBufAllocator(allocator),
              true, 1);
      int expected = 4;
      buf.setInt(0, expected);
      NettyArrowBuf buf2 = NettyArrowBuf.unwrapBuffer(allocator.buffer(20));
      // composite buffers are a bit weird, need to jump hoops
      // to set capacity.
      byteBufs.addComponent(true, buf2);
      byteBufs.capacity(20);
      NettyArrowBuf.unwrapBuffer(buf).getBytes(0, byteBufs, 4);
      int actual = byteBufs.getInt(0);
      Assert.assertEquals(expected, actual);
      byteBufs.component(0).release();
    }
  }

  @Test
  public void testUnwrapReturnsNull() {
    try (BufferAllocator allocator = new RootAllocator(128);
         ArrowBuf buf = allocator.buffer(20);
    ) {
      NettyArrowBuf nettyBuf = NettyArrowBuf.unwrapBuffer(buf);
      // NettyArrowBuf cannot be unwrapped, so unwrap() should return null per the Netty ByteBuf API
      Assert.assertNull(nettyBuf.unwrap());
    }
  }
}
