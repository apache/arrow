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
      NettyArrowBuf nettyBuf = buf.asNettyBuffer();
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
      NettyArrowBuf nettyBuf = buf.asNettyBuffer();
      ByteBuffer byteBuffer = nettyBuf.nioBuffer(4 ,6);
      // Nio Buffers should always be 0 indexed
      Assert.assertEquals(0, byteBuffer.position());
      Assert.assertEquals(6, byteBuffer.limit());
      // Underlying buffer has size 32 excluding 4 should have capacity of 28.
      Assert.assertEquals(28, byteBuffer.capacity());

    }
  }
}
