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
package org.apache.arrow.memory.netty;

import static org.junit.jupiter.api.Assertions.assertEquals;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.NettyArrowBuf;
import java.nio.ByteOrder;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.junit.jupiter.api.Test;

public class TestEndianness {

  @Test
  public void testNativeEndian() {
    final BufferAllocator a = new RootAllocator(10000);
    final ByteBuf b = NettyArrowBuf.unwrapBuffer(a.buffer(4));
    b.setInt(0, 35);
    if (ByteOrder.nativeOrder() == ByteOrder.LITTLE_ENDIAN) {
      assertEquals(35, b.getByte(0));
      assertEquals(0, b.getByte(1));
      assertEquals(0, b.getByte(2));
      assertEquals(0, b.getByte(3));
    } else {
      assertEquals(0, b.getByte(0));
      assertEquals(0, b.getByte(1));
      assertEquals(0, b.getByte(2));
      assertEquals(35, b.getByte(3));
    }
    b.release();
    a.close();
  }
}
