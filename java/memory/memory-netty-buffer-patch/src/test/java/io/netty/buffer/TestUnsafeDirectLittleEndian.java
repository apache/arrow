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


import static org.junit.Assert.assertEquals;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;

import org.junit.Test;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.LargeBuffer;
import io.netty.buffer.Unpooled;
import io.netty.buffer.UnsafeDirectLittleEndian;

public class TestUnsafeDirectLittleEndian {
  private static final boolean LITTLE_ENDIAN = ByteOrder.nativeOrder() == ByteOrder.LITTLE_ENDIAN;

  @Test
  public void testPrimitiveGetSet() {
    ByteBuf byteBuf = Unpooled.directBuffer(64);
    UnsafeDirectLittleEndian unsafeDirect = new UnsafeDirectLittleEndian(new LargeBuffer(byteBuf));

    unsafeDirect.setByte(0, Byte.MAX_VALUE);
    unsafeDirect.setByte(1, -1); // 0xFF
    unsafeDirect.setShort(2, Short.MAX_VALUE);
    unsafeDirect.setShort(4, -2); // 0xFFFE
    unsafeDirect.setInt(8, Integer.MAX_VALUE);
    unsafeDirect.setInt(12, -66052); // 0xFFFE FDFC
    unsafeDirect.setLong(16, Long.MAX_VALUE);
    unsafeDirect.setLong(24, -4295098372L); // 0xFFFF FFFE FFFD FFFC
    unsafeDirect.setFloat(32, 1.23F);
    unsafeDirect.setFloat(36, -1.23F);
    unsafeDirect.setDouble(40, 1.234567D);
    unsafeDirect.setDouble(48, -1.234567D);

    assertEquals(Byte.MAX_VALUE, unsafeDirect.getByte(0));
    assertEquals(-1, unsafeDirect.getByte(1));
    assertEquals(Short.MAX_VALUE, unsafeDirect.getShort(2));
    assertEquals(-2, unsafeDirect.getShort(4));
    assertEquals((char) 65534, unsafeDirect.getChar(4));
    assertEquals(Integer.MAX_VALUE, unsafeDirect.getInt(8));
    assertEquals(-66052, unsafeDirect.getInt(12));
    assertEquals(4294901244L, unsafeDirect.getUnsignedInt(12));
    assertEquals(Long.MAX_VALUE, unsafeDirect.getLong(16));
    assertEquals(-4295098372L, unsafeDirect.getLong(24));
    assertEquals(1.23F, unsafeDirect.getFloat(32), 0.0);
    assertEquals(-1.23F, unsafeDirect.getFloat(36), 0.0);
    assertEquals(1.234567D, unsafeDirect.getDouble(40), 0.0);
    assertEquals(-1.234567D, unsafeDirect.getDouble(48), 0.0);

    byte[] inBytes = "1234567".getBytes(StandardCharsets.UTF_8);
    try (ByteArrayInputStream bais = new ByteArrayInputStream(inBytes);
         ByteArrayOutputStream baos = new ByteArrayOutputStream()) {
      assertEquals(5, unsafeDirect.setBytes(56, bais, 5));
      unsafeDirect.getBytes(56, baos, 5);
      assertEquals("12345", new String(baos.toByteArray(), StandardCharsets.UTF_8));
    } catch (IOException e) {
      e.printStackTrace();
    }
  }
}
