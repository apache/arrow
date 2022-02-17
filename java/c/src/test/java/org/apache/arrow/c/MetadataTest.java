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

package org.apache.arrow.c;

import static org.junit.jupiter.api.Assertions.*;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.HashMap;
import java.util.Map;

import org.apache.arrow.c.Metadata;
import org.apache.arrow.c.NativeUtil;
import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.memory.util.LargeMemoryUtil;
import org.apache.arrow.memory.util.MemoryUtil;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class MetadataTest {
  private RootAllocator allocator = null;

  private static Map<String, String> metadata;
  private static byte[] encoded;

  @BeforeAll
  static void beforeAll() {
    metadata = new HashMap<>();
    metadata.put("key1", "");
    metadata.put("key2", "bar");

    if (ByteOrder.nativeOrder() == ByteOrder.LITTLE_ENDIAN) {
      encoded = new byte[] { 2, 0, 0, 0, 4, 0, 0, 0, 'k', 'e', 'y', '1', 0, 0, 0, 0, 4, 0, 0, 0, 'k', 'e', 'y', '2', 3,
          0, 0, 0, 'b', 'a', 'r' };
    } else {
      encoded = new byte[] { 0, 0, 0, 2, 0, 0, 0, 4, 'k', 'e', 'y', '1', 0, 0, 0, 0, 0, 0, 0, 4, 'k', 'e', 'y', '2', 0,
          0, 0, 3, 'b', 'a', 'r' };
    }
  }

  @BeforeEach
  public void setUp() {
    allocator = new RootAllocator(Long.MAX_VALUE);
  }

  @AfterEach
  public void tearDown() {
    allocator.close();
  }

  @Test
  public void testEncode() {
    try (ArrowBuf buffer = Metadata.encode(allocator, metadata)) {
      int totalSize = LargeMemoryUtil.checkedCastToInt(buffer.readableBytes());
      ByteBuffer reader = MemoryUtil.directBuffer(buffer.memoryAddress(), totalSize).order(ByteOrder.nativeOrder());
      byte[] result = new byte[totalSize];
      reader.get(result);
      assertArrayEquals(encoded, result);
    }
  }

  @Test
  public void testDecode() {
    try (ArrowBuf buffer = allocator.buffer(31)) {
      buffer.setBytes(0, encoded);
      Map<String, String> decoded = Metadata.decode(buffer.memoryAddress());
      assertNotNull(decoded);
      assertEquals(metadata, decoded);
    }
  }

  @Test
  public void testEncodeEmpty() {
    Map<String, String> metadata = new HashMap<>();
    try (ArrowBuf encoded = Metadata.encode(allocator, metadata)) {
      assertNull(encoded);
    }
  }

  @Test
  public void testDecodeEmpty() {
    Map<String, String> decoded = Metadata.decode(NativeUtil.NULL);
    assertNull(decoded);
  }

}
