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

import static org.apache.arrow.c.NativeUtil.NULL;
import static org.apache.arrow.util.Preconditions.checkState;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.util.MemoryUtil;

/**
 * Encode and decode metadata.
 */
final class Metadata {

  private Metadata() {
  }

  static ArrowBuf encode(BufferAllocator allocator, Map<String, String> metadata) {
    if (metadata == null || metadata.size() == 0) {
      return null;
    }

    List<byte[]> buffers = new ArrayList<>(metadata.size() * 2);
    int totalSize = 4 + metadata.size() * 8; // number of key/value pairs + buffer length fields
    for (Map.Entry<String, String> entry : metadata.entrySet()) {
      byte[] keyBuffer = entry.getKey().getBytes(StandardCharsets.UTF_8);
      byte[] valueBuffer = entry.getValue().getBytes(StandardCharsets.UTF_8);
      totalSize += keyBuffer.length;
      totalSize += valueBuffer.length;
      buffers.add(keyBuffer);
      buffers.add(valueBuffer);
    }

    ArrowBuf result = allocator.buffer(totalSize);
    ByteBuffer writer = MemoryUtil.directBuffer(result.memoryAddress(), totalSize).order(ByteOrder.nativeOrder());
    writer.putInt(metadata.size());
    for (byte[] buffer : buffers) {
      writer.putInt(buffer.length);
      writer.put(buffer);
    }
    return result.slice(0, totalSize);
  }

  static Map<String, String> decode(long bufferAddress) {
    if (bufferAddress == NULL) {
      return null;
    }

    ByteBuffer reader = MemoryUtil.directBuffer(bufferAddress, Integer.MAX_VALUE).order(ByteOrder.nativeOrder());

    int size = reader.getInt();
    checkState(size >= 0, "Metadata size must not be negative");
    if (size == 0) {
      return null;
    }

    Map<String, String> result = new HashMap<>(size);
    for (int i = 0; i < size; i++) {
      String key = readString(reader);
      String value = readString(reader);
      result.put(key, value);
    }
    return result;
  }

  private static String readString(ByteBuffer reader) {
    int length = reader.getInt();
    checkState(length >= 0, "Metadata item length must not be negative");
    String result = "";
    if (length > 0) {
      byte[] dst = new byte[length];
      reader.get(dst);
      result = new String(dst, StandardCharsets.UTF_8);
    }
    return result;
  }
}
