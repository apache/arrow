/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.arrow.vector;

import java.util.ArrayList;
import java.util.List;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.schema.ArrowFieldNode;

import io.netty.buffer.ArrowBuf;


public abstract class BaseDataValueVector extends BaseValueVector implements BufferBacked {

  protected final static byte[] emptyByteArray = new byte[]{}; // Nullable vectors use this

  public static void load(ArrowFieldNode fieldNode, List<BufferBacked> vectors, List<ArrowBuf> buffers) {
    int expectedSize = vectors.size();
    if (buffers.size() != expectedSize) {
      throw new IllegalArgumentException("Illegal buffer count, expected " + expectedSize + ", got: " + buffers.size());
    }
    for (int i = 0; i < expectedSize; i++) {
      vectors.get(i).load(fieldNode, buffers.get(i));
    }
  }

  public static void truncateBufferBasedOnSize(List<ArrowBuf> buffers, int bufferIndex, int byteSize) {
    if (bufferIndex >= buffers.size()) {
      throw new IllegalArgumentException("no buffer at index " + bufferIndex + ": " + buffers);
    }
    ArrowBuf buffer = buffers.get(bufferIndex);
    if (buffer.writerIndex() < byteSize) {
      throw new IllegalArgumentException("can not truncate buffer to a larger size " + byteSize + ": " + buffer.writerIndex());
    }
    buffer.writerIndex(byteSize);
  }

  public static List<ArrowBuf> unload(List<BufferBacked> vectors) {
    List<ArrowBuf> result = new ArrayList<>(vectors.size());
    for (BufferBacked vector : vectors) {
      result.add(vector.unLoad());
    }
    return result;
  }

  // TODO: Nullable vectors extend BaseDataValueVector but do not use the data field
  // We should fix the inheritance tree
  protected ArrowBuf data;

  public BaseDataValueVector(String name, BufferAllocator allocator) {
    super(name, allocator);
    data = allocator.getEmpty();
  }

  @Override
  public void clear() {
    if (data != null) {
      data.release();
    }
    data = allocator.getEmpty();
    super.clear();
  }

  @Override
  public void close() {
    clear();
    if (data != null) {
      data.release();
      data = null;
    }
    super.close();
  }

  @Override
  public ArrowBuf[] getBuffers(boolean clear) {
    ArrowBuf[] out;
    if (getBufferSize() == 0) {
      out = new ArrowBuf[0];
    } else {
      out = new ArrowBuf[]{data};
      data.readerIndex(0);
      if (clear) {
        data.retain(1);
      }
    }
    if (clear) {
      clear();
    }
    return out;
  }

  @Override
  public int getBufferSize() {
    if (getAccessor().getValueCount() == 0) {
      return 0;
    }
    return data.writerIndex();
  }

  public ArrowBuf getBuffer() {
    return data;
  }

  @Override
  public void load(ArrowFieldNode fieldNode, ArrowBuf data) {
    this.data.release();
    this.data = data.retain(allocator);
  }

  @Override
  public ArrowBuf unLoad() {
    return this.data.readerIndex(0);
  }

  /**
   * This method has a similar effect of allocateNew() without actually clearing and reallocating
   * the value vector. The purpose is to move the value vector to a "mutate" state
   */
  public void reset() {}
}
