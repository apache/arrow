/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.arrow.memory;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.CompositeByteBuf;
import io.netty.buffer.ExpandableByteBuf;

/**
 * An implementation of ByteBufAllocator that wraps a Arrow BufferAllocator. This allows the RPC
 * layer to be accounted
 * and managed using Arrow's BufferAllocator infrastructure. The only thin different from a
 * typical BufferAllocator is
 * the signature and the fact that this Allocator returns ExpandableByteBufs which enable
 * otherwise non-expandable
 * ArrowBufs to be expandable.
 */
public class ArrowByteBufAllocator implements ByteBufAllocator {

  private static final int DEFAULT_BUFFER_SIZE = 4096;
  private static final int DEFAULT_MAX_COMPOSITE_COMPONENTS = 16;
  private static final int CALCULATE_THRESHOLD = 1048576 * 4; // 4 MiB page

  private final BufferAllocator allocator;

  public ArrowByteBufAllocator(BufferAllocator allocator) {
    this.allocator = allocator;
  }

  public BufferAllocator unwrap() {
    return allocator;
  }

  @Override
  public ByteBuf buffer() {
    return buffer(DEFAULT_BUFFER_SIZE);
  }

  @Override
  public ByteBuf buffer(int initialCapacity) {
    return new ExpandableByteBuf(allocator.buffer(initialCapacity), allocator);
  }

  @Override
  public ByteBuf buffer(int initialCapacity, int maxCapacity) {
    return buffer(initialCapacity);
  }

  @Override
  public ByteBuf ioBuffer() {
    return buffer();
  }

  @Override
  public ByteBuf ioBuffer(int initialCapacity) {
    return buffer(initialCapacity);
  }

  @Override
  public ByteBuf ioBuffer(int initialCapacity, int maxCapacity) {
    return buffer(initialCapacity);
  }

  @Override
  public ByteBuf directBuffer() {
    return buffer();
  }

  @Override
  public ByteBuf directBuffer(int initialCapacity) {
    return allocator.buffer(initialCapacity);
  }

  @Override
  public ByteBuf directBuffer(int initialCapacity, int maxCapacity) {
    return buffer(initialCapacity, maxCapacity);
  }

  @Override
  public CompositeByteBuf compositeBuffer() {
    return compositeBuffer(DEFAULT_MAX_COMPOSITE_COMPONENTS);
  }

  @Override
  public CompositeByteBuf compositeBuffer(int maxNumComponents) {
    return new CompositeByteBuf(this, true, maxNumComponents);
  }

  @Override
  public CompositeByteBuf compositeDirectBuffer() {
    return compositeBuffer();
  }

  @Override
  public CompositeByteBuf compositeDirectBuffer(int maxNumComponents) {
    return compositeBuffer(maxNumComponents);
  }

  @Override
  public boolean isDirectBufferPooled() {
    return false;
  }

  @Override
  public ByteBuf heapBuffer() {
    throw fail();
  }

  @Override
  public ByteBuf heapBuffer(int initialCapacity) {
    throw fail();
  }

  @Override
  public ByteBuf heapBuffer(int initialCapacity, int maxCapacity) {
    throw fail();
  }

  @Override
  public CompositeByteBuf compositeHeapBuffer() {
    throw fail();
  }

  @Override
  public CompositeByteBuf compositeHeapBuffer(int maxNumComponents) {
    throw fail();
  }

  private RuntimeException fail() {
    throw new UnsupportedOperationException("Allocator doesn't support heap-based memory.");
  }

  /**
   * This method was copied from AbstractByteBufAllocator. Netty 4.1.x moved this method from
   * AbstractByteBuf to AbstractByteBufAllocator. However, as ArrowByteBufAllocator doesn't extend
   * AbstractByteBufAllocator, it doesn't get the implementation automatically and we have to copy
   * the codes.
   */
  @Override
  public int calculateNewCapacity(int minNewCapacity, int maxCapacity) {
    if (minNewCapacity < 0) {
      throw new IllegalArgumentException("minNewCapacity: " + minNewCapacity + " (expected: 0+)");
    }
    if (minNewCapacity > maxCapacity) {
      throw new IllegalArgumentException(String.format(
          "minNewCapacity: %d (expected: not greater than maxCapacity(%d)",
          minNewCapacity, maxCapacity));
    }
    final int threshold = CALCULATE_THRESHOLD; // 4 MiB page

    if (minNewCapacity == threshold) {
      return threshold;
    }

    // If over threshold, do not double but just increase by threshold.
    if (minNewCapacity > threshold) {
      int newCapacity = minNewCapacity / threshold * threshold;
      if (newCapacity > maxCapacity - threshold) {
        newCapacity = maxCapacity;
      } else {
        newCapacity += threshold;
      }
      return newCapacity;
    }

    // Not over threshold. Double up to 4 MiB, starting from 64.
    int newCapacity = 64;
    while (newCapacity < minNewCapacity) {
      newCapacity <<= 1;
    }

    return Math.min(newCapacity, maxCapacity);
  }
}
