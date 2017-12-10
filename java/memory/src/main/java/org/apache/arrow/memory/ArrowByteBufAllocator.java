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

import io.netty.buffer.AbstractByteBufAllocator;
import io.netty.buffer.ByteBuf;
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
public class ArrowByteBufAllocator extends AbstractByteBufAllocator {

  private static final int DEFAULT_BUFFER_SIZE = 4096;
  private static final int DEFAULT_MAX_COMPOSITE_COMPONENTS = 16;

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

  @Override
  protected ByteBuf newHeapBuffer(int initialCapacity, int maxCapacity) {
    throw fail();
  }

  @Override
  protected ByteBuf newDirectBuffer(int initialCapacity, int maxCapacity) {
    return buffer(initialCapacity, maxCapacity);
  }

  private RuntimeException fail() {
    throw new UnsupportedOperationException("Allocator doesn't support heap-based memory.");
  }
}
