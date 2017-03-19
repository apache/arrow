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

package io.netty.buffer;

import org.apache.arrow.memory.BufferAllocator;

/**
 * Allows us to decorate ArrowBuf to make it expandable so that we can use them in the context of
 * the Netty framework
 * (thus supporting RPC level memory accounting).
 */
public class ExpandableByteBuf extends MutableWrappedByteBuf {

  private final BufferAllocator allocator;

  public ExpandableByteBuf(ByteBuf buffer, BufferAllocator allocator) {
    super(buffer);
    this.allocator = allocator;
  }

  @Override
  public ByteBuf copy(int index, int length) {
    return new ExpandableByteBuf(buffer.copy(index, length), allocator);
  }

  @Override
  public ByteBuf capacity(int newCapacity) {
    if (newCapacity > capacity()) {
      ByteBuf newBuf = allocator.buffer(newCapacity);
      newBuf.writeBytes(buffer, 0, buffer.capacity());
      newBuf.readerIndex(buffer.readerIndex());
      newBuf.writerIndex(buffer.writerIndex());
      buffer.release();
      buffer = newBuf;
      return newBuf;
    } else {
      return super.capacity(newCapacity);
    }
  }

}
