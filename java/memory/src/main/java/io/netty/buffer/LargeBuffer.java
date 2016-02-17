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
package io.netty.buffer;

import java.util.concurrent.atomic.AtomicLong;

/**
 * A MutableWrappedByteBuf that also maintains a metric of the number of huge buffer bytes and counts.
 */
public class LargeBuffer extends MutableWrappedByteBuf {

  private final AtomicLong hugeBufferSize;
  private final AtomicLong hugeBufferCount;

  private final int initCap;

  public LargeBuffer(ByteBuf buffer, AtomicLong hugeBufferSize, AtomicLong hugeBufferCount) {
    super(buffer);
    initCap = buffer.capacity();
    this.hugeBufferCount = hugeBufferCount;
    this.hugeBufferSize = hugeBufferSize;
  }

  @Override
  public ByteBuf copy(int index, int length) {
    return new LargeBuffer(buffer.copy(index, length), hugeBufferSize, hugeBufferCount);
  }

  @Override
  public boolean release() {
    return release(1);
  }

  @Override
  public boolean release(int decrement) {
    boolean released = unwrap().release(decrement);
    if (released) {
      hugeBufferSize.addAndGet(-initCap);
      hugeBufferCount.decrementAndGet();
    }
    return released;
  }

}
