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

import org.apache.arrow.memory.BufferManager;
import org.apache.arrow.memory.RootAllocator;

import io.netty.buffer.ArrowBuf;

/**
 * Root allocator that returns buffers pre-filled with a given value.<br>
 * Useful for testing if value vectors are properly zeroing their buffers.
 */
public class DirtyRootAllocator extends RootAllocator {

  private final byte fillValue;

  public DirtyRootAllocator(final long limit, final byte fillValue) {
    super(limit);
    this.fillValue = fillValue;
  }

  @Override
  public ArrowBuf buffer(int size) {
    return buffer(size, null);
  }

  @Override
  public ArrowBuf buffer(int size, BufferManager manager) {
    ArrowBuf buffer = super.buffer(size, manager);
    // contaminate the buffer
    for (int i = 0; i < buffer.capacity(); i++) {
      buffer.setByte(i, fillValue);
    }

    return buffer;
  }
}
