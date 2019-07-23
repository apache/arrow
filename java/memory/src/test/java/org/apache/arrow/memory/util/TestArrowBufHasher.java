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

package org.apache.arrow.memory.util;

import static org.junit.Assert.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;

import org.apache.arrow.memory.util.hash.ArrowBufHasher;
import org.apache.arrow.memory.util.hash.DirectHasher;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import io.netty.buffer.ArrowBuf;

/**
 * Test cases for {@link ArrowBufHasher} and its subclasses.
 */
public class TestArrowBufHasher {

  private final int BUFFER_LENGTH = 1024;

  private BufferAllocator allocator;

  @Before
  public void prepare() {
    allocator = new RootAllocator(1024 * 1024);
  }

  @After
  public void shutdown() {
    allocator.close();
  }

  @Test
  public void testDirectHasher() {
    try (ArrowBuf buf1 = allocator.buffer(BUFFER_LENGTH);
         ArrowBuf buf2 = allocator.buffer(BUFFER_LENGTH)) {
      // prepare data
      for (int i = 0; i < BUFFER_LENGTH / 4; i++) {
        buf1.setFloat(i * 4, i / 10.0f);
        buf2.setFloat(i * 4, i / 10.0f);
      }

      ArrowBufHasher hasher = DirectHasher.INSTANCE;

      assertEquals(hasher.hashCode(buf1, 0, 100), hasher.hashCode(buf2, 0, 100));
      assertEquals(hasher.hashCode(buf1, 1, 5), hasher.hashCode(buf2, 1, 5));
      assertEquals(hasher.hashCode(buf1, 10, 17), hasher.hashCode(buf2, 10, 17));
      assertEquals(hasher.hashCode(buf1, 33, 25), hasher.hashCode(buf2, 33, 25));
      assertEquals(hasher.hashCode(buf1, 22, 22), hasher.hashCode(buf2, 22, 22));
      assertEquals(hasher.hashCode(buf1, 123, 333), hasher.hashCode(buf2, 123, 333));
      assertEquals(hasher.hashCode(buf1, 374, 1), hasher.hashCode(buf2, 374, 1));
      assertEquals(hasher.hashCode(buf1, 11, 0), hasher.hashCode(buf2, 11, 0));
      assertEquals(hasher.hashCode(buf1, 75, 25), hasher.hashCode(buf2, 75, 25));
      assertEquals(hasher.hashCode(buf1, 0, 1024), hasher.hashCode(buf2, 0, 1024));
    }
  }

  @Test
  public void testDirectHasherNegative() {
    try (ArrowBuf buf = allocator.buffer(BUFFER_LENGTH)) {
      // prepare data
      for (int i = 0; i < BUFFER_LENGTH / 4; i++) {
        buf.setFloat(i * 4, i / 10.0f);
      }

      ArrowBufHasher hasher = DirectHasher.INSTANCE;
      assertThrows(IllegalArgumentException.class, () -> {
        hasher.hashCode(buf, 0, -1);
      });

      assertThrows(IndexOutOfBoundsException.class, () -> {
        hasher.hashCode(buf, 0, 1028);
      });

      assertThrows(IndexOutOfBoundsException.class, () -> {
        hasher.hashCode(buf, 500, 1000);
      });
    }
  }
}
