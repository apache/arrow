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

package org.apache.arrow.memory.util.hash;

import static org.junit.Assert.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.Arrays;
import java.util.Collection;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import io.netty.buffer.ArrowBuf;

/**
 * Test cases for {@link ArrowBufHasher} and its subclasses.
 */
@RunWith(Parameterized.class)
public class TestArrowBufHasher {

  private final int BUFFER_LENGTH = 1024;

  private BufferAllocator allocator;

  private ArrowBufHasher hasher;

  public TestArrowBufHasher(String name, ArrowBufHasher hasher) {
    this.hasher = hasher;
  }

  @Before
  public void prepare() {
    allocator = new RootAllocator(1024 * 1024);
  }

  @After
  public void shutdown() {
    allocator.close();
  }

  @Test
  public void testHasher() {
    try (ArrowBuf buf1 = allocator.buffer(BUFFER_LENGTH);
         ArrowBuf buf2 = allocator.buffer(BUFFER_LENGTH)) {
      // prepare data
      for (int i = 0; i < BUFFER_LENGTH / 4; i++) {
        buf1.setFloat(i * 4, i / 10.0f);
        buf2.setFloat(i * 4, i / 10.0f);
      }

      verifyHashCodesEqual(buf1, 0, 100, buf2, 0, 100);
      verifyHashCodesEqual(buf1, 1, 5, buf2, 1, 5);
      verifyHashCodesEqual(buf1, 10, 17, buf2, 10, 17);
      verifyHashCodesEqual(buf1, 33, 25, buf2, 33, 25);
      verifyHashCodesEqual(buf1, 22, 22, buf2, 22, 22);
      verifyHashCodesEqual(buf1, 123, 333, buf2, 123, 333);
      verifyHashCodesEqual(buf1, 374, 1, buf2, 374, 1);
      verifyHashCodesEqual(buf1, 11, 0, buf2, 11, 0);
      verifyHashCodesEqual(buf1, 75, 25, buf2, 75, 25);
      verifyHashCodesEqual(buf1, 0, 1024, buf2, 0, 1024);
    }
  }

  private void verifyHashCodesEqual(ArrowBuf buf1, int offset1, int length1,
                                    ArrowBuf buf2, int offset2, int length2) {
    int hashCode1 = hasher.hashCode(buf1, offset1, length1);
    int hashCode2 = hasher.hashCode(buf2, offset2, length2);
    assertEquals(hashCode1, hashCode2);
  }

  @Test
  public void testHasherNegative() {
    try (ArrowBuf buf = allocator.buffer(BUFFER_LENGTH)) {
      // prepare data
      for (int i = 0; i < BUFFER_LENGTH / 4; i++) {
        buf.setFloat(i * 4, i / 10.0f);
      }

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

  @Parameterized.Parameters(name = "hasher = {0}")
  public static Collection<Object[]> getHasher() {
    return Arrays.asList(
      new Object[] {SimpleHasher.class.getSimpleName(),
        SimpleHasher.INSTANCE},
      new Object[] {MurmurHasher.class.getSimpleName(),
        new MurmurHasher()
      }
    );
  }
}
