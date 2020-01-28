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

package org.apache.arrow.memory;

import static org.junit.Assert.assertEquals;

import io.netty.buffer.ArrowBuf;

/**
 * Integration test for large (more than 2GB) {@link io.netty.buffer.ArrowBuf}.
 * To run this test, please
 *<li>Make sure there are 4GB memory available in the system.</li>
 * <li>
 *   Make sure the default allocation manager type is unsafe.
 *   This can be achieved by the environmental variable or system property.
 *   The details can be found in {@link DefaultAllocationManagerOption}.
 * </li>
 */
public class TestLargeArrowBuf {

  private static void testLargeArrowBuf() {
    final long bufSize = 4 * 1024 * 1024 * 1024L;
    try (BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE);
         ArrowBuf largeBuf = allocator.buffer(bufSize)) {
      assertEquals(bufSize, largeBuf.capacity());
      System.out.println("Successfully allocated a buffer with capacity " + largeBuf.capacity());

      for (long i = 0; i < bufSize / 8; i++) {
        largeBuf.setLong(i * 8, i);

        if ((i + 1) % 10000 == 0) {
          System.out.println("Successfully written " + (i + 1) + " long words");
        }
      }
      System.out.println("Successfully written " + (bufSize / 8) + " long words");

      for (long i = 0; i < bufSize / 8; i++) {
        long val = largeBuf.getLong(i * 8);
        assertEquals(i, val);

        if ((i + 1) % 10000 == 0) {
          System.out.println("Successfully read " + (i + 1) + " long words");
        }
      }
      System.out.println("Successfully read " + (bufSize / 8) + " long words");
    }
    System.out.println("Successfully released the large buffer.");
  }

  public static void main(String[] args) {
    testLargeArrowBuf();
  }
}
