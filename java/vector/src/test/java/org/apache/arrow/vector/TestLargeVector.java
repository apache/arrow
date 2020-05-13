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

package org.apache.arrow.vector;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;

/**
 * Integration test for a vector with a large (more than 2GB) {@link org.apache.arrow.memory.ArrowBuf} as
 * the data buffer.
 * To run this test, please make sure there is at least 4GB free memory in the system.
 * <p>
 *  Please note that this is not a standard test case, so please run it by manually invoking the
 *  main method.
 * </p>
 */
public class TestLargeVector {

  private static void testLargeLongVector() {
    System.out.println("Testing large big int vector.");

    final long bufSize = 4 * 1024 * 1024 * 1024L;
    final int vecLength = (int) (bufSize / BigIntVector.TYPE_WIDTH);

    try (BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE);
        BigIntVector largeVec = new BigIntVector("vec", allocator)) {
      largeVec.allocateNew(vecLength);

      System.out.println("Successfully allocated a vector with capacity " + vecLength);

      for (int i = 0; i < vecLength; i++) {
        largeVec.set(i, i * 10L);

        if ((i + 1) % 10000 == 0) {
          System.out.println("Successfully written " + (i + 1) + " values");
        }
      }
      System.out.println("Successfully written " + vecLength + " values");

      for (int i = 0; i < vecLength; i++) {
        long val = largeVec.get(i);
        assertEquals(i * 10L, val);

        if ((i + 1) % 10000 == 0) {
          System.out.println("Successfully read " + (i + 1) + " values");
        }
      }
      System.out.println("Successfully read " + vecLength + " values");
    }
    System.out.println("Successfully released the large vector.");
  }

  private static void testLargeIntVector() {
    System.out.println("Testing large int vector.");

    final long bufSize = 4 * 1024 * 1024 * 1024L;
    final int vecLength = (int) (bufSize / IntVector.TYPE_WIDTH);

    try (BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE);
         IntVector largeVec = new IntVector("vec", allocator)) {
      largeVec.allocateNew(vecLength);

      System.out.println("Successfully allocated a vector with capacity " + vecLength);

      for (int i = 0; i < vecLength; i++) {
        largeVec.set(i, i);

        if ((i + 1) % 10000 == 0) {
          System.out.println("Successfully written " + (i + 1) + " values");
        }
      }
      System.out.println("Successfully written " + vecLength + " values");

      for (int i = 0; i < vecLength; i++) {
        long val = largeVec.get(i);
        assertEquals(i, val);

        if ((i + 1) % 10000 == 0) {
          System.out.println("Successfully read " + (i + 1) + " values");
        }
      }
      System.out.println("Successfully read " + vecLength + " values");
    }
    System.out.println("Successfully released the large vector.");
  }

  private static void testLargeDecimalVector() {
    System.out.println("Testing large decimal vector.");

    final long bufSize = 4 * 1024 * 1024 * 1024L;
    final int vecLength = (int) (bufSize / DecimalVector.TYPE_WIDTH);

    try (BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE);
         DecimalVector largeVec = new DecimalVector("vec", allocator, 38, 16)) {
      largeVec.allocateNew(vecLength);

      System.out.println("Successfully allocated a vector with capacity " + vecLength);

      for (int i = 0; i < vecLength; i++) {
        largeVec.set(i, 0);

        if ((i + 1) % 10000 == 0) {
          System.out.println("Successfully written " + (i + 1) + " values");
        }
      }
      System.out.println("Successfully written " + vecLength + " values");

      for (int i = 0; i < vecLength; i++) {
        ArrowBuf buf = largeVec.get(i);
        assertEquals(buf.capacity(), DecimalVector.TYPE_WIDTH);
        assertEquals(0, buf.getLong(0));
        assertEquals(0, buf.getLong(8));

        if ((i + 1) % 10000 == 0) {
          System.out.println("Successfully read " + (i + 1) + " values");
        }
      }
      System.out.println("Successfully read " + vecLength + " values");
    }
    System.out.println("Successfully released the large vector.");
  }

  private static void testLargeFixedSizeBinaryVector() {
    System.out.println("Testing large fixed size binary vector.");

    final long bufSize = 4 * 1024 * 1024 * 1024L;
    final int typeWidth = 8;
    final int vecLength = (int) (bufSize / typeWidth);

    try (BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE);
         FixedSizeBinaryVector largeVec = new FixedSizeBinaryVector("vec", allocator, typeWidth)) {
      largeVec.allocateNew(vecLength);

      System.out.println("Successfully allocated a vector with capacity " + vecLength);

      byte[] value = new byte[] {'a', 'b', 'c', 'd', 'e', 'f', 'g', 'h'};
      for (int i = 0; i < vecLength; i++) {
        largeVec.set(i, value);

        if ((i + 1) % 10000 == 0) {
          System.out.println("Successfully written " + (i + 1) + " values");
        }
      }
      System.out.println("Successfully written " + vecLength + " values");

      for (int i = 0; i < vecLength; i++) {
        byte[] buf = largeVec.get(i);
        assertEquals(typeWidth, buf.length);
        assertArrayEquals(buf, value);

        if ((i + 1) % 10000 == 0) {
          System.out.println("Successfully read " + (i + 1) + " values");
        }
      }
      System.out.println("Successfully read " + vecLength + " values");
    }
    System.out.println("Successfully released the large vector.");
  }

  public static void main(String[] args) {
    testLargeLongVector();
    testLargeIntVector();
    testLargeDecimalVector();
    testLargeFixedSizeBinaryVector();
  }
}
