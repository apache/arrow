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

package org.apache.arrow.algorithm.swap;

import static junit.framework.TestCase.assertTrue;
import static org.junit.Assert.assertEquals;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;

import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.Float4Vector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.SmallIntVector;
import org.apache.arrow.vector.TinyIntVector;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * Test cases for {@link VectorDataSwapper}.
 */
public class TestVectorDataSwapper {

  private static final int VECTOR_LENGTH = 30;

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
  public void testByteSwap() {
    try (VectorDataSwapper<TinyIntVector> swapper = new VectorDataSwapper<>(TinyIntVector.TYPE_WIDTH, allocator);
         TinyIntVector vec1 = new TinyIntVector("vec1", allocator);
         TinyIntVector vec2 = new TinyIntVector("vec2", allocator)) {
      vec1.allocateNew(VECTOR_LENGTH);
      vec2.allocateNew(VECTOR_LENGTH);

      // prepare data
      for (int i = 0; i < VECTOR_LENGTH; i++) {
        if (i % 10 == 0) {
          vec1.setNull(i);
        } else {
          vec1.set(i, i);
        }
      }
      for (int i = 0; i < VECTOR_LENGTH; i++) {
        if (i % 5 == 0) {
          vec2.setNull(i);
        } else {
          vec2.set(i, i + 50);
        }
      }

      // swap
      swapper.swap(vec1, 0, vec2, 3);
      assertEquals((byte) 53, vec1.get(0));
      assertTrue(vec2.isNull(3));

      swapper.swap(vec1, 17, vec2, 15);
      assertTrue(vec1.isNull(17));
      assertEquals((byte) 17, vec2.get(15));

      swapper.swap(vec1, 20, vec2, 25);
      assertTrue(vec1.isNull(20));
      assertTrue(vec2.isNull(25));

      swapper.swap(vec1, 23, vec2, 27);
      assertEquals((byte) 77, vec1.get(23));
      assertEquals((byte) 23, vec2.get(27));
    }
  }

  @Test
  public void testShortSwap() {
    try (VectorDataSwapper<SmallIntVector> swapper = new VectorDataSwapper<>(SmallIntVector.TYPE_WIDTH, allocator);
         SmallIntVector vec1 = new SmallIntVector("vec1", allocator);
         SmallIntVector vec2 = new SmallIntVector("vec2", allocator)) {
      vec1.allocateNew(VECTOR_LENGTH);
      vec2.allocateNew(VECTOR_LENGTH);

      // prepare data
      for (int i = 0; i < VECTOR_LENGTH; i++) {
        if (i % 10 == 0) {
          vec1.setNull(i);
        } else {
          vec1.set(i, i);
        }
      }
      for (int i = 0; i < VECTOR_LENGTH; i++) {
        if (i % 5 == 0) {
          vec2.setNull(i);
        } else {
          vec2.set(i, i + 50);
        }
      }

      // swap
      swapper.swap(vec1, 0, vec2, 3);
      assertEquals((short) 53, vec1.get(0));
      assertTrue(vec2.isNull(3));

      swapper.swap(vec1, 17, vec2, 15);
      assertTrue(vec1.isNull(17));
      assertEquals((short) 17, vec2.get(15));

      swapper.swap(vec1, 20, vec2, 25);
      assertTrue(vec1.isNull(20));
      assertTrue(vec2.isNull(25));

      swapper.swap(vec1, 23, vec2, 27);
      assertEquals((short) 77, vec1.get(23));
      assertEquals((short) 23, vec2.get(27));
    }
  }

  @Test
  public void testIntSwap() {
    try (VectorDataSwapper<IntVector> swapper = new VectorDataSwapper<>(IntVector.TYPE_WIDTH, allocator);
    IntVector vec1 = new IntVector("vec1", allocator);
    IntVector vec2 = new IntVector("vec2", allocator)) {
      vec1.allocateNew(VECTOR_LENGTH);
      vec2.allocateNew(VECTOR_LENGTH);

      // prepare data
      for (int i = 0; i < VECTOR_LENGTH; i++) {
        if (i % 10 == 0) {
          vec1.setNull(i);
        } else {
          vec1.set(i, i);
        }
      }
      for (int i = 0; i < VECTOR_LENGTH; i++) {
        if (i % 5 == 0) {
          vec2.setNull(i);
        } else {
          vec2.set(i, i + 50);
        }
      }

      // swap
      swapper.swap(vec1, 0, vec2, 3);
      assertEquals(53, vec1.get(0));
      assertTrue(vec2.isNull(3));

      swapper.swap(vec1, 17, vec2, 15);
      assertTrue(vec1.isNull(17));
      assertEquals(17, vec2.get(15));

      swapper.swap(vec1, 20, vec2, 25);
      assertTrue(vec1.isNull(20));
      assertTrue(vec2.isNull(25));

      swapper.swap(vec1, 23, vec2, 27);
      assertEquals(77, vec1.get(23));
      assertEquals(23, vec2.get(27));
    }
  }

  @Test
  public void testLongSwap() {
    try (VectorDataSwapper<BigIntVector> swapper = new VectorDataSwapper<>(BigIntVector.TYPE_WIDTH, allocator);
         BigIntVector vec1 = new BigIntVector("vec1", allocator);
         BigIntVector vec2 = new BigIntVector("vec2", allocator)) {
      vec1.allocateNew(VECTOR_LENGTH);
      vec2.allocateNew(VECTOR_LENGTH);

      // prepare data
      for (int i = 0; i < VECTOR_LENGTH; i++) {
        if (i % 10 == 0) {
          vec1.setNull(i);
        } else {
          vec1.set(i, (long) i);
        }
      }
      for (int i = 0; i < VECTOR_LENGTH; i++) {
        if (i % 5 == 0) {
          vec2.setNull(i);
        } else {
          vec2.set(i, (long) (i + 50));
        }
      }

      // swap
      swapper.swap(vec1, 0, vec2, 3);
      assertEquals(53L, vec1.get(0));
      assertTrue(vec2.isNull(3));

      swapper.swap(vec1, 17, vec2, 15);
      assertTrue(vec1.isNull(17));
      assertEquals(17L, vec2.get(15));

      swapper.swap(vec1, 20, vec2, 25);
      assertTrue(vec1.isNull(20));
      assertTrue(vec2.isNull(25));

      swapper.swap(vec1, 23, vec2, 27);
      assertEquals(77L, vec1.get(23));
      assertEquals(23L, vec2.get(27));
    }
  }

  @Test
  public void testFloatSwap() {
    try (VectorDataSwapper<Float4Vector> swapper = new VectorDataSwapper<>(Float4Vector.TYPE_WIDTH, allocator);
         Float4Vector vec1 = new Float4Vector("vec1", allocator);
         Float4Vector vec2 = new Float4Vector("vec2", allocator)) {
      vec1.allocateNew(VECTOR_LENGTH);
      vec2.allocateNew(VECTOR_LENGTH);

      // prepare data
      for (int i = 0; i < VECTOR_LENGTH; i++) {
        if (i % 10 == 0) {
          vec1.setNull(i);
        } else {
          vec1.set(i, i * 1.0f);
        }
      }
      for (int i = 0; i < VECTOR_LENGTH; i++) {
        if (i % 5 == 0) {
          vec2.setNull(i);
        } else {
          vec2.set(i, (i + 50) * 1.0f);
        }
      }

      // swap
      swapper.swap(vec1, 0, vec2, 3);
      assertEquals(53f, vec1.get(0), 0);
      assertTrue(vec2.isNull(3));

      swapper.swap(vec1, 17, vec2, 15);
      assertTrue(vec1.isNull(17));
      assertEquals(17f, vec2.get(15), 0);

      swapper.swap(vec1, 20, vec2, 25);
      assertTrue(vec1.isNull(20));
      assertTrue(vec2.isNull(25));

      swapper.swap(vec1, 23, vec2, 27);
      assertEquals(77f, vec1.get(23), 0);
      assertEquals(23f, vec2.get(27), 0);
    }
  }

  @Test
  public void testDoubleSwap() {
    try (VectorDataSwapper<Float8Vector> swapper = new VectorDataSwapper<>(Float8Vector.TYPE_WIDTH, allocator);
         Float8Vector vec1 = new Float8Vector("vec1", allocator);
         Float8Vector vec2 = new Float8Vector("vec2", allocator)) {
      vec1.allocateNew(VECTOR_LENGTH);
      vec2.allocateNew(VECTOR_LENGTH);

      // prepare data
      for (int i = 0; i < VECTOR_LENGTH; i++) {
        if (i % 10 == 0) {
          vec1.setNull(i);
        } else {
          vec1.set(i, i * 1.0);
        }
      }
      for (int i = 0; i < VECTOR_LENGTH; i++) {
        if (i % 5 == 0) {
          vec2.setNull(i);
        } else {
          vec2.set(i, (i + 50) * 1.0);
        }
      }

      // swap
      swapper.swap(vec1, 0, vec2, 3);
      assertEquals(53.0, vec1.get(0), 0);
      assertTrue(vec2.isNull(3));

      swapper.swap(vec1, 17, vec2, 15);
      assertTrue(vec1.isNull(17));
      assertEquals(17.0, vec2.get(15), 0);

      swapper.swap(vec1, 20, vec2, 25);
      assertTrue(vec1.isNull(20));
      assertTrue(vec2.isNull(25));

      swapper.swap(vec1, 23, vec2, 27);
      assertEquals(77.0, vec1.get(23), 0);
      assertEquals(23.0, vec2.get(27), 0);
    }
  }
}
