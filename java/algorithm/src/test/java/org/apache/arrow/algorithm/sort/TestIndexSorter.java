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

package org.apache.arrow.algorithm.sort;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.IntVector;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * Test cases for {@link IndexSorter}.
 */
public class TestIndexSorter {

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
  public void testIndexSort() {
    try (IntVector vec = new IntVector("", allocator)) {
      vec.allocateNew(10);
      vec.setValueCount(10);

      // fill data to sort
      vec.set(0, 11);
      vec.set(1, 8);
      vec.set(2, 33);
      vec.set(3, 10);
      vec.set(4, 12);
      vec.set(5, 17);
      vec.setNull(6);
      vec.set(7, 23);
      vec.set(8, 35);
      vec.set(9, 2);

      // sort the index
      IndexSorter<IntVector> indexSorter = new IndexSorter<>();
      DefaultVectorComparators.IntComparator intComparator = new DefaultVectorComparators.IntComparator();
      intComparator.attachVector(vec);

      IntVector indices = new IntVector("", allocator);
      indices.setValueCount(10);
      indexSorter.sort(vec, indices, intComparator);

      int[] expected = new int[]{6, 9, 1, 3, 0, 4, 5, 7, 2, 8};

      for (int i = 0; i < expected.length; i++) {
        assertTrue(!indices.isNull(i));
        assertEquals(expected[i], indices.get(i));
      }
      indices.close();
    }
  }
}
