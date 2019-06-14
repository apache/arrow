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

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.stream.IntStream;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VarCharVector;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * Test cases for sorting vectors.
 */
public class TestVectorSort {

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

      int[] indices = IntStream.range(0, 10).toArray();
      indexSorter.sort(indices, intComparator);

      int[] expected = new int[] {6, 9, 1, 3, 0, 4, 5, 7, 2, 8};

      assertArrayEquals(expected, indices);
    }
  }

  @Test
  public void testSortInt() {
    try (IntVector vec = new IntVector("", allocator)) {
      vec.allocateNew(10);
      vec.setValueCount(10);

      // fill data to sort
      vec.set(0, 10);
      vec.set(1, 8);
      vec.setNull(2);
      vec.set(3, 10);
      vec.set(4, 12);
      vec.set(5, 17);
      vec.setNull(6);
      vec.set(7, 23);
      vec.set(8, 35);
      vec.set(9, 2);

      // sort the vector
      FixedWidthOutOfPlaceVectorSorter sorter = new FixedWidthOutOfPlaceVectorSorter();
      DefaultVectorComparators.IntComparator comparator = new DefaultVectorComparators.IntComparator();

      IntVector sortedVec = (IntVector) sorter.sort(vec, comparator);

      // verify results
      Assert.assertEquals(vec.getValueCount(), sortedVec.getValueCount());

      assertTrue(sortedVec.isNull(0));
      assertTrue(sortedVec.isNull(1));
      Assert.assertEquals(2, sortedVec.get(2));
      Assert.assertEquals(8, sortedVec.get(3));
      Assert.assertEquals(10, sortedVec.get(4));
      Assert.assertEquals(10, sortedVec.get(5));
      Assert.assertEquals(12, sortedVec.get(6));
      Assert.assertEquals(17, sortedVec.get(7));
      Assert.assertEquals(23, sortedVec.get(8));
      Assert.assertEquals(35, sortedVec.get(9));

      sortedVec.close();
    }
  }

  @Test
  public void testSortString() {
    try (VarCharVector vec = new VarCharVector("", allocator)) {
      vec.allocateNew(100, 10);
      vec.setValueCount(10);

      // fill data to sort
      vec.set(0, "hello".getBytes());
      vec.set(1, "abc".getBytes());
      vec.setNull(2);
      vec.set(3, "world".getBytes());
      vec.set(4, "12".getBytes());
      vec.set(5, "dictionary".getBytes());
      vec.setNull(6);
      vec.set(7, "hello".getBytes());
      vec.set(8, "good".getBytes());
      vec.set(9, "yes".getBytes());

      // sort the vector
      VariableWidthOutOfPlaceVectorSorter sorter = new VariableWidthOutOfPlaceVectorSorter();
      DefaultVectorComparators.VarCharComparator comparator = new DefaultVectorComparators.VarCharComparator();

      VarCharVector sortedVec = (VarCharVector) sorter.sort(vec, comparator);

      // verify results
      Assert.assertEquals(vec.getValueCount(), sortedVec.getValueCount());
      Assert.assertEquals(vec.getByteCapacity(), sortedVec.getByteCapacity());
      Assert.assertEquals(vec.getLastSet(), sortedVec.getLastSet());

      assertTrue(sortedVec.isNull(0));
      assertTrue(sortedVec.isNull(1));
      assertEquals("12", new String(sortedVec.get(2)));
      assertEquals("abc", new String(sortedVec.get(3)));
      assertEquals("dictionary", new String(sortedVec.get(4)));
      assertEquals("good", new String(sortedVec.get(5)));
      assertEquals("hello", new String(sortedVec.get(6)));
      assertEquals("hello", new String(sortedVec.get(7)));
      assertEquals("world", new String(sortedVec.get(8)));
      assertEquals("yes", new String(sortedVec.get(9)));

      sortedVec.close();
    }
  }
}
