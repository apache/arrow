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

import static org.junit.Assert.assertTrue;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.IntVector;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * Test cases for {@link FixedWidthOutOfPlaceVectorSorter}.
 */
public class TestFixedWidthOutOfPlaceVectorSorter {

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

      IntVector sortedVec = (IntVector) vec.getField().getFieldType().createNewSingleVector("", allocator, null);
      sortedVec.allocateNew(vec.getValueCount());
      sortedVec.setValueCount(vec.getValueCount());

      sorter.sortOutOfPlace(vec, sortedVec, comparator);

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
}
