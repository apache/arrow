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
import org.apache.arrow.vector.VarCharVector;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * Test cases for {@link VariableWidthOutOfPlaceVectorSorter}.
 */
public class TestVariableWidthOutOfPlaceVectorSorter {

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

      VarCharVector sortedVec =
              (VarCharVector) vec.getField().getFieldType().createNewSingleVector("", allocator, null);
      sortedVec.allocateNew(vec.getByteCapacity(), vec.getValueCount());
      sortedVec.setLastSet(vec.getValueCount() - 1);
      sortedVec.setValueCount(vec.getValueCount());

      sorter.sortOutOfPlace(vec, sortedVec, comparator);

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
