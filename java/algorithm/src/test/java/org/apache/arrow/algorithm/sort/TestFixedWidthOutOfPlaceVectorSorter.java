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

import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.Float4Vector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.SmallIntVector;
import org.apache.arrow.vector.TinyIntVector;
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
  public void testSortByte() {
    try (TinyIntVector vec = new TinyIntVector("", allocator)) {
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
      DefaultVectorComparators.ByteComparator comparator = new DefaultVectorComparators.ByteComparator();

      TinyIntVector sortedVec =
              (TinyIntVector) vec.getField().getFieldType().createNewSingleVector("", allocator, null);
      sortedVec.allocateNew(vec.getValueCount());
      sortedVec.setValueCount(vec.getValueCount());

      sorter.sortOutOfPlace(vec, sortedVec, comparator);

      // verify results
      Assert.assertEquals(vec.getValueCount(), sortedVec.getValueCount());

      assertTrue(sortedVec.isNull(0));
      assertTrue(sortedVec.isNull(1));
      Assert.assertEquals((byte) 2, sortedVec.get(2));
      Assert.assertEquals((byte) 8, sortedVec.get(3));
      Assert.assertEquals((byte) 10, sortedVec.get(4));
      Assert.assertEquals((byte) 10, sortedVec.get(5));
      Assert.assertEquals((byte) 12, sortedVec.get(6));
      Assert.assertEquals((byte) 17, sortedVec.get(7));
      Assert.assertEquals((byte) 23, sortedVec.get(8));
      Assert.assertEquals((byte) 35, sortedVec.get(9));

      sortedVec.close();
    }
  }

  @Test
  public void testSortShort() {
    try (SmallIntVector vec = new SmallIntVector("", allocator)) {
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
      DefaultVectorComparators.ShortComparator comparator = new DefaultVectorComparators.ShortComparator();

      SmallIntVector sortedVec =
              (SmallIntVector) vec.getField().getFieldType().createNewSingleVector("", allocator, null);
      sortedVec.allocateNew(vec.getValueCount());
      sortedVec.setValueCount(vec.getValueCount());

      sorter.sortOutOfPlace(vec, sortedVec, comparator);

      // verify results
      Assert.assertEquals(vec.getValueCount(), sortedVec.getValueCount());

      assertTrue(sortedVec.isNull(0));
      assertTrue(sortedVec.isNull(1));
      Assert.assertEquals((short) 2, sortedVec.get(2));
      Assert.assertEquals((short) 8, sortedVec.get(3));
      Assert.assertEquals((short) 10, sortedVec.get(4));
      Assert.assertEquals((short) 10, sortedVec.get(5));
      Assert.assertEquals((short) 12, sortedVec.get(6));
      Assert.assertEquals((short) 17, sortedVec.get(7));
      Assert.assertEquals((short) 23, sortedVec.get(8));
      Assert.assertEquals((short) 35, sortedVec.get(9));

      sortedVec.close();
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

  @Test
  public void testSortLong() {
    try (BigIntVector vec = new BigIntVector("", allocator)) {
      vec.allocateNew(10);
      vec.setValueCount(10);

      // fill data to sort
      vec.set(0, 10L);
      vec.set(1, 8L);
      vec.setNull(2);
      vec.set(3, 10L);
      vec.set(4, 12L);
      vec.set(5, 17L);
      vec.setNull(6);
      vec.set(7, 23L);
      vec.set(8, 1L << 35L);
      vec.set(9, 2L);

      // sort the vector
      FixedWidthOutOfPlaceVectorSorter sorter = new FixedWidthOutOfPlaceVectorSorter();
      DefaultVectorComparators.LongComparator comparator = new DefaultVectorComparators.LongComparator();

      BigIntVector sortedVec = (BigIntVector) vec.getField().getFieldType().createNewSingleVector("", allocator, null);
      sortedVec.allocateNew(vec.getValueCount());
      sortedVec.setValueCount(vec.getValueCount());

      sorter.sortOutOfPlace(vec, sortedVec, comparator);

      // verify results
      Assert.assertEquals(vec.getValueCount(), sortedVec.getValueCount());

      assertTrue(sortedVec.isNull(0));
      assertTrue(sortedVec.isNull(1));
      Assert.assertEquals(2L, sortedVec.get(2));
      Assert.assertEquals(8L, sortedVec.get(3));
      Assert.assertEquals(10L, sortedVec.get(4));
      Assert.assertEquals(10L, sortedVec.get(5));
      Assert.assertEquals(12L, sortedVec.get(6));
      Assert.assertEquals(17L, sortedVec.get(7));
      Assert.assertEquals(23L, sortedVec.get(8));
      Assert.assertEquals(1L << 35L, sortedVec.get(9));

      sortedVec.close();
    }
  }

  @Test
  public void testSortFloat() {
    try (Float4Vector vec = new Float4Vector("", allocator)) {
      vec.allocateNew(10);
      vec.setValueCount(10);

      // fill data to sort
      vec.set(0, 10f);
      vec.set(1, 8f);
      vec.setNull(2);
      vec.set(3, 10f);
      vec.set(4, 12f);
      vec.set(5, 17f);
      vec.setNull(6);
      vec.set(7, 23f);
      vec.set(8, Float.NaN);
      vec.set(9, 2f);

      // sort the vector
      FixedWidthOutOfPlaceVectorSorter sorter = new FixedWidthOutOfPlaceVectorSorter();
      DefaultVectorComparators.Float4Comparator comparator = new DefaultVectorComparators.Float4Comparator();

      Float4Vector sortedVec = (Float4Vector) vec.getField().getFieldType().createNewSingleVector("", allocator, null);
      sortedVec.allocateNew(vec.getValueCount());
      sortedVec.setValueCount(vec.getValueCount());

      sorter.sortOutOfPlace(vec, sortedVec, comparator);

      // verify results
      Assert.assertEquals(vec.getValueCount(), sortedVec.getValueCount());

      assertTrue(sortedVec.isNull(0));
      assertTrue(sortedVec.isNull(1));
      Assert.assertEquals(2f, sortedVec.get(2), 0f);
      Assert.assertEquals(8f, sortedVec.get(3), 0f);
      Assert.assertEquals(10f, sortedVec.get(4), 0f);
      Assert.assertEquals(10f, sortedVec.get(5), 0f);
      Assert.assertEquals(12f, sortedVec.get(6), 0f);
      Assert.assertEquals(17f, sortedVec.get(7), 0f);
      Assert.assertEquals(23f, sortedVec.get(8), 0f);
      Assert.assertEquals(Float.NaN, sortedVec.get(9), 0f);

      sortedVec.close();
    }
  }

  @Test
  public void testSortDobule() {
    try (Float8Vector vec = new Float8Vector("", allocator)) {
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
      vec.set(7, Double.NaN);
      vec.set(8, 35);
      vec.set(9, 2);

      // sort the vector
      FixedWidthOutOfPlaceVectorSorter sorter = new FixedWidthOutOfPlaceVectorSorter();
      DefaultVectorComparators.Float8Comparator comparator = new DefaultVectorComparators.Float8Comparator();

      Float8Vector sortedVec = (Float8Vector) vec.getField().getFieldType().createNewSingleVector("", allocator, null);
      sortedVec.allocateNew(vec.getValueCount());
      sortedVec.setValueCount(vec.getValueCount());

      sorter.sortOutOfPlace(vec, sortedVec, comparator);

      // verify results
      Assert.assertEquals(vec.getValueCount(), sortedVec.getValueCount());

      assertTrue(sortedVec.isNull(0));
      assertTrue(sortedVec.isNull(1));
      Assert.assertEquals(2, sortedVec.get(2), 0);
      Assert.assertEquals(8, sortedVec.get(3), 0);
      Assert.assertEquals(10, sortedVec.get(4), 0);
      Assert.assertEquals(10, sortedVec.get(5), 0);
      Assert.assertEquals(12, sortedVec.get(6), 0);
      Assert.assertEquals(17, sortedVec.get(7), 0);
      Assert.assertEquals(35, sortedVec.get(8), 0);
      Assert.assertEquals(Double.NaN, sortedVec.get(9), 0);

      sortedVec.close();
    }
  }
}
