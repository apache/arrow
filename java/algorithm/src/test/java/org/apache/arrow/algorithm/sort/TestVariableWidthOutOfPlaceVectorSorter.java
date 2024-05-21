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

import java.nio.charset.StandardCharsets;
import java.util.Objects;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.BaseVariableWidthVector;
import org.apache.arrow.vector.VarCharVector;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * Test cases for {@link VariableWidthOutOfPlaceVectorSorter}.
 */
public class TestVariableWidthOutOfPlaceVectorSorter extends TestOutOfPlaceVectorSorter {

  private BufferAllocator allocator;

  public TestVariableWidthOutOfPlaceVectorSorter(boolean generalSorter) {
    super(generalSorter);
  }

  <V extends BaseVariableWidthVector> OutOfPlaceVectorSorter<V> getSorter() {
    return generalSorter ? new GeneralOutOfPlaceVectorSorter<>() : new VariableWidthOutOfPlaceVectorSorter<V>();
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
  public void testSortString() {
    try (VarCharVector vec = new VarCharVector("", allocator)) {
      vec.allocateNew(100, 10);
      vec.setValueCount(10);

      // fill data to sort
      vec.set(0, "hello".getBytes(StandardCharsets.UTF_8));
      vec.set(1, "abc".getBytes(StandardCharsets.UTF_8));
      vec.setNull(2);
      vec.set(3, "world".getBytes(StandardCharsets.UTF_8));
      vec.set(4, "12".getBytes(StandardCharsets.UTF_8));
      vec.set(5, "dictionary".getBytes(StandardCharsets.UTF_8));
      vec.setNull(6);
      vec.set(7, "hello".getBytes(StandardCharsets.UTF_8));
      vec.set(8, "good".getBytes(StandardCharsets.UTF_8));
      vec.set(9, "yes".getBytes(StandardCharsets.UTF_8));

      // sort the vector
      OutOfPlaceVectorSorter<BaseVariableWidthVector> sorter = getSorter();
      VectorValueComparator<BaseVariableWidthVector> comparator =
              DefaultVectorComparators.createDefaultComparator(vec);

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
      assertEquals("12", new String(Objects.requireNonNull(sortedVec.get(2)), StandardCharsets.UTF_8));
      assertEquals("abc", new String(Objects.requireNonNull(sortedVec.get(3)), StandardCharsets.UTF_8));
      assertEquals("dictionary", new String(Objects.requireNonNull(sortedVec.get(4)), StandardCharsets.UTF_8));
      assertEquals("good", new String(Objects.requireNonNull(sortedVec.get(5)), StandardCharsets.UTF_8));
      assertEquals("hello", new String(Objects.requireNonNull(sortedVec.get(6)), StandardCharsets.UTF_8));
      assertEquals("hello", new String(Objects.requireNonNull(sortedVec.get(7)), StandardCharsets.UTF_8));
      assertEquals("world", new String(Objects.requireNonNull(sortedVec.get(8)), StandardCharsets.UTF_8));
      assertEquals("yes", new String(Objects.requireNonNull(sortedVec.get(9)), StandardCharsets.UTF_8));

      sortedVec.close();
    }
  }
}
