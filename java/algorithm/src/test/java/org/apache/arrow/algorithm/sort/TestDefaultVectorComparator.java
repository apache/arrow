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

import static org.apache.arrow.vector.complex.BaseRepeatedValueVector.OFFSET_WIDTH;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.pojo.FieldType;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * Test cases for {@link DefaultVectorComparators}.
 */
public class TestDefaultVectorComparator {

  private BufferAllocator allocator;

  @Before
  public void prepare() {
    allocator = new RootAllocator(1024 * 1024);
  }

  @After
  public void shutdown() {
    allocator.close();
  }

  private ListVector createListVector(int count) {
    ListVector listVector = ListVector.empty("list vector", allocator);
    Types.MinorType type = Types.MinorType.INT;
    listVector.addOrGetVector(FieldType.nullable(type.getType()));
    listVector.allocateNew();

    IntVector dataVector = (IntVector) listVector.getDataVector();

    for (int i = 0; i < count; i++) {
      dataVector.set(i, i);
    }
    dataVector.setValueCount(count);

    listVector.setNotNull(0);

    listVector.getOffsetBuffer().setInt(0, 0);
    listVector.getOffsetBuffer().setInt(OFFSET_WIDTH, count);

    listVector.setLastSet(0);
    listVector.setValueCount(1);

    return listVector;
  }

  @Test
  public void testCompareLists() {
    try (ListVector listVector1 = createListVector(10);
         ListVector listVector2 = createListVector(11)) {
      VectorValueComparator<ListVector> comparator =
              DefaultVectorComparators.createDefaultComparator(listVector1);
      comparator.attachVectors(listVector1, listVector2);

      // prefix is smaller
      assertTrue(comparator.compare(0, 0) < 0);
    }

    try (ListVector listVector1 = createListVector(11);
         ListVector listVector2 = createListVector(11)) {
      ((IntVector) listVector2.getDataVector()).set(10, 110);

      VectorValueComparator<ListVector> comparator =
              DefaultVectorComparators.createDefaultComparator(listVector1);
      comparator.attachVectors(listVector1, listVector2);

      // breaking tie by the last element
      assertTrue(comparator.compare(0, 0) < 0);
    }

    try (ListVector listVector1 = createListVector(10);
         ListVector listVector2 = createListVector(10)) {

      VectorValueComparator<ListVector> comparator =
              DefaultVectorComparators.createDefaultComparator(listVector1);
      comparator.attachVectors(listVector1, listVector2);

      // list vector elements equal
      assertTrue(comparator.compare(0, 0) == 0);
    }
  }
}
