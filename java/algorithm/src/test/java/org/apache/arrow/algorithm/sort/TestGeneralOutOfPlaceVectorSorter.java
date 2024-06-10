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

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.complex.StructVector;
import org.apache.arrow.vector.testing.ValueVectorDataPopulator;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/** Test cases for {@link GeneralOutOfPlaceVectorSorter}. */
public class TestGeneralOutOfPlaceVectorSorter {

  private BufferAllocator allocator;

  @Before
  public void prepare() {
    allocator = new RootAllocator(1024 * 1024);
  }

  @After
  public void shutdown() {
    allocator.close();
  }

  VectorValueComparator<StructVector> getComparator(StructVector structVector) {
    IntVector child0 = structVector.getChild("column0", IntVector.class);
    VectorValueComparator<IntVector> childComp0 =
        DefaultVectorComparators.createDefaultComparator(child0);
    childComp0.attachVector(child0);

    IntVector child1 = structVector.getChild("column1", IntVector.class);
    VectorValueComparator<IntVector> childComp1 =
        DefaultVectorComparators.createDefaultComparator(child1);
    childComp1.attachVector(child1);

    VectorValueComparator<StructVector> comp =
        new VectorValueComparator<StructVector>() {

          @Override
          public int compareNotNull(int index1, int index2) {
            // compare values by lexicographic order
            int result0 = childComp0.compare(index1, index2);
            if (result0 != 0) {
              return result0;
            }
            return childComp1.compare(index1, index2);
          }

          @Override
          public VectorValueComparator createNew() {
            return this;
          }
        };

    return comp;
  }

  @Test
  public void testSortStructVector() {
    final int vectorLength = 7;
    try (StructVector srcVector = StructVector.empty("src struct", allocator);
        StructVector dstVector = StructVector.empty("dst struct", allocator)) {

      IntVector srcChild0 =
          srcVector.addOrGet(
              "column0", FieldType.nullable(new ArrowType.Int(32, true)), IntVector.class);
      IntVector srcChild1 =
          srcVector.addOrGet(
              "column1", FieldType.nullable(new ArrowType.Int(32, true)), IntVector.class);

      IntVector dstChild0 =
          dstVector.addOrGet(
              "column0", FieldType.nullable(new ArrowType.Int(32, true)), IntVector.class);
      IntVector dstChild1 =
          dstVector.addOrGet(
              "column1", FieldType.nullable(new ArrowType.Int(32, true)), IntVector.class);

      // src struct vector values:
      // [
      //   (2, 1)
      //   (3, 4)
      //   (5, 4)
      //   (null, 3)
      //   (7, null)
      //   (null, null)
      //   (6, 6)
      // ]

      ValueVectorDataPopulator.setVector(srcChild0, 2, 3, 5, null, 7, null, 6);
      ValueVectorDataPopulator.setVector(srcChild1, 1, 4, 4, 3, null, null, 6);
      srcVector.setIndexDefined(0);
      srcVector.setIndexDefined(1);
      srcVector.setIndexDefined(2);
      srcVector.setIndexDefined(3);
      srcVector.setIndexDefined(4);
      srcVector.setIndexDefined(6);
      srcVector.setValueCount(vectorLength);

      dstChild0.allocateNew(vectorLength);
      dstChild1.allocateNew(vectorLength);
      dstVector.setValueCount(vectorLength);

      // construct the comparator
      VectorValueComparator<StructVector> comp = getComparator(srcVector);

      // sort the vector
      GeneralOutOfPlaceVectorSorter<StructVector> sorter = new GeneralOutOfPlaceVectorSorter<>();
      sorter.sortOutOfPlace(srcVector, dstVector, comp);

      // validate results
      assertEquals(vectorLength, dstVector.getValueCount());
      assertEquals(
          "["
              + "null, "
              + "{\"column1\":3}, "
              + "{\"column0\":2,\"column1\":1}, "
              + "{\"column0\":3,\"column1\":4}, "
              + "{\"column0\":5,\"column1\":4}, "
              + "{\"column0\":6,\"column1\":6}, "
              + "{\"column0\":7}"
              + "]",
          dstVector.toString());
    }
  }
}
