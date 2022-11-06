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

package org.apache.arrow.vector.table;

import static org.apache.arrow.vector.table.TestUtils.INT_VECTOR_NAME_1;
import static org.apache.arrow.vector.table.TestUtils.twoIntColumns;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.HashSet;
import java.util.Iterator;
import java.util.List;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class TableTest {

  private final ArrowType intArrowType = new ArrowType.Int(32, true);
  private final FieldType intFieldType = new FieldType(true, intArrowType, null);

  private BufferAllocator allocator;

  @BeforeEach
  public void init() {
    allocator = new RootAllocator(Long.MAX_VALUE);
  }

  @Test
  void of() {
    List<FieldVector> vectorList = twoIntColumns(allocator);
    try (Table t = Table.of(vectorList.toArray(new FieldVector[2]))) {
      Row c = t.immutableRow();
      assertEquals(2, t.getRowCount());
      assertEquals(2, t.getVectorCount());
      IntVector intVector1 = (IntVector) vectorList.get(0);
      assertEquals(INT_VECTOR_NAME_1, intVector1.getName());
      c.setPosition(0);

      // Now test changes to the first vector
      // first Table value is 1
      assertEquals(1, c.getInt(INT_VECTOR_NAME_1));

      // original vector is updated to set first value to 44
      intVector1.setSafe(0, 44);
      assertEquals(44, intVector1.get(0));

      // first Table value is still 1 for the zeroth vector
      assertEquals(1, c.getInt(0));
    }
  }

  @Test
  void constructor() {
    List<FieldVector> vectorList = twoIntColumns(allocator);
    try (Table t = new Table(vectorList, 2)) {
      assertEquals(2, t.getRowCount());
      assertEquals(2, t.getVectorCount());
      Row c = t.immutableRow();
      IntVector intVector1 = (IntVector) vectorList.get(0);
      c.setPosition(0);

      // Now test changes to the first vector
      // first Table value is 1
      assertEquals(1, c.getInt(INT_VECTOR_NAME_1));

      // original vector is updated to set first value to 44
      intVector1.setSafe(0, 44);
      assertEquals(44, intVector1.get(0));
      assertEquals(44, ((IntVector) vectorList.get(0)).get(0));

      // first Table value is still 1 for the zeroth vector
      assertEquals(1, c.getInt(INT_VECTOR_NAME_1));
    }
  }

  /**
   * Tests construction with an iterable that's not a list (there is a specialty constructor for Lists).
   */
  @Test
  void constructor2() {
    List<FieldVector> vectorList = twoIntColumns(allocator);
    Iterable<FieldVector> iterable = new HashSet<>(vectorList);
    try (Table t = new Table(iterable)) {
      assertEquals(2, t.getRowCount());
      assertEquals(2, t.getVectorCount());
      Row c = t.immutableRow();
      IntVector intVector1 = (IntVector) vectorList.get(0);
      c.setPosition(0);

      // Now test changes to the first vector
      // first Table value is 1
      assertEquals(1, c.getInt(INT_VECTOR_NAME_1));

      // original vector is updated to set first value to 44
      intVector1.setSafe(0, 44);
      assertEquals(44, intVector1.get(0));
      assertEquals(44, ((IntVector) vectorList.get(0)).get(0));

      // first Table value is still 1 for the zeroth vector
      assertEquals(1, c.getInt(INT_VECTOR_NAME_1));
    }
  }

  @Test
  void copy() {
    List<FieldVector> vectorList = twoIntColumns(allocator);
    try (Table t = new Table(vectorList)) {
      assertEquals(2, t.getVectorCount());
      try (Table copy = t.copy()) {
        for (FieldVector v: t.fieldVectors) {
          FieldVector vCopy = copy.getVector(v.getName());
          assertNotNull(vCopy);
          assertEquals(v.getValueCount(), vCopy.getValueCount());
          for (int i = 0; i < v.getValueCount(); i++) {
            Integer vValue = ((IntVector) v).getObject(i);
            Integer vCopyValue = ((IntVector) vCopy).getObject(i);
            assertEquals(vValue, vCopyValue);
          }
        }
      }
    }
  }

  @Test
  void addVector() {
    List<FieldVector> vectorList = twoIntColumns(allocator);
    try (Table t = new Table(vectorList)) {
      IntVector v3 = new IntVector("3", intFieldType, allocator);
      Table t2 = t.addVector(2, v3);
      assertEquals(3, t2.fieldVectors.size());
      assertTrue(t2.getVector("3").isNull(0));
      assertTrue(t2.getVector("3").isNull(1));
      t2.close();
    }
  }

  @Test
  void removeVector() {
    List<FieldVector> vectorList = twoIntColumns(allocator);
    IntVector v2 = (IntVector) vectorList.get(1);
    int val1 = v2.get(0);
    int val2 = v2.get(1);
    try (Table t = new Table(vectorList)) {

      Table t2 = t.removeVector(0);
      assertEquals(1, t2.fieldVectors.size());
      assertEquals(val1, ((IntVector) t2.getVector(0)).get(0));
      assertEquals(val2, ((IntVector) t2.getVector(0)).get(1));
    }
  }

  /** Tests table iterator in enhanced for loop. */
  @Test
  void iterator1() {
    List<FieldVector> vectorList = twoIntColumns(allocator);
    try (Table t = new Table(vectorList)) {
      Iterator<Row> iterator = t.iterator();
      assertNotNull(iterator);
      assertTrue(iterator.hasNext());
      int sum = 0;
      for (Row row : t) {
        sum += row.getInt(0);
      }
      assertEquals(3, sum);
    }
  }

  /** Tests explicit iterator. */
  @SuppressWarnings("WhileLoopReplaceableByForEach")
  @Test
  void iterator2() {
    List<FieldVector> vectorList = twoIntColumns(allocator);
    try (Table t = new Table(vectorList)) {
      Iterator<Row> iterator = t.iterator();
      assertNotNull(iterator);
      assertTrue(iterator.hasNext());
      int sum = 0;
      Iterator<Row> it = t.iterator();
      while (it.hasNext()) {
        Row row = it.next();
        sum += row.getInt(0);
      }
      assertEquals(3, sum);
    }
  }

  /**
   * Tests a slice operation where no length is provided, so the range extends to the end of the
   * table.
   */
  @Test
  void sliceToEnd() {
    List<FieldVector> vectorList = twoIntColumns(allocator);
    try (Table t = new Table(vectorList)) {
      Table slice = t.slice(1);
      assertEquals(1, slice.rowCount);
      assertEquals(2, t.rowCount); // memory is copied for slice, not transferred
      slice.close();
    }
  }

  /** Tests a slice operation with a given length parameter. */
  @Test
  void sliceRange() {
    List<FieldVector> vectorList = twoIntColumns(allocator);
    try (Table t = new Table(vectorList)) {
      Table slice = t.slice(1, 1);
      assertEquals(1, slice.rowCount);
      assertEquals(2, t.rowCount); // memory is copied for slice, not transferred
      slice.close();
    }
  }

  /**
   * Tests creation of a table from a VectorSchemaRoot.
   *
   * <p>Also tests that updates to the source Vectors do not impact the values in the Table
   */
  @Test
  void constructFromVsr() {
    List<FieldVector> vectorList = twoIntColumns(allocator);
    try (VectorSchemaRoot vsr = new VectorSchemaRoot(vectorList)) {
      Table t = new Table(vsr);
      Row c = t.immutableRow();
      assertEquals(2, t.rowCount);
      assertEquals(0, vsr.getRowCount()); // memory is copied for slice, not transferred
      IntVector intVector1 = (IntVector) vectorList.get(0);
      c.setPosition(0);

      // Now test changes to the first vector
      // first Table value is 1
      assertEquals(1, c.getInt(INT_VECTOR_NAME_1));

      // original vector is updated to set first value to 44
      intVector1.setSafe(0, 44);
      assertEquals(44, intVector1.get(0));
      assertEquals(44, ((IntVector) vsr.getVector(0)).get(0));

      // first Table value is still 1 for the zeroth vector
      assertEquals(1, c.getInt(INT_VECTOR_NAME_1));

      // TEST FIELDS //
      Schema schema = t.schema;
      Field f1 = t.getField(INT_VECTOR_NAME_1);
      FieldVector fv1 = vectorList.get(0);
      assertEquals(f1, fv1.getField());
      assertEquals(f1, schema.findField(INT_VECTOR_NAME_1));
      t.close();
    }
  }
}
