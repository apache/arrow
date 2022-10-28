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

import static org.apache.arrow.vector.table.TestUtils.*;
import static org.apache.arrow.vector.table.TestUtils.INT_VECTOR_NAME_1;
import static org.junit.jupiter.api.Assertions.*;

import java.util.ArrayList;
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
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class MutableTableTest {

  private static final String INT_VECTOR_NAME = "intCol";

  private final ArrowType intArrowType = new ArrowType.Int(32, true);
  private final FieldType intFieldType = new FieldType(true, intArrowType, null);

  private BufferAllocator allocator;
  private Schema schema1;

  @BeforeEach
  public void init() {
    allocator = new RootAllocator(Long.MAX_VALUE);
    List<Field> fieldList = new ArrayList<>();
    fieldList.add(new Field(INT_VECTOR_NAME, intFieldType, null));
    schema1 = new Schema(fieldList);
  }

  @AfterEach
  public void terminate() {
    allocator.close();
  }

  @Test
  void create() {
    MutableTable t = MutableTable.create(schema1, allocator);
    assertNotNull(t);
    assertEquals(0, t.getRowCount());
  }

  @Test
  void of() {
    List<FieldVector> vectorList = twoIntColumns(allocator);
    try (MutableTable t = MutableTable.of(vectorList.toArray(new FieldVector[2]))) {
      assertEquals(2, t.getRowCount());
      assertEquals(2, t.getVectorCount());
    }
  }

  @Test
  void constructor() {
    List<FieldVector> vectorList = twoIntColumns(allocator);
    try (MutableTable t = new MutableTable(vectorList, 2)) {
      assertEquals(2, t.getRowCount());
      assertEquals(2, t.getVectorCount());
    }
  }

  @Test
  void allocateNew() {
  }

  @Test
  void clear() {
    List<FieldVector> vectorList = twoIntColumns(allocator);
    try (MutableTable t = MutableTable.of(vectorList.toArray(new FieldVector[2]))) {
      assertEquals(2, t.getRowCount());
      assertEquals(2, t.getVectorCount());
      t.clear();
      assertEquals(0, t.getRowCount());
      assertEquals(0, t.getVector(0).getValueCount());
    }
  }

  @Test
  void compact() {
    List<FieldVector> vectorList = twoIntColumns(allocator);
    try (MutableTable t = new MutableTable(vectorList)) {
      MutableRow c = t.mutableRow();
      c.setPosition(0);
      assertFalse(c.isRowDeleted());
      c.deleteCurrentRow();
      assertTrue(c.isRowDeleted());
      assertTrue(t.isRowDeleted(0));
      t.compact();
      assertEquals(0, t.deletedRowCount());
      assertEquals(1, t.rowCount);
    }
  }

  @Test
  void compactTableWithNoDeletedRows() {
    List<FieldVector> vectorList = twoIntColumns(allocator);
    try (MutableTable t = MutableTable.of(vectorList.toArray(new FieldVector[2]))) {
      int initialRowCount = t.rowCount;
      int initialDeletedRowCount = t.deletedRowCount();
      t.mutableRow().compact();
      assertEquals(initialRowCount, t.rowCount);
      assertEquals(initialDeletedRowCount, t.deletedRowCount());
    }
  }

  @Test
  void setGetRowCount() {
  }

  @Test
  void markRowDeleted() {
    try (MutableTable t = MutableTable.create(schema1, allocator)) {
      t.allocateNew();
      IntVector v = (IntVector) t.getVector(0);
      v.set(0, 1);
      v.set(1, 2);
      v.set(2, 3);
      t.setRowCount(3);
      MutableRow c = t.mutableRow();
      c.setPosition(1).deleteCurrentRow();
      assertTrue(t.isRowDeleted(1));

      c.setPosition(-1);
      List<Integer> values = new ArrayList<>();
      while (c.hasNext()) {
        c.next();
        values.add(c.getInt(0));
      }
      assertEquals(2, values.size());
      assertTrue(values.contains(1));
      assertTrue(values.contains(3));
    }
  }

  @Test
  void getVectorByName() {
    MutableTable t = MutableTable.create(schema1, allocator);
    FieldVector v = t.getVector(INT_VECTOR_NAME);
    assertEquals(INT_VECTOR_NAME, v.getName());
  }

  @Test
  void getVectorByPosition() {
    MutableTable t = MutableTable.create(schema1, allocator);
    FieldVector v = t.getVector(0);
    assertEquals(INT_VECTOR_NAME, v.getName());
  }

  @Test
  void addVector() {
    List<FieldVector> vectorList = twoIntColumns(allocator);
    try (MutableTable t = new MutableTable(vectorList)) {
      IntVector v3 = new IntVector("3", intFieldType, allocator);
      MutableTable t2 = t.addVector(2, v3);
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

    try (MutableTable t = new MutableTable(vectorList)) {
      MutableTable t2 = t.removeVector(0);
      assertEquals(1, t2.fieldVectors.size());
      assertEquals(val1, ((IntVector) t2.getVector(0)).get(0));
      assertEquals(val2, ((IntVector) t2.getVector(0)).get(1));
      t2.close();
    }
  }

  @Test
  void toMutableTable() {
    List<FieldVector> vectorList = twoIntColumns(allocator);
    try (MutableTable t = new MutableTable(vectorList)) {
      assertEquals(t, t.toMutableTable());
    }
  }

  @Test
  void toImmutableTable() {
    Table table = null;
    List<FieldVector> vectorList = twoIntColumns(allocator);
    try (MutableTable t = new MutableTable(vectorList)) {
      assertNotNull(t.getVector(INT_VECTOR_NAME_1));
      assertNotNull(t.getVector(INT_VECTOR_NAME_2));
      table = t.toImmutableTable();
      assertNotNull(table.getVector(INT_VECTOR_NAME_1));
      assertNotNull(table.getVector(INT_VECTOR_NAME_2));
      assertEquals(t.getSchema().findField(INT_VECTOR_NAME_1), table.getSchema().findField(INT_VECTOR_NAME_1));
      assertEquals(0, t.rowCount);
    } finally {
      assertNotNull(table);
      table.close();
    }
  }

  /**
   * Tests creation of a table from a vectorSchemaRoot.
   */
  @Test
  void constructFromVsr() {
    List<FieldVector> vectorList = twoIntColumns(allocator);
    try (VectorSchemaRoot vsr = new VectorSchemaRoot(vectorList)) {
      MutableTable table = new MutableTable(vsr);
      assertEquals(2, table.rowCount);
      assertEquals(0, vsr.getRowCount()); // memory is copied for slice, not transferred
      table.close();
    }
  }

  /**
   * Tests creation of a table from multiple vectorSchemaRoots.
   */
  @Test
  void concatenateVsr() {
    List<FieldVector> vectorList1 = twoIntColumns(allocator);
    List<FieldVector> vectorList2 = twoIntColumns(allocator);
    try (VectorSchemaRoot vsr = new VectorSchemaRoot(vectorList1);
         VectorSchemaRoot vsr2 = new VectorSchemaRoot(vectorList2)) {

      List<VectorSchemaRoot> roots = new ArrayList<>();
      roots.add(vsr);
      roots.add(vsr2);
      MutableTable table = MutableTable.concatenate(allocator, roots);
      assertEquals(4, table.rowCount);
      table.close();
    }
  }

  @Test
  void iterator() {
    try (MutableTable t = MutableTable.create(schema1, allocator)) {
      t.allocateNew();
      IntVector v = (IntVector) t.getVector(0);
      v.set(0, 1);
      v.set(1, 2);
      v.set(2, 3);
      t.setRowCount(3);
      List<Integer> values = new ArrayList<>();
      for (MutableRow r : t) {
        values.add(r.getInt(INT_VECTOR_NAME));
      }
      assertEquals(3, values.size());
      List<Integer> intList = new ArrayList<>();
      intList.add(1);
      intList.add(2);
      intList.add(3);
      assertTrue(values.containsAll(intList));
    }
  }
}
