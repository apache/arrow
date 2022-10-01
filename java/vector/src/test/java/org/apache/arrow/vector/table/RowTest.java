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

import static org.apache.arrow.vector.table.TestUtils.BIGINT_INT_MAP_VECTOR_NAME;
import static org.apache.arrow.vector.table.TestUtils.INT_LIST_VECTOR_NAME;
import static org.apache.arrow.vector.table.TestUtils.INT_VECTOR_NAME_1;
import static org.apache.arrow.vector.table.TestUtils.STRUCT_VECTOR_NAME;
import static org.apache.arrow.vector.table.TestUtils.UNION_VECTOR_NAME;
import static org.apache.arrow.vector.table.TestUtils.fixedWidthVectors;
import static org.apache.arrow.vector.table.TestUtils.simpleDenseUnionVector;
import static org.apache.arrow.vector.table.TestUtils.simpleListVector;
import static org.apache.arrow.vector.table.TestUtils.simpleMapVector;
import static org.apache.arrow.vector.table.TestUtils.simpleStructVector;
import static org.apache.arrow.vector.table.TestUtils.simpleUnionVector;
import static org.apache.arrow.vector.table.TestUtils.twoIntColumns;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.charset.StandardCharsets;
import java.util.List;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.complex.DenseUnionVector;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.complex.MapVector;
import org.apache.arrow.vector.complex.StructVector;
import org.apache.arrow.vector.complex.UnionVector;
import org.apache.arrow.vector.util.JsonStringHashMap;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class RowTest {

  private BufferAllocator allocator;

  @BeforeEach
  public void init() {
    allocator = new RootAllocator(Long.MAX_VALUE);
  }

  @AfterEach
  public void terminate() {
    allocator.close();
  }

  @Test
  void constructor() {
    List<FieldVector> vectorList = twoIntColumns(allocator);
    try (Table t = new Table(vectorList)) {
      Row c = t.immutableRow();
      assertEquals(StandardCharsets.UTF_8, c.getDefaultCharacterSet());
    }
  }

  @Test
  void at() {
    List<FieldVector> vectorList = twoIntColumns(allocator);
    try (Table t = new Table(vectorList)) {
      Row c = t.immutableRow();
      assertEquals(c.getRowNumber(), -1);
      c.setPosition(1);
      assertEquals(c.getRowNumber(), 1);
    }
  }

  @Test
  void getIntByVectorIndex() {
    List<FieldVector> vectorList = twoIntColumns(allocator);
    try (Table t = new Table(vectorList)) {
      Row c = t.immutableRow();
      c.setPosition(1);
      assertEquals(2, c.getInt(0));
    }
  }

  @Test
  void getIntByVectorName() {
    List<FieldVector> vectorList = twoIntColumns(allocator);
    try (Table t = new Table(vectorList)) {
      Row c = t.immutableRow();
      c.setPosition(1);
      assertEquals(2, c.getInt(INT_VECTOR_NAME_1));
    }
  }

  @Test
  void hasNext() {
    List<FieldVector> vectorList = twoIntColumns(allocator);
    try (Table t = new Table(vectorList)) {
      Row c = t.immutableRow();
      assertTrue(c.hasNext());
      c.setPosition(1);
      assertFalse(c.hasNext());
    }
  }

  @Test
  void next() {
    List<FieldVector> vectorList = twoIntColumns(allocator);
    try (Table t = new Table(vectorList)) {
      Row c = t.immutableRow();
      c.setPosition(0);
      c.next();
      assertEquals(1, c.getRowNumber());
    }
  }

  @Test
  void isNull() {
    List<FieldVector> vectorList = twoIntColumns(allocator);
    try (Table t = new Table(vectorList)) {
      Row c = t.immutableRow();
      c.setPosition(1);
      assertFalse(c.isNull(0));
    }
  }

  @Test
  void isNullByFieldName() {
    List<FieldVector> vectorList = twoIntColumns(allocator);
    try (Table t = new Table(vectorList)) {
      Row c = t.immutableRow();
      c.setPosition(1);
      assertFalse(c.isNull(INT_VECTOR_NAME_1));
    }
  }

  @Test
  void fixedWidthVectorTest() {
    List<FieldVector> vectorList = fixedWidthVectors(allocator, 2);
    try (Table t = new Table(vectorList)) {
      Row c = t.immutableRow();
      c.setPosition(1);
      assertFalse(c.isNull("bigInt_vector"));
      assertEquals(c.getInt("int_vector"), c.getInt(0));
      assertEquals(c.getBigInt("bigInt_vector"), c.getBigInt(1));
      assertEquals(c.getSmallInt("smallInt_vector"), c.getSmallInt(2));
      assertEquals(c.getTinyInt("tinyInt_vector"), c.getTinyInt(3));

      assertEquals(c.getUInt1("uInt1_vector"), c.getUInt1(4));
      assertEquals(c.getUInt2("uInt2_vector"), c.getUInt2(5));
      assertEquals(c.getUInt4("uInt4_vector"), c.getUInt4(6));
      assertEquals(c.getUInt8("uInt8_vector"), c.getUInt8(7));

      assertEquals(c.getFloat4("float4_vector"), c.getFloat4(8));
      assertEquals(c.getFloat8("float8_vector"), c.getFloat8(9));

      assertEquals(c.getTimeSec("timeSec_vector"), c.getTimeSec(10));
      assertEquals(c.getTimeMilli("timeMilli_vector"), c.getTimeMilli(11));
      assertEquals(c.getTimeMicro("timeMicro_vector"), c.getTimeMicro(12));
      assertEquals(c.getTimeNano("timeNano_vector"), c.getTimeNano(13));

      assertEquals(c.getTimeStampSec("timeStampSec_vector"), c.getTimeStampSec(14));
      assertEquals(c.getTimeStampMilli("timeStampMilli_vector"), c.getTimeStampMilli(15));
      assertEquals(c.getTimeStampMicro("timeStampMicro_vector"), c.getTimeStampMicro(16));
      assertEquals(c.getTimeStampNano("timeStampNano_vector"), c.getTimeStampNano(17));
    }
  }

  @Test
  void testSimpleListVector1() {
    try (ListVector listVector = simpleListVector(allocator);
        VectorSchemaRoot vectorSchemaRoot = VectorSchemaRoot.of(listVector);
        Table table = new Table(vectorSchemaRoot)) {
      for (Row c : table) {
        @SuppressWarnings("unchecked")
        List<Integer> list = (List<Integer>) c.getList(INT_LIST_VECTOR_NAME);
        assertEquals(10, list.size());
      }
    }
  }

  @Test
  void testSimpleListVector2() {
    try (ListVector listVector = simpleListVector(allocator);
        VectorSchemaRoot vectorSchemaRoot = VectorSchemaRoot.of(listVector);
        Table table = new Table(vectorSchemaRoot)) {
      for (Row c : table) {
        @SuppressWarnings("unchecked")
        List<Integer> list = (List<Integer>) c.getList(0);
        assertEquals(10, list.size());
      }
    }
  }

  @Test
  void testSimpleStructVector1() {
    try (StructVector structVector = simpleStructVector(allocator);
        VectorSchemaRoot vectorSchemaRoot = VectorSchemaRoot.of(structVector);
        Table table = new Table(vectorSchemaRoot)) {
      for (Row c : table) {
        @SuppressWarnings("unchecked")
        JsonStringHashMap<String, ?> struct =
            (JsonStringHashMap<String, ?>) c.getStruct(STRUCT_VECTOR_NAME);
        int a = (int) struct.get("struct_int_child");
        double b = (double) struct.get("struct_flt_child");
        assertNotNull(struct);
        assertTrue(a >= 0);
        assertTrue(b <= a, String.format("a = %s and b = %s", a, b));
      }
    }
  }

  @Test
  void testSimpleUnionVector() {
    try (UnionVector unionVector = simpleUnionVector(allocator);
        VectorSchemaRoot vsr = VectorSchemaRoot.of(unionVector);
        Table table = new Table(vsr)) {
      Row c = table.immutableRow();
      c.setPosition(0);
      Object object0 = c.getUnion(UNION_VECTOR_NAME);
      c.setPosition(1);
      assertNull(c.getUnion(UNION_VECTOR_NAME));
      c.setPosition(2);
      Object object2 = c.getUnion(UNION_VECTOR_NAME);
      assertEquals(100, object0);
      assertEquals(100, object2);
    }
  }

  @Test
  void testSimpleDenseUnionVector() {
    try (DenseUnionVector unionVector = simpleDenseUnionVector(allocator);
        VectorSchemaRoot vsr = VectorSchemaRoot.of(unionVector);
        Table table = new Table(vsr)) {
      Row c = table.immutableRow();
      c.setPosition(0);
      Object object0 = c.getDenseUnion(UNION_VECTOR_NAME);
      c.setPosition(1);
      assertNull(c.getDenseUnion(UNION_VECTOR_NAME));
      c.setPosition(2);
      Object object2 = c.getDenseUnion(UNION_VECTOR_NAME);
      assertEquals(100, object0);
      assertEquals(100, object2);
    }
  }

  @Test
  void testSimpleMapVector1() {
    try (MapVector mapVector = simpleMapVector(allocator);
        Table table = Table.of(mapVector)) {

      int i = 1;
      for (Row c : table) {
        @SuppressWarnings("unchecked")
        List<JsonStringHashMap<String, ?>> list =
            (List<JsonStringHashMap<String, ?>>) c.getMap(BIGINT_INT_MAP_VECTOR_NAME);
        if (list != null && !list.isEmpty()) {
          assertEquals(i, list.size());
          for (JsonStringHashMap<String, ?> sv : list) {
            assertEquals(2, sv.size());
            Long o1 = (Long) sv.get("key");
            Integer o2 = (Integer) sv.get("value");
            assertEquals(o1, o2.longValue());
          }
        }
        i++;
      }
    }
  }

  @Test
  void resetPosition() {
    try (ListVector listVector = simpleListVector(allocator);
        VectorSchemaRoot vectorSchemaRoot = VectorSchemaRoot.of(listVector);
        Table table = new Table(vectorSchemaRoot)) {
      Row row = table.immutableRow();
      row.next();
      assertEquals(0, row.rowNumber);
      row.resetPosition();
      assertEquals(-1, row.rowNumber);
    }
  }
}
