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
import static org.apache.arrow.vector.table.TestUtils.VARCHAR_VECTOR_NAME_1;
import static org.apache.arrow.vector.table.TestUtils.intPlusVarcharColumns;
import static org.apache.arrow.vector.table.TestUtils.numericVectors;
import static org.apache.arrow.vector.table.TestUtils.twoIntColumns;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.dictionary.Dictionary;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.DictionaryEncoding;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class MutableRowTest {

  private BufferAllocator allocator;

  @BeforeEach
  public void init() {
    allocator = new RootAllocator(Long.MAX_VALUE);
  }

  @Test
  void constructor() {
    List<FieldVector> vectorList = twoIntColumns(allocator);
    try (MutableTable t = new MutableTable(vectorList)) {
      Row c = t.mutableRow();
      assertEquals(StandardCharsets.UTF_8, c.getDefaultCharacterSet());
    }
  }

  @Test
  void at() {
    List<FieldVector> vectorList = twoIntColumns(allocator);
    try (MutableTable t = new MutableTable(vectorList)) {
      Row c = t.immutableRow();
      assertEquals(c.getRowNumber(), -1);
      c.setPosition(1);
      assertEquals(c.getRowNumber(), 1);
      assertEquals(2, c.getInt(0));
    }
  }

  @Test
  void setNullByColumnIndex() {
    List<FieldVector> vectorList = twoIntColumns(allocator);
    try (MutableTable t = new MutableTable(vectorList)) {
      MutableRow c = t.mutableRow();
      c.setPosition(1);
      assertFalse(c.isNull(0));
      c.setNull(0);
      assertTrue(c.isNull(0));
    }
  }

  @Test
  void setNullByColumnName() {
    // TODO: Test setNull using complex types
    List<FieldVector> vectorList = twoIntColumns(allocator);
    try (MutableTable t = new MutableTable(vectorList)) {
      MutableRow c = t.mutableRow();
      c.setPosition(1);
      assertFalse(c.isNull(INT_VECTOR_NAME_1));
      c.setNull(INT_VECTOR_NAME_1);
      assertTrue(c.isNull(INT_VECTOR_NAME_1));
    }
  }

  @Test
  void setIntByColumnIndex() {
    List<FieldVector> vectorList = twoIntColumns(allocator);
    try (MutableTable t = new MutableTable(vectorList)) {
      MutableRow c = t.mutableRow();
      c.setPosition(1);
      assertNotEquals(132, c.getInt(0));
      c.setInt(0, 132).setInt(1, 146);
      assertEquals(132, c.getInt(0));
      assertEquals(146, c.getInt(1));
    }
  }

  @Test
  void setIntByColumnName() {
    List<FieldVector> vectorList = twoIntColumns(allocator);
    try (MutableTable t = new MutableTable(vectorList)) {
      MutableRow c = t.mutableRow();
      c.setPosition(1);
      assertNotEquals(132, c.getInt(INT_VECTOR_NAME_1));
      c.setInt(INT_VECTOR_NAME_1, 132);
      assertEquals(132, c.getInt(INT_VECTOR_NAME_1));
    }
  }

  @Test
  void setUInt4ByColumnName() {
    List<FieldVector> vectorList = numericVectors(allocator, 2);
    try (MutableTable t = new MutableTable(vectorList)) {
      MutableRow c = t.mutableRow();
      c.setPosition(1);
      c.setUInt4("uInt4_vector", 132);
      int result = c.getUInt4("uInt4_vector");
      assertEquals(132, result);
    }
  }

  @Test
  void setVarCharByColumnIndex() {
    List<FieldVector> vectorList = intPlusVarcharColumns(allocator);
    try (MutableTable t = new MutableTable(vectorList)) {
      assertEquals(2, t.rowCount);
      MutableRow c = t.mutableRow();
      c.setPosition(1);
      assertEquals(2, c.getInt(0));
      assertEquals("two", c.getVarCharObj(1));
      c.setVarChar(1, "2");
      c.setPosition(1);
      assertTrue(c.isRowDeleted());
      c.setPosition(2);
      assertEquals("2", c.getVarCharObj(1));
    }
  }

  @Test
  void setVarCharByColumnName() {
    List<FieldVector> vectorList = intPlusVarcharColumns(allocator);
    try (MutableTable t = new MutableTable(vectorList)) {
      assertEquals(2, t.rowCount);
      MutableRow c = t.mutableRow();
      c.setPosition(1);
      assertEquals(2, c.getInt(0));
      assertEquals("two", c.getVarCharObj(VARCHAR_VECTOR_NAME_1));
      c.setVarChar(VARCHAR_VECTOR_NAME_1, "2");
      c.setPosition(1);
      assertTrue(c.isRowDeleted());
      c.setPosition(2);
      assertEquals("2", c.getVarCharObj(VARCHAR_VECTOR_NAME_1));

      // ensure iteration works correctly
      List<String> values = new ArrayList<>();
      c.resetPosition();
      while (c.hasNext()) {
        c.next();
        values.add(new String(c.getVarChar(VARCHAR_VECTOR_NAME_1)));
      }
      assertTrue(values.contains("one"));
      assertTrue(values.contains("2"));
      assertEquals(2, values.size());
    }
  }

  @Test
  void setVarCharByColumnNameUsingDictionary() {
    List<FieldVector> vectorList = intPlusVarcharColumns(allocator);
    VarCharVector v1 = (VarCharVector) vectorList.get(1);
    Dictionary numbersDictionary = new Dictionary(v1,
        new DictionaryEncoding(1L, false, new ArrowType.Int(8, true)));

    try (MutableTable t = new MutableTable(vectorList)) {
      assertEquals(2, t.rowCount);
      MutableRow c = t.mutableRow();
      c.setPosition(1);
      assertEquals(2, c.getInt(0));
      assertEquals("two", c.getVarCharObj(VARCHAR_VECTOR_NAME_1));
      c.setVarChar(VARCHAR_VECTOR_NAME_1, "2");
      c.setPosition(1);
      assertTrue(c.isRowDeleted());
      c.setPosition(2);
      assertEquals("2", c.getVarCharObj(VARCHAR_VECTOR_NAME_1));

      // ensure iteration works correctly
      List<String> values = new ArrayList<>();
      c.resetPosition();
      while (c.hasNext()) {
        c.next();
        values.add(new String(c.getVarChar(VARCHAR_VECTOR_NAME_1)));
      }
      assertTrue(values.contains("one"));
      assertTrue(values.contains("2"));
      assertEquals(2, values.size());
    }
  }

  @Test
  void delete() {
    List<FieldVector> vectorList = twoIntColumns(allocator);
    try (MutableTable t = new MutableTable(vectorList)) {
      MutableRow c = t.mutableRow();
      c.setPosition(0);
      assertFalse(c.isRowDeleted());
      c.deleteCurrentRow();
      assertTrue(c.isRowDeleted());
      assertTrue(t.isRowDeleted(0));
    }
  }

  @Test
  void compact() {
    // TODO: Implement
  }
}
