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
import org.apache.arrow.vector.holders.NullableBigIntHolder;
import org.apache.arrow.vector.holders.NullableFloat4Holder;
import org.apache.arrow.vector.holders.NullableFloat8Holder;
import org.apache.arrow.vector.holders.NullableIntHolder;
import org.apache.arrow.vector.holders.NullableSmallIntHolder;
import org.apache.arrow.vector.holders.NullableTinyIntHolder;
import org.apache.arrow.vector.holders.NullableUInt1Holder;
import org.apache.arrow.vector.holders.NullableUInt2Holder;
import org.apache.arrow.vector.holders.NullableUInt4Holder;
import org.apache.arrow.vector.holders.NullableUInt8Holder;
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
    // TODO: Test setNull using complex types (and dictionaries?)
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
  void setTinyInt() {
    List<FieldVector> vectorList = numericVectors(allocator, 2);
    try (MutableTable t = new MutableTable(vectorList)) {
      MutableRow c = t.mutableRow();
      c.setPosition(1);
      c.setTinyInt("tinyInt_vector", (byte) 1);
      assertEquals(1, c.getTinyInt("tinyInt_vector"));
      c.setTinyInt("tinyInt_vector", (byte) 2);
      assertEquals(2, c.getTinyInt("tinyInt_vector"));
      c.setTinyInt(3, (byte) 3);
      assertEquals(3, c.getTinyInt("tinyInt_vector"));

      // test with holder
      NullableTinyIntHolder holder = new NullableTinyIntHolder();
      holder.value = 4;
      holder.isSet = 1;
      c.setTinyInt("tinyInt_vector", holder);
      assertEquals(4, c.getTinyInt("tinyInt_vector"));
      holder.value = 5;
      c.setTinyInt(3, holder);
      assertEquals(5, c.getTinyInt(3));
    }
  }

  @Test
  void setUint1() {
    List<FieldVector> vectorList = numericVectors(allocator, 2);
    try (MutableTable t = new MutableTable(vectorList)) {
      MutableRow c = t.mutableRow();
      c.setPosition(1);
      c.setUInt1("uInt1_vector", (byte) 1);
      assertEquals(1, c.getUInt1("uInt1_vector"));
      c.setUInt1("uInt1_vector", (byte) 2);
      assertEquals(2, c.getUInt1("uInt1_vector"));
      c.setUInt1(4, (byte) 3);
      assertEquals(3, c.getUInt1("uInt1_vector"));

      // test with holder
      NullableUInt1Holder holder = new NullableUInt1Holder();
      holder.value = 4;
      holder.isSet = 1;
      c.setUInt1("uInt1_vector", holder);
      assertEquals(4, c.getUInt1("uInt1_vector"));
      holder.value = 5;
      c.setUInt1(4, holder);
      assertEquals(5, c.getUInt1(4));
    }
  }

  @Test
  void setSmallInt() {
    List<FieldVector> vectorList = numericVectors(allocator, 2);
    try (MutableTable t = new MutableTable(vectorList)) {
      MutableRow c = t.mutableRow();
      c.setPosition(1);
      c.setSmallInt("smallInt_vector", (short) 1);
      assertEquals(1, c.getSmallInt("smallInt_vector"));
      c.setSmallInt("smallInt_vector", (short) 2);
      assertEquals(2, c.getSmallInt("smallInt_vector"));
      c.setSmallInt(2, (short) 3);
      assertEquals(3, c.getSmallInt("smallInt_vector"));

      // test with holder
      NullableSmallIntHolder holder = new NullableSmallIntHolder();
      holder.value = 4;
      holder.isSet = 1;
      c.setSmallInt("smallInt_vector", holder);
      assertEquals(4, c.getSmallInt("smallInt_vector"));
      holder.value = 5;
      c.setSmallInt(2, holder);
      assertEquals(5, c.getSmallInt(2));
    }
  }

  @Test
  void setUint2() {
    List<FieldVector> vectorList = numericVectors(allocator, 2);
    try (MutableTable t = new MutableTable(vectorList)) {
      MutableRow c = t.mutableRow();
      c.setPosition(1);
      c.setUInt2("uInt2_vector", (short) 1);
      assertEquals(1, c.getUInt2("uInt2_vector"));
      c.setUInt2("uInt2_vector", (short) 2);
      assertEquals(2, c.getUInt2("uInt2_vector"));
      c.setUInt2(5, (short) 3);
      assertEquals(3, c.getUInt2("uInt2_vector"));

      // test with holder
      NullableUInt2Holder holder = new NullableUInt2Holder();
      holder.value = 4;
      holder.isSet = 1;
      c.setUInt2("uInt2_vector", holder);
      assertEquals(4, c.getUInt2("uInt2_vector"));
      holder.value = 5;
      c.setUInt2(5, holder);
      assertEquals(5, c.getUInt2(5));
    }
  }

  @Test
  void setBigInt() {
    List<FieldVector> vectorList = numericVectors(allocator, 2);
    try (MutableTable t = new MutableTable(vectorList)) {
      MutableRow c = t.mutableRow();
      c.setPosition(1);
      c.setBigInt("bigInt_vector", 1);
      assertEquals(1, c.getBigInt("bigInt_vector"));
      c.setBigInt("bigInt_vector", 2);
      assertEquals(2, c.getBigInt("bigInt_vector"));
      c.setBigInt(1, 3);
      assertEquals(3, c.getBigInt("bigInt_vector"));

      // test with holder
      NullableBigIntHolder holder = new NullableBigIntHolder();
      holder.value = 4;
      holder.isSet = 1;
      c.setBigInt("bigInt_vector", holder);
      assertEquals(4, c.getBigInt("bigInt_vector"));
      holder.value = 5;
      c.setBigInt(1, holder);
      assertEquals(5, c.getBigInt(1));
    }
  }

  @Test
  void setUint8() {
    List<FieldVector> vectorList = numericVectors(allocator, 2);
    try (MutableTable t = new MutableTable(vectorList)) {
      MutableRow c = t.mutableRow();
      c.setPosition(1);
      c.setUInt8("uInt8_vector", 1);
      assertEquals(1, c.getUInt8("uInt8_vector"));
      c.setUInt8("uInt8_vector", 2);
      assertEquals(2, c.getUInt8("uInt8_vector"));
      c.setUInt8(7, 3);
      assertEquals(3, c.getUInt8("uInt8_vector"));

      // test with holder
      NullableUInt8Holder holder = new NullableUInt8Holder();
      holder.value = 4;
      holder.isSet = 1;
      c.setUInt8("uInt8_vector", holder);
      assertEquals(4, c.getUInt8("uInt8_vector"));
      holder.value = 5;
      c.setUInt8(7, holder);
      assertEquals(5, c.getUInt8(7));
    }
  }

  @Test
  void setInt() {
    List<FieldVector> vectorList = numericVectors(allocator, 2);
    try (MutableTable t = new MutableTable(vectorList)) {
      MutableRow c = t.mutableRow();
      c.setPosition(1);
      c.setInt("int_vector", 1);
      assertEquals(1, c.getInt("int_vector"));
      c.setInt("int_vector", 2);
      assertEquals(2, c.getInt("int_vector"));
      c.setInt(0, 3);
      assertEquals(3, c.getInt("int_vector"));

      // test with holder
      NullableIntHolder holder = new NullableIntHolder();
      holder.value = 4;
      holder.isSet = 1;
      c.setInt("int_vector", holder);
      assertEquals(4, c.getInt("int_vector"));
      holder.value = 5;
      c.setInt(0, holder);
      assertEquals(5, c.getInt(0));
    }
  }

  @Test
  void setUint4() {
    List<FieldVector> vectorList = numericVectors(allocator, 2);
    try (MutableTable t = new MutableTable(vectorList)) {
      MutableRow c = t.mutableRow();
      c.setPosition(1);
      c.setUInt4("uInt4_vector", 1);
      assertEquals(1, c.getUInt4("uInt4_vector"));
      c.setUInt4("uInt4_vector", 2);
      assertEquals(2, c.getUInt4("uInt4_vector"));
      c.setUInt4(6, 3);
      assertEquals(3, c.getUInt4("uInt4_vector"));

      // test with holder
      NullableUInt4Holder holder = new NullableUInt4Holder();
      holder.value = 4;
      holder.isSet = 1;
      c.setUInt4("uInt4_vector", holder);
      assertEquals(4, c.getUInt4("uInt4_vector"));
      holder.value = 5;
      c.setUInt4(6, holder);
      assertEquals(5, c.getUInt4(6));
    }
  }

  @Test
  void setFloat4() {
    List<FieldVector> vectorList = numericVectors(allocator, 2);
    try (MutableTable t = new MutableTable(vectorList)) {
      MutableRow c = t.mutableRow();
      c.setPosition(1);
      c.setFloat4("float4_vector", 1.1f);
      assertEquals(1.1, c.getFloat4("float4_vector"), 0.0001);
      c.setFloat4("float4_vector", 2.2f);
      assertEquals(2.2, c.getFloat4("float4_vector"), 0.0001);
      c.setFloat4(8, 3.2f);
      assertEquals(3.2, c.getFloat4("float4_vector"), 0.0001);

      // test with holder
      NullableFloat4Holder holder = new NullableFloat4Holder();
      holder.value = 4.4f;
      holder.isSet = 1;
      c.setFloat4("float4_vector", holder);
      assertEquals(4.4, c.getFloat4("float4_vector"), 0.0001);
      holder.value = 5.5f;
      c.setFloat4(8, holder);
      assertEquals(5.5, c.getFloat4(8), 0.0001);
    }
  }

  @Test
  void setFloat8() {
    List<FieldVector> vectorList = numericVectors(allocator, 2);
    try (MutableTable t = new MutableTable(vectorList)) {
      MutableRow c = t.mutableRow();
      c.setPosition(1);
      c.setFloat8("float8_vector", 1.1);
      assertEquals(1.1, c.getFloat8("float8_vector"));
      c.setFloat8("float8_vector", 2.2);
      assertEquals(2.2, c.getFloat8("float8_vector"));
      c.setFloat8(9, 3.2);
      assertEquals(3.2, c.getFloat8("float8_vector"));

      // test with holder
      NullableFloat8Holder holder = new NullableFloat8Holder();
      holder.value = 4.4;
      holder.isSet = 1;
      c.setFloat8("float8_vector", holder);
      assertEquals(4.4, c.getFloat8("float8_vector"));
      holder.value = 5.5;
      c.setFloat8(9, holder);
      assertEquals(5.5, c.getFloat8(9));
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
