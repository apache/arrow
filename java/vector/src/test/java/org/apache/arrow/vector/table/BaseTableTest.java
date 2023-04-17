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

import static org.apache.arrow.vector.table.TestUtils.INT_VECTOR_NAME;
import static org.apache.arrow.vector.table.TestUtils.INT_VECTOR_NAME_1;
import static org.apache.arrow.vector.table.TestUtils.INT_VECTOR_NAME_2;
import static org.apache.arrow.vector.table.TestUtils.intPlusVarcharColumns;
import static org.apache.arrow.vector.table.TestUtils.twoIntColumns;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.List;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.dictionary.Dictionary;
import org.apache.arrow.vector.dictionary.DictionaryEncoder;
import org.apache.arrow.vector.dictionary.DictionaryProvider;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.DictionaryEncoding;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class BaseTableTest {

  private BufferAllocator allocator;

  @BeforeEach
  public void init() {
    allocator = new RootAllocator(Long.MAX_VALUE);
  }

  @Test
  void getReaderByName() {
    List<FieldVector> vectorList = twoIntColumns(allocator);
    try (Table t = new Table(vectorList)) {
      assertNotNull(t.getReader(INT_VECTOR_NAME_1));
    }
  }

  @Test
  void getReaderByIndex() {
    List<FieldVector> vectorList = twoIntColumns(allocator);
    try (Table t = new Table(vectorList)) {
      assertNotNull(t.getReader(0));
    }
  }

  @Test
  void getReaderByField() {
    List<FieldVector> vectorList = twoIntColumns(allocator);
    try (Table t = new Table(vectorList)) {
      assertNotNull(t.getReader(t.getField(INT_VECTOR_NAME_1)));
    }
  }

  @Test
  void getSchema() {
    List<FieldVector> vectorList = twoIntColumns(allocator);
    try (Table t = new Table(vectorList)) {
      assertNotNull(t.getSchema());
      assertEquals(2, t.getSchema().getFields().size());
    }
  }

  @Test
  void insertVector() {
    List<FieldVector> vectorList = twoIntColumns(allocator);
    try (Table t = new Table(vectorList)) {
      ArrowType intArrowType = new ArrowType.Int(32, true);
      FieldType intFieldType = new FieldType(true, intArrowType, null);
      IntVector v3 = new IntVector("3", intFieldType, allocator);
      List<FieldVector> revisedVectors = t.insertVector(2, v3);
      assertEquals(3, revisedVectors.size());
      assertEquals(v3, revisedVectors.get(2));
    }
  }

  @Test
  void insertVectorFirstPosition() {
    List<FieldVector> vectorList = twoIntColumns(allocator);
    try (Table t = new Table(vectorList)) {
      ArrowType intArrowType = new ArrowType.Int(32, true);
      FieldType intFieldType = new FieldType(true, intArrowType, null);
      IntVector v3 = new IntVector("3", intFieldType, allocator);
      List<FieldVector> revisedVectors = t.insertVector(0, v3);
      assertEquals(3, revisedVectors.size());
      assertEquals(v3, revisedVectors.get(0));
    }
  }

  @Test
  void extractVector() {
    List<FieldVector> vectorList = twoIntColumns(allocator);
    try (Table t = new Table(vectorList)) {
      List<FieldVector> revisedVectors = t.extractVector(0);
      assertEquals(2, t.getVectorCount()); // vector not removed from table yet
      assertEquals(1, revisedVectors.size());
    }
  }

  @Test
  void close() {
    IntVector v = new IntVector(INT_VECTOR_NAME, allocator);
    v.setSafe(0, 132);
    List<FieldVector> vectors = new ArrayList<>();
    vectors.add(v);
    v.setValueCount(1);
    try (Table t = new Table(vectors)) {
      t.close();
      for (FieldVector fieldVector : t.fieldVectors) {
        assertEquals(0, fieldVector.getValueCount());
      }
    }
  }

  @Test
  void getRowCount() {
    List<FieldVector> vectorList = twoIntColumns(allocator);
    try (Table t = new Table(vectorList)) {
      assertEquals(2, t.getRowCount());
    }
  }

  @Test
  void toVectorSchemaRoot() {
    List<FieldVector> vectorList = twoIntColumns(allocator);
    try (Table t = new Table(vectorList)) {
      assertNotNull(t.getVector(INT_VECTOR_NAME_1));
      assertNotNull(t.getVector(INT_VECTOR_NAME_2));
      VectorSchemaRoot vsr = t.toVectorSchemaRoot();
      assertNotNull(vsr.getVector(INT_VECTOR_NAME_1));
      assertNotNull(vsr.getVector(INT_VECTOR_NAME_2));
      assertEquals(
          t.getSchema().findField(INT_VECTOR_NAME_1), vsr.getSchema().findField(INT_VECTOR_NAME_1));
    }
  }

  @Test
  void getVector() {
    List<FieldVector> vectorList = twoIntColumns(allocator);
    try (Table t = new Table(vectorList)) {
      assertNotNull(t.getVector(0));
    }
  }

  @Test
  void testGetVector() {
    List<FieldVector> vectorList = twoIntColumns(allocator);
    try (Table t = new Table(vectorList)) {
      assertNotNull(t.getVector(INT_VECTOR_NAME_1));
      assertThrows(IllegalArgumentException.class,
          () -> t.getVector("wrong name"));
    }
  }

  @Test
  void getVectorCopyByIndex() {
    List<FieldVector> vectorList = twoIntColumns(allocator);
    List<FieldVector> vectorList2 = twoIntColumns(allocator);
    try (Table t = new Table(vectorList)) {
      // compare value by value
      for (int vIdx = 0; vIdx < vectorList.size(); vIdx++) {
        IntVector original = (IntVector) vectorList2.get(vIdx);
        IntVector copy = (IntVector) t.getVectorCopy(vIdx);
        assertNotNull(copy);
        assertEquals(2, copy.getValueCount());
        assertEquals(0, copy.getNullCount());
        for (int i = 0; i < t.getRowCount(); i++) {
          assertEquals(original.getObject(i), copy.getObject(i));
        }
      }
      assertThrows(IllegalArgumentException.class,
          () -> t.getVector("wrong name"));
    }
  }

  @Test
  void getVectorCopyByName() {
    List<FieldVector> vectorList = twoIntColumns(allocator);
    List<FieldVector> vectorList2 = twoIntColumns(allocator);
    try (Table t = new Table(vectorList)) {
      assertNotNull(t.getVectorCopy(INT_VECTOR_NAME_1));
      for (int vIdx = 0; vIdx < vectorList.size(); vIdx++) {
        IntVector original = (IntVector) vectorList2.get(vIdx);
        IntVector copy = (IntVector) t.getVectorCopy(original.getName());
        assertEquals(2, copy.getValueCount());
        assertEquals(0, copy.getNullCount());
        for (int i = 0; i < t.getRowCount(); i++) {
          assertEquals(original.getObject(i), copy.getObject(i));
        }
      }
      assertThrows(IllegalArgumentException.class,
          () -> t.getVector("wrong name"));
    }
  }

  @Test
  void immutableCursor() {
    List<FieldVector> vectorList = twoIntColumns(allocator);
    try (Table t = new Table(vectorList)) {
      assertNotNull(t.immutableRow());
    }
  }

  @Test
  void contentToTsvString() {
    IntVector v = new IntVector(INT_VECTOR_NAME, allocator);
    v.setSafe(0, 1);
    v.setSafe(1, 2);
    v.setSafe(2, 3);
    v.setValueCount(3);

    try (Table t = Table.of(v)) {
      assertEquals(3, t.rowCount);
      List<Integer> values = new ArrayList<>();
      for (Row r : t) {
        values.add(r.getInt(INT_VECTOR_NAME));
      }
      assertEquals(3, values.size());
      List<Integer> intList = new ArrayList<>();
      intList.add(1);
      intList.add(2);
      intList.add(3);
      assertTrue(values.containsAll(intList));
      String printed = "intCol\n" + "1\n" + "2\n" + "3\n";
      assertEquals(printed, t.contentToTSVString());
    }
  }

  @Test
  void isDeletedRow() {
    List<FieldVector> vectorList = twoIntColumns(allocator);
    try (Table t = new Table(vectorList)) {
      assertFalse(t.isRowDeleted(0));
      assertFalse(t.isRowDeleted(1));
    }
  }

  @Test
  void testEncode() {
    List<FieldVector> vectorList = intPlusVarcharColumns(allocator);
    VarCharVector original = (VarCharVector) vectorList.get(1);
    DictionaryProvider provider = getDictionary();
    try (Table t = new Table(vectorList, vectorList.get(0).getValueCount(), provider)) {
      IntVector v = (IntVector) t.encode(original.getName(), 1L);
      assertNotNull(v);
      assertEquals(0, v.get(0));
      assertEquals(1, v.get(1));
    }
  }

  @Test
  void testDecode() {
    List<FieldVector> vectorList = intPlusVarcharColumns(allocator);
    VarCharVector original = (VarCharVector) vectorList.get(1);

    VarCharVector dictionaryVector = new VarCharVector("dictionary", allocator);
    dictionaryVector.allocateNew(2);
    dictionaryVector.set(0, "one".getBytes());
    dictionaryVector.set(1, "two".getBytes());
    dictionaryVector.setValueCount(2);
    Dictionary dictionary =
        new Dictionary(dictionaryVector, new DictionaryEncoding(1L, false, null));

    DictionaryEncoder encoder = new DictionaryEncoder(dictionary, allocator);
    IntVector encoded = (IntVector) encoder.encode(original);
    vectorList.remove(original);
    vectorList.add(encoded);
    DictionaryProvider provider = getDictionary();

    try (Table t = new Table(vectorList, vectorList.get(0).getValueCount(), provider)) {
      VarCharVector v = (VarCharVector) t.decode(encoded.getName(), 1L);
      assertNotNull(v);
      assertEquals("one", new String(v.get(0)));
      assertEquals("two", new String(v.get(1)));
    }
  }

  @Test
  void getProvider() {
    List<FieldVector> vectorList = intPlusVarcharColumns(allocator);
    DictionaryProvider provider = getDictionary();
    try (Table t = new Table(vectorList, vectorList.get(0).getValueCount(), provider)) {
      assertEquals(provider, t.getDictionaryProvider());
    }
  }

  private DictionaryProvider getDictionary() {

    DictionaryProvider.MapDictionaryProvider provider =
        new DictionaryProvider.MapDictionaryProvider();
    DictionaryEncoding encoding = new DictionaryEncoding(1L, false, null);

    VarCharVector dictionaryVector = new VarCharVector("dictionary", allocator);
    dictionaryVector.allocateNew(2);
    dictionaryVector.set(0, "one".getBytes());
    dictionaryVector.set(1, "two".getBytes());
    dictionaryVector.setValueCount(2);

    Dictionary dictionary = new Dictionary(dictionaryVector, encoding);
    provider.put(dictionary);
    return provider;
  }
}
