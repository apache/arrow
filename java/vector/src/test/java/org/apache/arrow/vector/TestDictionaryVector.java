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

package org.apache.arrow.vector;

import static org.apache.arrow.vector.TestUtils.newVarBinaryVector;
import static org.apache.arrow.vector.TestUtils.newVarCharVector;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.complex.FixedSizeListVector;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.complex.StructVector;
import org.apache.arrow.vector.complex.UnionVector;
import org.apache.arrow.vector.complex.impl.NullableStructWriter;
import org.apache.arrow.vector.complex.impl.UnionListWriter;
import org.apache.arrow.vector.dictionary.Dictionary;
import org.apache.arrow.vector.dictionary.DictionaryEncoder;
import org.apache.arrow.vector.dictionary.ListSubfieldEncoder;
import org.apache.arrow.vector.holders.NullableIntHolder;
import org.apache.arrow.vector.holders.NullableUInt4Holder;
import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.DictionaryEncoding;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.util.JsonStringArrayList;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class TestDictionaryVector {

  private BufferAllocator allocator;

  byte[] zero = "foo".getBytes(StandardCharsets.UTF_8);
  byte[] one = "bar".getBytes(StandardCharsets.UTF_8);
  byte[] two = "baz".getBytes(StandardCharsets.UTF_8);

  byte[][] data = new byte[][] {zero, one, two};

  @Before
  public void init() {
    allocator = new DirtyRootAllocator(Long.MAX_VALUE, (byte) 100);
  }

  @After
  public void terminate() throws Exception {
    allocator.close();
  }

  @Test
  public void testEncodeStrings() {
    // Create a new value vector
    try (final VarCharVector vector = newVarCharVector("foo", allocator);
        final VarCharVector dictionaryVector = newVarCharVector("dict", allocator);) {
      vector.allocateNew(512, 5);

      // set some values
      vector.setSafe(0, zero, 0, zero.length);
      vector.setSafe(1, one, 0, one.length);
      vector.setSafe(2, one, 0, one.length);
      vector.setSafe(3, two, 0, two.length);
      vector.setSafe(4, zero, 0, zero.length);
      vector.setValueCount(5);

      // set some dictionary values
      dictionaryVector.allocateNew(512, 3);
      dictionaryVector.setSafe(0, zero, 0, zero.length);
      dictionaryVector.setSafe(1, one, 0, one.length);
      dictionaryVector.setSafe(2, two, 0, two.length);
      dictionaryVector.setValueCount(3);

      Dictionary dictionary =
          new Dictionary(dictionaryVector, new DictionaryEncoding(1L, false, null));

      try (final ValueVector encoded = DictionaryEncoder.encode(vector, dictionary)) {
        // verify indices
        assertEquals(IntVector.class, encoded.getClass());

        IntVector index = ((IntVector) encoded);
        assertEquals(5, index.getValueCount());
        assertEquals(0, index.get(0));
        assertEquals(1, index.get(1));
        assertEquals(1, index.get(2));
        assertEquals(2, index.get(3));
        assertEquals(0, index.get(4));

        // now run through the decoder and verify we get the original back
        try (ValueVector decoded = DictionaryEncoder.decode(encoded, dictionary)) {
          assertEquals(vector.getClass(), decoded.getClass());
          assertEquals(vector.getValueCount(), ((VarCharVector) decoded).getValueCount());
          for (int i = 0; i < 5; i++) {
            assertEquals(vector.getObject(i), ((VarCharVector) decoded).getObject(i));
          }
        }
      }
    }
  }

  @Test
  public void testEncodeLargeVector() {
    // Create a new value vector
    try (final VarCharVector vector = newVarCharVector("foo", allocator);
        final VarCharVector dictionaryVector = newVarCharVector("dict", allocator);) {
      vector.allocateNew();

      int count = 10000;

      for (int i = 0; i < 10000; ++i) {
        vector.setSafe(i, data[i % 3], 0, data[i % 3].length);
      }
      vector.setValueCount(count);

      dictionaryVector.allocateNew(512, 3);
      dictionaryVector.setSafe(0, zero, 0, zero.length);
      dictionaryVector.setSafe(1, one, 0, one.length);
      dictionaryVector.setSafe(2, two, 0, two.length);
      dictionaryVector.setValueCount(3);

      Dictionary dictionary =
          new Dictionary(dictionaryVector, new DictionaryEncoding(1L, false, null));

      try (final ValueVector encoded = DictionaryEncoder.encode(vector, dictionary)) {
        // verify indices
        assertEquals(IntVector.class, encoded.getClass());

        IntVector index = ((IntVector) encoded);
        assertEquals(count, index.getValueCount());
        for (int i = 0; i < count; ++i) {
          assertEquals(i % 3, index.get(i));
        }

        // now run through the decoder and verify we get the original back
        try (ValueVector decoded = DictionaryEncoder.decode(encoded, dictionary)) {
          assertEquals(vector.getClass(), decoded.getClass());
          assertEquals(vector.getValueCount(), decoded.getValueCount());
          for (int i = 0; i < count; ++i) {
            assertEquals(vector.getObject(i), decoded.getObject(i));
          }
        }
      }
    }
  }

  @Test
  public void testEncodeList() {
    // Create a new value vector
    try (final ListVector vector = ListVector.empty("vector", allocator);
        final ListVector dictionaryVector = ListVector.empty("dict", allocator);) {

      UnionListWriter writer = vector.getWriter();
      writer.allocate();

      //set some values
      writeListVector(writer, new int[]{10, 20});
      writeListVector(writer, new int[]{10, 20});
      writeListVector(writer, new int[]{10, 20});
      writeListVector(writer, new int[]{30, 40, 50});
      writeListVector(writer, new int[]{30, 40, 50});
      writeListVector(writer, new int[]{10, 20});

      writer.setValueCount(6);

      UnionListWriter dictWriter = dictionaryVector.getWriter();
      dictWriter.allocate();

      writeListVector(dictWriter, new int[]{10, 20});
      writeListVector(dictWriter, new int[]{30, 40, 50});

      dictWriter.setValueCount(2);

      Dictionary dictionary = new Dictionary(dictionaryVector, new DictionaryEncoding(1L, false, null));

      try (final ValueVector encoded = DictionaryEncoder.encode(vector, dictionary)) {
        // verify indices
        assertEquals(IntVector.class, encoded.getClass());

        IntVector index = ((IntVector)encoded);
        assertEquals(6, index.getValueCount());
        assertEquals(0, index.get(0));
        assertEquals(0, index.get(1));
        assertEquals(0, index.get(2));
        assertEquals(1, index.get(3));
        assertEquals(1, index.get(4));
        assertEquals(0, index.get(5));

        // now run through the decoder and verify we get the original back
        try (ValueVector decoded = DictionaryEncoder.decode(encoded, dictionary)) {
          assertEquals(vector.getClass(), decoded.getClass());
          assertEquals(vector.getValueCount(), decoded.getValueCount());
          for (int i = 0; i < 5; i++) {
            assertEquals(vector.getObject(i), decoded.getObject(i));
          }
        }
      }
    }
  }

  @Test
  public void testEncodeStruct() {
    // Create a new value vector
    try (final StructVector vector = StructVector.empty("vector", allocator);
        final StructVector dictionaryVector = StructVector.empty("dict", allocator);) {
      vector.addOrGet("f0", FieldType.nullable(new ArrowType.Int(32, true)), IntVector.class);
      vector.addOrGet("f1", FieldType.nullable(new ArrowType.Int(64, true)), BigIntVector.class);
      dictionaryVector.addOrGet("f0", FieldType.nullable(new ArrowType.Int(32, true)), IntVector.class);
      dictionaryVector.addOrGet("f1", FieldType.nullable(new ArrowType.Int(64, true)), BigIntVector.class);

      NullableStructWriter writer = vector.getWriter();
      writer.allocate();

      writeStructVector(writer, 1, 10L);
      writeStructVector(writer, 1, 10L);
      writeStructVector(writer, 1, 10L);
      writeStructVector(writer, 2, 20L);
      writeStructVector(writer, 2, 20L);
      writeStructVector(writer, 2, 20L);
      writeStructVector(writer, 1, 10L);

      writer.setValueCount(7);

      NullableStructWriter dictWriter = dictionaryVector.getWriter();
      dictWriter.allocate();

      writeStructVector(dictWriter, 1, 10L);
      writeStructVector(dictWriter, 2, 20L);


      dictionaryVector.setValueCount(2);

      Dictionary dictionary = new Dictionary(dictionaryVector, new DictionaryEncoding(1L, false, null));

      try (final ValueVector encoded = DictionaryEncoder.encode(vector, dictionary)) {
        // verify indices
        assertEquals(IntVector.class, encoded.getClass());

        IntVector index = ((IntVector)encoded);
        assertEquals(7, index.getValueCount());
        assertEquals(0, index.get(0));
        assertEquals(0, index.get(1));
        assertEquals(0, index.get(2));
        assertEquals(1, index.get(3));
        assertEquals(1, index.get(4));
        assertEquals(1, index.get(5));
        assertEquals(0, index.get(6));

        // now run through the decoder and verify we get the original back
        try (ValueVector decoded = DictionaryEncoder.decode(encoded, dictionary)) {
          assertEquals(vector.getClass(), decoded.getClass());
          assertEquals(vector.getValueCount(), decoded.getValueCount());
          for (int i = 0; i < 5; i++) {
            assertEquals(vector.getObject(i), decoded.getObject(i));
          }
        }
      }
    }
  }

  @Test
  public void testEncodeBinaryVector() {
    // Create a new value vector
    try (final VarBinaryVector vector = newVarBinaryVector("foo", allocator);
        final VarBinaryVector dictionaryVector = newVarBinaryVector("dict", allocator);) {
      vector.allocateNew(512, 5);

      // set some values
      vector.setSafe(0, zero, 0, zero.length);
      vector.setSafe(1, one, 0, one.length);
      vector.setSafe(2, one, 0, one.length);
      vector.setSafe(3, two, 0, two.length);
      vector.setSafe(4, zero, 0, zero.length);
      vector.setValueCount(5);

      // set some dictionary values
      dictionaryVector.allocateNew(512, 3);
      dictionaryVector.setSafe(0, zero, 0, zero.length);
      dictionaryVector.setSafe(1, one, 0, one.length);
      dictionaryVector.setSafe(2, two, 0, two.length);
      dictionaryVector.setValueCount(3);

      Dictionary dictionary = new Dictionary(dictionaryVector, new DictionaryEncoding(1L, false, null));

      try (final ValueVector encoded = DictionaryEncoder.encode(vector, dictionary)) {
        // verify indices
        assertEquals(IntVector.class, encoded.getClass());

        IntVector index = ((IntVector)encoded);
        assertEquals(5, index.getValueCount());
        assertEquals(0, index.get(0));
        assertEquals(1, index.get(1));
        assertEquals(1, index.get(2));
        assertEquals(2, index.get(3));
        assertEquals(0, index.get(4));

        // now run through the decoder and verify we get the original back
        try (VarBinaryVector decoded = (VarBinaryVector) DictionaryEncoder.decode(encoded, dictionary)) {
          assertEquals(vector.getClass(), decoded.getClass());
          assertEquals(vector.getValueCount(), decoded.getValueCount());
          for (int i = 0; i < 5; i++) {
            assertTrue(Arrays.equals(vector.getObject(i), decoded.getObject(i)));
          }
        }
      }
    }
  }

  @Test
  public void testEncodeUnion() {
    // Create a new value vector
    try (final UnionVector vector = new UnionVector("vector", allocator, null);
        final UnionVector dictionaryVector = new UnionVector("dict", allocator, null);) {

      final NullableUInt4Holder uintHolder1 = new NullableUInt4Holder();
      uintHolder1.value = 10;
      uintHolder1.isSet = 1;

      final NullableIntHolder intHolder1 = new NullableIntHolder();
      intHolder1.value = 10;
      intHolder1.isSet = 1;

      final NullableIntHolder intHolder2 = new NullableIntHolder();
      intHolder2.value = 20;
      intHolder2.isSet = 1;

      //write data
      vector.setType(0, Types.MinorType.UINT4);
      vector.setSafe(0, uintHolder1);

      vector.setType(1, Types.MinorType.INT);
      vector.setSafe(1, intHolder1);

      vector.setType(2, Types.MinorType.INT);
      vector.setSafe(2, intHolder1);

      vector.setType(3, Types.MinorType.INT);
      vector.setSafe(3, intHolder2);

      vector.setType(4, Types.MinorType.INT);
      vector.setSafe(4, intHolder2);

      vector.setValueCount(5);

      //write dictionary
      dictionaryVector.setType(0, Types.MinorType.UINT4);
      dictionaryVector.setSafe(0, uintHolder1);

      dictionaryVector.setType(1, Types.MinorType.INT);
      dictionaryVector.setSafe(1, intHolder1);

      dictionaryVector.setType(2, Types.MinorType.INT);
      dictionaryVector.setSafe(2, intHolder2);

      dictionaryVector.setValueCount(3);

      Dictionary dictionary = new Dictionary(dictionaryVector, new DictionaryEncoding(1L, false, null));

      try (final ValueVector encoded = DictionaryEncoder.encode(vector, dictionary)) {
        // verify indices
        assertEquals(IntVector.class, encoded.getClass());

        IntVector index = ((IntVector)encoded);
        assertEquals(5, index.getValueCount());
        assertEquals(0, index.get(0));
        assertEquals(1, index.get(1));
        assertEquals(1, index.get(2));
        assertEquals(2, index.get(3));
        assertEquals(2, index.get(4));

        // now run through the decoder and verify we get the original back
        try (ValueVector decoded = DictionaryEncoder.decode(encoded, dictionary)) {
          assertEquals(vector.getClass(), decoded.getClass());
          assertEquals(vector.getValueCount(), decoded.getValueCount());
          for (int i = 0; i < 5; i++) {
            assertEquals(vector.getObject(i), decoded.getObject(i));
          }
        }
      }
    }
  }

  @Test
  public void testIntEquals() {
    //test Int
    try (final IntVector vector1 = new IntVector("int", allocator);
        final IntVector vector2 = new IntVector("int", allocator)) {

      Dictionary dict1 = new Dictionary(vector1, new DictionaryEncoding(1L, false, null));
      Dictionary dict2 = new Dictionary(vector2, new DictionaryEncoding(1L, false, null));

      vector1.allocateNew(3);
      vector1.setValueCount(3);
      vector2.allocateNew(3);
      vector2.setValueCount(3);

      vector1.setSafe(0, 1);
      vector1.setSafe(1, 2);
      vector1.setSafe(2, 3);

      vector2.setSafe(0, 1);
      vector2.setSafe(1, 2);
      vector2.setSafe(2, 0);

      assertFalse(dict1.equals(dict2));

      vector2.setSafe(2, 3);
      assertTrue(dict1.equals(dict2));
    }
  }

  @Test
  public void testVarcharEquals() {
    try (final VarCharVector vector1 = new VarCharVector("varchar", allocator);
        final VarCharVector vector2 = new VarCharVector("varchar", allocator)) {

      Dictionary dict1 = new Dictionary(vector1, new DictionaryEncoding(1L, false, null));
      Dictionary dict2 = new Dictionary(vector2, new DictionaryEncoding(1L, false, null));

      vector1.allocateNew();
      vector1.setValueCount(3);
      vector2.allocateNew();
      vector2.setValueCount(3);

      // set some values
      vector1.setSafe(0, zero, 0, zero.length);
      vector1.setSafe(1, one, 0, one.length);
      vector1.setSafe(2, two, 0, two.length);

      vector2.setSafe(0, zero, 0, zero.length);
      vector2.setSafe(1, one, 0, one.length);
      vector2.setSafe(2, one, 0, one.length);

      assertFalse(dict1.equals(dict2));

      vector2.setSafe(2, two, 0, two.length);
      assertTrue(dict1.equals(dict2));
    }
  }

  @Test
  public void testVarBinaryEquals() {
    try (final VarBinaryVector vector1 = new VarBinaryVector("binary", allocator);
        final VarBinaryVector vector2 = new VarBinaryVector("binary", allocator)) {

      Dictionary dict1 = new Dictionary(vector1, new DictionaryEncoding(1L, false, null));
      Dictionary dict2 = new Dictionary(vector2, new DictionaryEncoding(1L, false, null));

      vector1.allocateNew();
      vector1.setValueCount(3);
      vector2.allocateNew();
      vector2.setValueCount(3);

      // set some values
      vector1.setSafe(0, zero, 0, zero.length);
      vector1.setSafe(1, one, 0, one.length);
      vector1.setSafe(2, two, 0, two.length);

      vector2.setSafe(0, zero, 0, zero.length);
      vector2.setSafe(1, one, 0, one.length);
      vector2.setSafe(2, one, 0, one.length);

      assertFalse(dict1.equals(dict2));

      vector2.setSafe(2, two, 0, two.length);
      assertTrue(dict1.equals(dict2));
    }
  }

  @Test
  public void testListEquals() {
    try (final ListVector vector1 = ListVector.empty("list", allocator);
        final ListVector vector2 = ListVector.empty("list", allocator);) {

      Dictionary dict1 = new Dictionary(vector1, new DictionaryEncoding(1L, false, null));
      Dictionary dict2 = new Dictionary(vector2, new DictionaryEncoding(1L, false, null));

      UnionListWriter writer1 = vector1.getWriter();
      writer1.allocate();

      //set some values
      writeListVector(writer1, new int[] {1, 2});
      writeListVector(writer1, new int[] {3, 4});
      writeListVector(writer1, new int[] {5, 6});
      writer1.setValueCount(3);

      UnionListWriter writer2 = vector2.getWriter();
      writer2.allocate();

      //set some values
      writeListVector(writer2, new int[] {1, 2});
      writeListVector(writer2, new int[] {3, 4});
      writeListVector(writer2, new int[] {5, 6});
      writer2.setValueCount(3);

      assertTrue(dict1.equals(dict2));
    }
  }

  @Test
  public void testStructEquals() {
    try (final StructVector vector1 = StructVector.empty("struct", allocator);
        final StructVector vector2 = StructVector.empty("struct", allocator);) {
      vector1.addOrGet("f0", FieldType.nullable(new ArrowType.Int(32, true)), IntVector.class);
      vector1.addOrGet("f1", FieldType.nullable(new ArrowType.Int(64, true)), BigIntVector.class);
      vector2.addOrGet("f0", FieldType.nullable(new ArrowType.Int(32, true)), IntVector.class);
      vector2.addOrGet("f1", FieldType.nullable(new ArrowType.Int(64, true)), BigIntVector.class);

      Dictionary dict1 = new Dictionary(vector1, new DictionaryEncoding(1L, false, null));
      Dictionary dict2 = new Dictionary(vector2, new DictionaryEncoding(1L, false, null));

      NullableStructWriter writer1 = vector1.getWriter();
      writer1.allocate();

      writeStructVector(writer1, 1, 10L);
      writeStructVector(writer1, 2, 20L);
      writer1.setValueCount(2);

      NullableStructWriter writer2 = vector2.getWriter();
      writer2.allocate();

      writeStructVector(writer2, 1, 10L);
      writeStructVector(writer2, 2, 20L);
      writer2.setValueCount(2);

      assertTrue(dict1.equals(dict2));
    }
  }

  @Test
  public void testUnionEquals() {
    try (final UnionVector vector1 = new UnionVector("union", allocator, null);
        final UnionVector vector2 = new UnionVector("union", allocator, null);) {

      final NullableUInt4Holder uInt4Holder = new NullableUInt4Holder();
      uInt4Holder.value = 10;
      uInt4Holder.isSet = 1;

      final NullableIntHolder intHolder = new NullableIntHolder();
      uInt4Holder.value = 20;
      uInt4Holder.isSet = 1;

      vector1.setType(0, Types.MinorType.UINT4);
      vector1.setSafe(0, uInt4Holder);

      vector1.setType(2, Types.MinorType.INT);
      vector1.setSafe(2, intHolder);
      vector1.setValueCount(3);

      vector2.setType(0, Types.MinorType.UINT4);
      vector2.setSafe(0, uInt4Holder);

      vector2.setType(2, Types.MinorType.INT);
      vector2.setSafe(2, intHolder);
      vector2.setValueCount(3);

      Dictionary dict1 = new Dictionary(vector1, new DictionaryEncoding(1L, false, null));
      Dictionary dict2 = new Dictionary(vector2, new DictionaryEncoding(1L, false, null));

      assertTrue(dict1.equals(dict2));
    }
  }

  @Test
  public void testEncodeWithEncoderInstance() {
    // Create a new value vector
    try (final VarCharVector vector = newVarCharVector("vector", allocator);
        final VarCharVector dictionaryVector = newVarCharVector("dict", allocator);) {
      vector.allocateNew(512, 5);

      // set some values
      vector.setSafe(0, zero, 0, zero.length);
      vector.setSafe(1, one, 0, one.length);
      vector.setSafe(2, one, 0, one.length);
      vector.setSafe(3, two, 0, two.length);
      vector.setSafe(4, zero, 0, zero.length);
      vector.setValueCount(5);

      // set some dictionary values
      dictionaryVector.allocateNew(512, 3);
      dictionaryVector.setSafe(0, zero, 0, zero.length);
      dictionaryVector.setSafe(1, one, 0, one.length);
      dictionaryVector.setSafe(2, two, 0, two.length);
      dictionaryVector.setValueCount(3);

      Dictionary dictionary =
          new Dictionary(dictionaryVector, new DictionaryEncoding(1L, false, null));
      DictionaryEncoder encoder = new DictionaryEncoder(dictionary, allocator);

      try (final ValueVector encoded = encoder.encode(vector)) {
        // verify indices
        assertEquals(IntVector.class, encoded.getClass());

        IntVector index = ((IntVector) encoded);
        assertEquals(5, index.getValueCount());
        assertEquals(0, index.get(0));
        assertEquals(1, index.get(1));
        assertEquals(1, index.get(2));
        assertEquals(2, index.get(3));
        assertEquals(0, index.get(4));

        // now run through the decoder and verify we get the original back
        try (ValueVector decoded = encoder.decode(encoded)) {
          assertEquals(vector.getClass(), decoded.getClass());
          assertEquals(vector.getValueCount(), (decoded).getValueCount());
          for (int i = 0; i < 5; i++) {
            assertEquals(vector.getObject(i), ((VarCharVector) decoded).getObject(i));
          }
        }
      }
    }
  }

  @Test
  public void testEncodeMultiVectors() {
    // Create a new value vector
    try (final VarCharVector vector1 = newVarCharVector("vector1", allocator);
        final VarCharVector vector2 = newVarCharVector("vector2", allocator);
        final VarCharVector dictionaryVector = newVarCharVector("dict", allocator);) {

      vector1.allocateNew(512, 5);

      // set some values
      vector1.setSafe(0, zero, 0, zero.length);
      vector1.setSafe(1, one, 0, one.length);
      vector1.setSafe(2, one, 0, one.length);
      vector1.setSafe(3, two, 0, two.length);
      vector1.setSafe(4, zero, 0, zero.length);
      vector1.setValueCount(5);

      vector2.allocateNew(512, 3);

      // set some values
      vector2.setSafe(0, zero, 0, zero.length);
      vector2.setSafe(1, one, 0, one.length);
      vector2.setSafe(2, one, 0, one.length);
      vector2.setValueCount(3);

      // set some dictionary values
      dictionaryVector.allocateNew(512, 3);
      dictionaryVector.setSafe(0, zero, 0, zero.length);
      dictionaryVector.setSafe(1, one, 0, one.length);
      dictionaryVector.setSafe(2, two, 0, two.length);
      dictionaryVector.setValueCount(3);

      Dictionary dictionary =
          new Dictionary(dictionaryVector, new DictionaryEncoding(1L, false, null));
      DictionaryEncoder encoder = new DictionaryEncoder(dictionary, allocator);

      try (final ValueVector encoded = encoder.encode(vector1)) {
        // verify indices
        assertEquals(IntVector.class, encoded.getClass());

        IntVector index = ((IntVector) encoded);
        assertEquals(5, index.getValueCount());
        assertEquals(0, index.get(0));
        assertEquals(1, index.get(1));
        assertEquals(1, index.get(2));
        assertEquals(2, index.get(3));
        assertEquals(0, index.get(4));

        // now run through the decoder and verify we get the original back
        try (ValueVector decoded = encoder.decode(encoded)) {
          assertEquals(vector1.getClass(), decoded.getClass());
          assertEquals(vector1.getValueCount(), (decoded).getValueCount());
          for (int i = 0; i < 5; i++) {
            assertEquals(vector1.getObject(i), ((VarCharVector) decoded).getObject(i));
          }
        }
      }

      try (final ValueVector encoded = encoder.encode(vector2)) {
        // verify indices
        assertEquals(IntVector.class, encoded.getClass());

        IntVector index = ((IntVector) encoded);
        assertEquals(3, index.getValueCount());
        assertEquals(0, index.get(0));
        assertEquals(1, index.get(1));
        assertEquals(1, index.get(2));

        // now run through the decoder and verify we get the original back
        try (ValueVector decoded = encoder.decode(encoded)) {
          assertEquals(vector2.getClass(), decoded.getClass());
          assertEquals(vector2.getValueCount(), (decoded).getValueCount());
          for (int i = 0; i < 3; i++) {
            assertEquals(vector2.getObject(i), ((VarCharVector) decoded).getObject(i));
          }
        }
      }
    }
  }

  @Test
  public void testEncodeListSubField() {
    // Create a new value vector
    try (final ListVector vector = ListVector.empty("vector", allocator);
        final ListVector dictionaryVector = ListVector.empty("dict", allocator);) {

      UnionListWriter writer = vector.getWriter();
      writer.allocate();

      //set some values
      writeListVector(writer, new int[]{10, 20});
      writeListVector(writer, new int[]{10, 20});
      writeListVector(writer, new int[]{10, 20});
      writeListVector(writer, new int[]{30, 40, 50});
      writeListVector(writer, new int[]{30, 40, 50});
      writeListVector(writer, new int[]{10, 20});
      writer.setValueCount(6);

      UnionListWriter dictWriter = dictionaryVector.getWriter();
      dictWriter.allocate();
      writeListVector(dictWriter, new int[]{10, 20, 30, 40, 50});
      dictionaryVector.setValueCount(1);

      Dictionary dictionary = new Dictionary(dictionaryVector, new DictionaryEncoding(1L, false, null));
      ListSubfieldEncoder encoder = new ListSubfieldEncoder(dictionary, allocator);

      try (final ListVector encoded = (ListVector) encoder.encodeListSubField(vector)) {
        // verify indices
        assertEquals(ListVector.class, encoded.getClass());

        assertEquals(6, encoded.getValueCount());
        int[] realValue1 = convertListToIntArray((JsonStringArrayList) encoded.getObject(0));
        assertTrue(Arrays.equals(new int[] {0,1}, realValue1));
        int[] realValue2 = convertListToIntArray((JsonStringArrayList) encoded.getObject(1));
        assertTrue(Arrays.equals(new int[] {0,1}, realValue2));
        int[] realValue3 = convertListToIntArray((JsonStringArrayList) encoded.getObject(2));
        assertTrue(Arrays.equals(new int[] {0,1}, realValue3));
        int[] realValue4 = convertListToIntArray((JsonStringArrayList) encoded.getObject(3));
        assertTrue(Arrays.equals(new int[] {2,3,4}, realValue4));
        int[] realValue5 = convertListToIntArray((JsonStringArrayList) encoded.getObject(4));
        assertTrue(Arrays.equals(new int[] {2,3,4}, realValue5));
        int[] realValue6 = convertListToIntArray((JsonStringArrayList) encoded.getObject(5));
        assertTrue(Arrays.equals(new int[] {0,1}, realValue6));

        // now run through the decoder and verify we get the original back
        try (ValueVector decoded = encoder.decodeListSubField(encoded)) {
          assertEquals(vector.getClass(), decoded.getClass());
          assertEquals(vector.getValueCount(), decoded.getValueCount());
          for (int i = 0; i < 5; i++) {
            assertEquals(vector.getObject(i), decoded.getObject(i));
          }
        }
      }
    }
  }

  @Test
  public void testEncodeFixedSizeListSubField() {
    // Create a new value vector
    try (final FixedSizeListVector vector = FixedSizeListVector.empty("vector", 2, allocator);
        final FixedSizeListVector dictionaryVector = FixedSizeListVector.empty("dict", 2, allocator)) {

      vector.allocateNew();
      vector.setValueCount(4);

      IntVector dataVector =
          (IntVector) vector.addOrGetVector(FieldType.nullable(Types.MinorType.INT.getType())).getVector();
      dataVector.allocateNew(8);
      dataVector.setValueCount(8);
      // set value at index 0
      vector.setNotNull(0);
      dataVector.set(0, 10);
      dataVector.set(1, 20);
      // set value at index 1
      vector.setNotNull(1);
      dataVector.set(2, 10);
      dataVector.set(3, 20);
      // set value at index 2
      vector.setNotNull(2);
      dataVector.set(4, 30);
      dataVector.set(5, 40);
      // set value at index 3
      vector.setNotNull(3);
      dataVector.set(6, 10);
      dataVector.set(7, 20);

      dictionaryVector.allocateNew();
      dictionaryVector.setValueCount(2);
      IntVector dictDataVector =
          (IntVector) dictionaryVector.addOrGetVector(FieldType.nullable(Types.MinorType.INT.getType())).getVector();
      dictDataVector.allocateNew(4);
      dictDataVector.setValueCount(4);

      dictionaryVector.setNotNull(0);
      dictDataVector.set(0, 10);
      dictDataVector.set(1, 20);
      dictionaryVector.setNotNull(1);
      dictDataVector.set(2, 30);
      dictDataVector.set(3, 40);

      Dictionary dictionary = new Dictionary(dictionaryVector, new DictionaryEncoding(1L, false, null));
      ListSubfieldEncoder encoder = new ListSubfieldEncoder(dictionary, allocator);

      try (final FixedSizeListVector encoded =
          (FixedSizeListVector) encoder.encodeListSubField(vector)) {
        // verify indices
        assertEquals(FixedSizeListVector.class, encoded.getClass());

        assertEquals(4, encoded.getValueCount());
        int[] realValue1 = convertListToIntArray((JsonStringArrayList) encoded.getObject(0));
        assertTrue(Arrays.equals(new int[] {0,1}, realValue1));
        int[] realValue2 = convertListToIntArray((JsonStringArrayList) encoded.getObject(1));
        assertTrue(Arrays.equals(new int[] {0,1}, realValue2));
        int[] realValue3 = convertListToIntArray((JsonStringArrayList) encoded.getObject(2));
        assertTrue(Arrays.equals(new int[] {2,3}, realValue3));
        int[] realValue4 = convertListToIntArray((JsonStringArrayList) encoded.getObject(3));
        assertTrue(Arrays.equals(new int[] {0,1}, realValue4));

        // now run through the decoder and verify we get the original back
        try (ValueVector decoded = encoder.decodeListSubField(encoded)) {
          assertEquals(vector.getClass(), decoded.getClass());
          assertEquals(vector.getValueCount(), decoded.getValueCount());
          for (int i = 0; i < 5; i++) {
            assertEquals(vector.getObject(i), decoded.getObject(i));
          }
        }
      }
    }
  }

  private int[] convertListToIntArray(JsonStringArrayList list) {
    int[] values = new int[list.size()];
    for (int i = 0; i < list.size(); i++) {
      values[i] = (int) list.get(i);
    }
    return values;
  }

  private void writeStructVector(NullableStructWriter writer, int value1, long value2) {
    writer.start();
    writer.integer("f0").writeInt(value1);
    writer.bigInt("f1").writeBigInt(value2);
    writer.end();
  }

  private void writeListVector(UnionListWriter writer, int[] values) {
    writer.startList();
    for (int v: values) {
      writer.integer().writeInt(v);
    }
    writer.endList();
  }
}
