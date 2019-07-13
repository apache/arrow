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
import static org.junit.Assert.assertTrue;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.complex.StructVector;
import org.apache.arrow.vector.complex.impl.NullableStructWriter;
import org.apache.arrow.vector.complex.impl.UnionListWriter;
import org.apache.arrow.vector.dictionary.Dictionary;
import org.apache.arrow.vector.dictionary.DictionaryEncoder;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.DictionaryEncoding;
import org.apache.arrow.vector.types.pojo.FieldType;
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

      Dictionary dictionary = new Dictionary(dictionaryVector, new DictionaryEncoding(1L, false, null));

      try (final ValueVector encoded = (FieldVector) DictionaryEncoder.encode(vector, dictionary)) {
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
        try (ValueVector decoded = DictionaryEncoder.decode(encoded, dictionary)) {
          assertEquals(vector.getClass(), decoded.getClass());
          assertEquals(vector.getValueCount(), ((VarCharVector)decoded).getValueCount());
          for (int i = 0; i < 5; i++) {
            assertEquals(vector.getObject(i), ((VarCharVector)decoded).getObject(i));
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

      Dictionary dictionary = new Dictionary(dictionaryVector, new DictionaryEncoding(1L, false, null));


      try (final ValueVector encoded = (FieldVector) DictionaryEncoder.encode(vector, dictionary)) {
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

  private void writeListVector(UnionListWriter writer, int[] values) {
    writer.startList();
    for (int v: values) {
      writer.integer().writeInt(v);
    }
    writer.endList();
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

      try (final ValueVector encoded = (FieldVector) DictionaryEncoder.encode(vector, dictionary)) {
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

  private void writeStructVector(NullableStructWriter writer, int value1, long value2) {
    writer.start();
    writer.integer("f0").writeInt(value1);
    writer.bigInt("f1").writeBigInt(value2);
    writer.end();
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
}
