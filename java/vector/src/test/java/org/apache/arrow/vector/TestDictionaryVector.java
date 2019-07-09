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

import static org.apache.arrow.vector.TestUtils.newVarCharVector;
import static org.junit.Assert.assertEquals;

import java.nio.charset.StandardCharsets;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.complex.impl.UnionListWriter;
import org.apache.arrow.vector.dictionary.Dictionary;
import org.apache.arrow.vector.dictionary.DictionaryEncoder;
import org.apache.arrow.vector.types.pojo.DictionaryEncoding;
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

  @Test
  public void testEncodeList() {
    // Create a new value vector
    try (final ListVector vector = ListVector.empty("vector", allocator);
        final ListVector dictionaryVector = ListVector.empty("dict", allocator);) {

      UnionListWriter writer = vector.getWriter();
      writer.allocate();

      // set some values
      writer.setPosition(0);
      writer.startList();
      writer.integer().writeInt(10);
      writer.integer().writeInt(20);
      writer.endList();

      writer.setPosition(1);
      writer.startList();
      writer.integer().writeInt(10);
      writer.integer().writeInt(20);
      writer.endList();

      writer.setPosition(2);
      writer.startList();
      writer.integer().writeInt(10);
      writer.integer().writeInt(20);
      writer.endList();

      writer.setPosition(3);
      writer.startList();
      writer.integer().writeInt(30);
      writer.integer().writeInt(40);
      writer.integer().writeInt(50);
      writer.endList();

      writer.setPosition(4);
      writer.startList();
      writer.integer().writeInt(30);
      writer.integer().writeInt(40);
      writer.integer().writeInt(50);
      writer.endList();

      writer.setPosition(5);
      writer.startList();
      writer.integer().writeInt(10);
      writer.integer().writeInt(20);
      writer.endList();

      writer.setValueCount(6);

      UnionListWriter dictWriter = dictionaryVector.getWriter();
      dictWriter.allocate();

      dictWriter.setPosition(0);
      dictWriter.startList();
      dictWriter.integer().writeInt(10);
      dictWriter.integer().writeInt(20);
      dictWriter.endList();

      dictWriter.setPosition(1);
      dictWriter.startList();
      dictWriter.integer().writeInt(30);
      dictWriter.integer().writeInt(40);
      dictWriter.integer().writeInt(50);
      dictWriter.endList();

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
}
