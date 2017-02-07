/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.arrow.vector;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.complex.DictionaryVector;
import org.apache.arrow.vector.types.Dictionary;
import org.apache.arrow.vector.types.Types.MinorType;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.nio.charset.StandardCharsets;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

public class TestDictionaryVector {

  private BufferAllocator allocator;

  byte[] zero = "foo".getBytes(StandardCharsets.UTF_8);
  byte[] one  = "bar".getBytes(StandardCharsets.UTF_8);
  byte[] two  = "baz".getBytes(StandardCharsets.UTF_8);

  @Before
  public void init() {
    allocator = new DirtyRootAllocator(Long.MAX_VALUE, (byte) 100);
  }

  @After
  public void terminate() throws Exception {
    allocator.close();
  }

  @Test
  public void testEncodeStringsWithGeneratedDictionary() {
    // Create a new value vector
    try (final NullableVarCharVector vector = (NullableVarCharVector) MinorType.VARCHAR.getNewVector("foo", allocator, null)) {
      final NullableVarCharVector.Mutator m = vector.getMutator();
      vector.allocateNew(512, 5);

      // set some values
      m.setSafe(0, zero, 0, zero.length);
      m.setSafe(1, one, 0, one.length);
      m.setSafe(2, one, 0, one.length);
      m.setSafe(3, two, 0, two.length);
      m.setSafe(4, zero, 0, zero.length);
      m.setValueCount(5);

      DictionaryVector encoded = DictionaryVector.encode(vector);

      try {
        // verify values in the dictionary
        ValueVector dictionary = encoded.getDictionaryVector();
        assertEquals(vector.getClass(), dictionary.getClass());

        NullableVarCharVector.Accessor dictionaryAccessor = ((NullableVarCharVector) dictionary).getAccessor();
        assertEquals(3, dictionaryAccessor.getValueCount());
        assertArrayEquals(zero, dictionaryAccessor.get(0));
        assertArrayEquals(one, dictionaryAccessor.get(1));
        assertArrayEquals(two, dictionaryAccessor.get(2));

        // verify indices
        ValueVector indices = encoded.getIndexVector();
        assertEquals(NullableIntVector.class, indices.getClass());

        NullableIntVector.Accessor indexAccessor = ((NullableIntVector) indices).getAccessor();
        assertEquals(5, indexAccessor.getValueCount());
        assertEquals(0, indexAccessor.get(0));
        assertEquals(1, indexAccessor.get(1));
        assertEquals(1, indexAccessor.get(2));
        assertEquals(2, indexAccessor.get(3));
        assertEquals(0, indexAccessor.get(4));

        // now run through the decoder and verify we get the original back
        try (ValueVector decoded = DictionaryVector.decode(indices, encoded.getDictionary())) {
          assertEquals(vector.getClass(), decoded.getClass());
          assertEquals(vector.getAccessor().getValueCount(), decoded.getAccessor().getValueCount());
          for (int i = 0; i < 5; i++) {
            assertEquals(vector.getAccessor().getObject(i), decoded.getAccessor().getObject(i));
          }
        }
      } finally {
        encoded.getDictionaryVector().close();
        encoded.getIndexVector().close();
      }
    }
  }

  @Test
  public void testEncodeStringsWithProvidedDictionary() {
    // Create a new value vector
    try (final NullableVarCharVector vector = (NullableVarCharVector) MinorType.VARCHAR.getNewVector("foo", allocator, null);
         final NullableVarCharVector dictionary = (NullableVarCharVector) MinorType.VARCHAR.getNewVector("dict", allocator, null)) {
      final NullableVarCharVector.Mutator m = vector.getMutator();
      vector.allocateNew(512, 5);

      // set some values
      m.setSafe(0, zero, 0, zero.length);
      m.setSafe(1, one, 0, one.length);
      m.setSafe(2, one, 0, one.length);
      m.setSafe(3, two, 0, two.length);
      m.setSafe(4, zero, 0, zero.length);
      m.setValueCount(5);

      // set some dictionary values
      final NullableVarCharVector.Mutator m2 = dictionary.getMutator();
      dictionary.allocateNew(512, 3);
      m2.setSafe(0, zero, 0, zero.length);
      m2.setSafe(1, one, 0, one.length);
      m2.setSafe(2, two, 0, two.length);
      m2.setValueCount(3);

      try(final DictionaryVector encoded = DictionaryVector.encode(vector, new Dictionary(dictionary, false))) {
        // verify indices
        ValueVector indices = encoded.getIndexVector();
        assertEquals(NullableIntVector.class, indices.getClass());

        NullableIntVector.Accessor indexAccessor = ((NullableIntVector) indices).getAccessor();
        assertEquals(5, indexAccessor.getValueCount());
        assertEquals(0, indexAccessor.get(0));
        assertEquals(1, indexAccessor.get(1));
        assertEquals(1, indexAccessor.get(2));
        assertEquals(2, indexAccessor.get(3));
        assertEquals(0, indexAccessor.get(4));

        // now run through the decoder and verify we get the original back
        try (ValueVector decoded = DictionaryVector.decode(indices, encoded.getDictionary())) {
          assertEquals(vector.getClass(), decoded.getClass());
          assertEquals(vector.getAccessor().getValueCount(), decoded.getAccessor().getValueCount());
          for (int i = 0; i < 5; i++) {
            assertEquals(vector.getAccessor().getObject(i), decoded.getAccessor().getObject(i));
          }
        }
      }
    }
  }
}
