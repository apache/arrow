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

import static org.apache.arrow.vector.TestUtils.newNullableVarCharVector;
import static org.junit.Assert.assertEquals;

import java.nio.charset.StandardCharsets;

import org.apache.arrow.memory.BufferAllocator;
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
    try (final NullableVarCharVector vector = newNullableVarCharVector("foo", allocator);
         final NullableVarCharVector dictionaryVector = newNullableVarCharVector("dict", allocator);) {
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
      final NullableVarCharVector.Mutator m2 = dictionaryVector.getMutator();
      dictionaryVector.allocateNew(512, 3);
      m2.setSafe(0, zero, 0, zero.length);
      m2.setSafe(1, one, 0, one.length);
      m2.setSafe(2, two, 0, two.length);
      m2.setValueCount(3);

      Dictionary dictionary = new Dictionary(dictionaryVector, new DictionaryEncoding(1L, false, null));

      try (final ValueVector encoded = (FieldVector) DictionaryEncoder.encode(vector, dictionary)) {
        // verify indices
        assertEquals(NullableIntVector.class, encoded.getClass());

        NullableIntVector.Accessor indexAccessor = ((NullableIntVector) encoded).getAccessor();
        assertEquals(5, indexAccessor.getValueCount());
        assertEquals(0, indexAccessor.get(0));
        assertEquals(1, indexAccessor.get(1));
        assertEquals(1, indexAccessor.get(2));
        assertEquals(2, indexAccessor.get(3));
        assertEquals(0, indexAccessor.get(4));

        // now run through the decoder and verify we get the original back
        try (ValueVector decoded = DictionaryEncoder.decode(encoded, dictionary)) {
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
