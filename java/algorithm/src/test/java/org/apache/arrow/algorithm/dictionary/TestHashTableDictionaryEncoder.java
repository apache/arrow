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

package org.apache.arrow.algorithm.dictionary;

import static junit.framework.TestCase.assertTrue;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Random;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VarBinaryVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.dictionary.Dictionary;
import org.apache.arrow.vector.dictionary.DictionaryEncoder;
import org.apache.arrow.vector.types.pojo.DictionaryEncoding;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * Test cases for {@link HashTableDictionaryEncoder}.
 */
public class TestHashTableDictionaryEncoder {

  private final int VECTOR_LENGTH = 50;

  private final int DICTIONARY_LENGTH = 10;

  private BufferAllocator allocator;

  byte[] zero = "000".getBytes(StandardCharsets.UTF_8);
  byte[] one = "111".getBytes(StandardCharsets.UTF_8);
  byte[] two = "222".getBytes(StandardCharsets.UTF_8);

  byte[][] data = new byte[][]{zero, one, two};

  @Before
  public void prepare() {
    allocator = new RootAllocator(1024 * 1024);
  }

  @After
  public void shutdown() {
    allocator.close();
  }

  @Test
  public void testEncodeAndDecode() {
    Random random = new Random();
    try (VarCharVector rawVector = new VarCharVector("original vector", allocator);
         IntVector encodedVector = new IntVector("encoded vector", allocator);
         VarCharVector dictionary = new VarCharVector("dictionary", allocator)) {

      // set up dictionary
      dictionary.allocateNew();
      for (int i = 0; i < DICTIONARY_LENGTH; i++) {
        // encode "i" as i
        dictionary.setSafe(i, String.valueOf(i).getBytes(StandardCharsets.UTF_8));
      }
      dictionary.setValueCount(DICTIONARY_LENGTH);

      // set up raw vector
      rawVector.allocateNew(10 * VECTOR_LENGTH, VECTOR_LENGTH);
      for (int i = 0; i < VECTOR_LENGTH; i++) {
        int val = (random.nextInt() & Integer.MAX_VALUE) % DICTIONARY_LENGTH;
        rawVector.set(i, String.valueOf(val).getBytes(StandardCharsets.UTF_8));
      }
      rawVector.setValueCount(VECTOR_LENGTH);

      HashTableDictionaryEncoder<IntVector, VarCharVector> encoder =
              new HashTableDictionaryEncoder<>(dictionary, false);

      // perform encoding
      encodedVector.allocateNew();
      encoder.encode(rawVector, encodedVector);

      // verify encoding results
      assertEquals(rawVector.getValueCount(), encodedVector.getValueCount());
      for (int i = 0; i < VECTOR_LENGTH; i++) {
        assertArrayEquals(rawVector.get(i), String.valueOf(encodedVector.get(i)).getBytes(StandardCharsets.UTF_8));
      }

      // perform decoding
      Dictionary dict = new Dictionary(dictionary, new DictionaryEncoding(1L, false, null));
      try (VarCharVector decodedVector = (VarCharVector) DictionaryEncoder.decode(encodedVector, dict)) {

        // verify decoding results
        assertEquals(encodedVector.getValueCount(), decodedVector.getValueCount());
        for (int i = 0; i < VECTOR_LENGTH; i++) {
          assertArrayEquals(String.valueOf(encodedVector.get(i)).getBytes(StandardCharsets.UTF_8),
              decodedVector.get(i));
        }
      }
    }
  }

  @Test
  public void testEncodeAndDecodeWithNull() {
    Random random = new Random();
    try (VarCharVector rawVector = new VarCharVector("original vector", allocator);
         IntVector encodedVector = new IntVector("encoded vector", allocator);
         VarCharVector dictionary = new VarCharVector("dictionary", allocator)) {

      // set up dictionary
      dictionary.allocateNew();
      dictionary.setNull(0);
      for (int i = 1; i < DICTIONARY_LENGTH; i++) {
        // encode "i" as i
        dictionary.setSafe(i, String.valueOf(i).getBytes(StandardCharsets.UTF_8));
      }
      dictionary.setValueCount(DICTIONARY_LENGTH);

      // set up raw vector
      rawVector.allocateNew(10 * VECTOR_LENGTH, VECTOR_LENGTH);
      for (int i = 0; i < VECTOR_LENGTH; i++) {
        if (i % 10 == 0) {
          rawVector.setNull(i);
        } else {
          int val = (random.nextInt() & Integer.MAX_VALUE) % (DICTIONARY_LENGTH - 1) + 1;
          rawVector.set(i, String.valueOf(val).getBytes(StandardCharsets.UTF_8));
        }
      }
      rawVector.setValueCount(VECTOR_LENGTH);

      HashTableDictionaryEncoder<IntVector, VarCharVector> encoder =
              new HashTableDictionaryEncoder<>(dictionary, true);

      // perform encoding
      encodedVector.allocateNew();
      encoder.encode(rawVector, encodedVector);

      // verify encoding results
      assertEquals(rawVector.getValueCount(), encodedVector.getValueCount());
      for (int i = 0; i < VECTOR_LENGTH; i++) {
        if (i % 10 == 0) {
          assertEquals(0, encodedVector.get(i));
        } else {
          assertArrayEquals(rawVector.get(i), String.valueOf(encodedVector.get(i)).getBytes(StandardCharsets.UTF_8));
        }
      }

      // perform decoding
      Dictionary dict = new Dictionary(dictionary, new DictionaryEncoding(1L, false, null));
      try (VarCharVector decodedVector = (VarCharVector) DictionaryEncoder.decode(encodedVector, dict)) {
        // verify decoding results
        assertEquals(encodedVector.getValueCount(), decodedVector.getValueCount());
        for (int i = 0; i < VECTOR_LENGTH; i++) {
          if (i % 10 == 0) {
            assertTrue(decodedVector.isNull(i));
          } else {
            assertArrayEquals(String.valueOf(encodedVector.get(i)).getBytes(StandardCharsets.UTF_8),
                decodedVector.get(i));
          }
        }
      }
    }
  }

  @Test
  public void testEncodeNullWithoutNullInDictionary() {
    try (VarCharVector rawVector = new VarCharVector("original vector", allocator);
         IntVector encodedVector = new IntVector("encoded vector", allocator);
         VarCharVector dictionary = new VarCharVector("dictionary", allocator)) {

      // set up dictionary, with no null in it.
      dictionary.allocateNew();
      for (int i = 0; i < DICTIONARY_LENGTH; i++) {
        // encode "i" as i
        dictionary.setSafe(i, String.valueOf(i).getBytes(StandardCharsets.UTF_8));
      }
      dictionary.setValueCount(DICTIONARY_LENGTH);

      // the vector to encode has a null inside.
      rawVector.allocateNew(1);
      rawVector.setNull(0);
      rawVector.setValueCount(1);

      encodedVector.allocateNew();

      HashTableDictionaryEncoder<IntVector, VarCharVector> encoder =
              new HashTableDictionaryEncoder<>(dictionary, true);

      // the encoder should encode null, but no null in the dictionary,
      // so an exception should be thrown.
      assertThrows(IllegalArgumentException.class, () -> {
        encoder.encode(rawVector, encodedVector);
      });
    }
  }

  @Test
  public void testEncodeStrings() {
    // Create a new value vector
    try (final VarCharVector vector = new VarCharVector("foo", allocator);
         final IntVector encoded = new IntVector("encoded", allocator);
         final VarCharVector dictionaryVector = new VarCharVector("dict", allocator)) {

      vector.allocateNew(512, 5);
      encoded.allocateNew();

      // set some values
      vector.setSafe(0, zero, 0, zero.length);
      vector.setSafe(1, one, 0, one.length);
      vector.setSafe(2, one, 0, one.length);
      vector.setSafe(3, two, 0, two.length);
      vector.setSafe(4, zero, 0, zero.length);
      vector.setValueCount(5);

      // set some dictionary values
      dictionaryVector.allocateNew(512, 3);
      dictionaryVector.setSafe(0, zero, 0, one.length);
      dictionaryVector.setSafe(1, one, 0, two.length);
      dictionaryVector.setSafe(2, two, 0, zero.length);
      dictionaryVector.setValueCount(3);

      HashTableDictionaryEncoder<IntVector, VarCharVector> encoder =
              new HashTableDictionaryEncoder<>(dictionaryVector);
      encoder.encode(vector, encoded);

      // verify indices
      assertEquals(5, encoded.getValueCount());
      assertEquals(0, encoded.get(0));
      assertEquals(1, encoded.get(1));
      assertEquals(1, encoded.get(2));
      assertEquals(2, encoded.get(3));
      assertEquals(0, encoded.get(4));

      // now run through the decoder and verify we get the original back
      Dictionary dict = new Dictionary(dictionaryVector, new DictionaryEncoding(1L, false, null));
      try (VarCharVector decoded = (VarCharVector) DictionaryEncoder.decode(encoded, dict)) {

        assertEquals(vector.getValueCount(), decoded.getValueCount());
        for (int i = 0; i < 5; i++) {
          assertEquals(vector.getObject(i), decoded.getObject(i));
        }
      }
    }
  }

  @Test
  public void testEncodeLargeVector() {
    // Create a new value vector
    try (final VarCharVector vector = new VarCharVector("foo", allocator);
         final IntVector encoded = new IntVector("encoded", allocator);
         final VarCharVector dictionaryVector = new VarCharVector("dict", allocator)) {
      vector.allocateNew();
      encoded.allocateNew();

      int count = 10000;

      for (int i = 0; i < 10000; ++i) {
        vector.setSafe(i, data[i % 3], 0, data[i % 3].length);
      }
      vector.setValueCount(count);

      dictionaryVector.allocateNew(512, 3);
      dictionaryVector.setSafe(0, zero, 0, one.length);
      dictionaryVector.setSafe(1, one, 0, two.length);
      dictionaryVector.setSafe(2, two, 0, zero.length);
      dictionaryVector.setValueCount(3);

      HashTableDictionaryEncoder<IntVector, VarCharVector> encoder =
              new HashTableDictionaryEncoder<>(dictionaryVector);
      encoder.encode(vector, encoded);

      assertEquals(count, encoded.getValueCount());
      for (int i = 0; i < count; ++i) {
        assertEquals(i % 3, encoded.get(i));
      }

      // now run through the decoder and verify we get the original back
      Dictionary dict = new Dictionary(dictionaryVector, new DictionaryEncoding(1L, false, null));
      try (VarCharVector decoded = (VarCharVector) DictionaryEncoder.decode(encoded, dict)) {
        assertEquals(vector.getClass(), decoded.getClass());
        assertEquals(vector.getValueCount(), decoded.getValueCount());
        for (int i = 0; i < count; ++i) {
          assertEquals(vector.getObject(i), decoded.getObject(i));
        }
      }
    }
  }

  @Test
  public void testEncodeBinaryVector() {
    // Create a new value vector
    try (final VarBinaryVector vector = new VarBinaryVector("foo", allocator);
         final VarBinaryVector dictionaryVector = new VarBinaryVector("dict", allocator);
         final IntVector encoded = new IntVector("encoded", allocator)) {
      vector.allocateNew(512, 5);
      vector.allocateNew();
      encoded.allocateNew();

      // set some values
      vector.setSafe(0, zero, 0, zero.length);
      vector.setSafe(1, one, 0, one.length);
      vector.setSafe(2, one, 0, one.length);
      vector.setSafe(3, two, 0, two.length);
      vector.setSafe(4, zero, 0, zero.length);
      vector.setValueCount(5);

      // set some dictionary values
      dictionaryVector.allocateNew(512, 3);
      dictionaryVector.setSafe(0, zero, 0, one.length);
      dictionaryVector.setSafe(1, one, 0, two.length);
      dictionaryVector.setSafe(2, two, 0, zero.length);
      dictionaryVector.setValueCount(3);

      HashTableDictionaryEncoder<IntVector, VarBinaryVector> encoder =
              new HashTableDictionaryEncoder<>(dictionaryVector);
      encoder.encode(vector, encoded);

      assertEquals(5, encoded.getValueCount());
      assertEquals(0, encoded.get(0));
      assertEquals(1, encoded.get(1));
      assertEquals(1, encoded.get(2));
      assertEquals(2, encoded.get(3));
      assertEquals(0, encoded.get(4));

      // now run through the decoder and verify we get the original back
      Dictionary dict = new Dictionary(dictionaryVector, new DictionaryEncoding(1L, false, null));
      try (VarBinaryVector decoded = (VarBinaryVector) DictionaryEncoder.decode(encoded, dict)) {

        assertEquals(vector.getClass(), decoded.getClass());
        assertEquals(vector.getValueCount(), decoded.getValueCount());
        for (int i = 0; i < 5; i++) {
          assertTrue(Arrays.equals(vector.getObject(i), decoded.getObject(i)));
        }
      }
    }
  }
}
