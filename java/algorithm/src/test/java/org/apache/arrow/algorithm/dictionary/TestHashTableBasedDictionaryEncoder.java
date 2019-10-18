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
import static org.junit.Assert.assertNull;
import static org.junit.jupiter.api.Assertions.assertEquals;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VarCharVector;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * Test cases for {@link HashTableBasedDictionaryBuilder}.
 */
public class TestHashTableBasedDictionaryEncoder {

  private BufferAllocator allocator;

  @Before
  public void prepare() {
    allocator = new RootAllocator(1024 * 1024);
  }

  @After
  public void shutdown() {
    allocator.close();
  }

  @Test
  public void testBuildVariableWidthDictionaryWithNull() {
    try (VarCharVector vec = new VarCharVector("", allocator);
         VarCharVector dictionary = new VarCharVector("", allocator)) {

      vec.allocateNew(100, 10);
      vec.setValueCount(10);

      dictionary.allocateNew();

      // fill data
      vec.set(0, "hello".getBytes());
      vec.set(1, "abc".getBytes());
      vec.setNull(2);
      vec.set(3, "world".getBytes());
      vec.set(4, "12".getBytes());
      vec.set(5, "dictionary".getBytes());
      vec.setNull(6);
      vec.set(7, "hello".getBytes());
      vec.set(8, "good".getBytes());
      vec.set(9, "abc".getBytes());

      HashTableBasedDictionaryBuilder<VarCharVector> dictionaryBuilder =
              new HashTableBasedDictionaryBuilder<>(dictionary, true);

      int result = dictionaryBuilder.addValues(vec);

      assertEquals(7, result);
      assertEquals(7, dictionary.getValueCount());

      assertEquals("hello", new String(dictionary.get(0)));
      assertEquals("abc", new String(dictionary.get(1)));
      assertNull(dictionary.get(2));
      assertEquals("world", new String(dictionary.get(3)));
      assertEquals("12", new String(dictionary.get(4)));
      assertEquals("dictionary", new String(dictionary.get(5)));
      assertEquals("good", new String(dictionary.get(6)));
    }
  }

  @Test
  public void testBuildVariableWidthDictionaryWithoutNull() {
    try (VarCharVector vec = new VarCharVector("", allocator);
         VarCharVector dictionary = new VarCharVector("", allocator)) {

      vec.allocateNew(100, 10);
      vec.setValueCount(10);

      dictionary.allocateNew();

      // fill data
      vec.set(0, "hello".getBytes());
      vec.set(1, "abc".getBytes());
      vec.setNull(2);
      vec.set(3, "world".getBytes());
      vec.set(4, "12".getBytes());
      vec.set(5, "dictionary".getBytes());
      vec.setNull(6);
      vec.set(7, "hello".getBytes());
      vec.set(8, "good".getBytes());
      vec.set(9, "abc".getBytes());

      HashTableBasedDictionaryBuilder<VarCharVector> dictionaryBuilder =
              new HashTableBasedDictionaryBuilder<>(dictionary, false);

      int result = dictionaryBuilder.addValues(vec);

      assertEquals(6, result);
      assertEquals(6, dictionary.getValueCount());

      assertEquals("hello", new String(dictionary.get(0)));
      assertEquals("abc", new String(dictionary.get(1)));
      assertEquals("world", new String(dictionary.get(2)));
      assertEquals("12", new String(dictionary.get(3)));
      assertEquals("dictionary", new String(dictionary.get(4)));
      assertEquals("good", new String(dictionary.get(5)));

    }
  }

  @Test
  public void testBuildFixedWidthDictionaryWithNull() {
    try (IntVector vec = new IntVector("", allocator);
         IntVector dictionary = new IntVector("", allocator)) {
      vec.allocateNew(10);
      vec.setValueCount(10);

      dictionary.allocateNew();

      // fill data
      vec.set(0, 4);
      vec.set(1, 8);
      vec.set(2, 32);
      vec.set(3, 8);
      vec.set(4, 16);
      vec.set(5, 32);
      vec.setNull(6);
      vec.set(7, 4);
      vec.set(8, 4);
      vec.setNull(9);

      HashTableBasedDictionaryBuilder<IntVector> dictionaryBuilder =
              new HashTableBasedDictionaryBuilder<>(dictionary, true);

      int result = dictionaryBuilder.addValues(vec);

      assertEquals(5, result);
      assertEquals(5, dictionary.getValueCount());

      assertEquals(4, dictionary.get(0));
      assertEquals(8, dictionary.get(1));
      assertEquals(32, dictionary.get(2));
      assertEquals(16, dictionary.get(3));
      assertTrue(dictionary.isNull(4));
    }
  }

  @Test
  public void testBuildFixedWidthDictionaryWithoutNull() {
    try (IntVector vec = new IntVector("", allocator);
         IntVector dictionary = new IntVector("", allocator)) {
      vec.allocateNew(10);
      vec.setValueCount(10);

      dictionary.allocateNew();

      // fill data
      vec.set(0, 4);
      vec.set(1, 8);
      vec.set(2, 32);
      vec.set(3, 8);
      vec.set(4, 16);
      vec.set(5, 32);
      vec.setNull(6);
      vec.set(7, 4);
      vec.set(8, 4);
      vec.setNull(9);

      HashTableBasedDictionaryBuilder<IntVector> dictionaryBuilder =
              new HashTableBasedDictionaryBuilder<>(dictionary, false);

      int result = dictionaryBuilder.addValues(vec);

      assertEquals(4, result);
      assertEquals(4, dictionary.getValueCount());

      assertEquals(4, dictionary.get(0));
      assertEquals(8, dictionary.get(1));
      assertEquals(32, dictionary.get(2));
      assertEquals(16, dictionary.get(3));

    }
  }
}
