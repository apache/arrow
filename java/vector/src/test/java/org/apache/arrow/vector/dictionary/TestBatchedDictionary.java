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

package org.apache.arrow.vector.dictionary;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.UInt2Vector;
import org.apache.arrow.vector.ValueVector;
import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.DictionaryEncoding;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

public class TestBatchedDictionary {

  private static final DictionaryEncoding DELTA =
      new DictionaryEncoding(42, false, new ArrowType.Int(16, false), true);
  private static final DictionaryEncoding SINGLE =
      new DictionaryEncoding(24, false, new ArrowType.Int(16, false));
  private static final byte[] FOO = "foo".getBytes(StandardCharsets.UTF_8);
  private static final byte[] BAR = "bar".getBytes(StandardCharsets.UTF_8);
  private static final byte[] BAZ = "baz".getBytes(StandardCharsets.UTF_8);

  private BufferAllocator allocator;

  private static List<ArrowType> validDictionaryTypes = Arrays.asList(
      new ArrowType.Utf8(),
      ArrowType.Binary.INSTANCE
  );
  private static List<ArrowType> invalidDictionaryTypes = Arrays.asList(
      new ArrowType.LargeUtf8(),
      new ArrowType.LargeBinary(),
      new ArrowType.Bool(),
      new ArrowType.Int(8, false)
  );
  private static List<ArrowType> validIndexTypes = Arrays.asList(
      new ArrowType.Int(16, false),
      new ArrowType.Int(8, false),
      new ArrowType.Int(32, false),
      new ArrowType.Int(64, false),
      new ArrowType.Int(16, true),
      new ArrowType.Int(8, true),
      new ArrowType.Int(32, true),
      new ArrowType.Int(64, true)
  );
  private static List<ArrowType> invalidIndexTypes = Arrays.asList(
      new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE),
      new ArrowType.Bool(),
      new ArrowType.Utf8()
  );

  @BeforeEach
  public void prepare() {
    allocator = new RootAllocator(1024 * 1024);
  }

  @AfterEach
  public void shutdown() {
    allocator.close();
  }

  public static Collection<Arguments> validTypes() {
    List<Arguments> params = new ArrayList<>();
    for (ArrowType dictType : validDictionaryTypes) {
      for (ArrowType indexType : validIndexTypes) {
        params.add(Arguments.arguments(dictType, indexType));
      }
    }
    return params;
  }

  public static Collection<Arguments> invalidTypes() {
    List<Arguments> params = new ArrayList<>();
    for (ArrowType dictType : invalidDictionaryTypes) {
      for (ArrowType indexType : validIndexTypes) {
        params.add(Arguments.arguments(dictType, indexType));
      }
    }
    for (ArrowType dictType : validDictionaryTypes) {
      for (ArrowType indexType : invalidIndexTypes) {
        params.add(Arguments.arguments(dictType, indexType));
      }
    }
    return params;
  }

  @ParameterizedTest
  @MethodSource("validTypes")
  public void testValidDictionaryTypes(ArrowType dictType, ArrowType indexType) throws IOException {
    new BatchedDictionary(
        "vector",
        DELTA,
        dictType,
        indexType,
        allocator
    ).close();
  }

  @ParameterizedTest
  @MethodSource("validTypes")
  public void testValidDictionaryVectors(ArrowType dictType, ArrowType indexType) throws IOException {
    try (FieldVector dictVector = new FieldType(false, dictType, SINGLE)
             .createNewSingleVector("d", allocator, null);
         FieldVector indexVector = new FieldType(false, indexType, SINGLE)
             .createNewSingleVector("i", allocator, null);) {
      new BatchedDictionary(
          dictVector,
          indexVector
      ).close();
    }
  }

  @ParameterizedTest
  @MethodSource("invalidTypes")
  public void testInvalidTypes(ArrowType dictType, ArrowType indexType) {
    assertThrows(IllegalArgumentException.class, () -> {
      new BatchedDictionary(
          "vector",
          SINGLE,
          dictType,
          indexType,
          allocator
      );
    });
  }

  @ParameterizedTest
  @MethodSource("invalidTypes")
  public void testInvalidValidVectors(ArrowType dictType, ArrowType indexType) {
    assertThrows(IllegalArgumentException.class, () -> {
      try (FieldVector dictVector = new FieldType(false, dictType, SINGLE)
               .createNewSingleVector("d", allocator, null);
           FieldVector indexVector = new FieldType(false, indexType, SINGLE)
               .createNewSingleVector("i", allocator, null);) {
        new BatchedDictionary(
            dictVector,
            indexVector
        ).close();
      }
    });
  }

  @Test
  public void testSuffix() throws IOException {
    try (BatchedDictionary dictionary = new BatchedDictionary(
        "vector",
        SINGLE,
        validDictionaryTypes.get(0),
        validIndexTypes.get(0),
        allocator
    )) {
      assertEquals("vector-dictionary", dictionary.getVector().getField().getName());
      assertEquals("vector", dictionary.getIndexVector().getField().getName());
    }
  }

  @Test
  public void testTypedUnique() throws IOException {
    try (BatchedDictionary dictionary = new BatchedDictionary(
        "vector",
        SINGLE,
        validDictionaryTypes.get(0),
        validIndexTypes.get(0),
        allocator
    )) {
      dictionary.setSafe(0, FOO);
      dictionary.setSafe(1, BAR);
      dictionary.setSafe(2, BAZ);
      dictionary.mark();
      assertEquals(3, dictionary.getVector().getValueCount());
      assertEquals(0, dictionary.getIndexVector().getValueCount());
      dictionary.getIndexVector().setValueCount(3);
      assertDecoded(dictionary, "foo", "bar", "baz");
    }
  }

  @Test
  public void testExistingUnique() throws IOException {
    List<FieldVector> vectors = existingVectors(SINGLE);
    try (BatchedDictionary dictionary = new BatchedDictionary(
        vectors.get(0),
        vectors.get(1)
    )) {
      dictionary.setSafe(0, FOO);
      dictionary.setSafe(1, BAR);
      dictionary.setSafe(2, BAZ);
      dictionary.mark();
      assertEquals(3, dictionary.getVector().getValueCount());
      assertEquals(0, dictionary.getIndexVector().getValueCount());
      dictionary.getIndexVector().setValueCount(3);
      assertDecoded(dictionary, "foo", "bar", "baz");
    }
    vectors.forEach(vector -> vector.close());
  }

  @Test
  public void testTypedUniqueNulls() throws IOException {
    try (BatchedDictionary dictionary = new BatchedDictionary(
        "vector",
        SINGLE,
        validDictionaryTypes.get(0),
        validIndexTypes.get(0),
        allocator
    )) {
      dictionary.setNull(0);
      dictionary.setSafe(1, BAR);
      dictionary.setNull(2);
      dictionary.mark();
      assertEquals(1, dictionary.getVector().getValueCount());
      assertEquals(0, dictionary.getIndexVector().getValueCount());
      dictionary.getIndexVector().setValueCount(3);
      assertDecoded(dictionary, null, "bar", null);
    }
  }

  @Test
  public void testExistingUniqueNulls() throws IOException {
    List<FieldVector> vectors = existingVectors(SINGLE);
    try (BatchedDictionary dictionary = new BatchedDictionary(
        vectors.get(0),
        vectors.get(1)
    )) {
      dictionary.setNull(0);
      dictionary.setSafe(1, BAR);
      dictionary.setNull(2);
      dictionary.mark();
      assertEquals(1, dictionary.getVector().getValueCount());
      assertEquals(0, dictionary.getIndexVector().getValueCount());
      dictionary.getIndexVector().setValueCount(3);
      assertDecoded(dictionary, null, "bar", null);
    }
    vectors.forEach(vector -> vector.close());
  }

  @Test
  public void testTypedReused() throws IOException {
    try (BatchedDictionary dictionary = new BatchedDictionary(
        "vector",
        SINGLE,
        validDictionaryTypes.get(0),
        validIndexTypes.get(0),
        allocator
    )) {
      dictionary.setSafe(0, FOO);
      dictionary.setSafe(1, BAR);
      dictionary.setSafe(2, FOO);
      dictionary.setSafe(3, FOO);
      dictionary.mark();
      assertEquals(2, dictionary.getVector().getValueCount());
      assertEquals(0, dictionary.getIndexVector().getValueCount());
      dictionary.getIndexVector().setValueCount(4);
      assertDecoded(dictionary, "foo", "bar", "foo", "foo");
    }
  }

  @Test
  public void testTypedResetReplacement() throws IOException {
    try (BatchedDictionary dictionary = new BatchedDictionary(
        "vector",
        SINGLE,
        validDictionaryTypes.get(0),
        validIndexTypes.get(0),
        allocator
    )) {
      dictionary.setSafe(0, FOO);
      dictionary.setSafe(1, BAR);
      dictionary.mark();
      assertEquals(2, dictionary.getVector().getValueCount());
      assertEquals(0, dictionary.getIndexVector().getValueCount());
      dictionary.getIndexVector().setValueCount(2);
      assertDecoded(dictionary, "foo", "bar");

      dictionary.reset();
      assertEquals(0, dictionary.getHashTable().size);
      dictionary.setSafe(0, BAZ);
      dictionary.setNull(1);
      dictionary.mark();
      assertEquals(1, dictionary.getVector().getValueCount());
      assertEquals(0, dictionary.getIndexVector().getValueCount());
      dictionary.getIndexVector().setValueCount(2);
      assertDecoded(dictionary, "baz", null);
    }
  }

  @Test
  public void testTypedResetDelta() throws IOException {
    try (BatchedDictionary dictionary = new BatchedDictionary(
        "vector",
        DELTA,
        validDictionaryTypes.get(0),
        validIndexTypes.get(0),
        allocator
    )) {
      dictionary.setSafe(0, FOO);
      dictionary.setSafe(1, BAR);
      dictionary.mark();
      assertEquals(2, dictionary.getVector().getValueCount());
      assertEquals(0, dictionary.getIndexVector().getValueCount());
      dictionary.getIndexVector().setValueCount(2);
      assertDecoded(dictionary, "foo", "bar");

      dictionary.reset();
      assertEquals(2, dictionary.getHashTable().size);
      dictionary.setSafe(0, BAZ);
      dictionary.setSafe(1, FOO);
      dictionary.setSafe(2, BAR);
      dictionary.mark();
      assertEquals(1, dictionary.getVector().getValueCount());
      assertEquals(0, dictionary.getIndexVector().getValueCount());
      dictionary.getIndexVector().setValueCount(3);
      assertEquals(3, dictionary.getHashTable().size);

      // on read the dictionaries must be merged. Let's look at the int index.
      UInt2Vector index = (UInt2Vector) dictionary.getIndexVector();
      assertEquals(2, index.get(0));
      assertEquals(0, index.get(1));
      assertEquals(1, index.get(2));
    }
  }

  @Test
  public void testTypedNullData() throws IOException {
    try (BatchedDictionary dictionary = new BatchedDictionary(
        "vector",
        SINGLE,
        validDictionaryTypes.get(0),
        validIndexTypes.get(0),
        allocator
    )) {
      dictionary.setSafe(0, null);
      dictionary.setSafe(1, BAR);
      dictionary.mark();
      assertEquals(1, dictionary.getVector().getValueCount());
      assertEquals(0, dictionary.getIndexVector().getValueCount());
      dictionary.getIndexVector().setValueCount(2);
      assertDecoded(dictionary, null, "bar");
    }
  }

  void assertDecoded(BatchedDictionary dictionary, String... expected) {
    try (ValueVector decoded = DictionaryEncoder.decode(dictionary.getIndexVector(), dictionary)) {
      assertEquals(expected.length, decoded.getValueCount());
      for (int i = 0; i < expected.length; i++) {
        if (expected[i] == null) {
          assertNull(decoded.getObject(i));
        } else {
          assertNotNull(decoded.getObject(i));
          assertEquals(expected[i], decoded.getObject(i).toString());
        }
      }
    }
  }

  private List<FieldVector> existingVectors(DictionaryEncoding encoding) {
    FieldVector dictionaryVector = new FieldType(false, validDictionaryTypes.get(0), null)
        .createNewSingleVector("vector-dictionary", allocator, null);
    FieldVector indexVector = new FieldType(true, validIndexTypes.get(0), encoding)
        .createNewSingleVector("fector", allocator, null);
    return Arrays.asList(new FieldVector[] { dictionaryVector, indexVector });
  }
}
