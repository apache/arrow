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

package org.apache.arrow.vector.ipc;

import static org.apache.arrow.vector.testing.ValueVectorDataPopulator.setVector;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.channels.Channels;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Map;
import java.util.function.ToIntBiFunction;
import java.util.stream.Stream;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.UInt1Vector;
import org.apache.arrow.vector.UInt2Vector;
import org.apache.arrow.vector.UInt4Vector;
import org.apache.arrow.vector.UInt8Vector;
import org.apache.arrow.vector.ValueVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.dictionary.Dictionary;
import org.apache.arrow.vector.dictionary.DictionaryProvider;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.DictionaryEncoding;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.arrow.vector.util.ByteArrayReadableSeekableByteChannel;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * Test the round-trip of dictionary encoding,
 * with unsigned integer as indices.
 */
public class TestUIntDictionaryRoundTrip {

  private BufferAllocator allocator;

  private DictionaryProvider.MapDictionaryProvider dictionaryProvider;

  @BeforeEach
  public void init() {
    allocator = new RootAllocator(Long.MAX_VALUE);
    dictionaryProvider = new DictionaryProvider.MapDictionaryProvider();
  }

  @AfterEach
  public void terminate() throws Exception {
    allocator.close();
  }

  private byte[] writeData(boolean streamMode, FieldVector encodedVector) throws IOException {
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    VectorSchemaRoot root =
        new VectorSchemaRoot(
            Arrays.asList(encodedVector.getField()), Arrays.asList(encodedVector), encodedVector.getValueCount());
    try (ArrowWriter writer = streamMode ?
        new ArrowStreamWriter(root, dictionaryProvider, out) :
        new ArrowFileWriter(root, dictionaryProvider, Channels.newChannel(out))) {
      writer.start();
      writer.writeBatch();
      writer.end();

      return out.toByteArray();
    }
  }

  private void readData(
      boolean streamMode,
      byte[] data,
      Field expectedField,
      ToIntBiFunction<ValueVector, Integer> valGetter,
      long dictionaryID,
      int[] expectedIndices,
      String[] expectedDictItems) throws IOException {
    try (ArrowReader reader = streamMode ?
             new ArrowStreamReader(new ByteArrayInputStream(data), allocator) :
             new ArrowFileReader(new SeekableReadChannel(new ByteArrayReadableSeekableByteChannel(data)), allocator)) {

      // verify schema
      Schema readSchema = reader.getVectorSchemaRoot().getSchema();
      assertEquals(1, readSchema.getFields().size());
      assertEquals(expectedField, readSchema.getFields().get(0));

      // verify vector schema root
      assertTrue(reader.loadNextBatch());
      VectorSchemaRoot root = reader.getVectorSchemaRoot();

      assertEquals(1, root.getFieldVectors().size());
      ValueVector encodedVector = root.getVector(0);
      assertEquals(expectedIndices.length, encodedVector.getValueCount());

      for (int i = 0; i < expectedIndices.length; i++) {
        assertEquals(expectedIndices[i], valGetter.applyAsInt(encodedVector, i));
      }

      // verify dictionary
      Map<Long, Dictionary> dictVectors = reader.getDictionaryVectors();
      assertEquals(1, dictVectors.size());
      Dictionary dictionary = dictVectors.get(dictionaryID);
      assertNotNull(dictionary);

      assertTrue(dictionary.getVector() instanceof VarCharVector);
      VarCharVector dictVector = (VarCharVector) dictionary.getVector();
      assertEquals(expectedDictItems.length, dictVector.getValueCount());
      for (int i = 0; i < dictVector.getValueCount(); i++) {
        assertArrayEquals(expectedDictItems[i].getBytes(StandardCharsets.UTF_8), dictVector.get(i));
      }
    }
  }

  private ValueVector createEncodedVector(int bitWidth, VarCharVector dictionaryVector) {
    final DictionaryEncoding dictionaryEncoding =
        new DictionaryEncoding(bitWidth, false, new ArrowType.Int(bitWidth, false));
    Dictionary dictionary = new Dictionary(dictionaryVector, dictionaryEncoding);
    dictionaryProvider.put(dictionary);

    final FieldType type =
        new FieldType(true, dictionaryEncoding.getIndexType(), dictionaryEncoding, null);
    final Field field = new Field("encoded", type, null);
    return field.createVector(allocator);
  }

  @ParameterizedTest(name = "stream mode = {0}")
  @MethodSource("getRepeat")
  public void testUInt1RoundTrip(boolean streamMode) throws IOException {
    final int vectorLength = UInt1Vector.MAX_UINT1 & UInt1Vector.PROMOTION_MASK;
    try (VarCharVector dictionaryVector = new VarCharVector("dictionary", allocator);
         UInt1Vector encodedVector1 = (UInt1Vector) createEncodedVector(8, dictionaryVector)) {
      int[] indices = new int[vectorLength];
      String[] dictionaryItems = new String[vectorLength];
      for (int i = 0; i < vectorLength; i++) {
        encodedVector1.setSafe(i, (byte) i);
        indices[i] = i;
        dictionaryItems[i] = String.valueOf(i);
      }
      encodedVector1.setValueCount(vectorLength);
      setVector(dictionaryVector, dictionaryItems);
      byte[] data = writeData(streamMode, encodedVector1);
      readData(streamMode, data, encodedVector1.getField(),
          (vector, index) -> (int) ((UInt1Vector) vector).getValueAsLong(index), 8L, indices, dictionaryItems);
    }
  }

  @ParameterizedTest(name = "stream mode = {0}")
  @MethodSource("getRepeat")
  public void testUInt2RoundTrip(boolean streamMode) throws IOException {
    try (VarCharVector dictionaryVector = new VarCharVector("dictionary", allocator);
        UInt2Vector encodedVector2 = (UInt2Vector) createEncodedVector(16, dictionaryVector)) {
      int[] indices = new int[]{1, 3, 5, 7, 9, UInt2Vector.MAX_UINT2};
      String[] dictItems = new String[UInt2Vector.MAX_UINT2];
      for (int i = 0; i < UInt2Vector.MAX_UINT2; i++) {
        dictItems[i] = String.valueOf(i);
      }

      setVector(encodedVector2, (char) 1, (char) 3, (char) 5, (char) 7, (char) 9, UInt2Vector.MAX_UINT2);
      setVector(dictionaryVector, dictItems);

      byte[] data = writeData(streamMode, encodedVector2);
      readData(streamMode, data, encodedVector2.getField(),
          (vector, index) -> (int) ((UInt2Vector) vector).getValueAsLong(index), 16L, indices, dictItems);
    }
  }

  @ParameterizedTest(name = "stream mode = {0}")
  @MethodSource("getRepeat")
  public void testUInt4RoundTrip(boolean streamMode) throws IOException {
    final int dictLength = 10;
    try (VarCharVector dictionaryVector = new VarCharVector("dictionary", allocator);
        UInt4Vector encodedVector4 = (UInt4Vector) createEncodedVector(32, dictionaryVector)) {
      int[] indices = new int[]{1, 3, 5, 7, 9};
      String[] dictItems = new String[dictLength];
      for (int i = 0; i < dictLength; i++) {
        dictItems[i] = String.valueOf(i);
      }

      setVector(encodedVector4, 1, 3, 5, 7, 9);
      setVector(dictionaryVector, dictItems);

      setVector(encodedVector4, 1, 3, 5, 7, 9);
      byte[] data = writeData(streamMode, encodedVector4);
      readData(streamMode, data, encodedVector4.getField(),
          (vector, index) -> (int) ((UInt4Vector) vector).getValueAsLong(index), 32L, indices, dictItems);
    }
  }

  @ParameterizedTest(name = "stream mode = {0}")
  @MethodSource("getRepeat")
  public void testUInt8RoundTrip(boolean streamMode) throws IOException {
    final int dictLength = 10;
    try (VarCharVector dictionaryVector = new VarCharVector("dictionary", allocator);
        UInt8Vector encodedVector8 = (UInt8Vector) createEncodedVector(64, dictionaryVector)) {
      int[] indices = new int[]{1, 3, 5, 7, 9};
      String[] dictItems = new String[dictLength];
      for (int i = 0; i < dictLength; i++) {
        dictItems[i] = String.valueOf(i);
      }

      setVector(encodedVector8, 1L, 3L, 5L, 7L, 9L);
      setVector(dictionaryVector, dictItems);

      byte[] data = writeData(streamMode, encodedVector8);
      readData(streamMode, data, encodedVector8.getField(),
          (vector, index) -> (int) ((UInt8Vector) vector).getValueAsLong(index), 64L, indices, dictItems);
    }
  }

  static Stream<Arguments> getRepeat() {
    return Stream.of(
        Arguments.of(true),
        Arguments.of(false)
    );
  }
}
