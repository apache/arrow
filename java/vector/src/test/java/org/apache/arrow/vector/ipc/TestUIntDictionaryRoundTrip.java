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
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.channels.Channels;
import java.util.Arrays;
import java.util.Collection;
import java.util.Map;
import java.util.function.ToIntBiFunction;

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
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

/**
 * Test the round-trip of dictionary encoding,
 * with unsigned integer as indices.
 */
@RunWith(Parameterized.class)
public class TestUIntDictionaryRoundTrip {

  private final boolean streamMode;

  public TestUIntDictionaryRoundTrip(boolean streamMode) {
    this.streamMode = streamMode;
  }

  private BufferAllocator allocator;

  private DictionaryProvider.MapDictionaryProvider dictionaryProvider;

  @Before
  public void init() {
    allocator = new RootAllocator(Long.MAX_VALUE);
    dictionaryProvider = new DictionaryProvider.MapDictionaryProvider();
  }

  @After
  public void terminate() throws Exception {
    allocator.close();
  }

  private byte[] writeData(FieldVector encodedVector) throws IOException {
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
        assertArrayEquals(expectedDictItems[i].getBytes(), dictVector.get(i));
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

  @Test
  public void testUInt1RoundTrip() throws IOException {
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
      byte[] data = writeData(encodedVector1);
      readData(
          data, encodedVector1.getField(), (vector, index) -> (int) ((UInt1Vector) vector).getValueAsLong(index),
          8L, indices, dictionaryItems);
    }
  }

  @Test
  public void testUInt2RoundTrip() throws IOException {
    try (VarCharVector dictionaryVector = new VarCharVector("dictionary", allocator);
        UInt2Vector encodedVector2 = (UInt2Vector) createEncodedVector(16, dictionaryVector)) {
      int[] indices = new int[]{1, 3, 5, 7, 9, UInt2Vector.MAX_UINT2};
      String[] dictItems = new String[UInt2Vector.MAX_UINT2];
      for (int i = 0; i < UInt2Vector.MAX_UINT2; i++) {
        dictItems[i] = String.valueOf(i);
      }

      setVector(encodedVector2, (char) 1, (char) 3, (char) 5, (char) 7, (char) 9, UInt2Vector.MAX_UINT2);
      setVector(dictionaryVector, dictItems);

      byte[] data = writeData(encodedVector2);
      readData(data, encodedVector2.getField(), (vector, index) -> (int) ((UInt2Vector) vector).getValueAsLong(index),
          16L, indices, dictItems);
    }
  }

  @Test
  public void testUInt4RoundTrip() throws IOException {
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
      byte[] data = writeData(encodedVector4);
      readData(data, encodedVector4.getField(), (vector, index) -> (int) ((UInt4Vector) vector).getValueAsLong(index),
          32L, indices, dictItems);
    }
  }

  @Test
  public void testUInt8RoundTrip() throws IOException {
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

      byte[] data = writeData(encodedVector8);
      readData(data, encodedVector8.getField(), (vector, index) -> (int) ((UInt8Vector) vector).getValueAsLong(index),
          64L, indices, dictItems);
    }
  }

  @Parameterized.Parameters(name = "stream mode = {0}")
  public static Collection<Object[]> getRepeat() {
    return Arrays.asList(
        new Object[]{true},
        new Object[]{false}
    );
  }
}
