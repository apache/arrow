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

  private VarCharVector dictionaryVector;

  private DictionaryProvider.MapDictionaryProvider dictionaryProvider;

  @Before
  public void init() {
    allocator = new RootAllocator(Long.MAX_VALUE);
    dictionaryVector = new VarCharVector("dict vector", allocator);
    setVector(dictionaryVector, "0", "1", "2", "3", "4", "5", "6", "7", "8", "9");

    dictionaryProvider = new DictionaryProvider.MapDictionaryProvider();
  }

  @After
  public void terminate() throws Exception {
    dictionaryVector.close();
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
      long dictionaryID) throws IOException {
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
      assertEquals(5, encodedVector.getValueCount());

      assertEquals(1, valGetter.applyAsInt(encodedVector, 0));
      assertEquals(3, valGetter.applyAsInt(encodedVector, 1));
      assertEquals(5, valGetter.applyAsInt(encodedVector, 2));
      assertEquals(7, valGetter.applyAsInt(encodedVector, 3));
      assertEquals(9, valGetter.applyAsInt(encodedVector, 4));

      // verify dictionary
      Map<Long, Dictionary> dictVectors = reader.getDictionaryVectors();
      assertEquals(1, dictVectors.size());
      Dictionary dictionary = dictVectors.get(dictionaryID);
      assertNotNull(dictionary);

      assertTrue(dictionary.getVector() instanceof VarCharVector);
      VarCharVector dictVector = (VarCharVector) dictionary.getVector();
      assertEquals(10, dictVector.getValueCount());
      for (int i = 0; i < dictVector.getValueCount(); i++) {
        assertArrayEquals(String.valueOf(i).getBytes(), dictVector.get(i));
      }
    }
  }

  private ValueVector createEncodedVector(int bitWidth) {
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
    try (UInt1Vector encodedVector1 = (UInt1Vector) createEncodedVector(8)) {
      setVector(encodedVector1, (byte) 1, (byte) 3, (byte) 5, (byte) 7, (byte) 9);
      byte[] data = writeData(encodedVector1);
      readData(data, encodedVector1.getField(), (vector, index) -> ((UInt1Vector) vector).get(index), 8L);
    }
  }

  @Test
  public void testUInt2RoundTrip() throws IOException {
    try (UInt2Vector encodedVector2 = (UInt2Vector) createEncodedVector(16)) {
      setVector(encodedVector2, (char) 1, (char) 3, (char) 5, (char) 7, (char) 9);
      byte[] data = writeData(encodedVector2);
      readData(data, encodedVector2.getField(), (vector, index) -> ((UInt2Vector) vector).get(index), 16L);
    }
  }

  @Test
  public void testUInt4RoundTrip() throws IOException {
    try (UInt4Vector encodedVector4 = (UInt4Vector) createEncodedVector(32)) {
      setVector(encodedVector4, 1, 3, 5, 7, 9);
      byte[] data = writeData(encodedVector4);
      readData(data, encodedVector4.getField(), (vector, index) -> ((UInt4Vector) vector).get(index), 32L);
    }
  }

  @Test
  public void testUInt8RoundTrip() throws IOException {
    try (UInt8Vector encodedVector8 = (UInt8Vector) createEncodedVector(64)) {
      setVector(encodedVector8, 1L, 3L, 5L, 7L, 9L);
      byte[] data = writeData(encodedVector8);
      readData(data, encodedVector8.getField(), (vector, index) -> (int) ((UInt8Vector) vector).get(index), 64L);
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
