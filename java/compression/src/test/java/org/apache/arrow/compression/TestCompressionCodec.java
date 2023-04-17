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

package org.apache.arrow.compression;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.channels.Channels;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.function.BiConsumer;
import java.util.stream.Stream;

import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.util.AutoCloseables;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VarBinaryVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.VectorUnloader;
import org.apache.arrow.vector.compression.CompressionCodec;
import org.apache.arrow.vector.compression.CompressionUtil;
import org.apache.arrow.vector.compression.NoCompressionCodec;
import org.apache.arrow.vector.dictionary.DictionaryProvider;
import org.apache.arrow.vector.ipc.ArrowFileReader;
import org.apache.arrow.vector.ipc.ArrowFileWriter;
import org.apache.arrow.vector.ipc.ArrowStreamReader;
import org.apache.arrow.vector.ipc.ArrowStreamWriter;
import org.apache.arrow.vector.ipc.message.ArrowFieldNode;
import org.apache.arrow.vector.ipc.message.IpcOption;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.arrow.vector.util.ByteArrayReadableSeekableByteChannel;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * Test cases for {@link CompressionCodec}s.
 */
class TestCompressionCodec {
  private BufferAllocator allocator;

  @BeforeEach
  void init() {
    allocator = new RootAllocator(Integer.MAX_VALUE);
  }

  @AfterEach
  void terminate() {
    allocator.close();
  }

  static Collection<Arguments> codecs() {
    List<Arguments> params = new ArrayList<>();

    int[] lengths = new int[] {10, 100, 1000};
    for (int len : lengths) {
      CompressionCodec dumbCodec = NoCompressionCodec.INSTANCE;
      params.add(Arguments.arguments(len, dumbCodec));

      CompressionCodec lz4Codec = new Lz4CompressionCodec();
      params.add(Arguments.arguments(len, lz4Codec));

      CompressionCodec zstdCodec = new ZstdCompressionCodec();
      params.add(Arguments.arguments(len, zstdCodec));
    }
    return params;
  }

  private List<ArrowBuf> compressBuffers(CompressionCodec codec, List<ArrowBuf> inputBuffers) {
    List<ArrowBuf> outputBuffers = new ArrayList<>(inputBuffers.size());
    for (ArrowBuf buf : inputBuffers) {
      outputBuffers.add(codec.compress(allocator, buf));
    }
    return outputBuffers;
  }

  private List<ArrowBuf> deCompressBuffers(CompressionCodec codec, List<ArrowBuf> inputBuffers) {
    List<ArrowBuf> outputBuffers = new ArrayList<>(inputBuffers.size());
    for (ArrowBuf buf : inputBuffers) {
      outputBuffers.add(codec.decompress(allocator, buf));
    }
    return outputBuffers;
  }

  @ParameterizedTest
  @MethodSource("codecs")
  void testCompressFixedWidthBuffers(int vectorLength, CompressionCodec codec) throws Exception {
    // prepare vector to compress
    IntVector origVec = new IntVector("vec", allocator);
    origVec.allocateNew(vectorLength);
    for (int i = 0; i < vectorLength; i++) {
      if (i % 10 == 0) {
        origVec.setNull(i);
      } else {
        origVec.set(i, i);
      }
    }
    origVec.setValueCount(vectorLength);
    int nullCount = origVec.getNullCount();

    // compress & decompress
    List<ArrowBuf> origBuffers = origVec.getFieldBuffers();
    List<ArrowBuf> compressedBuffers = compressBuffers(codec, origBuffers);
    List<ArrowBuf> decompressedBuffers = deCompressBuffers(codec, compressedBuffers);

    assertEquals(2, decompressedBuffers.size());

    // orchestrate new vector
    IntVector newVec = new IntVector("new vec", allocator);
    newVec.loadFieldBuffers(new ArrowFieldNode(vectorLength, nullCount), decompressedBuffers);

    // verify new vector
    assertEquals(vectorLength, newVec.getValueCount());
    for (int i = 0; i < vectorLength; i++) {
      if (i % 10 == 0) {
        assertTrue(newVec.isNull(i));
      } else {
        assertEquals(i, newVec.get(i));
      }
    }

    newVec.close();
    AutoCloseables.close(decompressedBuffers);
  }

  @ParameterizedTest
  @MethodSource("codecs")
  void testCompressVariableWidthBuffers(int vectorLength, CompressionCodec codec) throws Exception {
    // prepare vector to compress
    VarCharVector origVec = new VarCharVector("vec", allocator);
    origVec.allocateNew();
    for (int i = 0; i < vectorLength; i++) {
      if (i % 10 == 0) {
        origVec.setNull(i);
      } else {
        origVec.setSafe(i, String.valueOf(i).getBytes());
      }
    }
    origVec.setValueCount(vectorLength);
    int nullCount = origVec.getNullCount();

    // compress & decompress
    List<ArrowBuf> origBuffers = origVec.getFieldBuffers();
    List<ArrowBuf> compressedBuffers = compressBuffers(codec, origBuffers);
    List<ArrowBuf> decompressedBuffers = deCompressBuffers(codec, compressedBuffers);

    assertEquals(3, decompressedBuffers.size());

    // orchestrate new vector
    VarCharVector newVec = new VarCharVector("new vec", allocator);
    newVec.loadFieldBuffers(new ArrowFieldNode(vectorLength, nullCount), decompressedBuffers);

    // verify new vector
    assertEquals(vectorLength, newVec.getValueCount());
    for (int i = 0; i < vectorLength; i++) {
      if (i % 10 == 0) {
        assertTrue(newVec.isNull(i));
      } else {
        assertArrayEquals(String.valueOf(i).getBytes(), newVec.get(i));
      }
    }

    newVec.close();
    AutoCloseables.close(decompressedBuffers);
  }

  @ParameterizedTest
  @MethodSource("codecs")
  void testEmptyBuffer(int vectorLength, CompressionCodec codec) throws Exception {
    final VarBinaryVector origVec = new VarBinaryVector("vec", allocator);

    origVec.allocateNew(vectorLength);

    // Do not set any values (all missing)
    origVec.setValueCount(vectorLength);

    final List<ArrowBuf> origBuffers = origVec.getFieldBuffers();
    final List<ArrowBuf> compressedBuffers = compressBuffers(codec, origBuffers);
    final List<ArrowBuf> decompressedBuffers = deCompressBuffers(codec, compressedBuffers);

    // orchestrate new vector
    VarBinaryVector newVec = new VarBinaryVector("new vec", allocator);
    newVec.loadFieldBuffers(new ArrowFieldNode(vectorLength, vectorLength), decompressedBuffers);

    // verify new vector
    assertEquals(vectorLength, newVec.getValueCount());
    for (int i = 0; i < vectorLength; i++) {
      assertTrue(newVec.isNull(i));
    }

    newVec.close();
    AutoCloseables.close(decompressedBuffers);
  }

  private static Stream<CompressionUtil.CodecType> codecTypes() {
    return Arrays.stream(CompressionUtil.CodecType.values());
  }

  @ParameterizedTest
  @MethodSource("codecTypes")
  void testReadWriteStream(CompressionUtil.CodecType codec) throws Exception {
    withRoot(codec, (factory, root) -> {
      ByteArrayOutputStream compressedStream = new ByteArrayOutputStream();
      try (final ArrowStreamWriter writer = new ArrowStreamWriter(
          root, new DictionaryProvider.MapDictionaryProvider(),
          Channels.newChannel(compressedStream),
          IpcOption.DEFAULT, factory, codec)) {
        writer.start();
        writer.writeBatch();
        writer.end();
      } catch (IOException e) {
        throw new RuntimeException(e);
      }

      try (ArrowStreamReader reader = new ArrowStreamReader(
          new ByteArrayReadableSeekableByteChannel(compressedStream.toByteArray()), allocator, factory)) {
        assertTrue(reader.loadNextBatch());
        assertTrue(root.equals(reader.getVectorSchemaRoot()));
        assertFalse(reader.loadNextBatch());
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    });
  }

  @ParameterizedTest
  @MethodSource("codecTypes")
  void testReadWriteFile(CompressionUtil.CodecType codec) throws Exception {
    withRoot(codec, (factory, root) -> {
      ByteArrayOutputStream compressedStream = new ByteArrayOutputStream();
      try (final ArrowFileWriter writer = new ArrowFileWriter(
          root, new DictionaryProvider.MapDictionaryProvider(),
          Channels.newChannel(compressedStream),
          new HashMap<>(), IpcOption.DEFAULT, factory, codec)) {
        writer.start();
        writer.writeBatch();
        writer.end();
      } catch (IOException e) {
        throw new RuntimeException(e);
      }

      try (ArrowFileReader reader = new ArrowFileReader(
          new ByteArrayReadableSeekableByteChannel(compressedStream.toByteArray()), allocator, factory)) {
        assertTrue(reader.loadNextBatch());
        assertTrue(root.equals(reader.getVectorSchemaRoot()));
        assertFalse(reader.loadNextBatch());
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    });
  }

  /** Unloading a vector should not free source buffers. */
  @ParameterizedTest
  @MethodSource("codecTypes")
  void testUnloadCompressed(CompressionUtil.CodecType codec) {
    withRoot(codec, (factory, root) -> {
      root.getFieldVectors().forEach((vector) -> {
        Arrays.stream(vector.getBuffers(/*clear*/ false)).forEach((buf) -> {
          assertNotEquals(0, buf.getReferenceManager().getRefCount());
        });
      });

      final VectorUnloader unloader = new VectorUnloader(
          root, /*includeNullCount*/ true, factory.createCodec(codec), /*alignBuffers*/ true);
      unloader.getRecordBatch().close();

      root.getFieldVectors().forEach((vector) -> {
        Arrays.stream(vector.getBuffers(/*clear*/ false)).forEach((buf) -> {
          assertNotEquals(0, buf.getReferenceManager().getRefCount());
        });
      });
    });
  }

  void withRoot(CompressionUtil.CodecType codec, BiConsumer<CompressionCodec.Factory, VectorSchemaRoot> testBody) {
    final Schema schema = new Schema(Arrays.asList(
        Field.nullable("ints", new ArrowType.Int(32, true)),
        Field.nullable("strings", ArrowType.Utf8.INSTANCE)));
    CompressionCodec.Factory factory = codec == CompressionUtil.CodecType.NO_COMPRESSION ?
        NoCompressionCodec.Factory.INSTANCE : CommonsCompressionFactory.INSTANCE;
    try (final VectorSchemaRoot root = VectorSchemaRoot.create(schema, allocator)) {
      final IntVector ints = (IntVector) root.getVector(0);
      final VarCharVector strings = (VarCharVector) root.getVector(1);
      // Doesn't get compresed
      ints.setSafe(0, 0x4a3e);
      ints.setSafe(1, 0x8aba);
      ints.setSafe(2, 0x4362);
      ints.setSafe(3, 0x383f);
      // Gets compressed
      String compressibleString = "                "; // 16 bytes
      compressibleString = compressibleString + compressibleString;
      compressibleString = compressibleString + compressibleString;
      compressibleString = compressibleString + compressibleString;
      compressibleString = compressibleString + compressibleString;
      compressibleString = compressibleString + compressibleString; // 512 bytes
      byte[] compressibleData = compressibleString.getBytes(StandardCharsets.UTF_8);
      strings.setSafe(0, compressibleData);
      strings.setSafe(1, compressibleData);
      strings.setSafe(2, compressibleData);
      strings.setSafe(3, compressibleData);
      root.setRowCount(4);

      testBody.accept(factory, root);
    }
  }
}
