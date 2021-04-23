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
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.util.AutoCloseables;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VarBinaryVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.compression.CompressionCodec;
import org.apache.arrow.vector.compression.CompressionUtil;
import org.apache.arrow.vector.compression.NoCompressionCodec;
import org.apache.arrow.vector.ipc.message.ArrowFieldNode;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

/**
 * Test cases for {@link CompressionCodec}s.
 */
@RunWith(Parameterized.class)
public class TestCompressionCodec {

  private final CompressionCodec codec;

  private BufferAllocator allocator;

  private final int vectorLength;

  @Before
  public void init() {
    allocator = new RootAllocator(Integer.MAX_VALUE);
  }

  @After
  public void terminate() {
    allocator.close();
  }

  public TestCompressionCodec(CompressionUtil.CodecType type, int vectorLength, CompressionCodec codec) {
    this.codec = codec;
    this.vectorLength = vectorLength;
  }

  @Parameterized.Parameters(name = "codec = {0}, length = {1}")
  public static Collection<Object[]> getCodecs() {
    List<Object[]> params = new ArrayList<>();

    int[] lengths = new int[] {10, 100, 1000};
    for (int len : lengths) {
      CompressionCodec dumbCodec = NoCompressionCodec.INSTANCE;
      params.add(new Object[]{dumbCodec.getCodecType(), len, dumbCodec});

      CompressionCodec lz4Codec = new Lz4CompressionCodec();
      params.add(new Object[]{lz4Codec.getCodecType(), len, lz4Codec});

      CompressionCodec zstdCodec = new ZstdCompressionCodec();
      params.add(new Object[]{zstdCodec.getCodecType(), len, zstdCodec});

    }
    return params;
  }

  private List<ArrowBuf> compressBuffers(List<ArrowBuf> inputBuffers) {
    List<ArrowBuf> outputBuffers = new ArrayList<>(inputBuffers.size());
    for (ArrowBuf buf : inputBuffers) {
      outputBuffers.add(codec.compress(allocator, buf));
    }
    return outputBuffers;
  }

  private List<ArrowBuf> deCompressBuffers(List<ArrowBuf> inputBuffers) {
    List<ArrowBuf> outputBuffers = new ArrayList<>(inputBuffers.size());
    for (ArrowBuf buf : inputBuffers) {
      outputBuffers.add(codec.decompress(allocator, buf));
    }
    return outputBuffers;
  }

  @Test
  public void testCompressFixedWidthBuffers() throws Exception {
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
    List<ArrowBuf> compressedBuffers = compressBuffers(origBuffers);
    List<ArrowBuf> decompressedBuffers = deCompressBuffers(compressedBuffers);

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

  @Test
  public void testCompressVariableWidthBuffers() throws Exception {
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
    List<ArrowBuf> compressedBuffers = compressBuffers(origBuffers);
    List<ArrowBuf> decompressedBuffers = deCompressBuffers(compressedBuffers);

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

  @Test
  public void testEmptyBuffer() throws Exception {
    final VarBinaryVector origVec = new VarBinaryVector("vec", allocator);

    origVec.allocateNew(vectorLength);

    // Do not set any values (all missing)
    origVec.setValueCount(vectorLength);

    final List<ArrowBuf> origBuffers = origVec.getFieldBuffers();
    final List<ArrowBuf> compressedBuffers = compressBuffers(origBuffers);
    final List<ArrowBuf> decompressedBuffers = deCompressBuffers(compressedBuffers);

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
}
