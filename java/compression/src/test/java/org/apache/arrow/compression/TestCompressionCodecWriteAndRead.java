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

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.BitVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.compression.CompressionCodec;
import org.apache.arrow.vector.compression.CompressionUtil;
import org.apache.arrow.vector.compression.NoCompressionCodec;
import org.apache.arrow.vector.ipc.ArrowFileReader;
import org.apache.arrow.vector.ipc.ArrowFileWriter;
import org.apache.arrow.vector.ipc.ArrowReader;
import org.apache.arrow.vector.ipc.ArrowStreamReader;
import org.apache.arrow.vector.ipc.ArrowStreamWriter;
import org.apache.arrow.vector.types.pojo.Field;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

@RunWith(Parameterized.class)
public class TestCompressionCodecWriteAndRead {
  private final CompressionCodec codec;
  private final int vectorLength;
  private final File file;

  public TestCompressionCodecWriteAndRead(CompressionUtil.CodecType type, int vectorLength,
                                          CompressionCodec codec, File file) {
    this.codec = codec;
    this.file = file;
    this.vectorLength = vectorLength;
  }

  @Parameterized.Parameters(name = "codec = {0}, length = {1}, file = {2}")
  public static Collection<Object[]> getCodecs() {
    List<Object[]> params = new ArrayList<>();

    int[] lengths = new int[]{10, 100, 1000};
    for (int len : lengths) {
      CompressionCodec dumbCodec = NoCompressionCodec.INSTANCE;
      File fileNoCompression = new File("target/write_no_compression_" + len + ".arrow");
      params.add(new Object[]{dumbCodec.getCodecType(), len, dumbCodec, fileNoCompression});

      CompressionCodec lz4Codec = new Lz4CompressionCodec();
      File fileLZ4Compression = new File("target/write_lz4_compression_" + len + ".arrow");
      params.add(new Object[]{lz4Codec.getCodecType(), len, lz4Codec, fileLZ4Compression});

      CompressionCodec zstdCodec = new ZstdCompressionCodec();
      File fileZSTDCompression = new File("target/write_zstd_compression_" + len + ".arrow");
      params.add(new Object[]{zstdCodec.getCodecType(), len, zstdCodec, fileZSTDCompression});

      CompressionCodec zstdCodecAndLevel = new ZstdCompressionCodec(1);
      File fileZSTDCompressionAndLevel = new File("target/write_zstd_compression_level_" + len + ".arrow");
      params.add(new Object[]{zstdCodecAndLevel.getCodecType(), len, zstdCodecAndLevel, fileZSTDCompressionAndLevel});

    }
    return params;
  }

  @Test
  public void writeReadRandomAccessFile() throws IOException {
    RootAllocator allocator = new RootAllocator(Long.MAX_VALUE);
    BitVector bitVector = new BitVector("boolean", allocator);
    VarCharVector varCharVector = new VarCharVector("varchar", allocator);
    for (int i = 0; i < vectorLength; i++) {
      bitVector.setSafe(i, i % 2 == 0 ? 0 : 1);
      varCharVector.setSafe(i, ("test" + i).getBytes(StandardCharsets.UTF_8));
    }
    bitVector.setValueCount(vectorLength);
    varCharVector.setValueCount(vectorLength);

    List<Field> fields = Arrays.asList(bitVector.getField(), varCharVector.getField());
    List<FieldVector> vectors = Arrays.asList(bitVector, varCharVector);

    VectorSchemaRoot schemaRootWrite = new VectorSchemaRoot(fields, vectors);

    // write
    FileOutputStream fileOutputStream = new FileOutputStream(file);
    ArrowFileWriter writer = new ArrowFileWriter.Builder(schemaRootWrite,
        fileOutputStream.getChannel()).setCodec(codec).build();
    writer.start();
    writer.writeBatch();
    writer.end();

    // validations
    Assert.assertEquals(vectorLength, schemaRootWrite.getRowCount());

    // read
    FileInputStream fileInputStream = new FileInputStream(file);
    ArrowFileReader reader = new ArrowFileReader(fileInputStream.getChannel(), allocator,
        CommonsCompressionFactory.INSTANCE);
    BitVector bitVectorRead = (BitVector) reader.getVectorSchemaRoot().getFieldVectors().get(0);
    VarCharVector varCharVectorRead = (VarCharVector) reader.getVectorSchemaRoot().getFieldVectors().get(1);

    // validations read compressed file
    validateDataRead(reader, bitVectorRead, varCharVectorRead);
  }

  @Test
  public void writeReadStreamingFormat() throws IOException {
    RootAllocator allocator = new RootAllocator(Long.MAX_VALUE);
    BitVector bitVector = new BitVector("boolean", allocator);
    VarCharVector varCharVector = new VarCharVector("varchar", allocator);
    for (int i = 0; i < vectorLength; i++) {
      bitVector.setSafe(i, i % 2 == 0 ? 0 : 1);
      varCharVector.setSafe(i, ("test" + i).getBytes(StandardCharsets.UTF_8));
    }
    bitVector.setValueCount(vectorLength);
    varCharVector.setValueCount(vectorLength);

    List<Field> fields = Arrays.asList(bitVector.getField(), varCharVector.getField());
    List<FieldVector> vectors = Arrays.asList(bitVector, varCharVector);

    VectorSchemaRoot schemaRootWrite = new VectorSchemaRoot(fields, vectors);

    // write
    FileOutputStream fileOutputStream = new FileOutputStream(file);
    ArrowStreamWriter writer = new ArrowStreamWriter.Builder(schemaRootWrite, fileOutputStream).setCodec(codec).build();

    writer.start();
    writer.writeBatch();
    writer.end();

    // validations
    Assert.assertEquals(vectorLength, schemaRootWrite.getRowCount());

    // read
    FileInputStream fileInputStream = new FileInputStream(file);
    ArrowStreamReader reader = new ArrowStreamReader(fileInputStream.getChannel(), allocator,
        CommonsCompressionFactory.INSTANCE);
    BitVector bitVectorRead = (BitVector) reader.getVectorSchemaRoot().getFieldVectors().get(0);
    VarCharVector varCharVectorRead = (VarCharVector) reader.getVectorSchemaRoot().getFieldVectors().get(1);

    // validations read compressed file
    validateDataRead(reader, bitVectorRead, varCharVectorRead);
  }

  protected void validateDataRead(ArrowReader reader, BitVector bitVectorRead, VarCharVector varCharVectorRead)
      throws IOException {
    reader.loadNextBatch();
    for (int i = 0; i < vectorLength; i++) {
      //validate bit record data read
      assertEquals(bitVectorRead.get(i), i % 2 == 0 ? 0 : 1);
      //validate varchar record data read
      assertArrayEquals(varCharVectorRead.get(i), ("test" + i).getBytes(StandardCharsets.UTF_8));
      assertEquals(new String(varCharVectorRead.get(i)), "test" + i);
    }
  }
}
