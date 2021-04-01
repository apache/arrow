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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.util.Preconditions;
import org.apache.arrow.vector.compression.AbstractCompressionCodec;
import org.apache.arrow.vector.compression.CompressionUtil;
import org.apache.commons.compress.compressors.lz4.FramedLZ4CompressorInputStream;
import org.apache.commons.compress.compressors.lz4.FramedLZ4CompressorOutputStream;
import org.apache.commons.compress.utils.IOUtils;

/**
 * Compression codec for the LZ4 algorithm.
 */
public class Lz4CompressionCodec extends AbstractCompressionCodec {

  @Override
  protected ArrowBuf doCompress(BufferAllocator allocator, ArrowBuf uncompressedBuffer) {
    Preconditions.checkArgument(uncompressedBuffer.writerIndex() <= Integer.MAX_VALUE,
        "The uncompressed buffer size exceeds the integer limit %s.", Integer.MAX_VALUE);

    byte[] inBytes = new byte[(int) uncompressedBuffer.writerIndex()];
    uncompressedBuffer.getBytes(/*index=*/0, inBytes);
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    try (InputStream in = new ByteArrayInputStream(inBytes);
         OutputStream out = new FramedLZ4CompressorOutputStream(baos)) {
      IOUtils.copy(in, out);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }

    byte[] outBytes = baos.toByteArray();

    ArrowBuf compressedBuffer = allocator.buffer(CompressionUtil.SIZE_OF_UNCOMPRESSED_LENGTH + outBytes.length);
    compressedBuffer.setBytes(CompressionUtil.SIZE_OF_UNCOMPRESSED_LENGTH, outBytes);
    compressedBuffer.writerIndex(CompressionUtil.SIZE_OF_UNCOMPRESSED_LENGTH + outBytes.length);
    return compressedBuffer;
  }

  @Override
  protected ArrowBuf doDecompress(BufferAllocator allocator, ArrowBuf compressedBuffer) {
    Preconditions.checkArgument(compressedBuffer.writerIndex() <= Integer.MAX_VALUE,
        "The compressed buffer size exceeds the integer limit %s", Integer.MAX_VALUE);

    long decompressedLength = readUncompressedLength(compressedBuffer);

    byte[] inBytes = new byte[(int) (compressedBuffer.writerIndex() - CompressionUtil.SIZE_OF_UNCOMPRESSED_LENGTH)];
    compressedBuffer.getBytes(CompressionUtil.SIZE_OF_UNCOMPRESSED_LENGTH, inBytes);
    ByteArrayOutputStream out = new ByteArrayOutputStream((int) decompressedLength);
    try (InputStream in = new FramedLZ4CompressorInputStream(new ByteArrayInputStream(inBytes))) {
      IOUtils.copy(in, out);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }

    byte[] outBytes = out.toByteArray();
    ArrowBuf decompressedBuffer = allocator.buffer(outBytes.length);
    decompressedBuffer.setBytes(/*index=*/0, outBytes);
    return decompressedBuffer;
  }

  @Override
  public CompressionUtil.CodecType getCodecType() {
    return CompressionUtil.CodecType.LZ4_FRAME;
  }
}
