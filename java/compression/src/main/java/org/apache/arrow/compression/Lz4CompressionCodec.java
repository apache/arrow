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

import org.apache.arrow.flatbuf.CompressionType;
import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.util.MemoryUtil;
import org.apache.arrow.util.Preconditions;
import org.apache.arrow.vector.compression.CompressionCodec;
import org.apache.arrow.vector.compression.CompressionUtil;
import org.apache.commons.compress.compressors.lz4.FramedLZ4CompressorInputStream;
import org.apache.commons.compress.compressors.lz4.FramedLZ4CompressorOutputStream;
import org.apache.commons.compress.utils.IOUtils;

import io.netty.util.internal.PlatformDependent;

/**
 * Compression codec for the LZ4 algorithm.
 */
public class Lz4CompressionCodec implements CompressionCodec {

  @Override
  public ArrowBuf compress(BufferAllocator allocator, ArrowBuf uncompressedBuffer) {
    Preconditions.checkArgument(uncompressedBuffer.writerIndex() <= Integer.MAX_VALUE,
        "The uncompressed buffer size exceeds the integer limit");

    if (uncompressedBuffer.writerIndex() == 0L) {
      // shortcut for empty buffer
      ArrowBuf compressedBuffer = allocator.buffer(CompressionUtil.SIZE_OF_UNCOMPRESSED_LENGTH);
      compressedBuffer.setLong(0, 0);
      compressedBuffer.writerIndex(CompressionUtil.SIZE_OF_UNCOMPRESSED_LENGTH);
      uncompressedBuffer.close();
      return compressedBuffer;
    }

    try {
      ArrowBuf compressedBuffer = doCompress(allocator, uncompressedBuffer);
      long compressedLength = compressedBuffer.writerIndex() - CompressionUtil.SIZE_OF_UNCOMPRESSED_LENGTH;
      if (compressedLength > uncompressedBuffer.writerIndex()) {
        // compressed buffer is larger, send the raw buffer
        compressedBuffer.close();
        compressedBuffer = CompressionUtil.packageRawBuffer(allocator, uncompressedBuffer);
      }

      uncompressedBuffer.close();
      return compressedBuffer;
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  private ArrowBuf doCompress(BufferAllocator allocator, ArrowBuf uncompressedBuffer) throws IOException {
    byte[] inBytes = new byte[(int) uncompressedBuffer.writerIndex()];
    PlatformDependent.copyMemory(uncompressedBuffer.memoryAddress(), inBytes, 0, uncompressedBuffer.writerIndex());
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    try (InputStream in = new ByteArrayInputStream(inBytes);
         OutputStream out = new FramedLZ4CompressorOutputStream(baos)) {
      IOUtils.copy(in, out);
    }

    byte[] outBytes = baos.toByteArray();

    ArrowBuf compressedBuffer = allocator.buffer(CompressionUtil.SIZE_OF_UNCOMPRESSED_LENGTH + outBytes.length);

    long uncompressedLength = uncompressedBuffer.writerIndex();
    if (!MemoryUtil.LITTLE_ENDIAN) {
      uncompressedLength = Long.reverseBytes(uncompressedLength);
    }
    // first 8 bytes reserved for uncompressed length, according to the specification
    compressedBuffer.setLong(0, uncompressedLength);

    PlatformDependent.copyMemory(
        outBytes, 0, compressedBuffer.memoryAddress() + CompressionUtil.SIZE_OF_UNCOMPRESSED_LENGTH, outBytes.length);
    compressedBuffer.writerIndex(CompressionUtil.SIZE_OF_UNCOMPRESSED_LENGTH + outBytes.length);
    return compressedBuffer;
  }

  @Override
  public ArrowBuf decompress(BufferAllocator allocator, ArrowBuf compressedBuffer) {
    Preconditions.checkArgument(compressedBuffer.writerIndex() <= Integer.MAX_VALUE,
        "The compressed buffer size exceeds the integer limit");

    Preconditions.checkArgument(compressedBuffer.writerIndex() >= CompressionUtil.SIZE_OF_UNCOMPRESSED_LENGTH,
        "Not enough data to decompress.");

    long decompressedLength = compressedBuffer.getLong(0);
    if (!MemoryUtil.LITTLE_ENDIAN) {
      decompressedLength = Long.reverseBytes(decompressedLength);
    }

    if (decompressedLength == 0L) {
      // shortcut for empty buffer
      compressedBuffer.close();
      return allocator.getEmpty();
    }

    if (decompressedLength == CompressionUtil.NO_COMPRESSION_LENGTH) {
      // no compression
      return CompressionUtil.extractUncompressedBuffer(compressedBuffer);
    }

    try {
      ArrowBuf decompressedBuffer = doDecompress(allocator, compressedBuffer);
      compressedBuffer.close();
      return decompressedBuffer;
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  private ArrowBuf doDecompress(BufferAllocator allocator, ArrowBuf compressedBuffer) throws IOException {
    long decompressedLength = compressedBuffer.getLong(0);
    if (!MemoryUtil.LITTLE_ENDIAN) {
      decompressedLength = Long.reverseBytes(decompressedLength);
    }

    byte[] inBytes = new byte[(int) (compressedBuffer.writerIndex() - CompressionUtil.SIZE_OF_UNCOMPRESSED_LENGTH)];
    PlatformDependent.copyMemory(
        compressedBuffer.memoryAddress() + CompressionUtil.SIZE_OF_UNCOMPRESSED_LENGTH, inBytes, 0, inBytes.length);
    ByteArrayOutputStream out = new ByteArrayOutputStream((int) decompressedLength);
    try (InputStream in = new FramedLZ4CompressorInputStream(new ByteArrayInputStream(inBytes))) {
      IOUtils.copy(in, out);
    }

    byte[] outBytes = out.toByteArray();
    ArrowBuf decompressedBuffer = allocator.buffer(outBytes.length);
    PlatformDependent.copyMemory(outBytes, 0, decompressedBuffer.memoryAddress(), outBytes.length);
    decompressedBuffer.writerIndex(decompressedLength);
    return decompressedBuffer;
  }

  @Override
  public String getCodecName() {
    return CompressionType.name(CompressionType.LZ4_FRAME);
  }
}
