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

package org.apache.arrow.vector.compression;

import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.util.MemoryUtil;
import org.apache.arrow.util.Preconditions;

/**
 * The base class for concrete compression codecs, providing
 * common logic for all compression codecs.
 */
public abstract class AbstractCompressionCodec implements CompressionCodec {

  @Override
  public ArrowBuf compress(BufferAllocator allocator, ArrowBuf uncompressedBuffer) {
    if (uncompressedBuffer.writerIndex() == 0L) {
      // shortcut for empty buffer
      ArrowBuf compressedBuffer = allocator.buffer(CompressionUtil.SIZE_OF_UNCOMPRESSED_LENGTH);
      compressedBuffer.setLong(0, 0);
      compressedBuffer.writerIndex(CompressionUtil.SIZE_OF_UNCOMPRESSED_LENGTH);
      uncompressedBuffer.close();
      return compressedBuffer;
    }

    ArrowBuf compressedBuffer = doCompress(allocator, uncompressedBuffer);
    long compressedLength = compressedBuffer.writerIndex() - CompressionUtil.SIZE_OF_UNCOMPRESSED_LENGTH;
    long uncompressedLength = uncompressedBuffer.writerIndex();

    if (compressedLength > uncompressedLength) {
      // compressed buffer is larger, send the raw buffer
      compressedBuffer.close();
      // XXX: this makes a copy of uncompressedBuffer
      compressedBuffer = CompressionUtil.packageRawBuffer(allocator, uncompressedBuffer);
    } else {
      writeUncompressedLength(compressedBuffer, uncompressedLength);
    }

    uncompressedBuffer.close();
    return compressedBuffer;
  }

  @Override
  public ArrowBuf decompress(BufferAllocator allocator, ArrowBuf compressedBuffer) {
    Preconditions.checkArgument(compressedBuffer.writerIndex() >= CompressionUtil.SIZE_OF_UNCOMPRESSED_LENGTH,
        "Not enough data to decompress.");

    long decompressedLength = readUncompressedLength(compressedBuffer);

    if (decompressedLength == 0L) {
      // shortcut for empty buffer
      compressedBuffer.close();
      return allocator.getEmpty();
    }

    if (decompressedLength == CompressionUtil.NO_COMPRESSION_LENGTH) {
      // no compression
      return CompressionUtil.extractUncompressedBuffer(compressedBuffer);
    }

    ArrowBuf decompressedBuffer = doDecompress(allocator, compressedBuffer);
    compressedBuffer.close();
    return decompressedBuffer;
  }

  protected void writeUncompressedLength(ArrowBuf compressedBuffer, long uncompressedLength) {
    if (!MemoryUtil.LITTLE_ENDIAN) {
      uncompressedLength = Long.reverseBytes(uncompressedLength);
    }
    // first 8 bytes reserved for uncompressed length, according to the specification
    compressedBuffer.setLong(0, uncompressedLength);
  }

  protected long readUncompressedLength(ArrowBuf compressedBuffer) {
    long decompressedLength = compressedBuffer.getLong(0);
    if (!MemoryUtil.LITTLE_ENDIAN) {
      decompressedLength = Long.reverseBytes(decompressedLength);
    }
    return decompressedLength;
  }

  /**
   * The method that actually performs the data compression.
   * The layout of the returned compressed buffer is the compressed data,
   * plus 8 bytes reserved at the beginning of the buffer for the uncompressed data size.
   * <p>
   *   Please note that this method is not responsible for releasing the uncompressed buffer.
   * </p>
   */
  protected abstract ArrowBuf doCompress(BufferAllocator allocator, ArrowBuf uncompressedBuffer);

  /**
   * The method that actually performs the data decompression.
   * The layout of the compressed buffer is the compressed data,
   * plus 8 bytes at the beginning of the buffer storing the uncompressed data size.
   * <p>
   *   Please note that this method is not responsible for releasing the compressed buffer.
   * </p>
   */
  protected abstract ArrowBuf doDecompress(BufferAllocator allocator, ArrowBuf compressedBuffer);
}
