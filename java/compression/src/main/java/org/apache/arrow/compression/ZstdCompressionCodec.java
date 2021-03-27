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


import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.compression.AbstractCompressionCodec;
import org.apache.arrow.vector.compression.CompressionUtil;

import com.github.luben.zstd.Zstd;

/**
 * Compression codec for the ZSTD algorithm.
 */
public class ZstdCompressionCodec extends AbstractCompressionCodec {

  @Override
  protected ArrowBuf doCompress(BufferAllocator allocator, ArrowBuf uncompressedBuffer) {
    long maxSize = Zstd.compressBound(uncompressedBuffer.writerIndex());
    long dstSize = CompressionUtil.SIZE_OF_UNCOMPRESSED_LENGTH + maxSize;
    ArrowBuf compressedBuffer = allocator.buffer(dstSize);
    long bytesWritten = Zstd.compressUnsafe(
                          compressedBuffer.memoryAddress() + CompressionUtil.SIZE_OF_UNCOMPRESSED_LENGTH, dstSize,
                          /*src*/uncompressedBuffer.memoryAddress(), /*srcSize=*/uncompressedBuffer.writerIndex(),
                          /*level=*/3);
    if (Zstd.isError(bytesWritten)) {
      compressedBuffer.close();
      throw new RuntimeException("Error compressing: " + Zstd.getErrorName(bytesWritten));
    }
    compressedBuffer.writerIndex(CompressionUtil.SIZE_OF_UNCOMPRESSED_LENGTH + bytesWritten);
    return compressedBuffer;
  }

  @Override
  protected ArrowBuf doDecompress(BufferAllocator allocator, ArrowBuf compressedBuffer) {
    long decompressedLength = readUncompressedLength(compressedBuffer);
    ArrowBuf uncompressedBuffer = allocator.buffer(decompressedLength);
    long decompressedSize = Zstd.decompressUnsafe(uncompressedBuffer.memoryAddress(), decompressedLength,
          /*src=*/compressedBuffer.memoryAddress() + CompressionUtil.SIZE_OF_UNCOMPRESSED_LENGTH,
          compressedBuffer.writerIndex() - CompressionUtil.SIZE_OF_UNCOMPRESSED_LENGTH);
    if (Zstd.isError(decompressedSize)) {
      uncompressedBuffer.close();
      throw new RuntimeException("Error decompressing: " + Zstd.getErrorName(decompressedLength));
    }
    if (decompressedLength != decompressedSize) {
      uncompressedBuffer.close();
      throw new RuntimeException("Expected != actual decompressed length: " + 
                                 decompressedLength + " != " + decompressedSize);
    }
    uncompressedBuffer.writerIndex(decompressedLength);
    return uncompressedBuffer;
  }

  @Override
  public CompressionUtil.CodecType getCodecType() {
    return CompressionUtil.CodecType.ZSTD;
  }
}
