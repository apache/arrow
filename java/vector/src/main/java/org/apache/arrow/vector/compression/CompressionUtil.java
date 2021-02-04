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

import org.apache.arrow.flatbuf.BodyCompressionMethod;
import org.apache.arrow.flatbuf.CompressionType;
import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.ipc.message.ArrowBodyCompression;

/**
 * Utilities for data compression/decompression.
 */
public class CompressionUtil {

  static final long SIZE_OF_UNCOMPRESSED_LENGTH = 8L;

  /**
   * Special flag to indicate no compression.
   * (e.g. when the compressed buffer has a larger size.)
   */
  static final long NO_COMPRESSION_LENGTH = -1L;

  private CompressionUtil() {
  }

  /**
   * Creates the {@link ArrowBodyCompression} object, given the {@link CompressionCodec}.
   * The implementation of this method should depend on the values of {@link CompressionType#names}.
   */
  public static ArrowBodyCompression createBodyCompression(CompressionCodec codec) {
    switch (codec.getCodecName()) {
      case "default":
        return NoCompressionCodec.DEFAULT_BODY_COMPRESSION;
      case "LZ4_FRAME":
        return new ArrowBodyCompression(CompressionType.LZ4_FRAME, BodyCompressionMethod.BUFFER);
      case "ZSTD":
        return new ArrowBodyCompression(CompressionType.ZSTD, BodyCompressionMethod.BUFFER);
      default:
        throw new IllegalArgumentException("Unknown codec: " + codec.getCodecName());
    }
  }

  /**
   * Creates the {@link CompressionCodec} given the compression type.
   */
  public static CompressionCodec createCodec(byte compressionType) {
    switch (compressionType) {
      case NoCompressionCodec.COMPRESSION_TYPE:
        return NoCompressionCodec.INSTANCE;
      case CompressionType.LZ4_FRAME:
        return new Lz4CompressionCodec();
      default:
        throw new IllegalArgumentException("Compression type not supported: " + compressionType);
    }
  }

  /**
   * Process compression by compressing the buffer as is.
   */
  public static ArrowBuf compressRawBuffer(BufferAllocator allocator, ArrowBuf inputBuffer) {
    ArrowBuf compressedBuffer = allocator.buffer(SIZE_OF_UNCOMPRESSED_LENGTH + inputBuffer.writerIndex());
    compressedBuffer.setLong(0, NO_COMPRESSION_LENGTH);
    compressedBuffer.setBytes(SIZE_OF_UNCOMPRESSED_LENGTH, inputBuffer, 0, inputBuffer.writerIndex());
    compressedBuffer.writerIndex(SIZE_OF_UNCOMPRESSED_LENGTH + inputBuffer.writerIndex());
    return compressedBuffer;
  }

  /**
   * Process decompression by decompressing the buffer as is.
   */
  public static ArrowBuf decompressRawBuffer(ArrowBuf inputBuffer) {
    return inputBuffer.slice(SIZE_OF_UNCOMPRESSED_LENGTH,
        inputBuffer.writerIndex() - SIZE_OF_UNCOMPRESSED_LENGTH);
  }
}
