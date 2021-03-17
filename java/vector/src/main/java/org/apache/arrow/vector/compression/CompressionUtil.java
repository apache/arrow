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

  /**
   * Compression codec types corresponding to flat buffer implementation in {@link CompressionType}.
   */
  public enum CodecType {

    NO_COMPRESSION(NoCompressionCodec.COMPRESSION_TYPE),

    LZ4_FRAME(org.apache.arrow.flatbuf.CompressionType.LZ4_FRAME),

    ZSTD(org.apache.arrow.flatbuf.CompressionType.ZSTD);

    private final byte type;

    CodecType(byte type) {
      this.type = type;
    }

    public byte getType() {
      return type;
    }

    /**
     * Gets the codec type from the compression type defined in {@link CompressionType}.
     */
    public static CodecType fromCompressionType(byte type) {
      for (CodecType codecType : values()) {
        if (codecType.type == type) {
          return codecType;
        }
      }
      return NO_COMPRESSION;
    }
  }

  public static final long SIZE_OF_UNCOMPRESSED_LENGTH = 8L;

  /**
   * Special flag to indicate no compression.
   * (e.g. when the compressed buffer has a larger size.)
   */
  public static final long NO_COMPRESSION_LENGTH = -1L;

  private CompressionUtil() {
  }

  /**
   * Creates the {@link ArrowBodyCompression} object, given the {@link CompressionCodec}.
   * The implementation of this method should depend on the values of
   * {@link org.apache.arrow.flatbuf.CompressionType#names}.
   */
  public static ArrowBodyCompression createBodyCompression(CompressionCodec codec) {
    return new ArrowBodyCompression(codec.getCodecType().getType(), BodyCompressionMethod.BUFFER);
  }

  /**
   * Process compression by compressing the buffer as is.
   */
  public static ArrowBuf packageRawBuffer(BufferAllocator allocator, ArrowBuf inputBuffer) {
    ArrowBuf compressedBuffer = allocator.buffer(SIZE_OF_UNCOMPRESSED_LENGTH + inputBuffer.writerIndex());
    compressedBuffer.setLong(0, NO_COMPRESSION_LENGTH);
    compressedBuffer.setBytes(SIZE_OF_UNCOMPRESSED_LENGTH, inputBuffer, 0, inputBuffer.writerIndex());
    compressedBuffer.writerIndex(SIZE_OF_UNCOMPRESSED_LENGTH + inputBuffer.writerIndex());
    return compressedBuffer;
  }

  /**
   * Process decompression by slicing the buffer that contains the uncompressed bytes.
   */
  public static ArrowBuf extractUncompressedBuffer(ArrowBuf inputBuffer) {
    return inputBuffer.slice(SIZE_OF_UNCOMPRESSED_LENGTH,
        inputBuffer.writerIndex() - SIZE_OF_UNCOMPRESSED_LENGTH);
  }
}
