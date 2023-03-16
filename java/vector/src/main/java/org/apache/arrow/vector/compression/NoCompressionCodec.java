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
import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.ipc.message.ArrowBodyCompression;

/**
 * The default compression codec that does no compression.
 */
public class NoCompressionCodec implements CompressionCodec {

  public static final NoCompressionCodec INSTANCE = new NoCompressionCodec();

  public static final byte COMPRESSION_TYPE = -1;

  public static final ArrowBodyCompression DEFAULT_BODY_COMPRESSION =
      new ArrowBodyCompression(COMPRESSION_TYPE, BodyCompressionMethod.BUFFER);

  private NoCompressionCodec() {
  }

  @Override
  public ArrowBuf compress(BufferAllocator allocator, ArrowBuf uncompressedBuffer) {
    return uncompressedBuffer;
  }

  @Override
  public ArrowBuf decompress(BufferAllocator allocator, ArrowBuf compressedBuffer) {
    return compressedBuffer;
  }

  @Override
  public CompressionUtil.CodecType getCodecType() {
    return CompressionUtil.CodecType.NO_COMPRESSION;
  }

  /**
   * The default factory that creates a {@link NoCompressionCodec}.
   */
  public static class Factory implements CompressionCodec.Factory {

    public static final NoCompressionCodec.Factory INSTANCE = new NoCompressionCodec.Factory();

    @Override
    public CompressionCodec createCodec(CompressionUtil.CodecType codecType) {
      switch (codecType) {
        case NO_COMPRESSION:
          return NoCompressionCodec.INSTANCE;
        case LZ4_FRAME:
        case ZSTD:
          throw new IllegalArgumentException(
            "Please add arrow-compression module to use CommonsCompressionFactory for " + codecType);
        default:
          throw new IllegalArgumentException("Unsupported codec type: " + codecType);
      }
    }
  }
}
