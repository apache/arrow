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
import org.apache.arrow.vector.ipc.message.ArrowBodyCompression;

/**
 * Utilities for data compression/decompression.
 */
public class CompressionUtil {

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
      default:
        throw new IllegalArgumentException("Compression type not supported: " + compressionType);
    }
  }
}
