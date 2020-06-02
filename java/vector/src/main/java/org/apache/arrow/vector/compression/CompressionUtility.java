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
public class CompressionUtility {

  private CompressionUtility() {
  }

  /**
   * Creates the {@link ArrowBodyCompression} object, given the {@link CompressionCodec}.
   */
  public static ArrowBodyCompression createBodyCompression(CompressionCodec codec) {
    if (codec == null) {
      return ArrowBodyCompression.NO_BODY_COMPRESSION;
    }

    for (int i = 0; i < CompressionType.names.length; i++) {
      if (CompressionType.names[i].equals(codec.getCodecName())) {
        return new ArrowBodyCompression((byte) i, BodyCompressionMethod.BUFFER);
      }
    }
    throw new IllegalArgumentException("Unknown codec: " + codec.getCodecName());
  }

  /**
   * Creates the {@link CompressionCodec} given the compression type.
   */
  public static CompressionCodec createCodec(byte compressionType) {
    switch (compressionType) {
      case ArrowBodyCompression.NO_COMPRESSION_TYPE:
        return null;
      default:
        throw new IllegalArgumentException("Compression type not supported: " + compressionType);
    }
  }
}
