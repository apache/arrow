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

/**
 * The codec for compression/decompression.
 */
public interface CompressionCodec {

  /**
   * Given a buffer, estimate the compressed size.
   * Please note this operation is optional, and some compression methods may not support it.
   *
   * @param input the input buffer to be estimated.
   * @return the estimated size of the compressed data.
   */
  long estimateCompressedSize(ArrowBuf input);

  /**
   * Compress a buffer.
   * @param input the buffer to compress.
   * @param estimatedOutputSize the estimated size of the output data.
   *     Note that this is optional, and may not be equal to the actual output size.
   * @return the compressed buffer.
   */
  ArrowBuf compress(ArrowBuf input, long estimatedOutputSize);

  /**
   * Given a compressed buffer, estimate the decompressed size.
   * Please note this operation is optional, and some compression methods may not support it.
   *
   * @param input the input buffer to be estimated.
   * @return the estimated size of the decompressed data.
   */
  long estimateDecompressedSize(ArrowBuf input);

  /**
   * Decompress a buffer.
   * @param input the buffer to be decompressed.
   * @param estimatedOutputSize the estimated size of the output data.
   *      Note that this is optional, and may not be equal to the actual output size.
   * @return the decompressed buffer.
   */
  ArrowBuf decompress(ArrowBuf input, long estimatedOutputSize);

  /**
   * Gets the name of the codec.
   * @return the name of the codec.
   */
  String getCodecName();
}
