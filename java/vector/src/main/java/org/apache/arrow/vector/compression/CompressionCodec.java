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

import java.util.EnumMap;
import java.util.Map;
import java.util.ServiceLoader;
import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.memory.BufferAllocator;

/** The codec for compression/decompression. */
public interface CompressionCodec {

  /**
   * Compress a buffer.
   *
   * @param allocator the allocator for allocating memory for compressed buffer.
   * @param uncompressedBuffer the buffer to compress. Implementation of this method should take
   *     care of releasing this buffer.
   * @return the compressed buffer
   */
  ArrowBuf compress(BufferAllocator allocator, ArrowBuf uncompressedBuffer);

  /**
   * Decompress a buffer.
   *
   * @param allocator the allocator for allocating memory for decompressed buffer.
   * @param compressedBuffer the buffer to be decompressed. Implementation of this method should
   *     take care of releasing this buffer.
   * @return the decompressed buffer.
   */
  ArrowBuf decompress(BufferAllocator allocator, ArrowBuf compressedBuffer);

  /**
   * Gets the type of the codec.
   *
   * @return the type of the codec.
   */
  CompressionUtil.CodecType getCodecType();

  /** Factory to create compression codec. */
  interface Factory {
    /**
     * This combines all the available factories registered as service providers in the module path.
     * For each {@link CompressionUtil.CodecType compression codec type}, it will use whatever
     * factory supports it, i.e. doesn't throw on `createCodec(type)`. If multiple factories
     * registered as service providers support the same codec type, the first one encountered while
     * iterating over the {@link ServiceLoader} will be selected. A codec type that is not supported
     * by any registered service provider will fall back to {@link
     * NoCompressionCodec.Factory#INSTANCE} for backwards compatibility.
     */
    Factory INSTANCE = bestEffort();

    /** Creates the codec based on the codec type. */
    CompressionCodec createCodec(CompressionUtil.CodecType codecType);

    /** Creates the codec based on the codec type and compression level. */
    CompressionCodec createCodec(CompressionUtil.CodecType codecType, int compressionLevel);

    private static Factory bestEffort() {
      final ServiceLoader<Factory> serviceLoader = ServiceLoader.load(Factory.class);
      final Map<CompressionUtil.CodecType, Factory> factories =
          new EnumMap<>(CompressionUtil.CodecType.class);
      for (Factory factory : serviceLoader) {
        for (CompressionUtil.CodecType codecType : CompressionUtil.CodecType.values()) {
          try {
            factory.createCodec(codecType); // will throw if not supported
            factories.putIfAbsent(codecType, factory);
          } catch (Throwable ignored) {
          }
        }
      }

      final Factory fallback = NoCompressionCodec.Factory.INSTANCE;
      return new Factory() {
        @Override
        public CompressionCodec createCodec(CompressionUtil.CodecType codecType) {
          return factories.getOrDefault(codecType, fallback).createCodec(codecType);
        }

        @Override
        public CompressionCodec createCodec(
            CompressionUtil.CodecType codecType, int compressionLevel) {
          return factories
              .getOrDefault(codecType, fallback)
              .createCodec(codecType, compressionLevel);
        }
      };
    }
  }
}
