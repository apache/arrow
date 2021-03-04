// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#pragma once

#include <memory>

#include "arrow/util/compression.h"  // IWYU pragma: export

namespace arrow {
namespace util {

// ----------------------------------------------------------------------
// Internal Codec factories

namespace internal {

// Brotli compression quality is max (11) by default, which is slow.
// We use 8 as a default as it is the best trade-off for Parquet workload.
constexpr int kBrotliDefaultCompressionLevel = 8;

// Brotli codec.
std::unique_ptr<Codec> MakeBrotliCodec(
    int compression_level = kBrotliDefaultCompressionLevel);

// BZ2 codec.
constexpr int kBZ2DefaultCompressionLevel = 9;
std::unique_ptr<Codec> MakeBZ2Codec(int compression_level = kBZ2DefaultCompressionLevel);

// GZip
constexpr int kGZipDefaultCompressionLevel = 9;

struct GZipFormat {
  enum type {
    ZLIB,
    DEFLATE,
    GZIP,
  };
};

std::unique_ptr<Codec> MakeGZipCodec(int compression_level = kGZipDefaultCompressionLevel,
                                     GZipFormat::type format = GZipFormat::GZIP);

// Snappy
std::unique_ptr<Codec> MakeSnappyCodec();

// Lz4 "raw" format codec.
std::unique_ptr<Codec> MakeLz4RawCodec();

// Lz4 "Hadoop" format codec (== Lz4 raw codec prefixed with lengths header)
std::unique_ptr<Codec> MakeLz4HadoopRawCodec();

// Lz4 frame format codec.
std::unique_ptr<Codec> MakeLz4FrameCodec();

// ZSTD codec.

// XXX level = 1 probably doesn't compress very much
constexpr int kZSTDDefaultCompressionLevel = 1;

std::unique_ptr<Codec> MakeZSTDCodec(
    int compression_level = kZSTDDefaultCompressionLevel);

}  // namespace internal
}  // namespace util
}  // namespace arrow
