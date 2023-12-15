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

#include "arrow/util/compression_internal.h"

#include <algorithm>
#include <cstdint>
#include <cstring>
#include <limits>
#include <memory>

#include "arrow/result.h"
#include "arrow/status.h"
#include "arrow/util/logging.h"
#include "arrow/util/macros.h"

namespace arrow {
namespace util {
namespace internal {

namespace {

// ----------------------------------------------------------------------
// gzip implementation

constexpr int kGZipMinCompressionLevel = 1;
constexpr int kGZipMaxCompressionLevel = 9;

// ----------------------------------------------------------------------
// gzip decompressor implementation

class GZipDecompressor : public Decompressor {
 public:
  explicit GZipDecompressor(GZipFormat format, int window_bits) {
    // todo: switch stmt or other clases for Deflate and Zlib?
    cjformat_ = cramjam::StreamingGzip;
  }
};

// ----------------------------------------------------------------------
// gzip compressor implementation

class GZipCompressor : public Compressor {
 public:
  explicit GZipCompressor(int32_t compression_level) {
    cjformat_ = cramjam::StreamingGzip;
    compression_level_ = compression_level;
  }
};

// ----------------------------------------------------------------------
// gzip codec implementation

class GZipCodec : public Codec {
 public:
  explicit GZipCodec(int32_t compression_level, GZipFormat format, int window_bits) {
    cjformat_ = cramjam::Gzip;
    compression_level_ = compression_level == kUseDefaultCompressionLevel
                             ? kGZipDefaultCompressionLevel
                             : compression_level;
  }

  int64_t MaxCompressedLen(int64_t input_len,
                           const uint8_t* ARROW_ARG_UNUSED(input)) override {
    DCHECK_GE(input_len, 0);
    return cramjam::gzip_max_compressed_len(input_len, compression_level_);
  }

  Result<std::shared_ptr<Compressor>> MakeCompressor() override {
    auto ptr = std::make_shared<GZipCompressor>(compression_level_);
    RETURN_NOT_OK(ptr->Init());
    return ptr;
  }

  Result<std::shared_ptr<Decompressor>> MakeDecompressor() override {
    auto ptr = std::make_shared<GZipDecompressor>(GZipFormat::GZIP, 0);
    RETURN_NOT_OK(ptr->Init());
    return ptr;
  }

  Compression::type compression_type() const override { return Compression::GZIP; }

  int compression_level() const override { return compression_level_; }
  int minimum_compression_level() const override { return kGZipMinCompressionLevel; }
  int maximum_compression_level() const override { return kGZipMaxCompressionLevel; }
  int default_compression_level() const override { return kGZipDefaultCompressionLevel; }
};

}  // namespace

std::unique_ptr<Codec> MakeGZipCodec(int compression_level, GZipFormat format,
                                     std::optional<int> window_bits) {
  return std::make_unique<GZipCodec>(compression_level, format, window_bits.value_or(0));
}

}  // namespace internal
}  // namespace util
}  // namespace arrow
