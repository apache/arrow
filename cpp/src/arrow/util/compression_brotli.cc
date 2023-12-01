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

#include <cstddef>
#include <cstdint>
#include <memory>

#include <brotli/decode.h>
#include <brotli/encode.h>
#include <brotli/types.h>

#include "arrow/result.h"
#include "arrow/status.h"
#include "arrow/util/logging.h"
#include "arrow/util/macros.h"

namespace arrow {
namespace util {
namespace internal {

namespace {

class BrotliDecompressor : public Decompressor {
 public:
  BrotliDecompressor() { cjformat_ = cramjam::StreamingBrotli; };
};

// ----------------------------------------------------------------------
// Brotli compressor implementation

class BrotliCompressor : public Compressor {
 public:
  explicit BrotliCompressor(int compression_level, int window_bits) {
    cjformat_ = cramjam::StreamingBrotli;
    compression_level_ = compression_level;
  }
};

// ----------------------------------------------------------------------
// Brotli codec implementation

class BrotliCodec : public Codec {
 public:
  explicit BrotliCodec(int compression_level, int window_bits) {
    cjformat_ = cramjam::Brotli;
    compression_level_ = compression_level == kUseDefaultCompressionLevel
                             ? kBrotliDefaultCompressionLevel
                             : compression_level;
  }

  Result<std::shared_ptr<Compressor>> MakeCompressor() override {
    auto ptr = std::make_shared<BrotliCompressor>(kBrotliDefaultCompressionLevel, 0);
    RETURN_NOT_OK(ptr->Init());
    return ptr;
  }

  Result<std::shared_ptr<Decompressor>> MakeDecompressor() override {
    auto ptr = std::make_shared<BrotliDecompressor>();
    RETURN_NOT_OK(ptr->Init());
    return ptr;
  }

  int64_t MaxCompressedLen(int64_t input_len,
                           const uint8_t* ARROW_ARG_UNUSED(input)) override {
    DCHECK_GE(input_len, 0);
    return cramjam::brotli_max_compressed_len(input_len);
  }

  Compression::type compression_type() const override { return Compression::BROTLI; }

  int compression_level() const override { return compression_level_; }
  int minimum_compression_level() const override { return BROTLI_MIN_QUALITY; }
  int maximum_compression_level() const override { return BROTLI_MAX_QUALITY; }
  int default_compression_level() const override {
    return kBrotliDefaultCompressionLevel;
  }
};

}  // namespace

std::unique_ptr<Codec> MakeBrotliCodec(int compression_level,
                                       std::optional<int> window_bits) {
  return std::make_unique<BrotliCodec>(compression_level, window_bits.value_or(0));
}

}  // namespace internal
}  // namespace util
}  // namespace arrow
