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

#include <zstd.h>  // TODO: cramjam min and max level

#include "arrow/result.h"
#include "arrow/status.h"
#include "arrow/util/logging.h"
#include "arrow/util/macros.h"

using std::size_t;

namespace arrow {
namespace util {
namespace internal {

namespace {

// ----------------------------------------------------------------------
// ZSTD decompressor implementation

class ZSTDDecompressor : public Decompressor {
 public:
  ZSTDDecompressor() { cjformat_ = cramjam::StreamingZstd; }
};

// ----------------------------------------------------------------------
// ZSTD compressor implementation

class ZSTDCompressor : public Compressor {
 public:
  explicit ZSTDCompressor(int compression_level) {
    compression_level_ = compression_level;
    cjformat_ = cramjam::StreamingZstd;
  }
};

// ----------------------------------------------------------------------
// ZSTD codec implementation

class ZSTDCodec : public Codec {
 public:
  explicit ZSTDCodec(int compression_level) {
    cjformat_ = cramjam::Zstd;
    compression_level_ = compression_level == kUseDefaultCompressionLevel
                             ? kZSTDDefaultCompressionLevel
                             : compression_level;
  }

  int64_t MaxCompressedLen(int64_t input_len,
                           const uint8_t* ARROW_ARG_UNUSED(input)) override {
    DCHECK_GE(input_len, 0);
    return cramjam::zstd_max_compressed_len(input_len);
  }

  Result<std::shared_ptr<Compressor>> MakeCompressor() override {
    auto ptr = std::make_shared<ZSTDCompressor>(compression_level_);
    RETURN_NOT_OK(ptr->Init());
    return ptr;
  }

  Result<std::shared_ptr<Decompressor>> MakeDecompressor() override {
    auto ptr = std::make_shared<ZSTDDecompressor>();
    RETURN_NOT_OK(ptr->Init());
    return ptr;
  }

  Compression::type compression_type() const override { return Compression::ZSTD; }
  int minimum_compression_level() const override { return ZSTD_minCLevel(); }
  int maximum_compression_level() const override { return ZSTD_maxCLevel(); }
  int default_compression_level() const override { return kZSTDDefaultCompressionLevel; }

  int compression_level() const override { return compression_level_; }
};

}  // namespace

std::unique_ptr<Codec> MakeZSTDCodec(int compression_level) {
  return std::make_unique<ZSTDCodec>(compression_level);
}

}  // namespace internal
}  // namespace util
}  // namespace arrow
