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

#include "arrow/buffer.h"
#include "arrow/util/compression_internal.h"

#include <algorithm>
#include <cstdint>
#include <cstring>
#include <limits>
#include <memory>
#include <sstream>

// Avoid defining max() macro
#include "arrow/util/windows_compatibility.h"

#include "arrow/result.h"
#include "arrow/status.h"
#include "arrow/util/logging.h"
#include "arrow/util/macros.h"

namespace arrow {
namespace util {
namespace internal {

namespace {

constexpr int kBZ2MinCompressionLevel = 1;
constexpr int kBZ2MaxCompressionLevel = 9;

// ----------------------------------------------------------------------
// bz2 decompressor implementation

class BZ2Decompressor : public Decompressor {
 public:
  BZ2Decompressor() { cjformat_ = cramjam::StreamingBzip2; };
};

// ----------------------------------------------------------------------
// bz2 compressor implementation

class BZ2Compressor : public Compressor {
 public:
  BZ2Compressor() {
    compression_level_ = DEFAULT_COMPRESSION_LEVEL;
    cjformat_ = cramjam::StreamingBzip2;
  };
  explicit BZ2Compressor(int compression_level) {
    compression_level_ = compression_level;
    cjformat_ = cramjam::StreamingBzip2;
  }
};

// ----------------------------------------------------------------------
// bz2 codec implementation

class BZ2Codec : public Codec {
 public:
  explicit BZ2Codec(int compression_level) {
    cjformat_ = cramjam::Bzip2;
    compression_level_ = compression_level == kUseDefaultCompressionLevel
                             ? kBZ2DefaultCompressionLevel
                             : compression_level;
  }

  int64_t MaxCompressedLen(int64_t input_len,
                           const uint8_t* ARROW_ARG_UNUSED(input)) override {
    // Cannot determine upper bound for bz2-compressed data
    return 0;
  }

  Result<std::shared_ptr<Compressor>> MakeCompressor() override {
    auto ptr = std::make_shared<BZ2Compressor>(compression_level_);
    RETURN_NOT_OK(ptr->Init());
    return ptr;
  }

  Result<std::shared_ptr<Decompressor>> MakeDecompressor() override {
    auto ptr = std::make_shared<BZ2Decompressor>();
    RETURN_NOT_OK(ptr->Init());
    return ptr;
  }

  Compression::type compression_type() const override { return Compression::BZ2; }

  int compression_level() const override { return compression_level_; }
  int minimum_compression_level() const override { return kBZ2MinCompressionLevel; }
  int maximum_compression_level() const override { return kBZ2MaxCompressionLevel; }
  int default_compression_level() const override { return kBZ2DefaultCompressionLevel; }
};

}  // namespace

std::unique_ptr<Codec> MakeBZ2Codec(int compression_level) {
  return std::make_unique<BZ2Codec>(compression_level);
}

}  // namespace internal
}  // namespace util
}  // namespace arrow
