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
// Snappy decompressor implementation

class SnappyFrameDecompressor : public Decompressor {
 public:
  SnappyFrameDecompressor() { cjformat_ = cramjam::StreamingSnappy; };
};

// ----------------------------------------------------------------------
// Snappy compressor implementation

class SnappyFrameCompressor : public Compressor {
 public:
  explicit SnappyFrameCompressor(int compression_level) {
    compression_level_ = compression_level;
    cjformat_ = cramjam::StreamingSnappy;
  };
  SnappyFrameCompressor() {
    compression_level_ = 0;  // Snappy has no compression level
    cjformat_ = cramjam::StreamingSnappy;
  };
};

// ----------------------------------------------------------------------
// Snappy raw implementation

class SnappyCodec : public Codec {
 public:
  SnappyCodec() { cjformat_ = cramjam::SnappyRaw; }

  int64_t MaxCompressedLen(int64_t input_len,
                           const uint8_t* ARROW_ARG_UNUSED(input)) override {
    DCHECK_GE(input_len, 0);
    return cramjam::snappy_raw_max_compressed_len(input_len);
  }

  Result<std::shared_ptr<Compressor>> MakeCompressor() override {
    auto ptr = std::make_shared<SnappyFrameCompressor>(compression_level_);
    RETURN_NOT_OK(ptr->Init());
    return ptr;
  }

  Result<std::shared_ptr<Decompressor>> MakeDecompressor() override {
    auto ptr = std::make_shared<SnappyFrameDecompressor>();
    RETURN_NOT_OK(ptr->Init());
    return ptr;
  }

  Compression::type compression_type() const override { return Compression::SNAPPY; }
  int minimum_compression_level() const override { return kUseDefaultCompressionLevel; }
  int maximum_compression_level() const override { return kUseDefaultCompressionLevel; }
  int default_compression_level() const override { return kUseDefaultCompressionLevel; }
};

}  // namespace

std::unique_ptr<Codec> MakeSnappyCodec() { return std::make_unique<SnappyCodec>(); }

}  // namespace internal
}  // namespace util
}  // namespace arrow
