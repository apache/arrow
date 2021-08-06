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
#include <sstream>

// Avoid defining max() macro
#include "arrow/util/windows_compatibility.h"

#include <bzlib.h>

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

// Max number of bytes the bz2 APIs accept at a time
constexpr auto kSizeLimit =
    static_cast<int64_t>(std::numeric_limits<unsigned int>::max());

Status BZ2Error(const char* prefix_msg, int bz_result) {
  ARROW_CHECK(bz_result != BZ_OK && bz_result != BZ_RUN_OK && bz_result != BZ_FLUSH_OK &&
              bz_result != BZ_FINISH_OK && bz_result != BZ_STREAM_END);
  StatusCode code;
  std::stringstream ss;
  ss << prefix_msg;
  switch (bz_result) {
    case BZ_CONFIG_ERROR:
      code = StatusCode::UnknownError;
      ss << "bz2 library improperly configured (internal error)";
      break;
    case BZ_SEQUENCE_ERROR:
      code = StatusCode::UnknownError;
      ss << "wrong sequence of calls to bz2 library (internal error)";
      break;
    case BZ_PARAM_ERROR:
      code = StatusCode::UnknownError;
      ss << "wrong parameter to bz2 library (internal error)";
      break;
    case BZ_MEM_ERROR:
      code = StatusCode::OutOfMemory;
      ss << "could not allocate memory for bz2 library";
      break;
    case BZ_DATA_ERROR:
      code = StatusCode::IOError;
      ss << "invalid bz2 data";
      break;
    case BZ_DATA_ERROR_MAGIC:
      code = StatusCode::IOError;
      ss << "data is not bz2-compressed (no magic header)";
      break;
    default:
      code = StatusCode::UnknownError;
      ss << "unknown bz2 error " << bz_result;
      break;
  }
  return Status(code, ss.str());
}

// ----------------------------------------------------------------------
// bz2 decompressor implementation

class BZ2Decompressor : public Decompressor {
 public:
  BZ2Decompressor() : initialized_(false) {}

  ~BZ2Decompressor() override {
    if (initialized_) {
      ARROW_UNUSED(BZ2_bzDecompressEnd(&stream_));
    }
  }

  Status Init() {
    DCHECK(!initialized_);
    memset(&stream_, 0, sizeof(stream_));
    int ret;
    ret = BZ2_bzDecompressInit(&stream_, 0, 0);
    if (ret != BZ_OK) {
      return BZ2Error("bz2 decompressor init failed: ", ret);
    }
    initialized_ = true;
    finished_ = false;
    return Status::OK();
  }

  Status Reset() override {
    if (initialized_) {
      ARROW_UNUSED(BZ2_bzDecompressEnd(&stream_));
      initialized_ = false;
    }
    return Init();
  }

  Result<DecompressResult> Decompress(int64_t input_len, const uint8_t* input,
                                      int64_t output_len, uint8_t* output) override {
    stream_.next_in = const_cast<char*>(reinterpret_cast<const char*>(input));
    stream_.avail_in = static_cast<unsigned int>(std::min(input_len, kSizeLimit));
    stream_.next_out = reinterpret_cast<char*>(output);
    stream_.avail_out = static_cast<unsigned int>(std::min(output_len, kSizeLimit));
    int ret;

    ret = BZ2_bzDecompress(&stream_);
    if (ret == BZ_OK || ret == BZ_STREAM_END) {
      finished_ = (ret == BZ_STREAM_END);
      int64_t bytes_read = input_len - stream_.avail_in;
      int64_t bytes_written = output_len - stream_.avail_out;
      return DecompressResult{bytes_read, bytes_written,
                              (!finished_ && bytes_read == 0 && bytes_written == 0)};
    } else {
      return BZ2Error("bz2 decompress failed: ", ret);
    }
  }

  bool IsFinished() override { return finished_; }

 protected:
  bz_stream stream_;
  bool initialized_;
  bool finished_;
};

// ----------------------------------------------------------------------
// bz2 compressor implementation

class BZ2Compressor : public Compressor {
 public:
  explicit BZ2Compressor(int compression_level)
      : initialized_(false), compression_level_(compression_level) {}

  ~BZ2Compressor() override {
    if (initialized_) {
      ARROW_UNUSED(BZ2_bzCompressEnd(&stream_));
    }
  }

  Status Init() {
    DCHECK(!initialized_);
    memset(&stream_, 0, sizeof(stream_));
    int ret;
    ret = BZ2_bzCompressInit(&stream_, compression_level_, 0, 0);
    if (ret != BZ_OK) {
      return BZ2Error("bz2 compressor init failed: ", ret);
    }
    initialized_ = true;
    return Status::OK();
  }

  Result<CompressResult> Compress(int64_t input_len, const uint8_t* input,
                                  int64_t output_len, uint8_t* output) override {
    stream_.next_in = const_cast<char*>(reinterpret_cast<const char*>(input));
    stream_.avail_in = static_cast<unsigned int>(std::min(input_len, kSizeLimit));
    stream_.next_out = reinterpret_cast<char*>(output);
    stream_.avail_out = static_cast<unsigned int>(std::min(output_len, kSizeLimit));
    int ret;

    ret = BZ2_bzCompress(&stream_, BZ_RUN);
    if (ret == BZ_RUN_OK) {
      return CompressResult{input_len - stream_.avail_in, output_len - stream_.avail_out};
    } else {
      return BZ2Error("bz2 compress failed: ", ret);
    }
  }

  Result<FlushResult> Flush(int64_t output_len, uint8_t* output) override {
    stream_.next_in = nullptr;
    stream_.avail_in = 0;
    stream_.next_out = reinterpret_cast<char*>(output);
    stream_.avail_out = static_cast<unsigned int>(std::min(output_len, kSizeLimit));
    int ret;

    ret = BZ2_bzCompress(&stream_, BZ_FLUSH);
    if (ret == BZ_RUN_OK || ret == BZ_FLUSH_OK) {
      return FlushResult{output_len - stream_.avail_out, (ret == BZ_FLUSH_OK)};
    } else {
      return BZ2Error("bz2 compress failed: ", ret);
    }
  }

  Result<EndResult> End(int64_t output_len, uint8_t* output) override {
    stream_.next_in = nullptr;
    stream_.avail_in = 0;
    stream_.next_out = reinterpret_cast<char*>(output);
    stream_.avail_out = static_cast<unsigned int>(std::min(output_len, kSizeLimit));
    int ret;

    ret = BZ2_bzCompress(&stream_, BZ_FINISH);
    if (ret == BZ_STREAM_END || ret == BZ_FINISH_OK) {
      return EndResult{output_len - stream_.avail_out, (ret == BZ_FINISH_OK)};
    } else {
      return BZ2Error("bz2 compress failed: ", ret);
    }
  }

 protected:
  bz_stream stream_;
  bool initialized_;
  int compression_level_;
};

// ----------------------------------------------------------------------
// bz2 codec implementation

class BZ2Codec : public Codec {
 public:
  explicit BZ2Codec(int compression_level) : compression_level_(compression_level) {
    compression_level_ = compression_level == kUseDefaultCompressionLevel
                             ? kBZ2DefaultCompressionLevel
                             : compression_level;
  }

  Result<int64_t> Decompress(int64_t input_len, const uint8_t* input,
                             int64_t output_buffer_len, uint8_t* output_buffer) override {
    return Status::NotImplemented("One-shot bz2 decompression not supported");
  }

  Result<int64_t> Compress(int64_t input_len, const uint8_t* input,
                           int64_t output_buffer_len, uint8_t* output_buffer) override {
    return Status::NotImplemented("One-shot bz2 compression not supported");
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

 private:
  int compression_level_;
};

}  // namespace

std::unique_ptr<Codec> MakeBZ2Codec(int compression_level) {
  return std::unique_ptr<Codec>(new BZ2Codec(compression_level));
}

}  // namespace internal
}  // namespace util
}  // namespace arrow
