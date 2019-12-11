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

#include "arrow/util/compression_lz4.h"

#include <cstdint>
#include <cstring>

#include <lz4.h>
#include <lz4frame.h>

#include "arrow/result.h"
#include "arrow/status.h"
#include "arrow/util/logging.h"
#include "arrow/util/macros.h"

#ifndef LZ4F_HEADER_SIZE_MAX
#define LZ4F_HEADER_SIZE_MAX 19
#endif

namespace arrow {
namespace util {

static Status LZ4Error(LZ4F_errorCode_t ret, const char* prefix_msg) {
  return Status::IOError(prefix_msg, LZ4F_getErrorName(ret));
}

// ----------------------------------------------------------------------
// Lz4 decompressor implementation

class LZ4Decompressor : public Decompressor {
 public:
  LZ4Decompressor() {}

  ~LZ4Decompressor() override {
    if (ctx_ != nullptr) {
      ARROW_UNUSED(LZ4F_freeDecompressionContext(ctx_));
    }
  }

  Status Init() {
    LZ4F_errorCode_t ret;
    finished_ = false;

    ret = LZ4F_createDecompressionContext(&ctx_, LZ4F_VERSION);
    if (LZ4F_isError(ret)) {
      return LZ4Error(ret, "LZ4 init failed: ");
    } else {
      return Status::OK();
    }
  }

  Status Reset() override {
#if defined(LZ4_VERSION_NUMBER) && LZ4_VERSION_NUMBER >= 10800
    // LZ4F_resetDecompressionContext appeared in 1.8.0
    DCHECK_NE(ctx_, nullptr);
    LZ4F_resetDecompressionContext(ctx_);
    finished_ = false;
    return Status::OK();
#else
    if (ctx_ != nullptr) {
      ARROW_UNUSED(LZ4F_freeDecompressionContext(ctx_));
    }
    return Init();
#endif
  }

  Result<DecompressResult> Decompress(int64_t input_len, const uint8_t* input,
                                      int64_t output_len, uint8_t* output) override {
    auto src = input;
    auto dst = output;
    auto src_size = static_cast<size_t>(input_len);
    auto dst_capacity = static_cast<size_t>(output_len);
    size_t ret;

    ret =
        LZ4F_decompress(ctx_, dst, &dst_capacity, src, &src_size, nullptr /* options */);
    if (LZ4F_isError(ret)) {
      return LZ4Error(ret, "LZ4 decompress failed: ");
    }
    finished_ = (ret == 0);
    return DecompressResult{static_cast<int64_t>(src_size),
                            static_cast<int64_t>(dst_capacity),
                            (src_size == 0 && dst_capacity == 0)};
  }

  bool IsFinished() override { return finished_; }

 protected:
  LZ4F_decompressionContext_t ctx_ = nullptr;
  bool finished_;
};

// ----------------------------------------------------------------------
// Lz4 compressor implementation

class LZ4Compressor : public Compressor {
 public:
  LZ4Compressor() {}

  ~LZ4Compressor() override {
    if (ctx_ != nullptr) {
      ARROW_UNUSED(LZ4F_freeCompressionContext(ctx_));
    }
  }

  Status Init() {
    LZ4F_errorCode_t ret;
    memset(&prefs_, 0, sizeof(prefs_));
    first_time_ = true;

    ret = LZ4F_createCompressionContext(&ctx_, LZ4F_VERSION);
    if (LZ4F_isError(ret)) {
      return LZ4Error(ret, "LZ4 init failed: ");
    } else {
      return Status::OK();
    }
  }

#define BEGIN_COMPRESS(dst, dst_capacity, output_too_small)     \
  if (first_time_) {                                            \
    if (dst_capacity < LZ4F_HEADER_SIZE_MAX) {                  \
      /* Output too small to write LZ4F header */               \
      return (output_too_small);                                \
    }                                                           \
    ret = LZ4F_compressBegin(ctx_, dst, dst_capacity, &prefs_); \
    if (LZ4F_isError(ret)) {                                    \
      return LZ4Error(ret, "LZ4 compress begin failed: ");      \
    }                                                           \
    first_time_ = false;                                        \
    dst += ret;                                                 \
    dst_capacity -= ret;                                        \
    bytes_written += static_cast<int64_t>(ret);                 \
  }

  Result<CompressResult> Compress(int64_t input_len, const uint8_t* input,
                                  int64_t output_len, uint8_t* output) override {
    auto src = input;
    auto dst = output;
    auto src_size = static_cast<size_t>(input_len);
    auto dst_capacity = static_cast<size_t>(output_len);
    size_t ret;
    int64_t bytes_written = 0;

    BEGIN_COMPRESS(dst, dst_capacity, (CompressResult{0, 0}));

    if (dst_capacity < LZ4F_compressBound(src_size, &prefs_)) {
      // Output too small to compress into
      return CompressResult{0, bytes_written};
    }
    ret = LZ4F_compressUpdate(ctx_, dst, dst_capacity, src, src_size,
                              nullptr /* options */);
    if (LZ4F_isError(ret)) {
      return LZ4Error(ret, "LZ4 compress update failed: ");
    }
    bytes_written += static_cast<int64_t>(ret);
    DCHECK_LE(bytes_written, output_len);
    return CompressResult{input_len, bytes_written};
  }

  Result<FlushResult> Flush(int64_t output_len, uint8_t* output) override {
    auto dst = output;
    auto dst_capacity = static_cast<size_t>(output_len);
    size_t ret;
    int64_t bytes_written = 0;

    BEGIN_COMPRESS(dst, dst_capacity, (FlushResult{0, true}));

    if (dst_capacity < LZ4F_compressBound(0, &prefs_)) {
      // Output too small to flush into
      return FlushResult{bytes_written, true};
    }

    ret = LZ4F_flush(ctx_, dst, dst_capacity, nullptr /* options */);
    if (LZ4F_isError(ret)) {
      return LZ4Error(ret, "LZ4 flush failed: ");
    }
    bytes_written += static_cast<int64_t>(ret);
    DCHECK_LE(bytes_written, output_len);
    return FlushResult{bytes_written, false};
  }

  Result<EndResult> End(int64_t output_len, uint8_t* output) override {
    auto dst = output;
    auto dst_capacity = static_cast<size_t>(output_len);
    size_t ret;
    int64_t bytes_written = 0;

    BEGIN_COMPRESS(dst, dst_capacity, (EndResult{0, true}));

    if (dst_capacity < LZ4F_compressBound(0, &prefs_)) {
      // Output too small to end frame into
      return EndResult{bytes_written, true};
    }

    ret = LZ4F_compressEnd(ctx_, dst, dst_capacity, nullptr /* options */);
    if (LZ4F_isError(ret)) {
      return LZ4Error(ret, "LZ4 end failed: ");
    }
    bytes_written += static_cast<int64_t>(ret);
    DCHECK_LE(bytes_written, output_len);
    return EndResult{bytes_written, false};
  }

#undef BEGIN_COMPRESS

 protected:
  LZ4F_compressionContext_t ctx_ = nullptr;
  LZ4F_preferences_t prefs_;
  bool first_time_;
};

// ----------------------------------------------------------------------
// Lz4 codec implementation

Result<std::shared_ptr<Compressor>> Lz4Codec::MakeCompressor() {
  auto ptr = std::make_shared<LZ4Compressor>();
  RETURN_NOT_OK(ptr->Init());
  return ptr;
}

Result<std::shared_ptr<Decompressor>> Lz4Codec::MakeDecompressor() {
  auto ptr = std::make_shared<LZ4Decompressor>();
  RETURN_NOT_OK(ptr->Init());
  return ptr;
}

Result<int64_t> Lz4Codec::Decompress(int64_t input_len, const uint8_t* input,
                                     int64_t output_buffer_len, uint8_t* output_buffer) {
  int64_t decompressed_size = LZ4_decompress_safe(
      reinterpret_cast<const char*>(input), reinterpret_cast<char*>(output_buffer),
      static_cast<int>(input_len), static_cast<int>(output_buffer_len));
  if (decompressed_size < 0) {
    return Status::IOError("Corrupt Lz4 compressed data.");
  }
  return decompressed_size;
}

int64_t Lz4Codec::MaxCompressedLen(int64_t input_len,
                                   const uint8_t* ARROW_ARG_UNUSED(input)) {
  return LZ4_compressBound(static_cast<int>(input_len));
}

Result<int64_t> Lz4Codec::Compress(int64_t input_len, const uint8_t* input,
                                   int64_t output_buffer_len, uint8_t* output_buffer) {
  int64_t output_len = LZ4_compress_default(
      reinterpret_cast<const char*>(input), reinterpret_cast<char*>(output_buffer),
      static_cast<int>(input_len), static_cast<int>(output_buffer_len));
  if (output_len == 0) {
    return Status::IOError("Lz4 compression failure.");
  }
  return output_len;
}

}  // namespace util
}  // namespace arrow
