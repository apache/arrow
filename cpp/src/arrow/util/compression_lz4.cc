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

#include "arrow/util/compression.h"

#include <cstdint>
#include <cstring>
#include <memory>

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

namespace {

static Status LZ4Error(LZ4F_errorCode_t ret, const char* prefix_msg) {
  return Status::IOError(prefix_msg, LZ4F_getErrorName(ret));
}

static LZ4F_preferences_t DefaultPreferences() {
  LZ4F_preferences_t prefs;
  memset(&prefs, 0, sizeof(prefs));
  return prefs;
}

// ----------------------------------------------------------------------
// Lz4 frame decompressor implementation

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
// Lz4 frame compressor implementation

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
    prefs_ = DefaultPreferences();
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
// Lz4 frame codec implementation

class Lz4FrameCodec : public Codec {
 public:
  Lz4FrameCodec() : prefs_(DefaultPreferences()) {}

  int64_t MaxCompressedLen(int64_t input_len,
                           const uint8_t* ARROW_ARG_UNUSED(input)) override {
    return static_cast<int64_t>(
        LZ4F_compressFrameBound(static_cast<size_t>(input_len), &prefs_));
  }

  Result<int64_t> Compress(int64_t input_len, const uint8_t* input,
                           int64_t output_buffer_len, uint8_t* output_buffer) override {
    auto output_len =
        LZ4F_compressFrame(output_buffer, static_cast<size_t>(output_buffer_len), input,
                           static_cast<size_t>(input_len), &prefs_);
    if (LZ4F_isError(output_len)) {
      return LZ4Error(output_len, "Lz4 compression failure: ");
    }
    return static_cast<int64_t>(output_len);
  }

  Result<int64_t> Decompress(int64_t input_len, const uint8_t* input,
                             int64_t output_buffer_len, uint8_t* output_buffer) override {
    ARROW_ASSIGN_OR_RAISE(auto decomp, MakeDecompressor());

    int64_t total_bytes_written = 0;
    while (!decomp->IsFinished() && input_len != 0) {
      ARROW_ASSIGN_OR_RAISE(
          auto res,
          decomp->Decompress(input_len, input, output_buffer_len, output_buffer));
      input += res.bytes_read;
      input_len -= res.bytes_read;
      output_buffer += res.bytes_written;
      output_buffer_len -= res.bytes_written;
      total_bytes_written += res.bytes_written;
      if (res.need_more_output) {
        return Status::IOError("Lz4 decompression buffer too small");
      }
    }
    if (!decomp->IsFinished()) {
      return Status::IOError("Lz4 compressed input contains less than one frame");
    }
    if (input_len != 0) {
      return Status::IOError("Lz4 compressed input contains more than one frame");
    }
    return total_bytes_written;
  }

  Result<std::shared_ptr<Compressor>> MakeCompressor() override {
    auto ptr = std::make_shared<LZ4Compressor>();
    RETURN_NOT_OK(ptr->Init());
    return ptr;
  }

  Result<std::shared_ptr<Decompressor>> MakeDecompressor() override {
    auto ptr = std::make_shared<LZ4Decompressor>();
    RETURN_NOT_OK(ptr->Init());
    return ptr;
  }

  const char* name() const override { return "lz4"; }

 protected:
  LZ4F_preferences_t prefs_;
};

// ----------------------------------------------------------------------
// Lz4 "raw" codec implementation

class Lz4Codec : public Codec {
 public:
  Result<int64_t> Decompress(int64_t input_len, const uint8_t* input,
                             int64_t output_buffer_len, uint8_t* output_buffer) override {
    int64_t decompressed_size = LZ4_decompress_safe(
        reinterpret_cast<const char*>(input), reinterpret_cast<char*>(output_buffer),
        static_cast<int>(input_len), static_cast<int>(output_buffer_len));
    if (decompressed_size < 0) {
      return Status::IOError("Corrupt Lz4 compressed data.");
    }
    return decompressed_size;
  }

  int64_t MaxCompressedLen(int64_t input_len,
                           const uint8_t* ARROW_ARG_UNUSED(input)) override {
    return LZ4_compressBound(static_cast<int>(input_len));
  }

  Result<int64_t> Compress(int64_t input_len, const uint8_t* input,
                           int64_t output_buffer_len, uint8_t* output_buffer) override {
    int64_t output_len = LZ4_compress_default(
        reinterpret_cast<const char*>(input), reinterpret_cast<char*>(output_buffer),
        static_cast<int>(input_len), static_cast<int>(output_buffer_len));
    if (output_len == 0) {
      return Status::IOError("Lz4 compression failure.");
    }
    return output_len;
  }

  Result<std::shared_ptr<Compressor>> MakeCompressor() override {
    return Status::NotImplemented(
        "Streaming compression unsupported with LZ4 raw format. "
        "Try using LZ4 frame format instead.");
  }

  Result<std::shared_ptr<Decompressor>> MakeDecompressor() override {
    return Status::NotImplemented(
        "Streaming decompression unsupported with LZ4 raw format. "
        "Try using LZ4 frame format instead.");
  }

  const char* name() const override { return "lz4_raw"; }
};

}  // namespace

namespace internal {

std::unique_ptr<Codec> MakeLz4FrameCodec() {
  return std::unique_ptr<Codec>(new Lz4FrameCodec());
}

std::unique_ptr<Codec> MakeLz4RawCodec() {
  return std::unique_ptr<Codec>(new Lz4Codec());
}

}  // namespace internal

}  // namespace util
}  // namespace arrow
