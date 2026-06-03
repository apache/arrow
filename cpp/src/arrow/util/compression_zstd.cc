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
#include <utility>
#include <vector>

#include <zstd.h>

#include "arrow/result.h"
#include "arrow/status.h"
#include "arrow/util/logging_internal.h"
#include "arrow/util/macros.h"

using std::size_t;

namespace arrow {
namespace util {
namespace internal {

namespace {

using CCtxPtr = std::unique_ptr<ZSTD_CCtx, decltype(&ZSTD_freeCCtx)>;
using DCtxPtr = std::unique_ptr<ZSTD_DCtx, decltype(&ZSTD_freeDCtx)>;

Status ZSTDError(size_t ret, const char* prefix_msg) {
  return Status::IOError(prefix_msg, ZSTD_getErrorName(ret));
}

// ----------------------------------------------------------------------
// ZSTD decompressor implementation

class ZSTDDecompressor : public Decompressor {
 public:
  explicit ZSTDDecompressor(DCtxPtr stream) : stream_(std::move(stream)) {}

  Result<DecompressResult> Decompress(int64_t input_len, const uint8_t* input,
                                      int64_t output_len, uint8_t* output) override {
    ZSTD_inBuffer in_buf;
    ZSTD_outBuffer out_buf;

    in_buf.src = input;
    in_buf.size = static_cast<size_t>(input_len);
    in_buf.pos = 0;
    out_buf.dst = output;
    out_buf.size = static_cast<size_t>(output_len);
    out_buf.pos = 0;

    size_t ret;
    ret = ZSTD_decompressStream(stream_.get(), &out_buf, &in_buf);
    if (ZSTD_isError(ret)) {
      return ZSTDError(ret, "ZSTD decompress failed: ");
    }
    finished_ = (ret == 0);
    return DecompressResult{static_cast<int64_t>(in_buf.pos),
                            static_cast<int64_t>(out_buf.pos),
                            in_buf.pos == 0 && out_buf.pos == 0};
  }

  Status Reset() override {
    finished_ = false;
    auto ret = ZSTD_DCtx_reset(stream_.get(), ZSTD_reset_session_only);
    if (ZSTD_isError(ret)) {
      return ZSTDError(ret, "ZSTD reset failed: ");
    }
    return {};
  }

  bool IsFinished() override { return finished_; }

 private:
  DCtxPtr stream_;
  bool finished_{false};
};

// ----------------------------------------------------------------------
// ZSTD compressor implementation

class ZSTDCompressor : public Compressor {
 public:
  explicit ZSTDCompressor(CCtxPtr stream) : stream_(std::move(stream)) {}

  Result<CompressResult> Compress(int64_t input_len, const uint8_t* input,
                                  int64_t output_len, uint8_t* output) override {
    ZSTD_inBuffer in_buf;
    ZSTD_outBuffer out_buf;

    in_buf.src = input;
    in_buf.size = static_cast<size_t>(input_len);
    in_buf.pos = 0;
    out_buf.dst = output;
    out_buf.size = static_cast<size_t>(output_len);
    out_buf.pos = 0;

    size_t ret;
    ret = ZSTD_compressStream(stream_.get(), &out_buf, &in_buf);
    if (ZSTD_isError(ret)) {
      return ZSTDError(ret, "ZSTD compress failed: ");
    }
    return CompressResult{static_cast<int64_t>(in_buf.pos),
                          static_cast<int64_t>(out_buf.pos)};
  }

  Result<FlushResult> Flush(int64_t output_len, uint8_t* output) override {
    ZSTD_outBuffer out_buf;

    out_buf.dst = output;
    out_buf.size = static_cast<size_t>(output_len);
    out_buf.pos = 0;

    size_t ret;
    ret = ZSTD_flushStream(stream_.get(), &out_buf);
    if (ZSTD_isError(ret)) {
      return ZSTDError(ret, "ZSTD flush failed: ");
    }
    return FlushResult{static_cast<int64_t>(out_buf.pos), ret > 0};
  }

  Result<EndResult> End(int64_t output_len, uint8_t* output) override {
    ZSTD_outBuffer out_buf;

    out_buf.dst = output;
    out_buf.size = static_cast<size_t>(output_len);
    out_buf.pos = 0;

    size_t ret;
    ret = ZSTD_endStream(stream_.get(), &out_buf);
    if (ZSTD_isError(ret)) {
      return ZSTDError(ret, "ZSTD end failed: ");
    }
    return EndResult{static_cast<int64_t>(out_buf.pos), ret > 0};
  }

 private:
  CCtxPtr stream_;
};

// ----------------------------------------------------------------------
// ZSTD codec implementation

class ZSTDCodec : public Codec {
 public:
  explicit ZSTDCodec(int compression_level,
                     std::vector<std::pair<int, int>> compression_context_params,
                     std::vector<std::pair<int, int>> decompression_context_params)
      : compression_level_(compression_level == kUseDefaultCompressionLevel
                               ? kZSTDDefaultCompressionLevel
                               : compression_level),
        compression_context_params_(std::move(compression_context_params)),
        decompression_context_params_(std::move(decompression_context_params)) {}

  Result<int64_t> Decompress(int64_t input_len, const uint8_t* input,
                             int64_t output_buffer_len, uint8_t* output_buffer) override {
    if (output_buffer == nullptr) {
      // We may pass a NULL 0-byte output buffer but some zstd versions demand
      // a valid pointer: https://github.com/facebook/zstd/issues/1385
      static uint8_t empty_buffer;
      DCHECK_EQ(output_buffer_len, 0);
      output_buffer = &empty_buffer;
    }

    ARROW_ASSIGN_OR_RAISE(auto dctx, CreateDCtx());
    size_t ret = ZSTD_decompressDCtx(dctx.get(), output_buffer,
                                     static_cast<size_t>(output_buffer_len), input,
                                     static_cast<size_t>(input_len));
    if (ZSTD_isError(ret)) {
      return ZSTDError(ret, "ZSTD decompression failed: ");
    }
    if (static_cast<int64_t>(ret) != output_buffer_len) {
      return Status::IOError("Corrupt ZSTD compressed data.");
    }
    return static_cast<int64_t>(ret);
  }

  int64_t MaxCompressedLen(int64_t input_len,
                           const uint8_t* ARROW_ARG_UNUSED(input)) override {
    DCHECK_GE(input_len, 0);
    return ZSTD_compressBound(static_cast<size_t>(input_len));
  }

  Result<int64_t> Compress(int64_t input_len, const uint8_t* input,
                           int64_t output_buffer_len, uint8_t* output_buffer) override {
    ARROW_ASSIGN_OR_RAISE(auto cctx, CreateCCtx());
    size_t ret =
        ZSTD_compress2(cctx.get(), output_buffer, static_cast<size_t>(output_buffer_len),
                       input, static_cast<size_t>(input_len));
    if (ZSTD_isError(ret)) {
      return ZSTDError(ret, "ZSTD compression failed: ");
    }
    return static_cast<int64_t>(ret);
  }

  Result<std::shared_ptr<Compressor>> MakeCompressor() override {
    ARROW_ASSIGN_OR_RAISE(auto cctx, CreateCCtx());
    return std::make_shared<ZSTDCompressor>(std::move(cctx));
  }

  Result<std::shared_ptr<Decompressor>> MakeDecompressor() override {
    ARROW_ASSIGN_OR_RAISE(auto dctx, CreateDCtx());
    return std::make_shared<ZSTDDecompressor>(std::move(dctx));
  }

  Compression::type compression_type() const override { return Compression::ZSTD; }
  int minimum_compression_level() const override { return ZSTD_minCLevel(); }
  int maximum_compression_level() const override { return ZSTD_maxCLevel(); }
  int default_compression_level() const override { return kZSTDDefaultCompressionLevel; }

  int compression_level() const override { return compression_level_; }

 private:
  Result<CCtxPtr> CreateCCtx() const {
    CCtxPtr cctx{ZSTD_createCCtx(), ZSTD_freeCCtx};
    if (cctx == nullptr) {
      return Status::OutOfMemory("ZSTD_CCtx create failed");
    }
    auto ret =
        ZSTD_CCtx_setParameter(cctx.get(), ZSTD_c_compressionLevel, compression_level_);
    if (ZSTD_isError(ret)) {
      return ZSTDError(ret, "ZSTD_CCtx create failed: ");
    }
    for (auto& [key, value] : compression_context_params_) {
      ret = ZSTD_CCtx_setParameter(cctx.get(), static_cast<ZSTD_cParameter>(key), value);
      if (ZSTD_isError(ret)) {
        return ZSTDError(ret, "ZSTD_CCtx create failed: ");
      }
    }
    return cctx;
  }

  Result<DCtxPtr> CreateDCtx() const {
    DCtxPtr dctx{ZSTD_createDCtx(), ZSTD_freeDCtx};
    if (dctx == nullptr) {
      return Status::OutOfMemory("ZSTD_DCtx create failed");
    }
    for (auto& [key, value] : decompression_context_params_) {
      auto ret =
          ZSTD_DCtx_setParameter(dctx.get(), static_cast<ZSTD_dParameter>(key), value);
      if (ZSTD_isError(ret)) {
        return ZSTDError(ret, "ZSTD_DCtx create failed: ");
      }
    }
    return dctx;
  }

  const int compression_level_;
  const std::vector<std::pair<int, int>> compression_context_params_;
  const std::vector<std::pair<int, int>> decompression_context_params_;
};

}  // namespace

std::unique_ptr<Codec> MakeZSTDCodec(
    int compression_level, std::vector<std::pair<int, int>> compression_context_params,
    std::vector<std::pair<int, int>> decompression_context_params) {
  return std::make_unique<ZSTDCodec>(compression_level,
                                     std::move(compression_context_params),
                                     std::move(decompression_context_params));
}

}  // namespace internal
}  // namespace util
}  // namespace arrow
