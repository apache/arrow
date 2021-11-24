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

#include <zstd.h>

#include "arrow/result.h"
#include "arrow/status.h"
#include "arrow/util/logging.h"
#include "arrow/util/macros.h"

using std::size_t;

namespace arrow {
namespace util {
namespace internal {

namespace {

Status ZSTDError(size_t ret, const char* prefix_msg) {
  return Status::IOError(prefix_msg, ZSTD_getErrorName(ret));
}

// ----------------------------------------------------------------------
// ZSTD decompressor implementation

class ZSTDDecompressor : public Decompressor {
 public:
  ZSTDDecompressor() : stream_(ZSTD_createDStream()) {}

  ~ZSTDDecompressor() override { ZSTD_freeDStream(stream_); }

  Status Init() {
    finished_ = false;
    size_t ret = ZSTD_initDStream(stream_);
    if (ZSTD_isError(ret)) {
      return ZSTDError(ret, "ZSTD init failed: ");
    } else {
      return Status::OK();
    }
  }

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
    ret = ZSTD_decompressStream(stream_, &out_buf, &in_buf);
    if (ZSTD_isError(ret)) {
      return ZSTDError(ret, "ZSTD decompress failed: ");
    }
    finished_ = (ret == 0);
    return DecompressResult{static_cast<int64_t>(in_buf.pos),
                            static_cast<int64_t>(out_buf.pos),
                            in_buf.pos == 0 && out_buf.pos == 0};
  }

  Status Reset() override { return Init(); }

  bool IsFinished() override { return finished_; }

 protected:
  ZSTD_DStream* stream_;
  bool finished_;
};

// ----------------------------------------------------------------------
// ZSTD compressor implementation

class ZSTDCompressor : public Compressor {
 public:
  explicit ZSTDCompressor(int compression_level)
      : stream_(ZSTD_createCStream()), compression_level_(compression_level) {}

  ~ZSTDCompressor() override { ZSTD_freeCStream(stream_); }

  Status Init() {
    size_t ret = ZSTD_initCStream(stream_, compression_level_);
    if (ZSTD_isError(ret)) {
      return ZSTDError(ret, "ZSTD init failed: ");
    } else {
      return Status::OK();
    }
  }

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
    ret = ZSTD_compressStream(stream_, &out_buf, &in_buf);
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
    ret = ZSTD_flushStream(stream_, &out_buf);
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
    ret = ZSTD_endStream(stream_, &out_buf);
    if (ZSTD_isError(ret)) {
      return ZSTDError(ret, "ZSTD end failed: ");
    }
    return EndResult{static_cast<int64_t>(out_buf.pos), ret > 0};
  }

 protected:
  ZSTD_CStream* stream_;

 private:
  int compression_level_;
};

// ----------------------------------------------------------------------
// ZSTD codec implementation

class ZSTDCodec : public Codec {
 public:
  explicit ZSTDCodec(int compression_level)
      : compression_level_(compression_level == kUseDefaultCompressionLevel
                               ? kZSTDDefaultCompressionLevel
                               : compression_level) {}

  Result<int64_t> Decompress(int64_t input_len, const uint8_t* input,
                             int64_t output_buffer_len, uint8_t* output_buffer) override {
    if (output_buffer == nullptr) {
      // We may pass a NULL 0-byte output buffer but some zstd versions demand
      // a valid pointer: https://github.com/facebook/zstd/issues/1385
      static uint8_t empty_buffer;
      DCHECK_EQ(output_buffer_len, 0);
      output_buffer = &empty_buffer;
    }

    size_t ret = ZSTD_decompress(output_buffer, static_cast<size_t>(output_buffer_len),
                                 input, static_cast<size_t>(input_len));
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
    size_t ret = ZSTD_compress(output_buffer, static_cast<size_t>(output_buffer_len),
                               input, static_cast<size_t>(input_len), compression_level_);
    if (ZSTD_isError(ret)) {
      return ZSTDError(ret, "ZSTD compression failed: ");
    }
    return static_cast<int64_t>(ret);
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

 private:
  const int compression_level_;
};

}  // namespace

std::unique_ptr<Codec> MakeZSTDCodec(int compression_level) {
  return std::unique_ptr<Codec>(new ZSTDCodec(compression_level));
}

}  // namespace internal
}  // namespace util
}  // namespace arrow
