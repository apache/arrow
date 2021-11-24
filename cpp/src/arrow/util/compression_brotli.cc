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
  ~BrotliDecompressor() override {
    if (state_ != nullptr) {
      BrotliDecoderDestroyInstance(state_);
    }
  }

  Status Init() {
    state_ = BrotliDecoderCreateInstance(nullptr, nullptr, nullptr);
    if (state_ == nullptr) {
      return BrotliError("Brotli init failed");
    }
    return Status::OK();
  }

  Status Reset() override {
    if (state_ != nullptr) {
      BrotliDecoderDestroyInstance(state_);
    }
    return Init();
  }

  Result<DecompressResult> Decompress(int64_t input_len, const uint8_t* input,
                                      int64_t output_len, uint8_t* output) override {
    auto avail_in = static_cast<size_t>(input_len);
    auto avail_out = static_cast<size_t>(output_len);
    BrotliDecoderResult ret;

    ret = BrotliDecoderDecompressStream(state_, &avail_in, &input, &avail_out, &output,
                                        nullptr /* total_out */);
    if (ret == BROTLI_DECODER_RESULT_ERROR) {
      return BrotliError(BrotliDecoderGetErrorCode(state_), "Brotli decompress failed: ");
    }
    return DecompressResult{static_cast<int64_t>(input_len - avail_in),
                            static_cast<int64_t>(output_len - avail_out),
                            (ret == BROTLI_DECODER_RESULT_NEEDS_MORE_OUTPUT)};
  }

  bool IsFinished() override { return BrotliDecoderIsFinished(state_); }

 protected:
  Status BrotliError(const char* msg) { return Status::IOError(msg); }

  Status BrotliError(BrotliDecoderErrorCode code, const char* prefix_msg) {
    return Status::IOError(prefix_msg, BrotliDecoderErrorString(code));
  }

  BrotliDecoderState* state_ = nullptr;
};

// ----------------------------------------------------------------------
// Brotli compressor implementation

class BrotliCompressor : public Compressor {
 public:
  explicit BrotliCompressor(int compression_level)
      : compression_level_(compression_level) {}

  ~BrotliCompressor() override {
    if (state_ != nullptr) {
      BrotliEncoderDestroyInstance(state_);
    }
  }

  Status Init() {
    state_ = BrotliEncoderCreateInstance(nullptr, nullptr, nullptr);
    if (state_ == nullptr) {
      return BrotliError("Brotli init failed");
    }
    if (!BrotliEncoderSetParameter(state_, BROTLI_PARAM_QUALITY, compression_level_)) {
      return BrotliError("Brotli set compression level failed");
    }
    return Status::OK();
  }

  Result<CompressResult> Compress(int64_t input_len, const uint8_t* input,
                                  int64_t output_len, uint8_t* output) override {
    auto avail_in = static_cast<size_t>(input_len);
    auto avail_out = static_cast<size_t>(output_len);
    BROTLI_BOOL ret;

    ret = BrotliEncoderCompressStream(state_, BROTLI_OPERATION_PROCESS, &avail_in, &input,
                                      &avail_out, &output, nullptr /* total_out */);
    if (!ret) {
      return BrotliError("Brotli compress failed");
    }
    return CompressResult{static_cast<int64_t>(input_len - avail_in),
                          static_cast<int64_t>(output_len - avail_out)};
  }

  Result<FlushResult> Flush(int64_t output_len, uint8_t* output) override {
    size_t avail_in = 0;
    const uint8_t* next_in = nullptr;
    auto avail_out = static_cast<size_t>(output_len);
    BROTLI_BOOL ret;

    ret = BrotliEncoderCompressStream(state_, BROTLI_OPERATION_FLUSH, &avail_in, &next_in,
                                      &avail_out, &output, nullptr /* total_out */);
    if (!ret) {
      return BrotliError("Brotli flush failed");
    }
    return FlushResult{static_cast<int64_t>(output_len - avail_out),
                       !!BrotliEncoderHasMoreOutput(state_)};
  }

  Result<EndResult> End(int64_t output_len, uint8_t* output) override {
    size_t avail_in = 0;
    const uint8_t* next_in = nullptr;
    auto avail_out = static_cast<size_t>(output_len);
    BROTLI_BOOL ret;

    ret =
        BrotliEncoderCompressStream(state_, BROTLI_OPERATION_FINISH, &avail_in, &next_in,
                                    &avail_out, &output, nullptr /* total_out */);
    if (!ret) {
      return BrotliError("Brotli end failed");
    }
    bool should_retry = !!BrotliEncoderHasMoreOutput(state_);
    DCHECK_EQ(should_retry, !BrotliEncoderIsFinished(state_));
    return EndResult{static_cast<int64_t>(output_len - avail_out), should_retry};
  }

 protected:
  Status BrotliError(const char* msg) { return Status::IOError(msg); }

  BrotliEncoderState* state_ = nullptr;

 private:
  const int compression_level_;
};

// ----------------------------------------------------------------------
// Brotli codec implementation

class BrotliCodec : public Codec {
 public:
  explicit BrotliCodec(int compression_level)
      : compression_level_(compression_level == kUseDefaultCompressionLevel
                               ? kBrotliDefaultCompressionLevel
                               : compression_level) {}

  Result<int64_t> Decompress(int64_t input_len, const uint8_t* input,
                             int64_t output_buffer_len, uint8_t* output_buffer) override {
    DCHECK_GE(input_len, 0);
    DCHECK_GE(output_buffer_len, 0);
    std::size_t output_size = static_cast<size_t>(output_buffer_len);
    if (BrotliDecoderDecompress(static_cast<size_t>(input_len), input, &output_size,
                                output_buffer) != BROTLI_DECODER_RESULT_SUCCESS) {
      return Status::IOError("Corrupt brotli compressed data.");
    }
    return output_size;
  }

  int64_t MaxCompressedLen(int64_t input_len,
                           const uint8_t* ARROW_ARG_UNUSED(input)) override {
    DCHECK_GE(input_len, 0);
    return BrotliEncoderMaxCompressedSize(static_cast<size_t>(input_len));
  }

  Result<int64_t> Compress(int64_t input_len, const uint8_t* input,
                           int64_t output_buffer_len, uint8_t* output_buffer) override {
    DCHECK_GE(input_len, 0);
    DCHECK_GE(output_buffer_len, 0);
    std::size_t output_size = static_cast<size_t>(output_buffer_len);
    if (BrotliEncoderCompress(compression_level_, BROTLI_DEFAULT_WINDOW,
                              BROTLI_DEFAULT_MODE, static_cast<size_t>(input_len), input,
                              &output_size, output_buffer) == BROTLI_FALSE) {
      return Status::IOError("Brotli compression failure.");
    }
    return output_size;
  }

  Result<std::shared_ptr<Compressor>> MakeCompressor() override {
    auto ptr = std::make_shared<BrotliCompressor>(compression_level_);
    RETURN_NOT_OK(ptr->Init());
    return ptr;
  }

  Result<std::shared_ptr<Decompressor>> MakeDecompressor() override {
    auto ptr = std::make_shared<BrotliDecompressor>();
    RETURN_NOT_OK(ptr->Init());
    return ptr;
  }

  Compression::type compression_type() const override { return Compression::BROTLI; }

  int compression_level() const override { return compression_level_; }
  int minimum_compression_level() const override { return BROTLI_MIN_QUALITY; }
  int maximum_compression_level() const override { return BROTLI_MAX_QUALITY; }
  int default_compression_level() const override {
    return kBrotliDefaultCompressionLevel;
  }

 private:
  const int compression_level_;
};

}  // namespace

std::unique_ptr<Codec> MakeBrotliCodec(int compression_level) {
  return std::unique_ptr<Codec>(new BrotliCodec(compression_level));
}

}  // namespace internal
}  // namespace util
}  // namespace arrow
