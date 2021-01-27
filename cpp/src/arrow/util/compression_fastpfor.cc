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

#include "compression_internal.h"

#include <memory>

#include <lz4.h>
#include <lz4frame.h>

#include "arrow/result.h"
#include "arrow/status.h"
#include "arrow/util/bit_util.h"
#include "arrow/util/logging.h"
#include "arrow/util/macros.h"

#include "fastpfor/codecfactory.h"

namespace arrow {
namespace util {
namespace internal {

namespace {

template <typename IntType>
class FastPForCodec : public Codec {
 public:
  FastPForCodec() = default;

  int64_t MaxCompressedLen(int64_t input_len,
                           const uint8_t* ARROW_ARG_UNUSED(input)) override {
    return input_len + 1024 * sizeof(IntType);
  }

  Result<int64_t> Compress(int64_t input_len, const uint8_t* input,
                           int64_t output_buffer_len, uint8_t* output_buffer) override {
    using input_type = IntType;
    using output_type = uint32_t;
    auto base_width = sizeof(input_type);
    if ((input_len & (base_width - 1)) != 0) {
      return Status::Invalid("Buffer size " + std::to_string(input_len) +
                             " is not multiple of " + std::to_string(base_width));
    }
    size_t length = input_len / base_width;
    size_t nvalue;
    fastpfor_codec_->encodeArray(
        reinterpret_cast<input_type*>(const_cast<uint8_t*>(input)), length,
        reinterpret_cast<output_type*>(output_buffer), nvalue);
    return nvalue * sizeof(output_type);
  }

  Result<int64_t> Decompress(int64_t input_len, const uint8_t* input,
                             int64_t output_buffer_len, uint8_t* output_buffer) override {
    using input_type = uint32_t;
    using output_type = IntType;
    auto base_width = sizeof(input_type);
    if ((input_len & (base_width - 1)) != 0) {
      return Status::Invalid("Buffer size " + std::to_string(input_len) +
                             " is not multiple of " + std::to_string(base_width));
    }
    size_t length = input_len / base_width;
    size_t nvalue;
    fastpfor_codec_->decodeArray(
        reinterpret_cast<input_type*>(const_cast<uint8_t*>(input)), length,
        reinterpret_cast<output_type*>(output_buffer), nvalue);
    return nvalue * sizeof(output_type);
  }

  Result<std::shared_ptr<Compressor>> MakeCompressor() override {
    return Status::NotImplemented("Streaming compression unsupported with FastPFor");
  }

  Result<std::shared_ptr<Decompressor>> MakeDecompressor() override {
    return Status::NotImplemented("Streaming decompression unsupported with FastPFor");
  }

  Compression::type compression_type() const override { return Compression::FASTPFOR; }

  Status Init() override {
    // FastPForLib::CODECFactory::getFromName("fastpfor256") is not thread safe.
    fastpfor_codec_ =
        std::make_shared<FastPForLib::CompositeCodec<FastPForLib::FastPFor<8>,
                                                     FastPForLib::VariableByte>>();
    return Status::OK();
  }

 private:
  std::shared_ptr<FastPForLib::IntegerCODEC> fastpfor_codec_;
};

}  // namespace

template <typename T>
std::unique_ptr<Codec> MakeFastPForCodec() {
  return std::unique_ptr<Codec>(new FastPForCodec<T>());
}

template std::unique_ptr<Codec> MakeFastPForCodec<uint32_t>();
template std::unique_ptr<Codec> MakeFastPForCodec<uint64_t>();

}  // namespace internal
}  // namespace util
}  // namespace arrow
