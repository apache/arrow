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

#include <snappy.h>

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
// Snappy implementation

class SnappyCodec : public Codec {
 public:
  Result<int64_t> Decompress(int64_t input_len, const uint8_t* input,
                             int64_t output_buffer_len, uint8_t* output_buffer) override {
    size_t decompressed_size;
    if (!snappy::GetUncompressedLength(reinterpret_cast<const char*>(input),
                                       static_cast<size_t>(input_len),
                                       &decompressed_size)) {
      return Status::IOError("Corrupt snappy compressed data.");
    }
    if (output_buffer_len < static_cast<int64_t>(decompressed_size)) {
      return Status::Invalid("Output buffer size (", output_buffer_len, ") must be ",
                             decompressed_size, " or larger.");
    }
    if (!snappy::RawUncompress(reinterpret_cast<const char*>(input),
                               static_cast<size_t>(input_len),
                               reinterpret_cast<char*>(output_buffer))) {
      return Status::IOError("Corrupt snappy compressed data.");
    }
    return static_cast<int64_t>(decompressed_size);
  }

  int64_t MaxCompressedLen(int64_t input_len,
                           const uint8_t* ARROW_ARG_UNUSED(input)) override {
    DCHECK_GE(input_len, 0);
    return snappy::MaxCompressedLength(static_cast<size_t>(input_len));
  }

  Result<int64_t> Compress(int64_t input_len, const uint8_t* input,
                           int64_t ARROW_ARG_UNUSED(output_buffer_len),
                           uint8_t* output_buffer) override {
    size_t output_size;
    snappy::RawCompress(reinterpret_cast<const char*>(input),
                        static_cast<size_t>(input_len),
                        reinterpret_cast<char*>(output_buffer), &output_size);
    return static_cast<int64_t>(output_size);
  }

  Result<std::shared_ptr<Compressor>> MakeCompressor() override {
    return Status::NotImplemented("Streaming compression unsupported with Snappy");
  }

  Result<std::shared_ptr<Decompressor>> MakeDecompressor() override {
    return Status::NotImplemented("Streaming decompression unsupported with Snappy");
  }

  Compression::type compression_type() const override { return Compression::SNAPPY; }
  int minimum_compression_level() const override { return kUseDefaultCompressionLevel; }
  int maximum_compression_level() const override { return kUseDefaultCompressionLevel; }
  int default_compression_level() const override { return kUseDefaultCompressionLevel; }
};

}  // namespace

std::unique_ptr<Codec> MakeSnappyCodec() {
  return std::unique_ptr<Codec>(new SnappyCodec());
}

}  // namespace internal
}  // namespace util
}  // namespace arrow
