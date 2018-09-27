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

#include "arrow/util/compression_brotli.h"

#include <cstddef>
#include <cstdint>

#include <brotli/decode.h>
#include <brotli/encode.h>
#include <brotli/types.h>

#include "arrow/status.h"
#include "arrow/util/macros.h"

namespace arrow {
namespace util {

// ----------------------------------------------------------------------
// Brotli implementation

Status BrotliCodec::Decompress(int64_t input_len, const uint8_t* input,
                               int64_t output_len, uint8_t* output_buffer) {
  std::size_t output_size = output_len;
  if (BrotliDecoderDecompress(input_len, input, &output_size, output_buffer) !=
      BROTLI_DECODER_RESULT_SUCCESS) {
    return Status::IOError("Corrupt brotli compressed data.");
  }
  return Status::OK();
}

int64_t BrotliCodec::MaxCompressedLen(int64_t input_len,
                                      const uint8_t* ARROW_ARG_UNUSED(input)) {
  return BrotliEncoderMaxCompressedSize(input_len);
}

Status BrotliCodec::Compress(int64_t input_len, const uint8_t* input,
                             int64_t output_buffer_len, uint8_t* output_buffer,
                             int64_t* output_length) {
  std::size_t output_len = output_buffer_len;
  // TODO: Make quality configurable. We use 8 as a default as it is the best
  //       trade-off for Parquet workload
  if (BrotliEncoderCompress(8, BROTLI_DEFAULT_WINDOW, BROTLI_DEFAULT_MODE, input_len,
                            input, &output_len, output_buffer) == BROTLI_FALSE) {
    return Status::IOError("Brotli compression failure.");
  }
  *output_length = output_len;
  return Status::OK();
}

}  // namespace util
}  // namespace arrow
