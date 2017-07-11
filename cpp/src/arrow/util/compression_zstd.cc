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

#include "arrow/util/compression_zstd.h"

#include <cstdint>
#include <memory>
#include <sstream>
#include <string>

#include <zstd.h>

#include "arrow/status.h"
#include "arrow/util/logging.h"

namespace arrow {

// ----------------------------------------------------------------------
// ZSTD implementation

Status ZSTDCodec::Decompress(
    int64_t input_len, const uint8_t* input, int64_t output_len, uint8_t* output_buffer) {
  int64_t decompressed_size = ZSTD_decompress(output_buffer,
      static_cast<size_t>(output_len), input, static_cast<size_t>(input_len));
  if (decompressed_size != output_len) {
    return Status::IOError("Corrupt ZSTD compressed data.");
  }
  return Status::OK();
}

int64_t ZSTDCodec::MaxCompressedLen(int64_t input_len, const uint8_t* input) {
  return ZSTD_compressBound(input_len);
}

Status ZSTDCodec::Compress(int64_t input_len, const uint8_t* input,
    int64_t output_buffer_len, uint8_t* output_buffer, int64_t* output_length) {
  *output_length = ZSTD_compress(output_buffer, static_cast<size_t>(output_buffer_len),
      input, static_cast<size_t>(input_len), 1);
  if (ZSTD_isError(*output_length)) {
    return Status::IOError("ZSTD compression failure.");
  }
  return Status::OK();
}

}  // namespace arrow
