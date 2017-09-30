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

#include <lz4.h>

#include "arrow/status.h"
#include "arrow/util/macros.h"

namespace arrow {

// ----------------------------------------------------------------------
// Lz4 implementation

Status Lz4Codec::Decompress(int64_t input_len, const uint8_t* input, int64_t output_len,
                            uint8_t* output_buffer) {
  int64_t decompressed_size = LZ4_decompress_safe(
      reinterpret_cast<const char*>(input), reinterpret_cast<char*>(output_buffer),
      static_cast<int>(input_len), static_cast<int>(output_len));
  if (decompressed_size < 1) {
    return Status::IOError("Corrupt Lz4 compressed data.");
  }
  return Status::OK();
}

int64_t Lz4Codec::MaxCompressedLen(int64_t input_len,
                                   const uint8_t* ARROW_ARG_UNUSED(input)) {
  return LZ4_compressBound(static_cast<int>(input_len));
}

Status Lz4Codec::Compress(int64_t input_len, const uint8_t* input,
                          int64_t output_buffer_len, uint8_t* output_buffer,
                          int64_t* output_length) {
  *output_length = LZ4_compress_default(
      reinterpret_cast<const char*>(input), reinterpret_cast<char*>(output_buffer),
      static_cast<int>(input_len), static_cast<int>(output_buffer_len));
  if (*output_length < 1) {
    return Status::IOError("Lz4 compression failure.");
  }
  return Status::OK();
}

}  // namespace arrow
