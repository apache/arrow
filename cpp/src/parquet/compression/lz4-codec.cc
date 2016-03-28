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

#include <lz4.h>
#include <cstdint>

#include "parquet/compression/codec.h"
#include "parquet/exception.h"

namespace parquet {

void Lz4Codec::Decompress(int64_t input_len, const uint8_t* input,
      int64_t output_len, uint8_t* output_buffer) {
  int64_t n = LZ4_decompress_fast(reinterpret_cast<const char*>(input),
      reinterpret_cast<char*>(output_buffer), output_len);
  if (n != input_len) {
    throw ParquetException("Corrupt lz4 compressed data.");
  }
}

int64_t Lz4Codec::MaxCompressedLen(int64_t input_len, const uint8_t* input) {
  return LZ4_compressBound(input_len);
}

int64_t Lz4Codec::Compress(int64_t input_len, const uint8_t* input,
    int64_t output_buffer_len, uint8_t* output_buffer) {
  return LZ4_compress(reinterpret_cast<const char*>(input),
      reinterpret_cast<char*>(output_buffer), input_len);
}

} // namespace parquet
