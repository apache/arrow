// Copyright 2012 Cloudera Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include "parquet/compression/codec.h"

#include <lz4.h>

namespace parquet_cpp {

void Lz4Codec::Decompress(int input_len, const uint8_t* input,
      int output_len, uint8_t* output_buffer) {
  int n = LZ4_uncompress(reinterpret_cast<const char*>(input),
      reinterpret_cast<char*>(output_buffer), output_len);
  if (n != input_len) {
    throw parquet_cpp::ParquetException("Corrupt lz4 compressed data.");
  }
}

int Lz4Codec::MaxCompressedLen(int input_len, const uint8_t* input) {
  return LZ4_compressBound(input_len);
}

int Lz4Codec::Compress(int input_len, const uint8_t* input,
    int output_buffer_len, uint8_t* output_buffer) {
  return LZ4_compress(reinterpret_cast<const char*>(input),
      reinterpret_cast<char*>(output_buffer), input_len);
}

} // namespace parquet_cpp
