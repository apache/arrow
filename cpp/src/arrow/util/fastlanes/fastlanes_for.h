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

// FastLanes + Frame-of-Reference encoder/decoder.
//
// Splits the input into 2048-value chunks. Per chunk:
//   1. Compute min over the 2048 values.
//   2. Subtract min from every value -> all values in [0, max-min].
//   3. Compute bit_width = ceil(log2(max-min+1)).
//   4. Apply the FastLanes FL_ORDER permutation within each 1024-block
//      (gather input[fromTransposed32(t)]) and pack with FastLanes
//      lane-interleaved bit-packing at bit_width.
//
// The decoded output is in the FastLanes transposed order (NO scatter
// step on decode), i.e. output[chunk*2048 + block*1024 + t] equals
// input[chunk*2048 + block*1024 + fromTransposed32(t)] + min.
//
// Per-chunk header (5 bytes): [min (4B int32_t little-endian)] [bit_width (1B)]
// followed by 2 * 128 * bit_width bytes of packed data (zero if bit_width=0).

#pragma once

#include <cstdint>

#include "arrow/result.h"
#include "arrow/status.h"

namespace arrow {
namespace util {
namespace fastlanes {

class FastLanesForCodec {
 public:
  // Number of values per encoded chunk (= 2 FastLanes 1024-blocks).
  static constexpr int64_t kChunkSize = 2048;

  // Per-chunk header size: 4 bytes (int32 min) + 1 byte (bit_width).
  static constexpr int64_t kChunkHeaderSize = 5;

  // Upper-bound size of the encoded output for `num_values` (must be a
  // multiple of kChunkSize).
  static int64_t MaxEncodedSize(int64_t num_values) {
    const int64_t num_chunks = (num_values + kChunkSize - 1) / kChunkSize;
    // Max bit_width = 32 → 2 * 128 * 32 = 8192 bytes per chunk packed body.
    return num_chunks * (kChunkHeaderSize + 2 * 128 * 32);
  }

  // Encode `num_values` int32_t values. `num_values` must be a multiple
  // of kChunkSize. Returns the number of bytes written to `output`.
  // `output` must be at least MaxEncodedSize(num_values) bytes.
  static Result<int64_t> Encode(const int32_t* input, int64_t num_values,
                                uint8_t* output);

  // Decode `num_values` int32_t values. Output is in FastLanes
  // transposed order within each 1024-block (see header comment).
  static Status Decode(int32_t* output, int64_t num_values,
                       const uint8_t* input, int64_t input_size);

  // Decode and scatter back to ORIGINAL input order: output[i] == input[i]
  // for the encoded input. Pays the FL_ORDER scatter cost on top of the
  // kernel — slower than Decode(), included for apples-to-apples
  // comparison against codecs that produce flat output.
  static Status DecodeFlat(int32_t* output, int64_t num_values,
                           const uint8_t* input, int64_t input_size);
};

}  // namespace fastlanes
}  // namespace util
}  // namespace arrow
