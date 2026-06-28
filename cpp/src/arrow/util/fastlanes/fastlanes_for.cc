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

#include "arrow/util/fastlanes/fastlanes_for.h"

#include <algorithm>
#include <cstring>
#include <limits>

#include "arrow/util/fastlanes/fastlanes_kernels.h"

namespace arrow {
namespace util {
namespace fastlanes {

namespace {

// Dispatch by runtime bit_width into the templated PackBlock<W>.
inline void PackBlockDispatch(uint32_t bit_width, const uint32_t* in,
                              uint32_t* out) {
  switch (bit_width) {
#define CASE_W(N) \
  case N:         \
    PackBlock<N>(in, out); \
    break
    CASE_W(1);  CASE_W(2);  CASE_W(3);  CASE_W(4);  CASE_W(5);  CASE_W(6);
    CASE_W(7);  CASE_W(8);  CASE_W(9);  CASE_W(10); CASE_W(11); CASE_W(12);
    CASE_W(13); CASE_W(14); CASE_W(15); CASE_W(16); CASE_W(17); CASE_W(18);
    CASE_W(19); CASE_W(20); CASE_W(21); CASE_W(22); CASE_W(23); CASE_W(24);
    CASE_W(25); CASE_W(26); CASE_W(27); CASE_W(28); CASE_W(29); CASE_W(30);
    CASE_W(31); CASE_W(32);
#undef CASE_W
    default: break;
  }
}

inline void UnpackBlockDispatch(uint32_t bit_width, const uint32_t* packed,
                                uint32_t* out) {
  switch (bit_width) {
#define CASE_W(N) \
  case N:         \
    UnpackBlock<N>(packed, out); \
    break
    CASE_W(1);  CASE_W(2);  CASE_W(3);  CASE_W(4);  CASE_W(5);  CASE_W(6);
    CASE_W(7);  CASE_W(8);  CASE_W(9);  CASE_W(10); CASE_W(11); CASE_W(12);
    CASE_W(13); CASE_W(14); CASE_W(15); CASE_W(16); CASE_W(17); CASE_W(18);
    CASE_W(19); CASE_W(20); CASE_W(21); CASE_W(22); CASE_W(23); CASE_W(24);
    CASE_W(25); CASE_W(26); CASE_W(27); CASE_W(28); CASE_W(29); CASE_W(30);
    CASE_W(31); CASE_W(32);
#undef CASE_W
    default: break;
  }
}

inline uint8_t BitsRequired(uint32_t range) {
  if (range == 0) return 0;
  return static_cast<uint8_t>(32 - __builtin_clz(range));
}

}  // namespace

Result<int64_t> FastLanesForCodec::Encode(const int32_t* input,
                                          int64_t num_values, uint8_t* output) {
  if (num_values % kChunkSize != 0) {
    return Status::Invalid("FastLanesForCodec::Encode: num_values (", num_values,
                           ") must be a multiple of ", kChunkSize);
  }
  const int64_t num_chunks = num_values / kChunkSize;
  uint8_t* out_ptr = output;

  // Scratch buffers: one chunk's worth of FOR-shifted values (uint32_t) in
  // transposed FL_ORDER, ready to feed PackBlock twice (one per 1024-block).
  alignas(64) uint32_t transposed[kChunkSize];

  for (int64_t chunk = 0; chunk < num_chunks; ++chunk) {
    const int32_t* in_chunk = input + chunk * kChunkSize;

    // Compute min over the 2048 values.
    int32_t min_val = std::numeric_limits<int32_t>::max();
    int32_t max_val = std::numeric_limits<int32_t>::min();
    for (int64_t i = 0; i < kChunkSize; ++i) {
      const int32_t v = in_chunk[i];
      if (v < min_val) min_val = v;
      if (v > max_val) max_val = v;
    }
    const uint32_t range = static_cast<uint32_t>(max_val - min_val);
    const uint8_t bit_width = BitsRequired(range);

    // Write 5-byte header: [min (4B LE int32_t)] [bit_width (1B)].
    std::memcpy(out_ptr, &min_val, sizeof(int32_t));
    out_ptr[4] = bit_width;
    out_ptr += kChunkHeaderSize;

    if (bit_width == 0) {
      // All values equal min; no payload bytes needed.
      continue;
    }

    // Build the transposed FOR-shifted scratch: per 1024-block, gather
    // input[fromTransposed32(t)] - min into transposed[t].
    for (int64_t block = 0; block < 2; ++block) {
      const int32_t* block_in = in_chunk + block * static_cast<int64_t>(kBlockSize);
      uint32_t* block_t = transposed + block * kBlockSize;
      for (size_t t = 0; t < kBlockSize; ++t) {
        block_t[t] = static_cast<uint32_t>(block_in[fromTransposed32(t)] - min_val);
      }
    }

    // Pack each 1024-block; payload is 2 * 128 * bit_width bytes total.
    const int64_t bytes_per_block = 128 * bit_width;
    for (int64_t block = 0; block < 2; ++block) {
      PackBlockDispatch(bit_width, transposed + block * kBlockSize,
                        reinterpret_cast<uint32_t*>(out_ptr + block * bytes_per_block));
    }
    out_ptr += 2 * bytes_per_block;
  }

  return out_ptr - output;
}

Status FastLanesForCodec::Decode(int32_t* output, int64_t num_values,
                                  const uint8_t* input, int64_t input_size) {
  if (num_values % kChunkSize != 0) {
    return Status::Invalid("FastLanesForCodec::Decode: num_values (", num_values,
                           ") must be a multiple of ", kChunkSize);
  }
  const int64_t num_chunks = num_values / kChunkSize;
  const uint8_t* in_ptr = input;
  const uint8_t* in_end = input + input_size;

  alignas(64) uint32_t transposed[kChunkSize];

  for (int64_t chunk = 0; chunk < num_chunks; ++chunk) {
    if (in_ptr + kChunkHeaderSize > in_end) {
      return Status::Invalid("FastLanesForCodec::Decode: truncated input header");
    }
    int32_t min_val;
    std::memcpy(&min_val, in_ptr, sizeof(int32_t));
    const uint8_t bit_width = in_ptr[4];
    in_ptr += kChunkHeaderSize;

    int32_t* out_chunk = output + chunk * kChunkSize;

    if (bit_width == 0) {
      // All values equal min; emit min in transposed order (just fill).
      for (int64_t i = 0; i < kChunkSize; ++i) out_chunk[i] = min_val;
      continue;
    }

    const int64_t bytes_per_block = 128 * bit_width;
    if (in_ptr + 2 * bytes_per_block > in_end) {
      return Status::Invalid("FastLanesForCodec::Decode: truncated input body");
    }

    // Unpack each 1024-block into the transposed scratch.
    for (int64_t block = 0; block < 2; ++block) {
      UnpackBlockDispatch(
          bit_width,
          reinterpret_cast<const uint32_t*>(in_ptr + block * bytes_per_block),
          transposed + block * kBlockSize);
    }
    in_ptr += 2 * bytes_per_block;

    // Add min back. Output stays in transposed FL_ORDER (no scatter).
    for (int64_t i = 0; i < kChunkSize; ++i) {
      out_chunk[i] = static_cast<int32_t>(transposed[i]) + min_val;
    }
  }

  return Status::OK();
}

Status FastLanesForCodec::DecodeFlat(int32_t* output, int64_t num_values,
                                      const uint8_t* input, int64_t input_size) {
  if (num_values % kChunkSize != 0) {
    return Status::Invalid("FastLanesForCodec::DecodeFlat: num_values (",
                           num_values, ") must be a multiple of ", kChunkSize);
  }
  const int64_t num_chunks = num_values / kChunkSize;
  const uint8_t* in_ptr = input;
  const uint8_t* in_end = input + input_size;

  alignas(64) uint32_t transposed[kChunkSize];

  for (int64_t chunk = 0; chunk < num_chunks; ++chunk) {
    if (in_ptr + kChunkHeaderSize > in_end) {
      return Status::Invalid("FastLanesForCodec::DecodeFlat: truncated header");
    }
    int32_t min_val;
    std::memcpy(&min_val, in_ptr, sizeof(int32_t));
    const uint8_t bit_width = in_ptr[4];
    in_ptr += kChunkHeaderSize;

    int32_t* out_chunk = output + chunk * kChunkSize;

    if (bit_width == 0) {
      for (int64_t i = 0; i < kChunkSize; ++i) out_chunk[i] = min_val;
      continue;
    }

    const int64_t bytes_per_block = 128 * bit_width;
    if (in_ptr + 2 * bytes_per_block > in_end) {
      return Status::Invalid("FastLanesForCodec::DecodeFlat: truncated body");
    }

    for (int64_t block = 0; block < 2; ++block) {
      UnpackBlockDispatch(
          bit_width,
          reinterpret_cast<const uint32_t*>(in_ptr + block * bytes_per_block),
          transposed + block * kBlockSize);
    }
    in_ptr += 2 * bytes_per_block;

    // Scatter via fromTransposed32 per 1024-block to restore original
    // input order: out_chunk[block*1024 + fromTransposed32(t)] = transposed +
    // min. This is the FL_ORDER inverse of the encode-time gather.
    for (int64_t block = 0; block < 2; ++block) {
      const uint32_t* block_t = transposed + block * kBlockSize;
      int32_t* block_out = out_chunk + block * static_cast<int64_t>(kBlockSize);
      for (size_t t = 0; t < kBlockSize; ++t) {
        block_out[fromTransposed32(t)] = static_cast<int32_t>(block_t[t]) + min_val;
      }
    }
  }

  return Status::OK();
}

}  // namespace fastlanes
}  // namespace util
}  // namespace arrow
