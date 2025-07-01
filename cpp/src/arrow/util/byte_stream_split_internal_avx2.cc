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

#include "arrow/util/byte_stream_split_internal.h"
#include "arrow/util/math_internal.h"
#include "arrow/util/simd.h"

#include <xsimd/types/xsimd_avx2_register.hpp>
#include <xsimd/xsimd.hpp>

#include <cassert>
#include <cstdint>
#include <cstring>

namespace arrow::util::internal {

using ::arrow::internal::ReversePow2;

// Faster implementation in AVX2 using native intrinsics because the zip/unpack
// on AVX2 really work on two bits lanes, which is not general enough for xsimd to
// abstract.
template <int kNumStreams>
void ByteStreamSplitDecodeAvx2(const uint8_t* data, int width, int64_t num_values,
                               int64_t stride, uint8_t* out) {
  constexpr int kBatchSize = static_cast<int>(sizeof(__m256i));
  static_assert(kNumStreams <= 16,
                "The algorithm works when the number of streams is smaller than 16.");
  assert(width == kNumStreams);
  constexpr int kNumStreamsLog2 = ReversePow2(kNumStreams);
  static_assert(kNumStreamsLog2 != 0,
                "The algorithm works for a number of streams being a power of two.");
  constexpr int64_t kBlockSize = kBatchSize * kNumStreams;

  const int64_t size = num_values * kNumStreams;
  if (size < kBlockSize)  // Back to SSE for small size
    return ByteStreamSplitDecodeSimd<xsimd::sse4_2, kNumStreams>(data, width, num_values,
                                                                 stride, out);
  const int64_t num_blocks = size / kBlockSize;

  // First handle suffix.
  const int64_t num_processed_elements = (num_blocks * kBlockSize) / kNumStreams;
  for (int64_t i = num_processed_elements; i < num_values; ++i) {
    uint8_t gathered_byte_data[kNumStreams];
    for (int b = 0; b < kNumStreams; ++b) {
      const int64_t byte_index = b * stride + i;
      gathered_byte_data[b] = data[byte_index];
    }
    std::memcpy(out + i * kNumStreams, gathered_byte_data, kNumStreams);
  }

  // Processed hierarchically using unpack intrinsics, then permute intrinsics.
  __m256i stage[kNumStreamsLog2 + 1][kNumStreams];
  __m256i final_result[kNumStreams];
  constexpr int kNumStreamsHalf = kNumStreams / 2;

  for (int64_t i = 0; i < num_blocks; ++i) {
    for (int j = 0; j < kNumStreams; ++j) {
      stage[0][j] = _mm256_loadu_si256(
          reinterpret_cast<const __m256i*>(&data[i * sizeof(__m256i) + j * stride]));
    }

    for (int step = 0; step < kNumStreamsLog2; ++step) {
      for (int j = 0; j < kNumStreamsHalf; ++j) {
        stage[step + 1][j * 2] =
            _mm256_unpacklo_epi8(stage[step][j], stage[step][kNumStreamsHalf + j]);
        stage[step + 1][j * 2 + 1] =
            _mm256_unpackhi_epi8(stage[step][j], stage[step][kNumStreamsHalf + j]);
      }
    }

    for (int j = 0; j < kNumStreamsHalf; ++j) {
      // Concatenate inputs lower 128-bit lanes: src1 to upper, src2 to lower
      final_result[j] = _mm256_permute2x128_si256(
          stage[kNumStreamsLog2][2 * j], stage[kNumStreamsLog2][2 * j + 1], 0b00100000);
      // Concatenate inputs upper 128-bit lanes: src1 to upper, src2 to lower
      final_result[j + kNumStreamsHalf] = _mm256_permute2x128_si256(
          stage[kNumStreamsLog2][2 * j], stage[kNumStreamsLog2][2 * j + 1], 0b00110001);
    }

    for (int j = 0; j < kNumStreams; ++j) {
      _mm256_storeu_si256(
          reinterpret_cast<__m256i*>(out + (i * kNumStreams + j) * sizeof(__m256i)),
          final_result[j]);
    }
  }
}

template void ByteStreamSplitDecodeAvx2<2>(const uint8_t*, int, int64_t, int64_t,
                                           uint8_t*);
template void ByteStreamSplitDecodeAvx2<4>(const uint8_t*, int, int64_t, int64_t,
                                           uint8_t*);
template void ByteStreamSplitDecodeAvx2<8>(const uint8_t*, int, int64_t, int64_t,
                                           uint8_t*);

// Faster implementation in AVX2 using native intrinsics because the zip/unpack
// on AVX2 really work on two bits lanes, which is not general enough for xsimd to
// abstract.
inline void ByteStreamSplitEncodeAvx2Impl4(const uint8_t* raw_values, int width,
                                           const int64_t num_values,
                                           uint8_t* output_buffer_raw) {
  constexpr int kNumStreams = 4;
  assert(width == kNumStreams);
  constexpr int kBlockSize = sizeof(__m256i) * kNumStreams;

  const int64_t size = num_values * kNumStreams;
  if (size < kBlockSize)  // Back to SSE for small size
    return ByteStreamSplitEncodeSimd<xsimd::sse4_2, kNumStreams>(
        raw_values, width, num_values, output_buffer_raw);
  const int64_t num_blocks = size / kBlockSize;
  const __m256i* raw_values_simd = reinterpret_cast<const __m256i*>(raw_values);
  __m256i* output_buffer_streams[kNumStreams];

  for (int i = 0; i < kNumStreams; ++i) {
    output_buffer_streams[i] =
        reinterpret_cast<__m256i*>(&output_buffer_raw[num_values * i]);
  }

  // First handle suffix.
  const int64_t num_processed_elements = (num_blocks * kBlockSize) / kNumStreams;
  for (int64_t i = num_processed_elements; i < num_values; ++i) {
    for (int j = 0; j < kNumStreams; ++j) {
      const uint8_t byte_in_value = raw_values[i * kNumStreams + j];
      output_buffer_raw[j * num_values + i] = byte_in_value;
    }
  }

  // Path for float.
  // 1. Processed hierarchically to 32i block using the unpack intrinsics.
  // 2. Pack 128i block using _mm256_permutevar8x32_epi32.
  // 3. Pack final 256i block with _mm256_permute2x128_si256.
  constexpr int kNumUnpack = 3;
  __m256i stage[kNumUnpack + 1][kNumStreams];
  __m256i permute[kNumStreams];
  __m256i final_result[kNumStreams];

  for (int64_t block_index = 0; block_index < num_blocks; ++block_index) {
    for (int i = 0; i < kNumStreams; ++i) {
      stage[0][i] = _mm256_loadu_si256(&raw_values_simd[block_index * kNumStreams + i]);
    }

    // We first make byte-level shuffling, until we have gather enough bytes together
    // and in the correct order to use a bigger data type.
    //
    // Loop order does not matter so we prefer higher locality
    constexpr int kNumStreamsHalf = kNumStreams / 2;
    for (int i = 0; i < kNumStreamsHalf; ++i) {
      for (int stage_lvl = 0; stage_lvl < kNumUnpack; ++stage_lvl) {
        stage[stage_lvl + 1][i * 2] =
            _mm256_unpacklo_epi8(stage[stage_lvl][i * 2], stage[stage_lvl][i * 2 + 1]);
        stage[stage_lvl + 1][i * 2 + 1] =
            _mm256_unpackhi_epi8(stage[stage_lvl][i * 2], stage[stage_lvl][i * 2 + 1]);
      }
    }

    for (int i = 0; i < kNumStreamsHalf; ++i) {
      permute[i] = _mm256_permute2x128_si256(
          stage[kNumUnpack][i], stage[kNumUnpack][i + kNumStreamsHalf], 0b00100000);
      permute[i + kNumStreamsHalf] = _mm256_permute2x128_si256(
          stage[kNumUnpack][i], stage[kNumUnpack][i + kNumStreamsHalf], 0b00110001);
    }

    for (int i = 0; i < kNumStreams / 2; ++i) {
      final_result[i * 2] =
          _mm256_unpacklo_epi32(permute[i], permute[i + kNumStreamsHalf]);
      final_result[i * 2 + 1] =
          _mm256_unpackhi_epi32(permute[i], permute[i + kNumStreamsHalf]);
    }

    for (int i = 0; i < kNumStreams; ++i) {
      _mm256_storeu_si256(&output_buffer_streams[i][block_index], final_result[i]);
    }
  }
}

template <int kNumStreams>
void ByteStreamSplitEncodeAvx2(const uint8_t* raw_values, int width,
                               const int64_t num_values, uint8_t* output_buffer_raw) {
  // Only size with a different implementation
  if constexpr (kNumStreams == 4) {
    return ByteStreamSplitEncodeAvx2Impl4(raw_values, width, num_values,
                                          output_buffer_raw);
  } else {
    return ByteStreamSplitEncodeSimd<xsimd::avx2, kNumStreams>(
        raw_values, width, num_values, output_buffer_raw);
  }
}

template void ByteStreamSplitEncodeAvx2<2>(const uint8_t*, int, const int64_t, uint8_t*);
template void ByteStreamSplitEncodeAvx2<4>(const uint8_t*, int, const int64_t, uint8_t*);
template void ByteStreamSplitEncodeAvx2<8>(const uint8_t*, int, const int64_t, uint8_t*);

}  // namespace arrow::util::internal
