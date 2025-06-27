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

#pragma once

#include "arrow/util/endian.h"
#include "arrow/util/simd.h"
#include "arrow/util/small_vector.h"
#include "arrow/util/type_traits.h"
#include "arrow/util/ubsan.h"

#include <algorithm>
#include <array>
#include <cassert>
#include <cstdint>
#include <cstring>

#if defined(ARROW_HAVE_NEON) || defined(ARROW_HAVE_SSE4_2)
#  include <xsimd/xsimd.hpp>
#  define ARROW_HAVE_SIMD_SPLIT
#endif

namespace arrow::util::internal {

#if defined(ARROW_HAVE_SIMD_SPLIT)

//
// XSIMD implementations
//

template <typename T>
constexpr T ReversePow2(T x) {
  for (T n = 0, y = 1; n <= (8 * static_cast<T>(sizeof(T))); ++n, y = y * 2) {
    if (y == x) {
      return n;
    }
  }
  return 0;
}

static_assert(ReversePow2(8) == 3);
static_assert(ReversePow2(4) == 2);
static_assert(ReversePow2(2) == 1);

template <typename Arch, int kNumStreams>
void ByteStreamSplitDecodeSimd(const uint8_t* data, int width, int64_t num_values,
                               int64_t stride, uint8_t* out) {
  using simd_batch = xsimd::batch<int8_t, Arch>;
  // For signed arithmetic
  constexpr int kBatchSize = static_cast<int>(simd_batch::size);

  static_assert(kBatchSize >= 16, "The smallest SIMD size is 128 bits");

  if constexpr (kBatchSize > 16) {
    if (num_values < kBatchSize) {
      using Arch128 = xsimd::make_sized_batch_t<int8_t, 16>::arch_type;
      return ByteStreamSplitDecodeSimd<Arch128, kNumStreams>(data, width, num_values,
                                                             stride, out);
    }
  }

  static_assert(kNumStreams <= kBatchSize,
                "The algorithm works when the number of streams is smaller than the SIMD "
                "batch size.");
  assert(width == kNumStreams);
  constexpr int kNumStreamsLog2 = ReversePow2(kNumStreams);
  static_assert(kNumStreamsLog2 != 0,
                "The algorithm works for a number of streams being a power of two.");
  constexpr int64_t kBlockSize = kBatchSize * kNumStreams;

  const int64_t size = num_values * kNumStreams;
  const int64_t num_blocks = size / kBlockSize;

  // First handle suffix.
  // This helps catch if the simd-based processing overflows into the suffix
  // since almost surely a test would fail.
  const int64_t num_processed_elements = (num_blocks * kBlockSize) / kNumStreams;
  for (int64_t i = num_processed_elements; i < num_values; ++i) {
    uint8_t gathered_byte_data[kNumStreams];
    for (int b = 0; b < kNumStreams; ++b) {
      const int64_t byte_index = b * stride + i;
      gathered_byte_data[b] = data[byte_index];
    }
    memcpy(out + i * kNumStreams, gathered_byte_data, kNumStreams);
  }

  // The blocks get processed hierarchically using the unpack intrinsics.
  // Example with four streams:
  // Stage 1: AAAA BBBB CCCC DDDD
  // Stage 2: ACAC ACAC BDBD BDBD
  // Stage 3: ABCD ABCD ABCD ABCD
  constexpr int kNumStreamsHalf = kNumStreams / 2U;

  for (int64_t i = 0; i < num_blocks; ++i) {
    simd_batch stage[kNumStreamsLog2 + 1][kNumStreams];
    for (int j = 0; j < kNumStreams; ++j) {
      stage[0][j] = simd_batch::load_unaligned(&data[i * kBatchSize + j * stride]);
    }
    for (int step = 0; step < kNumStreamsLog2; ++step) {
      for (int j = 0; j < kNumStreamsHalf; ++j) {
        stage[step + 1U][j * 2] =
            xsimd::zip_lo(stage[step][j], stage[step][kNumStreamsHalf + j]);
        stage[step + 1U][j * 2 + 1U] =
            xsimd::zip_hi(stage[step][j], stage[step][kNumStreamsHalf + j]);
      }
    }
    for (int j = 0; j < kNumStreams; ++j) {
      xsimd::store_unaligned(
          reinterpret_cast<int8_t*>(out + (i * kNumStreams + j) * kBatchSize),
          stage[kNumStreamsLog2][j]);
    }
  }
}

// Like xsimd::zip_lo, but zip groups of kNumBytes at once.
template <typename Arch, int kNumBytes>
auto zip_lo_n(xsimd::batch<int8_t, Arch> const& a, xsimd::batch<int8_t, Arch> const& b)
    -> xsimd::batch<int8_t, Arch> {
  using arrow::internal::SizedInt;
  using simd_batch = xsimd::batch<int8_t, Arch>;
  // For signed arithmetic
  constexpr int kBatchSize = static_cast<int>(simd_batch::size);

  if constexpr (kNumBytes == kBatchSize) {
    return a;
  } else if constexpr (kNumBytes <= 8) {
    return xsimd::bitwise_cast<int8_t>(
        xsimd::zip_lo(xsimd::bitwise_cast<SizedInt<kNumBytes>>(a),
                      xsimd::bitwise_cast<SizedInt<kNumBytes>>(b)));
  }
  // No data type for 128 bits.
  // This could be made generic by simply computing the shuffle permute constant
  else if constexpr (kNumBytes == 16 && kBatchSize == 32) {
    return xsimd::bitwise_cast<int8_t>(
        xsimd::shuffle(xsimd::bitwise_cast<int64_t>(a), xsimd::bitwise_cast<int64_t>(b),
                       xsimd::batch_constant<int64_t, Arch, 0, 1, 4, 5>{}));
  }
}

// Like xsimd::zip_hi, but zip groups of kNumBytes at once.
template <typename Arch, int kNumBytes>
auto zip_hi_n(xsimd::batch<int8_t, Arch> const& a, xsimd::batch<int8_t, Arch> const& b)
    -> xsimd::batch<int8_t, Arch> {
  using simd_batch = xsimd::batch<int8_t, Arch>;
  using arrow::internal::SizedInt;
  // For signed arithmetic
  constexpr int kBatchSize = static_cast<int>(simd_batch::size);

  if constexpr (kNumBytes == kBatchSize) {
    return b;
  } else if constexpr (kNumBytes <= 8) {
    return xsimd::bitwise_cast<int8_t>(
        xsimd::zip_hi(xsimd::bitwise_cast<SizedInt<kNumBytes>>(a),
                      xsimd::bitwise_cast<SizedInt<kNumBytes>>(b)));
  }
  // No data type for 128 bits
  // This could be made generic by simply computing the shuffle permute constant
  else if constexpr (kNumBytes == 16 && kBatchSize == 32) {
    return xsimd::bitwise_cast<int8_t>(
        xsimd::shuffle(xsimd::bitwise_cast<int64_t>(a), xsimd::bitwise_cast<int64_t>(b),
                       xsimd::batch_constant<int64_t, Arch, 2, 3, 6, 7>{}));
  }
}

template <typename Arch, int kNumStreams>
void ByteStreamSplitEncodeSimd(const uint8_t* raw_values, int width,
                               const int64_t num_values, uint8_t* output_buffer_raw) {
  using simd_batch = xsimd::batch<int8_t, Arch>;
  // For signed arithmetic
  constexpr int kBatchSize = static_cast<int>(simd_batch::size);

  static_assert(kBatchSize >= 16, "The smallest SIMD size is 128 bits");

  if constexpr (kBatchSize > 16) {
    if (num_values < kBatchSize) {
      using Arch128 = xsimd::make_sized_batch_t<int8_t, 16>::arch_type;
      return ByteStreamSplitEncodeSimd<Arch128, kNumStreams>(
          raw_values, width, num_values, output_buffer_raw);
    }
  }

  assert(width == kNumStreams);
  static_assert(kNumStreams <= kBatchSize,
                "The algorithm works when the number of streams is smaller than the SIMD "
                "batch size.");
  constexpr int kBlockSize = kBatchSize * kNumStreams;
  static_assert(ReversePow2(kNumStreams) != 0,
                "The algorithm works for a number of streams being a power of two.");

  const int64_t size = num_values * kNumStreams;
  const int64_t num_blocks = size / kBlockSize;
  int8_t* output_buffer_streams[kNumStreams];
  for (int i = 0; i < kNumStreams; ++i) {
    output_buffer_streams[i] =
        reinterpret_cast<int8_t*>(&output_buffer_raw[num_values * i]);
  }

  // First handle suffix.
  const int64_t num_processed_elements = (num_blocks * kBlockSize) / kNumStreams;
  for (int64_t i = num_processed_elements; i < num_values; ++i) {
    for (int j = 0; j < kNumStreams; ++j) {
      const uint8_t byte_in_value = raw_values[i * kNumStreams + j];
      output_buffer_raw[j * num_values + i] = byte_in_value;
    }
  }

  // Number of input values we can fit in a simd register
  constexpr int kNumValuesInBatch = kBatchSize / kNumStreams;
  static_assert(kNumValuesInBatch > 0);
  // Number of bytes we'll bring together in the first byte-level part of the algorithm.
  // Since we zip with the next batch, the number of values in a batch determines how many
  // bytes end up together before we can use a larger type
  constexpr int kNumBytes = 2 * kNumValuesInBatch;
  // Number of steps in the first part of the algorithm with byte-level zipping
  constexpr int kNumStepsByte = ReversePow2(kNumValuesInBatch) + 1;
  // Number of steps in the first part of the algorithm with large data type zipping
  constexpr int kNumStepsLarge = ReversePow2(static_cast<int>(kBatchSize) / kNumBytes);
  // Total number of steps
  constexpr int kNumSteps = kNumStepsByte + kNumStepsLarge;
  static_assert(kNumSteps == ReversePow2(kBatchSize));

  // Two step shuffling algorithm that starts with bytes and ends with a larger data type.
  // An algorithm similar to the decoding one with log2(kBatchSize) + 1 stages is
  // also valid but not as performant.
  for (int64_t block_index = 0; block_index < num_blocks; ++block_index) {
    simd_batch stage[kNumSteps + 1][kNumStreams];

    // First copy the data to stage 0.
    for (int i = 0; i < kNumStreams; ++i) {
      stage[0][i] = simd_batch::load_unaligned(
          &raw_values[(block_index * kNumStreams + i) * kBatchSize]);
    }

    // We first make byte-level shuffling, until we have gather enough bytes together
    // and in the correct order to use a bigger data type.
    //
    // Example with 32bit data on 128 bit register:
    //
    // 0: A0B0C0D0 A1B1C1D1 A2B2C2D2 A3B3C3D3 | A4B4C4D4 A5B5C5D5 A6B6C6D6 A7B7C7D7 | ...
    // 1: A0A4B0B4 C0C4D0D4 A1A5B1B5 C1C5D1D5 | A2A6B2B6 C2C6D2D6 A3A7B3B7 C3C7D3D7 | ...
    // 2: A0A2A4A6 B0B2B4B6 C0C2C4C6 D0D2D4D6 | A1A3A5A7 B1B3B5B7 C1C3C5C7 D1D3D5D7 | ...
    // 3: A0A1A2A3 A4A5A6A7 B0B1B2B3 B4B5B6B7 | C0C1C2C3 C4C5C6C7 D0D1D2D3 D4D5D6D7 | ...
    //
    // The shuffling of bytes is performed through the unpack intrinsics.
    // In my measurements this gives better performance then an implementation
    // which uses the shuffle intrinsics.
    //
    // Loop order does not matter so we prefer higher locality
    constexpr int kNumStreamsHalf = kNumStreams / 2;
    for (int i = 0; i < kNumStreamsHalf; ++i) {
      for (int step = 0; step < kNumStepsByte; ++step) {
        stage[step + 1][i * 2] =
            xsimd::zip_lo(stage[step][i * 2], stage[step][i * 2 + 1]);
        stage[step + 1][i * 2 + 1] =
            xsimd::zip_hi(stage[step][i * 2], stage[step][i * 2 + 1]);
      }
    }

    // We know have the bytes packed in a larger data type and in the correct order to
    // start using a bigger data type
    //
    // Example with 32bit data on 128 bit register.
    // The large data type is int64_t with NumBytes=8 bytes:
    //
    // 4: A0A1A2A3 A4A5A6A7 A8A9AAAB ACADAEAF | B0B1B2B3 B4B5B6B7 B8B9BABB BCBDBEBF | ...
    for (int step = kNumStepsByte; step < kNumSteps; ++step) {
      for (int i = 0; i < kNumStreamsHalf; ++i) {
        stage[step + 1][i * 2] =
            zip_lo_n<Arch, kNumBytes>(stage[step][i], stage[step][i + kNumStreamsHalf]);
        stage[step + 1][i * 2 + 1] =
            zip_hi_n<Arch, kNumBytes>(stage[step][i], stage[step][i + kNumStreamsHalf]);
      }
    }

    // Save the encoded data to the output buffer
    for (int i = 0; i < kNumStreams; ++i) {
      xsimd::store_unaligned(&output_buffer_streams[i][block_index * kBatchSize],
                             stage[kNumSteps][i]);
    }
  }
}

#  if defined(ARROW_HAVE_AVX2)
template <int kNumStreams>
void ByteStreamSplitDecodeAvx2(const uint8_t* data, int width, int64_t num_values,
                               int64_t stride, uint8_t* out) {
  assert(width == kNumStreams);
  static_assert(kNumStreams == 4 || kNumStreams == 8, "Invalid number of streams.");
  constexpr int kNumStreamsLog2 = (kNumStreams == 8 ? 3 : 2);
  constexpr int64_t kBlockSize = sizeof(__m256i) * kNumStreams;

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
    memcpy(out + i * kNumStreams, gathered_byte_data, kNumStreams);
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

    if constexpr (kNumStreams == 8) {
      // path for double, 128i index:
      //   {0x00, 0x08}, {0x01, 0x09}, {0x02, 0x0A}, {0x03, 0x0B},
      //   {0x04, 0x0C}, {0x05, 0x0D}, {0x06, 0x0E}, {0x07, 0x0F},
      final_result[0] = _mm256_permute2x128_si256(stage[kNumStreamsLog2][0],
                                                  stage[kNumStreamsLog2][1], 0b00100000);
      final_result[1] = _mm256_permute2x128_si256(stage[kNumStreamsLog2][2],
                                                  stage[kNumStreamsLog2][3], 0b00100000);
      final_result[2] = _mm256_permute2x128_si256(stage[kNumStreamsLog2][4],
                                                  stage[kNumStreamsLog2][5], 0b00100000);
      final_result[3] = _mm256_permute2x128_si256(stage[kNumStreamsLog2][6],
                                                  stage[kNumStreamsLog2][7], 0b00100000);
      final_result[4] = _mm256_permute2x128_si256(stage[kNumStreamsLog2][0],
                                                  stage[kNumStreamsLog2][1], 0b00110001);
      final_result[5] = _mm256_permute2x128_si256(stage[kNumStreamsLog2][2],
                                                  stage[kNumStreamsLog2][3], 0b00110001);
      final_result[6] = _mm256_permute2x128_si256(stage[kNumStreamsLog2][4],
                                                  stage[kNumStreamsLog2][5], 0b00110001);
      final_result[7] = _mm256_permute2x128_si256(stage[kNumStreamsLog2][6],
                                                  stage[kNumStreamsLog2][7], 0b00110001);
    } else {
      // path for float, 128i index:
      //   {0x00, 0x04}, {0x01, 0x05}, {0x02, 0x06}, {0x03, 0x07}
      final_result[0] = _mm256_permute2x128_si256(stage[kNumStreamsLog2][0],
                                                  stage[kNumStreamsLog2][1], 0b00100000);
      final_result[1] = _mm256_permute2x128_si256(stage[kNumStreamsLog2][2],
                                                  stage[kNumStreamsLog2][3], 0b00100000);
      final_result[2] = _mm256_permute2x128_si256(stage[kNumStreamsLog2][0],
                                                  stage[kNumStreamsLog2][1], 0b00110001);
      final_result[3] = _mm256_permute2x128_si256(stage[kNumStreamsLog2][2],
                                                  stage[kNumStreamsLog2][3], 0b00110001);
    }

    for (int j = 0; j < kNumStreams; ++j) {
      _mm256_storeu_si256(
          reinterpret_cast<__m256i*>(out + (i * kNumStreams + j) * sizeof(__m256i)),
          final_result[j]);
    }
  }
}

template <int kNumStreams>
void ByteStreamSplitEncodeAvx2(const uint8_t* raw_values, int width,
                               const int64_t num_values, uint8_t* output_buffer_raw) {
  assert(width == kNumStreams);
  static_assert(kNumStreams == 4, "Invalid number of streams.");
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
  static const __m256i kPermuteMask =
      _mm256_set_epi32(0x07, 0x03, 0x06, 0x02, 0x05, 0x01, 0x04, 0x00);
  __m256i permute[kNumStreams];
  __m256i final_result[kNumStreams];

  for (int64_t block_index = 0; block_index < num_blocks; ++block_index) {
    for (int i = 0; i < kNumStreams; ++i) {
      stage[0][i] = _mm256_loadu_si256(&raw_values_simd[block_index * kNumStreams + i]);
    }

    for (int stage_lvl = 0; stage_lvl < kNumUnpack; ++stage_lvl) {
      for (int i = 0; i < kNumStreams / 2; ++i) {
        stage[stage_lvl + 1][i * 2] =
            _mm256_unpacklo_epi8(stage[stage_lvl][i * 2], stage[stage_lvl][i * 2 + 1]);
        stage[stage_lvl + 1][i * 2 + 1] =
            _mm256_unpackhi_epi8(stage[stage_lvl][i * 2], stage[stage_lvl][i * 2 + 1]);
      }
    }

    for (int i = 0; i < kNumStreams; ++i) {
      permute[i] = _mm256_permutevar8x32_epi32(stage[kNumUnpack][i], kPermuteMask);
    }

    final_result[0] = _mm256_permute2x128_si256(permute[0], permute[2], 0b00100000);
    final_result[1] = _mm256_permute2x128_si256(permute[0], permute[2], 0b00110001);
    final_result[2] = _mm256_permute2x128_si256(permute[1], permute[3], 0b00100000);
    final_result[3] = _mm256_permute2x128_si256(permute[1], permute[3], 0b00110001);

    for (int i = 0; i < kNumStreams; ++i) {
      _mm256_storeu_si256(&output_buffer_streams[i][block_index], final_result[i]);
    }
  }
}
#  endif  // ARROW_HAVE_AVX2

#  if defined(ARROW_HAVE_AVX2)
using SimdArch = xsimd::avx2;
#  elif defined(ARROW_HAVE_SSE4_2)
using SimdArch = xsimd::sse4_2;
#  elif defined(ARROW_HAVE_NEON)
using SimdArch = xsimd::neon64;
#  else
#    error "No valid arch found"
#  endif

template <int kNumStreams>
void inline ByteStreamSplitDecodeSimdDispatch(const uint8_t* data, int width,
                                              int64_t num_values, int64_t stride,
                                              uint8_t* out) {
// Faster implementation in AVX2 using native intrinsics because the zip/unpack
// on AVX2 really work on two bits lanes, which is not general enough for xsimd to
// abstract.
#  if defined(ARROW_HAVE_AVX2)
  // Not implemented for other sizes
  if constexpr (kNumStreams == 4 || kNumStreams == 8) {
    return ByteStreamSplitDecodeAvx2<kNumStreams>(data, width, num_values, stride, out);
  }
#  endif
  return ByteStreamSplitDecodeSimd<SimdArch, kNumStreams>(data, width, num_values, stride,
                                                          out);
}

template <int kNumStreams>
void inline ByteStreamSplitEncodeSimdDispatch(const uint8_t* raw_values, int width,
                                              const int64_t num_values,
                                              uint8_t* output_buffer_raw) {
// Faster implementation in AVX2 using native intrinsics because the zip/unpack
// on AVX2 really work on two bits lanes, which is not general enough for xsimd to
// abstract.
#  if defined(ARROW_HAVE_AVX2)
  // Not implemented for other sizes
  if constexpr (kNumStreams == 4) {
    return ByteStreamSplitEncodeAvx2<kNumStreams>(raw_values, width, num_values,
                                                  output_buffer_raw);
  }
#  endif
  return ByteStreamSplitEncodeSimd<SimdArch, kNumStreams>(raw_values, width, num_values,
                                                          output_buffer_raw);
}
#endif

//
// Scalar implementations
//

inline void DoSplitStreams(const uint8_t* src, int width, int64_t nvalues,
                           uint8_t** dest_streams) {
  // Value empirically chosen to provide the best performance on the author's machine
  constexpr int kBlockSize = 32;

  while (nvalues >= kBlockSize) {
    for (int stream = 0; stream < width; ++stream) {
      uint8_t* dest = dest_streams[stream];
      for (int i = 0; i < kBlockSize; i += 8) {
        uint64_t a = src[stream + i * width];
        uint64_t b = src[stream + (i + 1) * width];
        uint64_t c = src[stream + (i + 2) * width];
        uint64_t d = src[stream + (i + 3) * width];
        uint64_t e = src[stream + (i + 4) * width];
        uint64_t f = src[stream + (i + 5) * width];
        uint64_t g = src[stream + (i + 6) * width];
        uint64_t h = src[stream + (i + 7) * width];
#if ARROW_LITTLE_ENDIAN
        uint64_t r = a | (b << 8) | (c << 16) | (d << 24) | (e << 32) | (f << 40) |
                     (g << 48) | (h << 56);
#else
        uint64_t r = (a << 56) | (b << 48) | (c << 40) | (d << 32) | (e << 24) |
                     (f << 16) | (g << 8) | h;
#endif
        arrow::util::SafeStore(&dest[i], r);
      }
      dest_streams[stream] += kBlockSize;
    }
    src += width * kBlockSize;
    nvalues -= kBlockSize;
  }

  // Epilog
  for (int stream = 0; stream < width; ++stream) {
    uint8_t* dest = dest_streams[stream];
    for (int64_t i = 0; i < nvalues; ++i) {
      dest[i] = src[stream + i * width];
    }
  }
}

inline void DoMergeStreams(const uint8_t** src_streams, int width, int64_t nvalues,
                           uint8_t* dest) {
  // Value empirically chosen to provide the best performance on the author's machine
  constexpr int kBlockSize = 128;

  while (nvalues >= kBlockSize) {
    for (int stream = 0; stream < width; ++stream) {
      // Take kBlockSize bytes from the given stream and spread them
      // to their logical places in destination.
      const uint8_t* src = src_streams[stream];
      for (int i = 0; i < kBlockSize; i += 8) {
        uint64_t v = arrow::util::SafeLoadAs<uint64_t>(&src[i]);
#if ARROW_LITTLE_ENDIAN
        dest[stream + i * width] = static_cast<uint8_t>(v);
        dest[stream + (i + 1) * width] = static_cast<uint8_t>(v >> 8);
        dest[stream + (i + 2) * width] = static_cast<uint8_t>(v >> 16);
        dest[stream + (i + 3) * width] = static_cast<uint8_t>(v >> 24);
        dest[stream + (i + 4) * width] = static_cast<uint8_t>(v >> 32);
        dest[stream + (i + 5) * width] = static_cast<uint8_t>(v >> 40);
        dest[stream + (i + 6) * width] = static_cast<uint8_t>(v >> 48);
        dest[stream + (i + 7) * width] = static_cast<uint8_t>(v >> 56);
#else
        dest[stream + i * width] = static_cast<uint8_t>(v >> 56);
        dest[stream + (i + 1) * width] = static_cast<uint8_t>(v >> 48);
        dest[stream + (i + 2) * width] = static_cast<uint8_t>(v >> 40);
        dest[stream + (i + 3) * width] = static_cast<uint8_t>(v >> 32);
        dest[stream + (i + 4) * width] = static_cast<uint8_t>(v >> 24);
        dest[stream + (i + 5) * width] = static_cast<uint8_t>(v >> 16);
        dest[stream + (i + 6) * width] = static_cast<uint8_t>(v >> 8);
        dest[stream + (i + 7) * width] = static_cast<uint8_t>(v);
#endif
      }
      src_streams[stream] += kBlockSize;
    }
    dest += width * kBlockSize;
    nvalues -= kBlockSize;
  }

  // Epilog
  for (int stream = 0; stream < width; ++stream) {
    const uint8_t* src = src_streams[stream];
    for (int64_t i = 0; i < nvalues; ++i) {
      dest[stream + i * width] = src[i];
    }
  }
}

template <int kNumStreams>
void ByteStreamSplitEncodeScalar(const uint8_t* raw_values, int width,
                                 const int64_t num_values, uint8_t* out) {
  assert(width == kNumStreams);
  std::array<uint8_t*, kNumStreams> dest_streams;
  for (int stream = 0; stream < kNumStreams; ++stream) {
    dest_streams[stream] = &out[stream * num_values];
  }
  DoSplitStreams(raw_values, kNumStreams, num_values, dest_streams.data());
}

inline void ByteStreamSplitEncodeScalarDynamic(const uint8_t* raw_values, int width,
                                               const int64_t num_values, uint8_t* out) {
  ::arrow::internal::SmallVector<uint8_t*, 16> dest_streams;
  dest_streams.resize(width);
  for (int stream = 0; stream < width; ++stream) {
    dest_streams[stream] = &out[stream * num_values];
  }
  DoSplitStreams(raw_values, width, num_values, dest_streams.data());
}

template <int kNumStreams>
void ByteStreamSplitDecodeScalar(const uint8_t* data, int width, int64_t num_values,
                                 int64_t stride, uint8_t* out) {
  assert(width == kNumStreams);
  std::array<const uint8_t*, kNumStreams> src_streams;
  for (int stream = 0; stream < kNumStreams; ++stream) {
    src_streams[stream] = &data[stream * stride];
  }
  DoMergeStreams(src_streams.data(), kNumStreams, num_values, out);
}

inline void ByteStreamSplitDecodeScalarDynamic(const uint8_t* data, int width,
                                               int64_t num_values, int64_t stride,
                                               uint8_t* out) {
  ::arrow::internal::SmallVector<const uint8_t*, 16> src_streams;
  src_streams.resize(width);
  for (int stream = 0; stream < width; ++stream) {
    src_streams[stream] = &data[stream * stride];
  }
  DoMergeStreams(src_streams.data(), width, num_values, out);
}

inline void ByteStreamSplitEncode(const uint8_t* raw_values, int width,
                                  const int64_t num_values, uint8_t* out) {
#if defined(ARROW_HAVE_SIMD_SPLIT)
#  define ByteStreamSplitEncodePerhapsSimd ByteStreamSplitEncodeSimdDispatch
#else
#  define ByteStreamSplitEncodePerhapsSimd ByteStreamSplitEncodeScalar
#endif
  switch (width) {
    case 1:
      memcpy(out, raw_values, num_values);
      return;
    case 2:
      return ByteStreamSplitEncodePerhapsSimd<2>(raw_values, width, num_values, out);
    case 4:
      return ByteStreamSplitEncodePerhapsSimd<4>(raw_values, width, num_values, out);
    case 8:
      return ByteStreamSplitEncodePerhapsSimd<8>(raw_values, width, num_values, out);
    case 16:
      return ByteStreamSplitEncodeScalar<16>(raw_values, width, num_values, out);
  }
  return ByteStreamSplitEncodeScalarDynamic(raw_values, width, num_values, out);
#undef ByteStreamSplitEncodePerhapsSimd
}

inline void ByteStreamSplitDecode(const uint8_t* data, int width, int64_t num_values,
                                  int64_t stride, uint8_t* out) {
#if defined(ARROW_HAVE_SIMD_SPLIT)
#  define ByteStreamSplitDecodePerhapsSimd ByteStreamSplitDecodeSimdDispatch
#else
#  define ByteStreamSplitDecodePerhapsSimd ByteStreamSplitDecodeScalar
#endif
  switch (width) {
    case 1:
      memcpy(out, data, num_values);
      return;
    case 2:
      return ByteStreamSplitDecodePerhapsSimd<2>(data, width, num_values, stride, out);
    case 4:
      return ByteStreamSplitDecodePerhapsSimd<4>(data, width, num_values, stride, out);
    case 8:
      return ByteStreamSplitDecodePerhapsSimd<8>(data, width, num_values, stride, out);
    case 16:
      return ByteStreamSplitDecodeScalar<16>(data, width, num_values, stride, out);
  }
  return ByteStreamSplitDecodeScalarDynamic(data, width, num_values, stride, out);
#undef ByteStreamSplitDecodePerhapsSimd
}

}  // namespace arrow::util::internal
