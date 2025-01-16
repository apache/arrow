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

//
// SIMD implementations
//

#if defined(ARROW_HAVE_NEON) || defined(ARROW_HAVE_SSE4_2)
template <int kNumStreams>
void ByteStreamSplitDecodeSimd128(const uint8_t* data, int width, int64_t num_values,
                                  int64_t stride, uint8_t* out) {
  using simd_batch = xsimd::make_sized_batch_t<int8_t, 16>;

  assert(width == kNumStreams);
  static_assert(kNumStreams == 4 || kNumStreams == 8, "Invalid number of streams.");
  constexpr int kNumStreamsLog2 = (kNumStreams == 8 ? 3 : 2);
  constexpr int64_t kBlockSize = sizeof(simd_batch) * kNumStreams;

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
  simd_batch stage[kNumStreamsLog2 + 1][kNumStreams];
  constexpr int kNumStreamsHalf = kNumStreams / 2U;

  for (int64_t i = 0; i < num_blocks; ++i) {
    for (int j = 0; j < kNumStreams; ++j) {
      stage[0][j] =
          simd_batch::load_unaligned(&data[i * sizeof(simd_batch) + j * stride]);
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
          reinterpret_cast<int8_t*>(out + (i * kNumStreams + j) * sizeof(simd_batch)),
          stage[kNumStreamsLog2][j]);
    }
  }
}

template <int kNumStreams>
void ByteStreamSplitEncodeSimd128(const uint8_t* raw_values, int width,
                                  const int64_t num_values, uint8_t* output_buffer_raw) {
  using simd_batch = xsimd::make_sized_batch_t<int8_t, 16>;

  assert(width == kNumStreams);
  static_assert(kNumStreams == 4 || kNumStreams == 8, "Invalid number of streams.");
  constexpr int kBlockSize = sizeof(simd_batch) * kNumStreams;

  simd_batch stage[3][kNumStreams];
  simd_batch final_result[kNumStreams];

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
  // The current shuffling algorithm diverges for float and double types but the compiler
  // should be able to remove the branch since only one path is taken for each template
  // instantiation.
  // Example run for 32-bit variables:
  // Step 0: copy from unaligned input bytes:
  //   0: ABCD ABCD ABCD ABCD 1: ABCD ABCD ABCD ABCD ...
  // Step 1: simd_batch<int8_t, 8>::zip_lo and simd_batch<int8_t, 8>::zip_hi:
  //   0: AABB CCDD AABB CCDD 1: AABB CCDD AABB CCDD ...
  // Step 2: apply simd_batch<int8_t, 8>::zip_lo and  simd_batch<int8_t, 8>::zip_hi again:
  //   0: AAAA BBBB CCCC DDDD 1: AAAA BBBB CCCC DDDD ...
  // Step 3: simd_batch<int8_t, 8>::zip_lo and simd_batch<int8_t, 8>::zip_hi:
  //   0: AAAA AAAA BBBB BBBB 1: CCCC CCCC DDDD DDDD ...
  // Step 4: simd_batch<int64_t, 2>::zip_lo and simd_batch<int64_t, 2>::zip_hi:
  //   0: AAAA AAAA AAAA AAAA 1: BBBB BBBB BBBB BBBB ...
  for (int64_t block_index = 0; block_index < num_blocks; ++block_index) {
    // First copy the data to stage 0.
    for (int i = 0; i < kNumStreams; ++i) {
      stage[0][i] = simd_batch::load_unaligned(
          reinterpret_cast<const int8_t*>(raw_values) +
          (block_index * kNumStreams + i) * sizeof(simd_batch));
    }

    // The shuffling of bytes is performed through the unpack intrinsics.
    // In my measurements this gives better performance then an implementation
    // which uses the shuffle intrinsics.
    for (int stage_lvl = 0; stage_lvl < 2; ++stage_lvl) {
      for (int i = 0; i < kNumStreams / 2; ++i) {
        stage[stage_lvl + 1][i * 2] =
            xsimd::zip_lo(stage[stage_lvl][i * 2], stage[stage_lvl][i * 2 + 1]);
        stage[stage_lvl + 1][i * 2 + 1] =
            xsimd::zip_hi(stage[stage_lvl][i * 2], stage[stage_lvl][i * 2 + 1]);
      }
    }
    if constexpr (kNumStreams == 8) {
      // This is the path for 64bits data.
      simd_batch tmp[8];
      using int32_batch = xsimd::make_sized_batch_t<int32_t, 4>;
      // This is a workaround, see: https://github.com/xtensor-stack/xsimd/issues/735
      auto from_int32_batch = [](int32_batch from) -> simd_batch {
        simd_batch dest;
        memcpy(&dest, &from, sizeof(simd_batch));
        return dest;
      };
      auto to_int32_batch = [](simd_batch from) -> int32_batch {
        int32_batch dest;
        memcpy(&dest, &from, sizeof(simd_batch));
        return dest;
      };
      for (int i = 0; i < 4; ++i) {
        tmp[i * 2] = from_int32_batch(
            xsimd::zip_lo(to_int32_batch(stage[2][i]), to_int32_batch(stage[2][i + 4])));
        tmp[i * 2 + 1] = from_int32_batch(
            xsimd::zip_hi(to_int32_batch(stage[2][i]), to_int32_batch(stage[2][i + 4])));
      }
      for (int i = 0; i < 4; ++i) {
        final_result[i * 2] = from_int32_batch(
            xsimd::zip_lo(to_int32_batch(tmp[i]), to_int32_batch(tmp[i + 4])));
        final_result[i * 2 + 1] = from_int32_batch(
            xsimd::zip_hi(to_int32_batch(tmp[i]), to_int32_batch(tmp[i + 4])));
      }
    } else {
      // This is the path for 32bits data.
      using int64_batch = xsimd::make_sized_batch_t<int64_t, 2>;
      // This is a workaround, see: https://github.com/xtensor-stack/xsimd/issues/735
      auto from_int64_batch = [](int64_batch from) -> simd_batch {
        simd_batch dest;
        memcpy(&dest, &from, sizeof(simd_batch));
        return dest;
      };
      auto to_int64_batch = [](simd_batch from) -> int64_batch {
        int64_batch dest;
        memcpy(&dest, &from, sizeof(simd_batch));
        return dest;
      };
      simd_batch tmp[4];
      for (int i = 0; i < 2; ++i) {
        tmp[i * 2] = xsimd::zip_lo(stage[2][i * 2], stage[2][i * 2 + 1]);
        tmp[i * 2 + 1] = xsimd::zip_hi(stage[2][i * 2], stage[2][i * 2 + 1]);
      }
      for (int i = 0; i < 2; ++i) {
        final_result[i * 2] = from_int64_batch(
            xsimd::zip_lo(to_int64_batch(tmp[i]), to_int64_batch(tmp[i + 2])));
        final_result[i * 2 + 1] = from_int64_batch(
            xsimd::zip_hi(to_int64_batch(tmp[i]), to_int64_batch(tmp[i + 2])));
      }
    }
    for (int i = 0; i < kNumStreams; ++i) {
      xsimd::store_unaligned(&output_buffer_streams[i][block_index * sizeof(simd_batch)],
                             final_result[i]);
    }
  }
}

#endif

#if defined(ARROW_HAVE_AVX2)
template <int kNumStreams>
void ByteStreamSplitDecodeAvx2(const uint8_t* data, int width, int64_t num_values,
                               int64_t stride, uint8_t* out) {
  assert(width == kNumStreams);
  static_assert(kNumStreams == 4 || kNumStreams == 8, "Invalid number of streams.");
  constexpr int kNumStreamsLog2 = (kNumStreams == 8 ? 3 : 2);
  constexpr int64_t kBlockSize = sizeof(__m256i) * kNumStreams;

  const int64_t size = num_values * kNumStreams;
  if (size < kBlockSize)  // Back to SSE for small size
    return ByteStreamSplitDecodeSimd128<kNumStreams>(data, width, num_values, stride,
                                                     out);
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
  static_assert(kNumStreams == 4 || kNumStreams == 8, "Invalid number of streams.");
  constexpr int kBlockSize = sizeof(__m256i) * kNumStreams;

  if constexpr (kNumStreams == 8)  // Back to SSE, currently no path for double.
    return ByteStreamSplitEncodeSimd128<kNumStreams>(raw_values, width, num_values,
                                                     output_buffer_raw);

  const int64_t size = num_values * kNumStreams;
  if (size < kBlockSize)  // Back to SSE for small size
    return ByteStreamSplitEncodeSimd128<kNumStreams>(raw_values, width, num_values,
                                                     output_buffer_raw);
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
#endif  // ARROW_HAVE_AVX2

#if defined(ARROW_HAVE_SIMD_SPLIT)
template <int kNumStreams>
void inline ByteStreamSplitDecodeSimd(const uint8_t* data, int width, int64_t num_values,
                                      int64_t stride, uint8_t* out) {
#  if defined(ARROW_HAVE_AVX2)
  return ByteStreamSplitDecodeAvx2<kNumStreams>(data, width, num_values, stride, out);
#  elif defined(ARROW_HAVE_SSE4_2) || defined(ARROW_HAVE_NEON)
  return ByteStreamSplitDecodeSimd128<kNumStreams>(data, width, num_values, stride, out);
#  else
#    error "ByteStreamSplitDecodeSimd not implemented"
#  endif
}

template <int kNumStreams>
void inline ByteStreamSplitEncodeSimd(const uint8_t* raw_values, int width,
                                      const int64_t num_values,
                                      uint8_t* output_buffer_raw) {
#  if defined(ARROW_HAVE_AVX2)
  return ByteStreamSplitEncodeAvx2<kNumStreams>(raw_values, width, num_values,
                                                output_buffer_raw);
#  elif defined(ARROW_HAVE_SSE4_2) || defined(ARROW_HAVE_NEON)
  return ByteStreamSplitEncodeSimd128<kNumStreams>(raw_values, width, num_values,
                                                   output_buffer_raw);
#  else
#    error "ByteStreamSplitEncodeSimd not implemented"
#  endif
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
#  define ByteStreamSplitEncodePerhapsSimd ByteStreamSplitEncodeSimd
#else
#  define ByteStreamSplitEncodePerhapsSimd ByteStreamSplitEncodeScalar
#endif
  switch (width) {
    case 1:
      memcpy(out, raw_values, num_values);
      return;
    case 2:
      return ByteStreamSplitEncodeScalar<2>(raw_values, width, num_values, out);
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
#  define ByteStreamSplitDecodePerhapsSimd ByteStreamSplitDecodeSimd
#else
#  define ByteStreamSplitDecodePerhapsSimd ByteStreamSplitDecodeScalar
#endif
  switch (width) {
    case 1:
      memcpy(out, data, num_values);
      return;
    case 2:
      return ByteStreamSplitDecodeScalar<2>(data, width, num_values, stride, out);
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
