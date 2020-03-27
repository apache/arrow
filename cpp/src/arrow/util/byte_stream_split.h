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

#ifndef ARROW_UTIL_BYTE_STREAM_SPLIT_H
#define ARROW_UTIL_BYTE_STREAM_SPLIT_H

#include "arrow/util/sse_util.h"
#include "arrow/util/ubsan.h"

#include <stdint.h>
#include <algorithm>

namespace arrow {
namespace util {
namespace internal {

#if defined(ARROW_HAVE_SSE2)

template <typename T>
void ByteStreamSlitDecodeSSE2(const uint8_t* data, int64_t num_values, int64_t stride,
                              T* out) {
  constexpr size_t kNumStreams = sizeof(T);
  static_assert(kNumStreams == 4U || kNumStreams == 8U, "Invalid number of streams.");
  constexpr size_t kNumStreamsLog2 = (kNumStreams == 8U ? 3U : 2U);

  const int64_t size = num_values * sizeof(T);
  const int64_t block_size = sizeof(__m128i) * kNumStreams;
  const int64_t num_blocks = size / block_size;
  uint8_t* output_data = reinterpret_cast<uint8_t*>(out);

  // First handle suffix.
  // This helps catch if the simd-based processing overflows into the suffix
  // since almost surely a test would fail.
  const int64_t num_processed_elements = (num_blocks * block_size) / kNumStreams;
  for (int64_t i = num_processed_elements; i < num_values; ++i) {
    uint8_t gathered_byte_data[kNumStreams];
    for (size_t b = 0; b < kNumStreams; ++b) {
      const size_t byte_index = b * stride + i;
      gathered_byte_data[b] = data[byte_index];
    }
    out[i] = arrow::util::SafeLoadAs<T>(&gathered_byte_data[0]);
  }

  // The blocks get processed hierahically using the unpack intrinsics.
  // Example with four streams:
  // Stage 1: AAAA BBBB CCCC DDDD
  // Stage 2: ACAC ACAC BDBD BDBD
  // Stage 3: ABCD ABCD ABCD ABCD
  __m128i stage[kNumStreamsLog2 + 1U][kNumStreams];
  const size_t half = kNumStreams / 2U;

  for (int64_t i = 0; i < num_blocks; ++i) {
    for (size_t j = 0; j < kNumStreams; ++j) {
      stage[0][j] = _mm_loadu_si128(
          reinterpret_cast<const __m128i*>(&data[i * sizeof(__m128i) + j * stride]));
    }
    for (size_t step = 0; step < kNumStreamsLog2; ++step) {
      for (size_t j = 0; j < half; ++j) {
        stage[step + 1U][j * 2] =
            _mm_unpacklo_epi8(stage[step][j], stage[step][half + j]);
        stage[step + 1U][j * 2 + 1U] =
            _mm_unpackhi_epi8(stage[step][j], stage[step][half + j]);
      }
    }
    for (size_t j = 0; j < kNumStreams; ++j) {
      _mm_storeu_si128(reinterpret_cast<__m128i*>(
                           &output_data[(i * kNumStreams + j) * sizeof(__m128i)]),
                       stage[kNumStreamsLog2][j]);
    }
  }
}

#endif

template <typename T>
void ByteStreamSlitDecodeScalar(const uint8_t* data, int64_t num_values, int64_t stride,
                                T* out) {
  constexpr size_t kNumStreams = sizeof(T);

  for (int64_t i = 0; i < num_values; ++i) {
    uint8_t gathered_byte_data[kNumStreams];
    for (size_t b = 0; b < kNumStreams; ++b) {
      const size_t byte_index = b * stride + i;
      gathered_byte_data[b] = data[byte_index];
    }
    out[i] = arrow::util::SafeLoadAs<T>(&gathered_byte_data[0]);
  }
}

}  // namespace internal
}  // namespace util
}  // namespace arrow

#endif  // ARROW_UTIL_BYTE_STREAM_SPLIT_H
