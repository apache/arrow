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

#if !defined(ARROW_HAVE_RUNTIME_AVX2)
#  error "This file must be compiled with AVX2 enabled"
#endif

#include <cstdint>

#include <immintrin.h>
#include <xsimd/xsimd.hpp>

#include "arrow/util/spaced_simd_internal.h"
#include "arrow/util/spaced_simd_kernel_internal.h"

namespace arrow::util::internal {

namespace {

using Arch = xsimd::avx2;

// For a kSubLanes bit mask, the 8 nibbles of row[mask] give the 32 bit word
// each 32 bit word of the result is taken from, bit 3 meaning that it is kept
// (the value is valid). 1 KiB for 4 byte values, 64 bytes for 8 byte values.
template <int kSubLanes>
struct PermuteTable {
  uint32_t row[1 << kSubLanes] = {};
  constexpr PermuteTable() {
    constexpr int kWords = 8 / kSubLanes;
    for (int m = 0; m < (1 << kSubLanes); ++m) {
      uint32_t r = 0;
      int prefix = 0;
      for (int lane = 0; lane < kSubLanes; ++lane) {
        if (((m >> lane) & 1) == 0) continue;
        for (int b = 0; b < kWords; ++b) {
          const uint32_t nibble = 8u | static_cast<uint32_t>(prefix * kWords + b);
          r |= nibble << (4 * (lane * kWords + b));
        }
        ++prefix;
      }
      row[m] = r;
    }
  }
};
template <int kSubLanes>
inline constexpr PermuteTable<kSubLanes> kPermute{};

// Chunk kernel (see spaced::ChunkedWordKernel) on 8 values: one 32 bit lane
// permutation per batch.
template <typename Uint>
struct ExpandChunk {
  using Batch = xsimd::batch<Uint, Arch>;
  using Words = xsimd::batch<uint32_t, Arch>;
  static_assert(Words::size == 8, "256 bit SIMD only");

  static constexpr int kLanes = 8;
  static constexpr int kSubLanes = static_cast<int>(Batch::size);
  static constexpr int kSubs = kLanes / kSubLanes;

  static void Expand(Uint* dst, const Uint* src, unsigned mask) {
    const Words shifts{0, 4, 8, 12, 16, 20, 24, 28};
    const uint32_t prefixes = kSubs > 1 ? spaced::kPrefix<kSubLanes>.row[mask] : 0;
    Batch sub[kSubs];
    unsigned prefix = 0;
    for (int j = 0; j < kSubs; ++j) {
      const unsigned m = (mask >> (j * kSubLanes)) & ((1u << kSubLanes) - 1u);
      const Words nibbles = (Words(kPermute<kSubLanes>.row[m]) >> shifts) & Words(0xF);
      const Words words =
          xsimd::bitwise_cast<uint32_t>(Batch::load_unaligned(src + prefix));
      // TODO: use xsimd::swizzle once xsimd emits vpermd for it
      // (https://github.com/xtensor-stack/xsimd/pull/1446). Up to xsimd 14.3 it
      // is emulated with AVX instructions and slower than the 128 bit kernel.
      // vpermd only looks at the low 3 bits of the indices.
      const Words permuted = _mm256_permutevar8x32_epi32(words, nibbles);
      // All ones where bit 3 is set
      const Words keep = Words(0) - (nibbles >> 3);
      sub[j] = xsimd::bitwise_cast<Uint>(permuted & keep);
      prefix = (prefixes >> (8 * j)) & 0xFF;
    }
    for (int j = 0; j < kSubs; ++j) {
      sub[j].store_unaligned(dst + j * kSubLanes);
    }
  }
};

// Words with at most that many valid values are scattered one by one.
constexpr int kSparseValues = 8;

template <typename Uint>
using Kernel = spaced::ChunkedWordKernel<Uint, ExpandChunk<Uint>>;

}  // namespace

template <typename Uint>
void SpacedExpandRightwardAvx2(Uint* buffer, int num_values, int null_count,
                               const uint8_t* valid_bits, int64_t valid_bits_offset) {
  // Tuned on Zen 4, where other values make little difference.
  constexpr int kDenseNulls = sizeof(Uint) == 4 ? 1 : 2;
  spaced::InPlaceExpander<Uint, Kernel<Uint>, kDenseNulls, kSparseValues>::Expand(
      buffer, num_values, null_count, valid_bits, valid_bits_offset);
}

template <typename Uint>
void SpacedExpandAvx2(const Uint* src, int num_values, int null_count,
                      const uint8_t* valid_bits, int64_t valid_bits_offset,
                      Uint* output) {
  spaced::OutOfPlaceExpander<Uint, Kernel<Uint>, kSparseValues>::Expand(
      src, num_values, null_count, valid_bits, valid_bits_offset, output);
}

template void SpacedExpandRightwardAvx2<uint32_t>(uint32_t*, int, int, const uint8_t*,
                                                  int64_t);
template void SpacedExpandRightwardAvx2<uint64_t>(uint64_t*, int, int, const uint8_t*,
                                                  int64_t);
template void SpacedExpandAvx2<uint32_t>(const uint32_t*, int, int, const uint8_t*,
                                         int64_t, uint32_t*);
template void SpacedExpandAvx2<uint64_t>(const uint64_t*, int, int, const uint8_t*,
                                         int64_t, uint64_t*);

}  // namespace arrow::util::internal
