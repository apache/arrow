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

#if defined(ARROW_HAVE_NEON)
#  define SPACED_EXPAND_RIGHTWARD_PLATFORM SpacedExpandRightwardNeon
#  define SPACED_EXPAND_PLATFORM SpacedExpandNeon
#elif defined(ARROW_HAVE_SSE4_2) || defined(ARROW_HAVE_RUNTIME_SSE4_2)
#  define SPACED_EXPAND_RIGHTWARD_PLATFORM SpacedExpandRightwardSse4_2
#  define SPACED_EXPAND_PLATFORM SpacedExpandSse4_2
#endif

#if defined(SPACED_EXPAND_PLATFORM)

#  include <cstdint>

#  include <xsimd/xsimd.hpp>

#  include "arrow/util/spaced_simd_internal.h"
#  include "arrow/util/spaced_simd_kernel_internal.h"

namespace arrow::util::internal {

namespace {

using Arch = xsimd::default_arch;

// Byte shuffle expanding the low lanes of a 16 byte register according to a
// kLanes bit mask. Null lanes read byte 0 so that every index stays in
// [0, 16), the only range where the semantics of a dynamic swizzle are
// defined on every architecture; `keep` then zeroes them.
template <int kLanes>
struct ExpandTable {
  struct alignas(32) Row {
    uint8_t shuffle[16];
    uint8_t keep[16];
  };
  Row rows[1 << kLanes] = {};
  constexpr ExpandTable() {
    constexpr int kValueBytes = 16 / kLanes;
    for (int m = 0; m < (1 << kLanes); ++m) {
      int prefix = 0;
      for (int lane = 0; lane < kLanes; ++lane) {
        const bool valid = ((m >> lane) & 1) != 0;
        for (int b = 0; b < kValueBytes; ++b) {
          rows[m].shuffle[lane * kValueBytes + b] =
              valid ? static_cast<uint8_t>(prefix * kValueBytes + b) : uint8_t{0};
          rows[m].keep[lane * kValueBytes + b] = valid ? uint8_t{0xFF} : uint8_t{0};
        }
        if (valid) ++prefix;
      }
    }
  }
};
template <int kLanes>
inline constexpr ExpandTable<kLanes> kExpand{};

// Chunk kernel (see spaced::ChunkedWordKernel) on 8 values: sub-batch j reads
// kSubLanes values at the number of valid lanes below it, which is at most
// 8 - kSubLanes.
template <typename Uint>
struct ExpandChunk {
  using Batch = xsimd::batch<Uint, Arch>;
  using Bytes = xsimd::batch<uint8_t, Arch>;
  static_assert(Bytes::size == 16, "128 bit SIMD only");

  static constexpr int kLanes = 8;
  static constexpr int kSubLanes = static_cast<int>(Batch::size);
  static constexpr int kSubs = kLanes / kSubLanes;

  static void Expand(Uint* dst, const Uint* src, unsigned mask) {
    const uint32_t prefixes = spaced::kPrefix<kSubLanes>.row[mask];
    Batch sub[kSubs];
    unsigned prefix = 0;
    for (int j = 0; j < kSubs; ++j) {
      const unsigned m = (mask >> (j * kSubLanes)) & ((1u << kSubLanes) - 1u);
      const auto& row = kExpand<kSubLanes>.rows[m];
      const auto bytes =
          xsimd::bitwise_cast<uint8_t>(Batch::load_unaligned(src + prefix));
      const auto expanded = xsimd::swizzle(bytes, Bytes::load_aligned(row.shuffle)) &
                            Bytes::load_aligned(row.keep);
      sub[j] = xsimd::bitwise_cast<Uint>(expanded);
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
void SPACED_EXPAND_RIGHTWARD_PLATFORM(Uint* buffer, int num_values, int null_count,
                                      const uint8_t* valid_bits,
                                      int64_t valid_bits_offset) {
  // Tuned on Apple M2: words with more nulls are faster through the kernel.
  constexpr int kDenseNulls = sizeof(Uint) == 4 ? 1 : 2;
  spaced::InPlaceExpander<Uint, Kernel<Uint>, kDenseNulls, kSparseValues>::Expand(
      buffer, num_values, null_count, valid_bits, valid_bits_offset);
}

template <typename Uint>
void SPACED_EXPAND_PLATFORM(const Uint* src, int num_values, int null_count,
                            const uint8_t* valid_bits, int64_t valid_bits_offset,
                            Uint* output) {
  spaced::OutOfPlaceExpander<Uint, Kernel<Uint>, kSparseValues>::Expand(
      src, num_values, null_count, valid_bits, valid_bits_offset, output);
}

template void SPACED_EXPAND_RIGHTWARD_PLATFORM<uint32_t>(uint32_t*, int, int,
                                                         const uint8_t*, int64_t);
template void SPACED_EXPAND_RIGHTWARD_PLATFORM<uint64_t>(uint64_t*, int, int,
                                                         const uint8_t*, int64_t);
template void SPACED_EXPAND_PLATFORM<uint32_t>(const uint32_t*, int, int, const uint8_t*,
                                               int64_t, uint32_t*);
template void SPACED_EXPAND_PLATFORM<uint64_t>(const uint64_t*, int, int, const uint8_t*,
                                               int64_t, uint64_t*);

}  // namespace arrow::util::internal

#  undef SPACED_EXPAND_RIGHTWARD_PLATFORM
#  undef SPACED_EXPAND_PLATFORM
#endif  // SPACED_EXPAND_PLATFORM
