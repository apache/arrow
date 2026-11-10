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

#include <bit>
#include <cstdint>

#include <xsimd/xsimd.hpp>

#include "arrow/util/spaced_simd_internal.h"
#include "arrow/util/spaced_simd_kernel_internal.h"

namespace arrow::util::internal {

namespace {

using Arch = xsimd::avx512bw;

// Word kernel (see spaced::Expander) on full 512 bit batches, with the native
// expand instruction.
template <typename Uint>
struct WordKernel {
  using Batch = xsimd::batch<Uint, Arch>;
  using Mask = xsimd::batch_bool<Uint, Arch>;

  static constexpr int kLanes = static_cast<int>(Batch::size);
  static constexpr int kChunks = 64 / kLanes;
  static constexpr uint64_t kChunkMask = (uint64_t{1} << kLanes) - 1;

  static void ExpandWord(Uint* dst, const Uint* src, int src_end, uint64_t bits) {
    auto chunk_mask = [&](int c) { return (bits >> (c * kLanes)) & kChunkMask; };
    // A chunk only reads below the slots of the chunks above it, so they can
    // be stored in any order. Loading every chunk before storing the one above
    // keeps the compiler from folding the loads into vpexpand: its memory
    // operand form is much slower on Zen 4.
    int c = kChunks - 1;
    src_end -= std::popcount(chunk_mask(c));
    Batch above = Batch::load_unaligned(src + src_end);
    for (c = kChunks - 2; c >= 0; --c) {
      src_end -= std::popcount(chunk_mask(c));
      const Batch values = Batch::load_unaligned(src + src_end);
      xsimd::expand(above, Mask::from_mask(chunk_mask(c + 1)))
          .store_unaligned(dst + (c + 1) * kLanes);
      above = values;
    }
    xsimd::expand(above, Mask::from_mask(chunk_mask(0))).store_unaligned(dst);
  }
};

// Words with at most that many valid values are scattered one by one.
constexpr int kSparseValues = 8;

template <typename Uint>
using Kernel = WordKernel<Uint>;

}  // namespace

template <typename Uint>
void SpacedExpandRightwardAvx512(Uint* buffer, int num_values, int null_count,
                                 const uint8_t* valid_bits, int64_t valid_bits_offset) {
  // Tuned on Zen 4: the kernel is cheap enough for words with a single null.
  constexpr int kDenseNulls = 0;
  spaced::InPlaceExpander<Uint, Kernel<Uint>, kDenseNulls, kSparseValues>::Expand(
      buffer, num_values, null_count, valid_bits, valid_bits_offset);
}

template <typename Uint>
void SpacedExpandAvx512(const Uint* src, int num_values, int null_count,
                        const uint8_t* valid_bits, int64_t valid_bits_offset,
                        Uint* output) {
  spaced::OutOfPlaceExpander<Uint, Kernel<Uint>, kSparseValues>::Expand(
      src, num_values, null_count, valid_bits, valid_bits_offset, output);
}

template void SpacedExpandRightwardAvx512<uint32_t>(uint32_t*, int, int, const uint8_t*,
                                                    int64_t);
template void SpacedExpandRightwardAvx512<uint64_t>(uint64_t*, int, int, const uint8_t*,
                                                    int64_t);
template void SpacedExpandAvx512<uint32_t>(const uint32_t*, int, int, const uint8_t*,
                                           int64_t, uint32_t*);
template void SpacedExpandAvx512<uint64_t>(const uint64_t*, int, int, const uint8_t*,
                                           int64_t, uint64_t*);

}  // namespace arrow::util::internal
