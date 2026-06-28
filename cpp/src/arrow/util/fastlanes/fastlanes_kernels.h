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

// FastLanes auto-vectorized bit-packing kernels (header-only).
//
// Portable C++ port of the lane-interleaved 1024-bit format from
// FastLanes (Afroozeh & Boncz, VLDB '23). No SIMD intrinsics — the
// inner lane loop is written so the compiler auto-vectorizes it to
// 4-wide NEON / 8-wide AVX2 / 16-wide AVX512 without source changes.
//
// Within a 1024-value block (32 lanes × 32 rows for uint32_t), the
// packed buffer holds w u32 rows of 32 u32 words, where w is the bit
// width. Row r, lane l contributes to packed[r * 32 + l] at bit shift
// (r*w) % 32, possibly straddling into packed[r * 32 + 32 + l].
//
// FL_ORDER (the 8x16 -> 16x8 within-sub-block transpose plus the
// 3-bit-reversal sub-block reorder) is applied outside these kernels:
// callers gather input[fromTransposed32(t)] before packing; the kernel
// produces output in the same transposed order (no scatter on decode).

#pragma once

#include <cstddef>
#include <cstdint>
#include <cstring>

namespace arrow {
namespace util {
namespace fastlanes {

constexpr size_t kBlockSize = 1024;
constexpr size_t kLanes = 32;            // 1024 / sizeof(uint32_t) / 8
constexpr size_t kRowsPerBlock = 32;     // 1024 / kLanes

// Sub-block reorder permutation (3-bit reversal). Used by fromTransposed32.
inline constexpr size_t kBlockReorder[8] = {0, 4, 2, 6, 1, 5, 3, 7};

// Convert a transposed (stream) index back to its original input index
// within a 1024-value block. Self-inverse: fromTransposed32 is also
// toTransposed32.
inline size_t fromTransposed32(size_t t) {
  const size_t outputBlock = t >> 7;        // t / 128
  const size_t transposedWithin = t & 0x7F;  // t % 128
  const size_t originalBlock = kBlockReorder[outputBlock];
  const size_t row = transposedWithin >> 3;  // / 8
  const size_t col = transposedWithin & 0x7;  // % 8
  const size_t withinBlock = (col << 4) | row;  // col * 16 + row
  return (originalBlock << 7) | withinBlock;
}

// ---------------------------------------------------------------------------
// Pack: 1024 transposed-order u32 inputs (already gathered via
// fromTransposed32) → w*32 u32 packed words.
// ---------------------------------------------------------------------------
template <uint32_t w>
inline void PackBlock(const uint32_t* __restrict__ in, uint32_t* __restrict__ out) {
  static_assert(w >= 1 && w <= 32);
  constexpr uint32_t kMask = (w == 32) ? 0xFFFFFFFFu : ((1u << w) - 1);

  if constexpr (w == 32) {
    std::memcpy(out, in, kBlockSize * sizeof(uint32_t));
    return;
  }

  std::memset(out, 0, w * kLanes * sizeof(uint32_t));

#pragma GCC unroll 32
  for (uint32_t row = 0; row < kRowsPerBlock; ++row) {
    constexpr uint32_t kT = 32;
    const uint32_t startBit = row * w;
    const uint32_t word = startBit / kT;
    const uint32_t shift = startBit % kT;
    const uint32_t endBit = startBit + w;
    const uint32_t endWord = (endBit - 1) / kT;

    if (word == endWord) {
      for (uint32_t lane = 0; lane < kLanes; ++lane) {
        const uint32_t v = in[row * kLanes + lane] & kMask;
        out[word * kLanes + lane] |= (v << shift);
      }
    } else {
      const uint32_t lowBits = kT - shift;
      for (uint32_t lane = 0; lane < kLanes; ++lane) {
        const uint32_t v = in[row * kLanes + lane] & kMask;
        out[word * kLanes + lane] |= (v << shift);
        out[endWord * kLanes + lane] |= (v >> lowBits);
      }
    }
  }
}

// ---------------------------------------------------------------------------
// Unpack: w*32 packed u32 words → 1024 u32 outputs in transposed order.
// (No scatter: output[t] is the value at stream-position t = row*32+lane.)
// ---------------------------------------------------------------------------
template <uint32_t w>
inline void UnpackBlock(const uint32_t* __restrict__ packed,
                        uint32_t* __restrict__ out) {
  static_assert(w >= 1 && w <= 32);
  constexpr uint32_t kMask = (w == 32) ? 0xFFFFFFFFu : ((1u << w) - 1);

  if constexpr (w == 32) {
    std::memcpy(out, packed, kBlockSize * sizeof(uint32_t));
    return;
  }

#pragma GCC unroll 32
  for (uint32_t row = 0; row < kRowsPerBlock; ++row) {
    constexpr uint32_t kT = 32;
    const uint32_t startBit = row * w;
    const uint32_t word = startBit / kT;
    const uint32_t shift = startBit % kT;
    const uint32_t endBit = startBit + w;
    const uint32_t endWord = (endBit - 1) / kT;

    if (word == endWord) {
      for (uint32_t lane = 0; lane < kLanes; ++lane) {
        out[row * kLanes + lane] =
            (packed[word * kLanes + lane] >> shift) & kMask;
      }
    } else {
      const uint32_t lowBits = kT - shift;
      for (uint32_t lane = 0; lane < kLanes; ++lane) {
        const uint32_t lo = packed[word * kLanes + lane] >> shift;
        const uint32_t hi = packed[endWord * kLanes + lane] << lowBits;
        out[row * kLanes + lane] = (lo | hi) & kMask;
      }
    }
  }
}

}  // namespace fastlanes
}  // namespace util
}  // namespace arrow
