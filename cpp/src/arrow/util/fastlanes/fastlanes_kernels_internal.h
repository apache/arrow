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

// Lane-interleaved bit-packing kernels (header-only).
//
// A portable port of the lane-interleaved 1024-bit container from FastLanes
// (Afroozeh & Boncz, VLDB '23). There are no SIMD intrinsics here: the inner
// lane loop is shaped so that every lane does identical work, which is what
// lets the compiler auto-vectorize one source body to 4-wide NEON, 8-wide
// AVX2 or 16-wide AVX-512 without a per-ISA variant.
//
// What this file is not
// ---------------------
// FastLanes couples two independent ideas, and only the first is here.
//
//   1. The interleaved container, below. It exists so that the values a
//      register loads together are the values the unpacker needs together.
//      This is what makes the layout fast, and it is layout-only: the values
//      keep their input positions.
//
//   2. The FL_ORDER permutation (the 8x16 -> 16x8 within-sub-block transpose
//      plus the 04261537 sub-block reorder). That exists to give a *sequential*
//      codec -- DELTA, RLE -- one independent chain per lane, and to let one
//      packed buffer be read at several lane widths. PFOR has no sequential
//      dependency to break, and Parquet's decoder contract is positional, so
//      FL_ORDER would have to be undone by a gather before any value could be
//      handed back. It is deliberately absent: this layout returns values in
//      input order, so nothing has to be permuted on the way out and exception
//      positions stay meaningful without translation.
//
// The layout
// ----------
// Within a 1024-value block (32 lanes x 32 rows for uint32_t), the packed
// buffer holds w u32 rows of 32 u32 words, where w is the bit width. Row r,
// lane l contributes to packed[word * 32 + l] at bit shift (r*w) % 32, where
// word = (r*w) / 32, possibly straddling into packed[word * 32 + 32 + l].
// (word == r only when w == 32.)
//
// Bits are assembled exactly as sequential bit-packing assembles them: within
// one lane, successive rows occupy successive bit positions LSB-first, and a
// value that runs off the end of a word continues in the low bits of the next.
// The two layouts differ only in which values land at adjacent bit positions --
// successive rows of one lane here, successive values of the stream in
// arrow::internal::unpack -- not in how the bits of a value are laid out. That
// is why the shift and the straddle depend on the row and never on the lane,
// and so why all 32 lanes do identical work.
//
// The payload is byte-identical in size to the sequential layout for a full
// block: 1024 * w bits either way, or 128 * w bytes.

#pragma once

#include <cstddef>
#include <cstdint>
#include <cstring>

#include "arrow/util/macros.h"

namespace arrow {
namespace util {
namespace fastlanes {

constexpr size_t kBlockSize = 1024;
constexpr size_t kLanes = 32;         // 1024 / sizeof(uint32_t) / 8
constexpr size_t kRowsPerBlock = 32;  // 1024 / kLanes

static_assert(kLanes * kRowsPerBlock == kBlockSize,
              "every value of a block must land in exactly one lane and row");

// ---------------------------------------------------------------------------
// Pack: 1024 u32 inputs, in input order -> w*32 u32 packed words.
// ---------------------------------------------------------------------------
template <uint32_t w>
inline void PackBlock(const uint32_t* ARROW_RESTRICT in, uint32_t* ARROW_RESTRICT out) {
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
      // shift is never 0 in this branch -- a value that straddles has to start
      // partway into its word -- so the >> below is never a shift by 32.
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
// Unpack: w*32 packed u32 words -> 1024 u32 outputs in input order.
//
// With kHasBias, `bias` is added to every value before it is stored, so a
// frame-of-reference decoder does not need a second pass over the output to
// add it. That pass is not cheap: measured against the unpack it follows, it
// costs 1.47x-2.40x, and a pass that only copies costs the same as one that
// adds, so what is paid for is the traversal rather than the arithmetic. The
// add is modular in uint32_t, matching the encoder's subtraction.
//
// kHasBias is a template parameter rather than a runtime argument so that the
// no-bias instantiations carry no test in their inner loop.
// ---------------------------------------------------------------------------
template <uint32_t w, bool kHasBias = false>
inline void UnpackBlock(const uint32_t* ARROW_RESTRICT packed,
                        uint32_t* ARROW_RESTRICT out, uint32_t bias = 0) {
  static_assert(w >= 1 && w <= 32);
  constexpr uint32_t kMask = (w == 32) ? 0xFFFFFFFFu : ((1u << w) - 1);

  if constexpr (w == 32) {
    if constexpr (kHasBias) {
      // A loop, not memcpy-then-add: both pointers are restrict-qualified
      // uint32_t*, so this vectorizes and stays a single traversal.
      for (size_t i = 0; i < kBlockSize; ++i) {
        out[i] = packed[i] + bias;
      }
    } else {
      std::memcpy(out, packed, kBlockSize * sizeof(uint32_t));
    }
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
        uint32_t v = (packed[word * kLanes + lane] >> shift) & kMask;
        if constexpr (kHasBias) {
          v += bias;
        }
        out[row * kLanes + lane] = v;
      }
    } else {
      const uint32_t lowBits = kT - shift;
      for (uint32_t lane = 0; lane < kLanes; ++lane) {
        const uint32_t lo = packed[word * kLanes + lane] >> shift;
        const uint32_t hi = packed[endWord * kLanes + lane] << lowBits;
        uint32_t v = (lo | hi) & kMask;
        if constexpr (kHasBias) {
          v += bias;
        }
        out[row * kLanes + lane] = v;
      }
    }
  }
}

}  // namespace fastlanes
}  // namespace util
}  // namespace arrow
