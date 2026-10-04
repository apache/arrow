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
// FastLanes couples two independent ideas, and only the container is here. The
// other is the FL_ORDER permutation (the 8x16 -> 16x8 within-sub-block
// transpose plus the 04261537 sub-block reorder), which exists to give a
// sequential codec -- DELTA, RLE -- one independent chain per lane, and to let
// one packed buffer be read at several lane widths. PFOR has no sequential
// dependency to break, and Parquet's decoder contract is positional, so
// FL_ORDER would have to be undone by a gather before any value could be handed
// back. It is deliberately absent: this layout returns values in input order,
// so nothing has to be permuted on the way out and exception positions stay
// meaningful without translation.
//
// A block holds kBlockSize values whatever the element width, so the grid is
// sizeof(T)*8 rows by kBlockSize/(sizeof(T)*8) lanes: 32 rows of 32 lanes for
// uint32_t, 16 rows of 64 lanes for uint16_t, 8 rows of 128 lanes for uint8_t.
// The packed buffer holds w rows of kLanes words. Row r, lane l contributes to
// packed[word * kLanes + l] at bit shift (r*w) % kBits, where
// word = (r*w) / kBits, possibly straddling into packed[endWord * kLanes + l].
// (word == r only when w == kBits.) The payload is 128 * w bytes at every
// element width, the same size the sequential layout needs for a full block.
//
// Bits are assembled exactly as sequential bit-packing assembles them: within
// one lane, successive rows occupy successive bit positions LSB-first, and a
// value that runs off the end of a word continues in the low bits of the next.
// The two layouts differ only in which values land at adjacent bit positions --
// successive rows of one lane here, successive values of the stream in
// arrow::internal::unpack -- not in how the bits of a value are laid out. So
// the shift and the straddle depend on the row and never on the lane, and all
// lanes do identical work.
//
// Both kernels take an Arch type parameter their bodies never mention, so that
// this one source compiled at two different instruction sets yields two
// distinct symbols. Without it, PackBlock<uint32_t, 16> compiled with NEON
// flags and the same instantiation compiled with SVE flags share a mangled
// name, the linker keeps one definition, and every caller silently gets
// whichever copy it kept -- undoing the per-instruction-set translation units
// of interleaved_dispatch_internal.h with no diagnostic.
// arrow::internal::bpacking carries the parameter for the same reason. A caller
// that compiles exactly one copy of these kernels leaves it at its default and
// is unaffected.

#pragma once

#include <cstddef>
#include <cstdint>
#include <cstring>
#include <type_traits>

#include "arrow/util/macros.h"

namespace arrow {
namespace util {
namespace fastlanes {

constexpr size_t kBlockSize = 1024;

// Grid geometry for one block of T.
template <typename T>
struct BlockGeometry {
  static_assert(std::is_unsigned_v<T> && !std::is_same_v<T, bool>,
                "element type must be an unsigned integer wider than bool");
  static constexpr size_t kBits = sizeof(T) * 8;
  static constexpr size_t kRowsPerBlock = kBits;
  static constexpr size_t kLanes = kBlockSize / kBits;

  static_assert(kLanes * kRowsPerBlock == kBlockSize,
                "every value of a block must land in exactly one lane and row");
};

// The 32-bit grid the PFOR and delta wire formats are written on. Those formats
// hold these values in their payload arithmetic, so they keep an unqualified
// name here; a codec at another element width asks BlockGeometry<T> instead.
constexpr size_t kLanes = BlockGeometry<uint32_t>::kLanes;
constexpr size_t kRowsPerBlock = BlockGeometry<uint32_t>::kRowsPerBlock;

// Shifts and masks are evaluated in this type, which is unsigned and never
// narrower than the element. uint8_t and uint16_t would otherwise promote to
// int. A masked value below 2^w shifted by less than the element width cannot
// reach 2^31, so the result is the same either way; naming the type keeps the
// kernel bodies free of promotion concerns.
template <typename T>
using BlockWord = std::conditional_t<(sizeof(T) < sizeof(uint32_t)), uint32_t, T>;

template <typename T, uint32_t w>
constexpr BlockWord<T> BlockMask() {
  using W = BlockWord<T>;
  constexpr size_t kBits = BlockGeometry<T>::kBits;
  // w == kBits would shift by the width of W when W is exactly T, so that case
  // takes the all-ones form instead.
  if constexpr (w >= kBits) {
    return static_cast<W>(~W(0) >> (sizeof(W) * 8 - kBits));
  } else {
    return static_cast<W>((W(1) << w) - 1);
  }
}

// ---------------------------------------------------------------------------
// Pack: kBlockSize inputs, in input order -> w * kLanes packed words.
// ---------------------------------------------------------------------------
template <typename T, uint32_t w, typename Arch = void>
inline void PackBlock(const T* ARROW_RESTRICT in, T* ARROW_RESTRICT out) {
  using G = BlockGeometry<T>;
  using W = BlockWord<T>;
  static_assert(w >= 1 && w <= G::kBits);
  constexpr W kMask = BlockMask<T, w>();
  constexpr uint32_t kT = static_cast<uint32_t>(G::kBits);

  if constexpr (w == G::kBits) {
    std::memcpy(out, in, kBlockSize * sizeof(T));
    return;
  } else {
    std::memset(out, 0, w * G::kLanes * sizeof(T));

#pragma GCC unroll 32
    for (uint32_t row = 0; row < G::kRowsPerBlock; ++row) {
      const uint32_t startBit = row * w;
      const uint32_t word = startBit / kT;
      const uint32_t shift = startBit % kT;
      const uint32_t endBit = startBit + w;
      const uint32_t endWord = (endBit - 1) / kT;

      if (word == endWord) {
        for (uint32_t lane = 0; lane < G::kLanes; ++lane) {
          const W v = static_cast<W>(in[row * G::kLanes + lane]) & kMask;
          out[word * G::kLanes + lane] |= static_cast<T>(v << shift);
        }
      } else {
        // shift is never 0 in this branch -- a value that straddles has to
        // start partway into its word -- so the >> below is never a shift by
        // the full word width.
        const uint32_t lowBits = kT - shift;
        for (uint32_t lane = 0; lane < G::kLanes; ++lane) {
          const W v = static_cast<W>(in[row * G::kLanes + lane]) & kMask;
          out[word * G::kLanes + lane] |= static_cast<T>(v << shift);
          out[endWord * G::kLanes + lane] |= static_cast<T>(v >> lowBits);
        }
      }
    }
  }
}

// ---------------------------------------------------------------------------
// Unpack: w * kLanes packed words -> kBlockSize outputs in input order.
//
// With kHasBias, `bias` is added to every value before it is stored, so a
// frame-of-reference decoder does not need a second pass over the output to add
// it. That pass costs 1.47x-2.40x of the unpack it follows, and a pass that
// only copies costs the same as one that adds, so what is paid for is the
// traversal rather than the arithmetic. The add is modular in T, matching the
// encoder's subtraction. kHasBias is a template parameter rather than a runtime
// argument so the no-bias instantiations carry no test in their inner loop.
// ---------------------------------------------------------------------------
template <typename T, uint32_t w, bool kHasBias = false, typename Arch = void>
inline void UnpackBlock(const T* ARROW_RESTRICT packed, T* ARROW_RESTRICT out,
                        T bias = 0) {
  using G = BlockGeometry<T>;
  using W = BlockWord<T>;
  static_assert(w >= 1 && w <= G::kBits);
  constexpr W kMask = BlockMask<T, w>();
  constexpr uint32_t kT = static_cast<uint32_t>(G::kBits);

  if constexpr (w == G::kBits) {
    if constexpr (kHasBias) {
      // A loop, not memcpy-then-add: both pointers are restrict-qualified T*,
      // so this vectorizes and stays a single traversal.
      for (size_t i = 0; i < kBlockSize; ++i) {
        out[i] = static_cast<T>(packed[i] + bias);
      }
    } else {
      std::memcpy(out, packed, kBlockSize * sizeof(T));
    }
    return;
  } else {
#pragma GCC unroll 32
    for (uint32_t row = 0; row < G::kRowsPerBlock; ++row) {
      const uint32_t startBit = row * w;
      const uint32_t word = startBit / kT;
      const uint32_t shift = startBit % kT;
      const uint32_t endBit = startBit + w;
      const uint32_t endWord = (endBit - 1) / kT;

      if (word == endWord) {
        for (uint32_t lane = 0; lane < G::kLanes; ++lane) {
          W v = (static_cast<W>(packed[word * G::kLanes + lane]) >> shift) & kMask;
          if constexpr (kHasBias) {
            v = static_cast<W>(v + static_cast<W>(bias));
          }
          out[row * G::kLanes + lane] = static_cast<T>(v);
        }
      } else {
        const uint32_t lowBits = kT - shift;
        for (uint32_t lane = 0; lane < G::kLanes; ++lane) {
          const W lo = static_cast<W>(packed[word * G::kLanes + lane]) >> shift;
          const W hi = static_cast<W>(packed[endWord * G::kLanes + lane]) << lowBits;
          W v = (lo | hi) & kMask;
          if constexpr (kHasBias) {
            v = static_cast<W>(v + static_cast<W>(bias));
          }
          out[row * G::kLanes + lane] = static_cast<T>(v);
        }
      }
    }
  }
}

}  // namespace fastlanes
}  // namespace util
}  // namespace arrow
