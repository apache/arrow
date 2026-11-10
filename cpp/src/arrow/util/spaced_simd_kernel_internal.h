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

// Word drivers shared by the implementations of SpacedExpandRightward (in
// place) and SpacedExpand (out of place) dispatched on the CPU.
//
// This header is included by translation units compiled with different
// instruction sets. Everything that generates code is therefore a member of
// a class template parameterized on the kernel, which each translation unit
// defines in an anonymous namespace: no instantiation can be shared between
// two of them by the linker.

#pragma once

#include <bit>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <utility>

#include "arrow/util/bit_util.h"
#include "arrow/util/endian.h"
#include "arrow/util/macros.h"
#include "arrow/util/ubsan.h"

namespace arrow::util::internal::spaced {

// ---------------------------------------------------------------------------
// Lookup tables (constant data only)
//
// AArch64 without FEAT_CSSC has no scalar popcount: std::popcount is a round
// trip through a SIMD register (fmov/cnt/addv/fmov), so the hot paths look
// small popcounts up instead.
// ---------------------------------------------------------------------------

struct PopCountTable {
  uint8_t count[256] = {};
  constexpr PopCountTable() {
    for (unsigned m = 0; m < 256; ++m) {
      count[m] = static_cast<uint8_t>(std::popcount(m));
    }
  }
};
inline constexpr PopCountTable kPopCount{};

// For an 8 bit chunk mask split in sub-batches of kSubLanes lanes, byte j of
// row[mask] is the number of valid lanes in sub-batches 0..j.
template <int kSubLanes>
struct PrefixTable {
  uint32_t row[256] = {};
  constexpr PrefixTable() {
    for (unsigned m = 0; m < 256; ++m) {
      uint32_t r = 0;
      for (int j = 0; j < 4 && (j + 1) * kSubLanes <= 8; ++j) {
        const unsigned low = (1u << ((j + 1) * kSubLanes)) - 1u;
        r |= static_cast<uint32_t>(std::popcount(m & low)) << (8 * j);
      }
      row[m] = r;
    }
  }
};
template <int kSubLanes>
inline constexpr PrefixTable<kSubLanes> kPrefix{};

// ---------------------------------------------------------------------------
// Word kernels
//
// A word kernel expands one 64 bit word of the bitmap:
//
//   static constexpr int kLanes;
//   static void ExpandWord(Uint* dst, const Uint* src, int src_end, uint64_t bits);
//
// writes the 64 slots of `dst`, taking the popcount(bits) values that end at
// src + src_end, and zeroing the null slots. It may read up to kLanes values
// past src + src_end, and `dst` may overlap the values it reads. `src` needs
// not be aligned.
// ---------------------------------------------------------------------------

/// Word kernel made of 64 / kLanes calls to a chunk kernel, from the top.
///
/// The chunk kernel provides `kLanes` and
/// `static void Expand(Uint* dst, const Uint* src, unsigned mask)`, which
/// expands the low popcount(mask) values of `src` to the kLanes slots of `dst`,
/// reading at most kLanes values and loading everything before storing.
/// A chunk reads below the slots of the chunks above it, so it never sees
/// what they wrote. Empty and full chunks do not go to the chunk kernel.
template <typename Uint, typename Chunk>
struct ChunkedWordKernel {
  static constexpr int kLanes = Chunk::kLanes;
  static constexpr int kChunks = 64 / kLanes;
  static constexpr uint64_t kChunkMask = (uint64_t{1} << kLanes) - 1;

  static void ExpandWord(Uint* dst, const Uint* src, int src_end, uint64_t bits) {
    auto expand_chunk = [&](int c) {
      const unsigned mask = static_cast<unsigned>((bits >> (c * kLanes)) & kChunkMask);
      const int pc = kLanes == 8 ? kPopCount.count[mask] : std::popcount(mask);
      if (pc == 0) {
        std::memset(dst + c * kLanes, 0, sizeof(Uint) * kLanes);
        return;
      }
      src_end -= pc;
      if (pc == kLanes) {
        // Much cheaper than the chunk kernel on words with few nulls. A memmove
        // of a constant size: loads then stores, no call.
        std::memmove(dst + c * kLanes, src + src_end, sizeof(Uint) * kLanes);
        return;
      }
      Chunk::Expand(dst + c * kLanes, src + src_end, mask);
    };
    // Unrolled by hand: Arrow builds with -O2, which keeps the loop.
    [&]<int... C>(std::integer_sequence<int, C...>) {
      (expand_chunk(kChunks - 1 - C), ...);
    }(std::make_integer_sequence<int, kChunks>{});
  }
};

/// Bits [pos, pos + 64) of the bitmap. Only reads the bytes holding those bits.
///
/// Templated on the kernel only to get one instantiation per translation unit.
template <typename Kernel>
uint64_t LoadWord(const uint8_t* bitmap, int64_t pos) {
  const uint8_t* p = bitmap + (pos >> 3);
  const int shift = static_cast<int>(pos & 7);
  const uint64_t lo = bit_util::FromLittleEndian(SafeLoadAs<uint64_t>(p));
  if (shift == 0) return lo;
  return (lo >> shift) | (static_cast<uint64_t>(p[8]) << (64 - shift));
}

// ---------------------------------------------------------------------------
// In place driver
//
// The bitmap is walked one 64 bit word at a time from the end of the buffer,
// and every word goes to one of three paths:
//  * runs (at most kDenseNulls nulls, or a single valid / null transition):
//    memmove whole runs of valid values and memset runs of nulls, keeping a
//    run open across words so that a long run is moved only once;
//  * sparse (at most kSparseValues valid values, far from their destination):
//    zero the word and scatter the values one by one;
//  * otherwise: the word kernel.
// ---------------------------------------------------------------------------

template <typename Uint, typename Kernel, int kDenseNulls, int kSparseValues>
struct InPlaceExpander {
  // Whether the word goes to the run path: at most kDenseNulls nulls, or a
  // single transition between a valid run and a null run (including empty
  // words). Neither needs a popcount.
  static bool IsRunWord(uint64_t bits) {
    uint64_t z = ~bits;
    for (int k = 0; k < kDenseNulls; ++k) z &= z - 1;
    return z == 0 || (bits & (bits + 1)) == 0 || (~bits & (~bits + 1)) == 0;
  }

  // Handle run words downwards from the one ending at `pos`, whose bits are
  // `bits`. `nulls_below` is the number of nulls below `pos` on entry, and
  // below the returned position on return. Kept out of line: having the open
  // run state in the main loop slows every path down.
  static ARROW_NOINLINE int ExpandRunWords(Uint* out, int pos, int& nulls_below,
                                           uint64_t bits, const uint8_t* valid_bits,
                                           int64_t valid_bits_offset) {
    // The open run: values [run_hi - x, run_hi) come from run_shift slots lower.
    bool pending = false;
    int run_hi = 0;
    int run_shift = 0;
    auto flush = [&](int run_lo) {
      if (pending && run_hi > run_lo) {
        std::memmove(out + run_lo, out + run_lo - run_shift,
                     sizeof(Uint) * static_cast<size_t>(run_hi - run_lo));
      }
      pending = false;
    };
    while (true) {
      const int lo = pos - 64;
      if (bits == 0) {
        // Empty words close the open run and are zeroed all at once.
        flush(pos);
        const int hi = pos;
        pos = lo;
        while (pos >= 64 &&
               LoadWord<Kernel>(valid_bits, valid_bits_offset + pos - 64) == 0) {
          pos -= 64;
        }
        std::memset(out + pos, 0, sizeof(Uint) * static_cast<size_t>(hi - pos));
        nulls_below -= hi - pos;
      } else {
        if (!pending && (bits >> 63) != 0) {
          pending = true;
          run_hi = pos;
          run_shift = nulls_below;
        }
        // One step per run of nulls [null_lo, null_hi], from the top. Zeroing
        // them is safe once the run above is moved: everything below still
        // comes from lower slots.
        uint64_t z = ~bits;
        while (z != 0) {
          const int null_hi = 63 - std::countl_zero(z);
          const uint64_t valid_below = bits & ((uint64_t{1} << null_hi) - 1);
          const int null_lo = valid_below == 0 ? 0 : 64 - std::countl_zero(valid_below);
          const int len = null_hi - null_lo + 1;
          nulls_below -= len;
          flush(lo + null_hi + 1);
          if (len == 1) {
            out[lo + null_lo] = 0;
          } else {
            std::memset(out + lo + null_lo, 0, sizeof(Uint) * static_cast<size_t>(len));
          }
          z &= (uint64_t{1} << null_lo) - 1;
          if (null_lo > 0) {
            pending = true;
            run_hi = lo + null_lo;
            run_shift = nulls_below;
          }
        }
        pos = lo;
      }
      // No nulls below: the open run (shift 0) and everything below it is
      // already in place.
      if (nulls_below == 0) return pos;
      if (pos < 64) break;
      bits = LoadWord<Kernel>(valid_bits, valid_bits_offset + pos - 64);
      if (bits == ~uint64_t{0}) {
        // Full words only extend the open run: skip them two at a time.
        if (!pending) {
          pending = true;
          run_hi = pos;
          run_shift = nulls_below;
        }
        pos -= 64;
        while (pos >= 128) {
          const uint64_t a = LoadWord<Kernel>(valid_bits, valid_bits_offset + pos - 64);
          const uint64_t b = LoadWord<Kernel>(valid_bits, valid_bits_offset + pos - 128);
          if ((a & b) != ~uint64_t{0}) break;
          pos -= 128;
        }
        if (pos < 64) break;
        bits = LoadWord<Kernel>(valid_bits, valid_bits_offset + pos - 64);
      }
      if (!IsRunWord(bits)) break;
    }
    flush(pos);
    return pos;
  }

  static void Expand(Uint* out, int num_values, int null_count, const uint8_t* valid_bits,
                     int64_t valid_bits_offset) {
    constexpr int kLanes = Kernel::kLanes;

    if (null_count == 0) return;
    int idx_decode = num_values - null_count;
    if (idx_decode == 0) {
      std::memset(out, 0, sizeof(Uint) * static_cast<size_t>(num_values));
      return;
    }
    int pos = num_values;
    while (pos >= 64) {
      const int lo = pos - 64;
      const uint64_t bits = LoadWord<Kernel>(valid_bits, valid_bits_offset + lo);
      if (IsRunWord(bits)) {
        int nulls_below = pos - idx_decode;
        pos = ExpandRunWords(out, pos, nulls_below, bits, valid_bits, valid_bits_offset);
        if (nulls_below == 0) return;
        idx_decode = pos - nulls_below;
        continue;
      }
      const int pc = std::popcount(bits);
      idx_decode -= pc;
      const int src_begin = idx_decode;
      const int shift = lo - src_begin;
      if (pc <= kSparseValues && shift >= 64) {
        // Source and destination do not overlap.
        std::memset(out + lo, 0, 64 * sizeof(Uint));
        int q = src_begin + pc;
        uint64_t b = bits;
        while (b != 0) {
          const int i = 63 - std::countl_zero(b);
          b ^= uint64_t{1} << i;
          out[lo + i] = out[--q];
        }
      } else if (src_begin + pc + kLanes > num_values) {
        // The kernel reads up to kLanes values past the valid values of the
        // word, which can go past the end of the buffer for the top words
        // only. Stage those in a buffer.
        Uint staged[64 + kLanes];
        std::memcpy(staged, out + src_begin, sizeof(Uint) * static_cast<size_t>(pc));
        Kernel::ExpandWord(out + lo, staged, pc, bits);
      } else {
        Kernel::ExpandWord(out + lo, out, src_begin + pc, bits);
      }
      pos = lo;
      if (shift == 0) return;
    }
    for (int i = pos - 1; i >= 0; --i) {
      if (bit_util::GetBit(valid_bits, valid_bits_offset + i)) {
        out[i] = out[--idx_decode];
      } else {
        out[i] = 0;
      }
    }
  }
};

// ---------------------------------------------------------------------------
// Out of place driver
//
// Without the in place constraints, runs of full and empty words are a single
// memcpy / memset, words with at most kSparseValues valid values are zeroed
// and scattered, and every other word goes to the kernel.
// ---------------------------------------------------------------------------

template <typename Uint, typename Kernel, int kSparseValues>
struct OutOfPlaceExpander {
  static void Expand(const Uint* src, int num_values, int null_count,
                     const uint8_t* valid_bits, int64_t valid_bits_offset, Uint* out) {
    constexpr int kLanes = Kernel::kLanes;
    const int num_valid = num_values - null_count;
    if (num_valid == 0 || null_count == 0) {
      // No need to look at the bitmap
      if (num_valid == 0) {
        std::memset(out, 0, sizeof(Uint) * static_cast<size_t>(num_values));
      } else {
        std::memcpy(out, src, sizeof(Uint) * static_cast<size_t>(num_values));
      }
      return;
    }
    // End of the values of src still to be written
    int src_end = num_valid;
    int pos = num_values;
    while (pos >= 64) {
      const int lo = pos - 64;
      const uint64_t bits = LoadWord<Kernel>(valid_bits, valid_bits_offset + lo);
      if (bits == ~uint64_t{0} || bits == 0) {
        const int hi = pos;
        pos = lo;
        while (pos >= 64 &&
               LoadWord<Kernel>(valid_bits, valid_bits_offset + pos - 64) == bits) {
          pos -= 64;
        }
        const auto size = sizeof(Uint) * static_cast<size_t>(hi - pos);
        if (bits == 0) {
          std::memset(out + pos, 0, size);
        } else {
          src_end -= hi - pos;
          std::memcpy(out + pos, src + src_end, size);
        }
        continue;
      }
      const int pc = std::popcount(bits);
      if (pc <= kSparseValues) {
        std::memset(out + lo, 0, 64 * sizeof(Uint));
        const Uint* values = src + src_end - pc;
        for (uint64_t b = bits; b != 0; b &= b - 1) {
          out[lo + std::countr_zero(b)] = SafeLoad(values++);
        }
      } else if (src_end + kLanes > num_valid) {
        // The kernel would read past the end of src
        Uint staged[64 + kLanes];
        std::memcpy(staged, src + src_end - pc, sizeof(Uint) * static_cast<size_t>(pc));
        Kernel::ExpandWord(out + lo, staged, pc, bits);
      } else {
        Kernel::ExpandWord(out + lo, src, src_end, bits);
      }
      src_end -= pc;
      pos = lo;
    }
    for (int i = pos - 1; i >= 0; --i) {
      out[i] = bit_util::GetBit(valid_bits, valid_bits_offset + i)
                   ? SafeLoad(src + --src_end)
                   : 0;
    }
  }
};

}  // namespace arrow::util::internal::spaced
