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
#  define SPACED_EXPAND_PLATFORM SpacedExpandRightwardNeon
#elif defined(ARROW_HAVE_SSE4_2) || defined(ARROW_HAVE_RUNTIME_SSE4_2)
#  define SPACED_EXPAND_PLATFORM SpacedExpandRightwardSse4_2
#endif

#if defined(SPACED_EXPAND_PLATFORM)

#  include <bit>
#  include <cstdint>
#  include <cstring>
#  include <utility>

#  include <xsimd/xsimd.hpp>

#  include "arrow/util/bit_util.h"
#  include "arrow/util/endian.h"
#  include "arrow/util/macros.h"
#  include "arrow/util/spaced_simd_internal.h"
#  include "arrow/util/ubsan.h"

namespace arrow::util::internal {

namespace {

using Arch = xsimd::default_arch;

// ---------------------------------------------------------------------------
// Lookup tables
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

// ---------------------------------------------------------------------------
// Chunk kernel: expand the low popcount(mask) values of `src` to the 8 slots
// of `dst` according to `mask`, zeroing the null slots.
//
// Reads at most 8 values from `src`: sub-batch j reads kSubLanes values at
// the number of valid lanes below it, which is at most 8 - kSubLanes.
// Everything is loaded before anything is stored, so `dst` may overlap `src`.
// ---------------------------------------------------------------------------

template <typename Uint>
struct ExpandChunk {
  using Batch = xsimd::batch<Uint, Arch>;
  using Bytes = xsimd::batch<uint8_t, Arch>;
  static_assert(Bytes::size == 16, "128 bit SIMD only");

  static constexpr int kLanes = 8;
  static constexpr int kSubLanes = static_cast<int>(Batch::size);
  static constexpr int kSubs = kLanes / kSubLanes;

  static void Expand(Uint* dst, const Uint* src, unsigned mask) {
    const uint32_t prefixes = kPrefix<kSubLanes>.row[mask];
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

// ---------------------------------------------------------------------------
// Word driver
//
// The bitmap is walked one 64 bit word at a time from the end of the buffer,
// and every word goes to one of three paths:
//  * runs (at most kDenseNulls nulls, or a single valid / null transition):
//    memmove whole runs of valid values and memset runs of nulls, keeping a
//    run open across words so that a long run is moved only once;
//  * sparse (few valid values, far from their destination): zero the word
//    and scatter the values one by one;
//  * otherwise: 8 chunks of the SIMD kernel.
// ---------------------------------------------------------------------------

// Bits [pos, pos + 64) of the bitmap. Only reads the bytes holding those bits.
inline uint64_t LoadWord(const uint8_t* bitmap, int64_t pos) {
  const uint8_t* p = bitmap + (pos >> 3);
  const int shift = static_cast<int>(pos & 7);
  const uint64_t lo = bit_util::FromLittleEndian(SafeLoadAs<uint64_t>(p));
  if (shift == 0) return lo;
  return (lo >> shift) | (static_cast<uint64_t>(p[8]) << (64 - shift));
}

// Whether the word goes to the run path: at most kDenseNulls nulls, or a
// single transition between a valid run and a null run (including empty
// words). Neither needs a popcount.
template <int kDenseNulls>
inline bool IsRunWord(uint64_t bits) {
  uint64_t z = ~bits;
  for (int k = 0; k < kDenseNulls; ++k) z &= z - 1;
  return z == 0 || (bits & (bits + 1)) == 0 || (~bits & (~bits + 1)) == 0;
}

// Handle run words downwards from the one ending at `pos`, whose bits are
// `bits`. `nulls_below` is the number of nulls below `pos` on entry, and
// below the returned position on return. Kept out of line: having the open
// run state in the main loop slows every path down.
template <typename Uint, int kDenseNulls>
ARROW_NOINLINE int ExpandRunWords(Uint* out, int pos, int& nulls_below, uint64_t bits,
                                  const uint8_t* valid_bits, int64_t valid_bits_offset) {
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
      while (pos >= 64 && LoadWord(valid_bits, valid_bits_offset + pos - 64) == 0) {
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
    bits = LoadWord(valid_bits, valid_bits_offset + pos - 64);
    if (bits == ~uint64_t{0}) {
      // Full words only extend the open run: skip them two at a time.
      if (!pending) {
        pending = true;
        run_hi = pos;
        run_shift = nulls_below;
      }
      pos -= 64;
      while (pos >= 128) {
        const uint64_t a = LoadWord(valid_bits, valid_bits_offset + pos - 64);
        const uint64_t b = LoadWord(valid_bits, valid_bits_offset + pos - 128);
        if ((a & b) != ~uint64_t{0}) break;
        pos -= 128;
      }
      if (pos < 64) break;
      bits = LoadWord(valid_bits, valid_bits_offset + pos - 64);
    }
    if (!IsRunWord<kDenseNulls>(bits)) break;
  }
  flush(pos);
  return pos;
}

template <typename Uint, int kDenseNulls>
void ExpandRightward(Uint* out, int num_values, int null_count, const uint8_t* valid_bits,
                     int64_t valid_bits_offset) {
  using Kernel = ExpandChunk<Uint>;
  constexpr int kLanes = Kernel::kLanes;
  constexpr int kChunks = 64 / kLanes;
  // Words with at most that many valid values, far enough from their
  // destination, are scattered one by one.
  constexpr int kSparseValues = 8;

  if (null_count == 0) return;
  int idx_decode = num_values - null_count;
  if (idx_decode == 0) {
    std::memset(out, 0, sizeof(Uint) * static_cast<size_t>(num_values));
    return;
  }
  int pos = num_values;
  while (pos >= 64) {
    const int lo = pos - 64;
    const uint64_t bits = LoadWord(valid_bits, valid_bits_offset + lo);
    if (IsRunWord<kDenseNulls>(bits)) {
      int nulls_below = pos - idx_decode;
      pos = ExpandRunWords<Uint, kDenseNulls>(out, pos, nulls_below, bits, valid_bits,
                                              valid_bits_offset);
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
    } else {
      // The kernel reads up to kLanes values from its source, which can go
      // past the valid values of the top word only. Stage those in a buffer.
      Uint staged[64 + kLanes];
      const Uint* base = out;
      int rem = src_begin + pc;
      if (rem + kLanes > num_values) {
        std::memcpy(staged, out + src_begin, sizeof(Uint) * static_cast<size_t>(pc));
        base = staged;
        rem = pc;
      }
      auto expand_chunk = [&](int c) {
        const unsigned mask = static_cast<unsigned>((bits >> (c * kLanes)) & 0xFF);
        const int pc_c = kPopCount.count[mask];
        if (pc_c == 0) {
          std::memset(out + lo + c * kLanes, 0, sizeof(Uint) * kLanes);
          return;
        }
        const int src = rem - pc_c;
        Kernel::Expand(out + lo + c * kLanes, base + src, mask);
        rem = src;
      };
      // Unrolled by hand: Arrow builds with -O2, which keeps the loop.
      [&]<int... C>(std::integer_sequence<int, C...>) {
        (expand_chunk(kChunks - 1 - C), ...);
      }(std::make_integer_sequence<int, kChunks>{});
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

}  // namespace

template <typename Uint>
void SPACED_EXPAND_PLATFORM(Uint* buffer, int num_values, int null_count,
                            const uint8_t* valid_bits, int64_t valid_bits_offset) {
  // Tuned on Apple M2: words with more nulls are faster through the kernel.
  constexpr int kDenseNulls = sizeof(Uint) == 4 ? 1 : 2;
  ExpandRightward<Uint, kDenseNulls>(buffer, num_values, null_count, valid_bits,
                                     valid_bits_offset);
}

template void SPACED_EXPAND_PLATFORM<uint32_t>(uint32_t*, int, int, const uint8_t*,
                                               int64_t);
template void SPACED_EXPAND_PLATFORM<uint64_t>(uint64_t*, int, int, const uint8_t*,
                                               int64_t);

}  // namespace arrow::util::internal

#  undef SPACED_EXPAND_PLATFORM
#endif  // SPACED_EXPAND_PLATFORM
