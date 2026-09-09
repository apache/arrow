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

// ---------------------------------------------------------------------------
// The FastLanes paper's lane assignment for DELTA, as opposed to the +32 stride
// in lane_delta.h.
//
// Both break DELTA_BINARY_PACKED's single 1024-long dependency chain into 32
// independent chains, and both therefore decode with one vector add per row.
// They differ in which value sits one step back in a lane, and that decides the
// compression ratio:
//
//   lane_delta.h        lane l holds original indices l, 32+l, ..., 992+l. The
//                       in-lane predecessor is 32 positions back in file order,
//                       so the stored differences are wider than the format's.
//                       Container order is file order, so decode writes
//                       straight to the output.
//
//   this file           lane l holds the contiguous run [32l, 32l+32). The
//                       in-lane predecessor is the immediately preceding value,
//                       so the stored differences are exactly the ones
//                       DELTA_BINARY_PACKED stores. Decode ends in transposed
//                       order and owes a 32x32 transpose.
//
// The second is the paper's: section 2.3 states the order for its own
// 16-value example as 0,4,8,12 | 1,5,9,13 | 2,6,10,14 | 3,7,11,15, i.e. lane
// 0 holds 0,1,2,3, and Figure 4c shows the entry points such a chain needs.
// Reconstructing the Unified Transposed Layout from Figure 6d's caption and
// walking the chain as section 2.4 describes it, the in-lane distance is +1
// for 100% of steps at all four lane widths; the 04261537 tile order is what
// makes one stored order do that for 8, 16, 32 and 64-bit lanes at once. A
// Parquet column has one physical type, so a 32x32 transpose reproduces the
// property this file needs.
//
// The price is the entry points: 32 per vector rather than the format's one.
// The paper charges itself "1 bit per value" for storing them plainly and about
// 0.21 bits for delta-encoding them across vectors, and both are provided here
// (TransposedBaseCoding).
//
// The base for lane l is the value *before* its run, v[32l - 1], rather than
// v[32l]. That way all 1024 payload slots carry a real difference: without it
// row 0 of every block would be a known zero and 1/32 of the packed words would
// be wasted.
//
// Wire layout for n values (n a multiple of 1024; any tail is stored raw):
//
//   [0, nblocks)                 uint8 payload bit width per block
//   padded to a 4-byte boundary
//   next 4 * nblocks             int32 payload min delta per block
//   next, per block              the 32 bases: 4 * 32 bytes raw, or, packed,
//                                one uint8 width + one int32 min + the 32
//                                base deltas bit-packed LSB-first
//   remainder                    per block, bit_width * 32 uint32 packed words
// ---------------------------------------------------------------------------

#pragma once

#include <cstddef>
#include <cstdint>
#include <cstring>
#include <limits>

#if defined(__ARM_NEON) || defined(__ARM_NEON__)
#include <arm_neon.h>
#define ARROW_TRANSPOSED_DELTA_NEON 1
#endif

#include "arrow/util/fastlanes/fastlanes_kernels_internal.h"

namespace arrow {
namespace util {
namespace fastlanes {

enum class TransposedBaseCoding { kRaw, kPacked };

// How a decoder gets back to file order. kNone is not a conforming decoder and
// exists only to price the other two. kSeparate walks the block twice, once for
// the prefix sums and once to permute; kFused does both in one pass, which is
// available because a lane's chain does not depend on any other lane.
enum class TransposedRepair { kNone, kSeparate, kFused };

// The output slot that container slot (row, lane) belongs to, and its inverse.
// Not used on the hot path -- the decoder transposes in blocks instead.
inline size_t TransposedSource(size_t t) {
  const size_t row = t / kLanes;
  const size_t lane = t % kLanes;
  return lane * kRowsPerBlock + row;
}

inline uint32_t TransposedBitsFor(uint64_t v) {
  uint32_t bits = 0;
  while (v != 0) {
    ++bits;
    v >>= 1;
  }
  return bits;
}

// An LSB-first bit codec for the 32 base deltas of one block. 32 values per 1024
// is not a hot path, so it does not need the register-width container -- but it
// does need to be word-based rather than bit-at-a-time, which costs an operation
// per value of the block rather than per value of the base stream.
inline void PackScalar32(const uint32_t* in, uint32_t w, uint8_t* out) {
  if (w == 0) return;
  uint64_t acc = 0;
  uint32_t have = 0;
  for (size_t i = 0; i < kLanes; ++i) {
    acc |= static_cast<uint64_t>(in[i]) << have;
    have += w;
    while (have >= 8) {
      *out++ = static_cast<uint8_t>(acc);
      acc >>= 8;
      have -= 8;
    }
  }
  if (have > 0) *out = static_cast<uint8_t>(acc);
}

inline void UnpackScalar32(const uint8_t* in, uint32_t w, uint32_t* out) {
  if (w == 0) {
    std::memset(out, 0, kLanes * sizeof(uint32_t));
    return;
  }
  const uint32_t mask = w == 32 ? ~uint32_t{0} : ((uint32_t{1} << w) - 1);
  // One unaligned 64-bit load per value covers any w <= 32 at any bit offset,
  // and the load for the last value or two reaches past the stream. That is off
  // the end of the page when the last block of a page needs no payload words,
  // so the stream is staged first: it is at most 128 bytes, one copy per 1024
  // values, and it buys a loop with no bounds test and no short loads. Bytes
  // above the stream never reach the output -- every value's bits lie inside
  // it, and the mask keeps only those.
  const size_t stream = (kLanes * w + 7) / 8;
  alignas(8) uint8_t staged[kLanes * sizeof(uint32_t) + sizeof(uint64_t)];
  std::memcpy(staged, in, stream);
  std::memset(staged + stream, 0, sizeof(uint64_t));
  for (size_t i = 0; i < kLanes; ++i) {
    const size_t bit = i * w;
    uint64_t word;
    std::memcpy(&word, staged + (bit >> 3), sizeof(word));
    out[i] = static_cast<uint32_t>(word >> (bit & 7)) & mask;
  }
}

template <TransposedBaseCoding kBases>
inline size_t TransposedBaseBytes(uint32_t base_width) {
  if constexpr (kBases == TransposedBaseCoding::kRaw) {
    return kLanes * sizeof(uint32_t);
  } else {
    return 1 + sizeof(int32_t) + (kLanes * base_width + 7) / 8;
  }
}

inline size_t TransposedFixedHeader(size_t n) {
  const size_t nblocks = n / kBlockSize;
  return ((nblocks + 3) & ~size_t(3)) + nblocks * sizeof(int32_t);
}

inline size_t TransposedMaxEncodedSize(size_t n) {
  const size_t nblocks = n / kBlockSize;
  // Per block: the widest base stream (a uint8 width, an int32 min and 32
  // deltas at 32 bits), up to 3 bytes of padding before the packed words, and
  // the payload, which n * sizeof(uint32_t) covers for every block at once.
  return TransposedFixedHeader(n) + nblocks * (kLanes * sizeof(uint32_t) + 5 + 3) +
         n * sizeof(uint32_t) + (n % kBlockSize) * sizeof(int32_t) + 8;
}

#define TPOSE_PACK_CASE(W)   \
  case W:                    \
    PackBlock<W>(grid, dst); \
    break;

#define TPOSE_UNPACK_CASE(W)               \
  case W:                                  \
    UnpackBlock<W, true>(src, grid, bias); \
    break;

#define TPOSE_WIDTH_CASES(M)                                                          \
  M(1) M(2) M(3) M(4) M(5) M(6) M(7) M(8) M(9) M(10) M(11) M(12) M(13) M(14) M(15)    \
  M(16) M(17) M(18) M(19) M(20) M(21) M(22) M(23) M(24) M(25) M(26) M(27) M(28) M(29) \
  M(30) M(31) M(32)

// Returns bytes written, 0 if some block cannot be represented.
template <TransposedBaseCoding kBases>
inline size_t TransposedDeltaEncode(const int32_t* in, size_t n, uint8_t* out) {
  const size_t nblocks = n / kBlockSize;
  uint8_t* widths = out;
  int32_t* mins = reinterpret_cast<int32_t*>(out + ((nblocks + 3) & ~size_t(3)));
  uint8_t* cur = out + TransposedFixedHeader(n);

  int64_t deltas[kBlockSize];
  uint32_t grid[kBlockSize];
  uint32_t bases[kLanes];
  uint32_t base_deltas[kLanes];

  for (size_t b = 0; b < nblocks; ++b) {
    const int32_t* blk = in + b * kBlockSize;
    // Previous value in file order for the block's very first element.
    // For the first block there is no predecessor; seeding from the first value
    // makes its delta zero rather than the value itself.
    const int64_t before = b == 0 ? static_cast<int64_t>(in[0])
                                  : static_cast<int64_t>(in[b * kBlockSize - 1]);

    // deltas[row * kLanes + lane] is the difference between the value at
    // output position lane * 32 + row and the one before it in file order.
    int64_t min_delta = std::numeric_limits<int64_t>::max();
    for (size_t lane = 0; lane < kLanes; ++lane) {
      const size_t run = lane * kRowsPerBlock;
      int64_t prev = run == 0 ? before : static_cast<int64_t>(blk[run - 1]);
      bases[lane] = static_cast<uint32_t>(prev);
      for (size_t row = 0; row < kRowsPerBlock; ++row) {
        const int64_t v = static_cast<int64_t>(blk[run + row]);
        const int64_t d = v - prev;
        deltas[row * kLanes + lane] = d;
        if (d < min_delta) min_delta = d;
        prev = v;
      }
    }

    uint64_t span = 0;
    for (size_t t = 0; t < kBlockSize; ++t) {
      const uint64_t adj = static_cast<uint64_t>(deltas[t] - min_delta);
      if (adj > span) span = adj;
    }
    const bool modular = span > 0xFFFFFFFFull;
    if (modular) min_delta = 0;
    const uint32_t w = modular ? 32 : TransposedBitsFor(span);
    widths[b] = static_cast<uint8_t>(w);
    mins[b] = static_cast<int32_t>(min_delta);

    if constexpr (kBases == TransposedBaseCoding::kRaw) {
      std::memcpy(cur, bases, sizeof(bases));
      cur += sizeof(bases);
    } else {
      // Bases sit 32 values apart, so their own differences are small. Chain
      // the first one to the previous block's last base.
      int64_t bmin = std::numeric_limits<int64_t>::max();
      int64_t bd[kLanes];
      const size_t last = (b - 1) * kBlockSize + (kLanes - 1) * kRowsPerBlock - 1;
      int64_t prev_base = b == 0 ? 0 : static_cast<int64_t>(in[last]);
      for (size_t lane = 0; lane < kLanes; ++lane) {
        const int64_t cur_base = static_cast<int32_t>(bases[lane]);
        bd[lane] = cur_base - prev_base;
        if (bd[lane] < bmin) bmin = bd[lane];
        prev_base = cur_base;
      }
      uint64_t bspan = 0;
      for (size_t lane = 0; lane < kLanes; ++lane) {
        const uint64_t adj = static_cast<uint64_t>(bd[lane] - bmin);
        if (adj > bspan) bspan = adj;
      }
      const bool bmodular = bspan > 0xFFFFFFFFull;
      if (bmodular) bmin = 0;
      const uint32_t bw = bmodular ? 32 : TransposedBitsFor(bspan);
      for (size_t lane = 0; lane < kLanes; ++lane) {
        base_deltas[lane] = static_cast<uint32_t>(bd[lane] - bmin);
      }
      *cur++ = static_cast<uint8_t>(bw);
      const int32_t bmin32 = static_cast<int32_t>(bmin);
      std::memcpy(cur, &bmin32, sizeof(bmin32));
      cur += sizeof(bmin32);
      PackScalar32(base_deltas, bw, cur);
      cur += (kLanes * bw + 7) / 8;
    }

    if (w > 0) {
      for (size_t t = 0; t < kBlockSize; ++t) {
        grid[t] = static_cast<uint32_t>(deltas[t] - min_delta);
      }
      // The packed words must be 4-byte aligned; the base stream keeps that
      // for kRaw but not for kPacked, so pad.
      const size_t pad = (4 - (static_cast<size_t>(cur - out) & 3)) & 3;
      std::memset(cur, 0, pad);
      cur += pad;
      uint32_t* dst = reinterpret_cast<uint32_t*>(cur);
      switch (w) {
        TPOSE_WIDTH_CASES(TPOSE_PACK_CASE)
        default:
          return 0;
      }
      cur += w * kLanes * sizeof(uint32_t);
    }
  }

  for (size_t b = nblocks; b < ((nblocks + 3) & ~size_t(3)); ++b) widths[b] = 0;

  const size_t tail = n % kBlockSize;
  if (tail > 0) {
    std::memcpy(cur, in + nblocks * kBlockSize, tail * sizeof(int32_t));
    cur += tail * sizeof(int32_t);
  }
  return static_cast<size_t>(cur - out);
}

// A blocked 8x8 transpose of the 32x32 grid. Reads and writes stay inside the
// 4 KB grid and the 4 KB output block, so this is an L1-resident permutation
// rather than the scattered stores a general gather would issue.
inline void Transpose32x32Scalar(const uint32_t* ARROW_RESTRICT grid,
                           int32_t* ARROW_RESTRICT out) {
  for (size_t lb = 0; lb < kLanes; lb += 8) {
    for (size_t rb = 0; rb < kRowsPerBlock; rb += 8) {
      for (size_t lane = lb; lane < lb + 8; ++lane) {
        int32_t* dst = out + lane * kRowsPerBlock + rb;
        const uint32_t* src = grid + rb * kLanes + lane;
        dst[0] = static_cast<int32_t>(src[0 * kLanes]);
        dst[1] = static_cast<int32_t>(src[1 * kLanes]);
        dst[2] = static_cast<int32_t>(src[2 * kLanes]);
        dst[3] = static_cast<int32_t>(src[3 * kLanes]);
        dst[4] = static_cast<int32_t>(src[4 * kLanes]);
        dst[5] = static_cast<int32_t>(src[5 * kLanes]);
        dst[6] = static_cast<int32_t>(src[6 * kLanes]);
        dst[7] = static_cast<int32_t>(src[7 * kLanes]);
      }
    }
  }
}

#ifdef ARROW_TRANSPOSED_DELTA_NEON
// Four 4x4 transposes per iteration. The permuted copy is one traversal either
// way, so what this removes is the strided access, not the traffic.
inline void Transpose32x32Neon(const uint32_t* ARROW_RESTRICT grid,
                               int32_t* ARROW_RESTRICT out) {
  for (size_t lane = 0; lane < kLanes; lane += 4) {
    for (size_t row = 0; row < kRowsPerBlock; row += 4) {
      const uint32_t* src = grid + row * kLanes + lane;
      const uint32x4x2_t a = vtrnq_u32(vld1q_u32(src), vld1q_u32(src + kLanes));
      const uint32x4x2_t b =
          vtrnq_u32(vld1q_u32(src + 2 * kLanes), vld1q_u32(src + 3 * kLanes));
      uint32_t* dst = reinterpret_cast<uint32_t*>(out) + lane * kRowsPerBlock + row;
      vst1q_u32(dst, vcombine_u32(vget_low_u32(a.val[0]), vget_low_u32(b.val[0])));
      dst += kRowsPerBlock;
      vst1q_u32(dst, vcombine_u32(vget_low_u32(a.val[1]), vget_low_u32(b.val[1])));
      dst += kRowsPerBlock;
      vst1q_u32(dst, vcombine_u32(vget_high_u32(a.val[0]), vget_high_u32(b.val[0])));
      dst += kRowsPerBlock;
      vst1q_u32(dst, vcombine_u32(vget_high_u32(a.val[1]), vget_high_u32(b.val[1])));
    }
  }
}
#endif

inline void Transpose32x32(const uint32_t* ARROW_RESTRICT grid,
                           int32_t* ARROW_RESTRICT out) {
#ifdef ARROW_TRANSPOSED_DELTA_NEON
  Transpose32x32Neon(grid, out);
#else
  Transpose32x32Scalar(grid, out);
#endif
}

#ifdef ARROW_TRANSPOSED_DELTA_NEON
// The 32 prefix sums and the permutation in one pass. Four lanes' chains run in
// one register, so four rows of results can be transposed and stored while they
// are still in registers, sparing the block a second 4 KB round trip.
inline void PrefixSumAndTranspose(const uint32_t* ARROW_RESTRICT grid,
                                  const uint32_t* ARROW_RESTRICT bases,
                                  int32_t* ARROW_RESTRICT out) {
  for (size_t lane = 0; lane < kLanes; lane += 4) {
    uint32x4_t acc = vld1q_u32(bases + lane);
    for (size_t row = 0; row < kRowsPerBlock; row += 4) {
      const uint32_t* src = grid + row * kLanes + lane;
      const uint32x4_t v0 = vaddq_u32(acc, vld1q_u32(src));
      const uint32x4_t v1 = vaddq_u32(v0, vld1q_u32(src + kLanes));
      const uint32x4_t v2 = vaddq_u32(v1, vld1q_u32(src + 2 * kLanes));
      const uint32x4_t v3 = vaddq_u32(v2, vld1q_u32(src + 3 * kLanes));
      acc = v3;
      const uint32x4x2_t a = vtrnq_u32(v0, v1);
      const uint32x4x2_t c = vtrnq_u32(v2, v3);
      uint32_t* dst = reinterpret_cast<uint32_t*>(out) + lane * kRowsPerBlock + row;
      vst1q_u32(dst, vcombine_u32(vget_low_u32(a.val[0]), vget_low_u32(c.val[0])));
      dst += kRowsPerBlock;
      vst1q_u32(dst, vcombine_u32(vget_low_u32(a.val[1]), vget_low_u32(c.val[1])));
      dst += kRowsPerBlock;
      vst1q_u32(dst, vcombine_u32(vget_high_u32(a.val[0]), vget_high_u32(c.val[0])));
      dst += kRowsPerBlock;
      vst1q_u32(dst, vcombine_u32(vget_high_u32(a.val[1]), vget_high_u32(c.val[1])));
    }
  }
}
#endif

// kRepair off leaves the block in transposed order, which isolates the cost of
// the transpose from the rest of the decode. Only kRepair on is a conforming
// Parquet decoder.
template <TransposedBaseCoding kBases,
          TransposedRepair kRepair = TransposedRepair::kFused>
inline void TransposedDeltaDecode(const uint8_t* in, size_t n, int32_t* out) {
  const size_t nblocks = n / kBlockSize;
  const uint8_t* widths = in;
  const int32_t* mins =
      reinterpret_cast<const int32_t*>(in + ((nblocks + 3) & ~size_t(3)));
  const uint8_t* cur = in + TransposedFixedHeader(n);

  uint32_t grid[kBlockSize];
  uint32_t bases[kLanes];

  for (size_t b = 0; b < nblocks; ++b) {
    const uint32_t w = widths[b];
    const uint32_t bias = static_cast<uint32_t>(mins[b]);

    if constexpr (kBases == TransposedBaseCoding::kRaw) {
      std::memcpy(bases, cur, sizeof(bases));
      cur += sizeof(bases);
    } else {
      const uint32_t bw = *cur++;
      int32_t bmin;
      std::memcpy(&bmin, cur, sizeof(bmin));
      cur += sizeof(bmin);
      uint32_t bd[kLanes];
      UnpackScalar32(cur, bw, bd);
      cur += (kLanes * bw + 7) / 8;
      // 32 serial adds per 1024 values, against the format's 1024.
      uint32_t running = b == 0 ? 0u : bases[kLanes - 1];
      for (size_t lane = 0; lane < kLanes; ++lane) {
        running += bd[lane] + static_cast<uint32_t>(bmin);
        bases[lane] = running;
      }
    }

    if (w == 0) {
      for (size_t t = 0; t < kBlockSize; ++t) grid[t] = bias;
    } else {
      cur += (4 - (static_cast<size_t>(cur - in) & 3)) & 3;
      const uint32_t* src = reinterpret_cast<const uint32_t*>(cur);
      switch (w) {
        TPOSE_WIDTH_CASES(TPOSE_UNPACK_CASE)
        default:
          break;
      }
      cur += w * kLanes * sizeof(uint32_t);
    }

#ifdef ARROW_TRANSPOSED_DELTA_NEON
    if constexpr (kRepair == TransposedRepair::kFused) {
      PrefixSumAndTranspose(grid, bases, out + b * kBlockSize);
      continue;
    }
#endif
    // 32 independent prefix sums, one vector add per row. Row 0 starts from
    // the base, which is the value before the lane's run.
    for (size_t lane = 0; lane < kLanes; ++lane) {
      grid[lane] += bases[lane];
    }
    for (size_t row = 1; row < kRowsPerBlock; ++row) {
      uint32_t* c = grid + row * kLanes;
      const uint32_t* p = c - kLanes;
      for (size_t lane = 0; lane < kLanes; ++lane) {
        c[lane] += p[lane];
      }
    }

    if constexpr (kRepair == TransposedRepair::kNone) {
      std::memcpy(out + b * kBlockSize, grid, sizeof(grid));
    } else {
      Transpose32x32(grid, out + b * kBlockSize);
    }
  }

  const size_t tail = n % kBlockSize;
  if (tail > 0) {
    std::memcpy(out + nblocks * kBlockSize, cur, tail * sizeof(int32_t));
  }
}

#undef TPOSE_PACK_CASE
#undef TPOSE_UNPACK_CASE
#undef TPOSE_WIDTH_CASES

}  // namespace fastlanes
}  // namespace util
}  // namespace arrow
