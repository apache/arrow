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
// Lane-parallel delta, an experiment: DELTA_BINARY_PACKED's cost is dominated
// by its prefix sum, which is one dependent add per value and therefore
// unvectorizable. Deltaing inside a bit-packing lane instead of along the file
// order replaces that single 1024-long chain with 32 independent chains of
// length 32, so one vector add per row advances all 32 at once.
//
// Two value orders are provided, differing only in which value sits one step
// back in a lane:
//
//   kInterleaved  lane l holds original indices l, 32+l, ..., 992+l, so the
//                 in-lane step is a uniform +32 in file order. Container order
//                 equals file order, so decode needs no permutation, and the
//                 step from one block's last row to the next block's first row
//                 is also +32 -- the lane seeds are written once per column.
//
// The kFlOrder arm this file once carried used Arrow's fromTransposed32, which
// is not the FastLanes order and has since been deleted from the tree. The
// paper's own lane assignment lives in transposed_delta.h.
//
// Wire layout for n values (n a multiple of 1024; any tail is stored raw):
//
//   [0, 128)                     32 uint32 lane seeds
//   [128, 128 + nblocks)         uint8 bit width per block
//   padded to a 4-byte boundary
//   next 4 * nblocks bytes       int32 min delta per block
//   remainder                    per block, bit_width * 32 uint32 packed words
//
// Headers are hoisted out of the payload so every block's packed words land
// 4-byte aligned without the decoder copying them to a scratch buffer first.
// ---------------------------------------------------------------------------

#pragma once

#include <cstddef>
#include <cstdint>
#include <cstring>
#include <limits>

#include "arrow/util/fastlanes/fastlanes_kernels_internal.h"

namespace arrow {
namespace util {
namespace fastlanes {

enum class LaneDeltaOrder { kInterleaved };

// Bits needed to hold v, 0 for v == 0. Encode-side only, so a loop is fine.
inline uint32_t LaneDeltaBitsFor(uint64_t v) {
  uint32_t bits = 0;
  while (v != 0) {
    ++bits;
    v >>= 1;
  }
  return bits;
}

// Original-block index held by container slot t.
template <LaneDeltaOrder kOrder>
inline size_t LaneDeltaSource(size_t t) {
  return t;
}

inline size_t LaneDeltaHeaderSize(size_t n) {
  const size_t nblocks = n / kBlockSize;
  const size_t widths = (nblocks + 3) & ~size_t(3);
  return kLanes * sizeof(uint32_t) + widths + nblocks * sizeof(int32_t);
}

inline size_t LaneDeltaMaxEncodedSize(size_t n) {
  return LaneDeltaHeaderSize(n) + n * sizeof(uint32_t) + (n % kBlockSize) * sizeof(int32_t);
}

#define LANE_DELTA_PACK_CASE(W) \
  case W:                       \
    PackBlock<W>(grid, dst);    \
    break;

#define LANE_DELTA_UNPACK_CASE(W)                 \
  case W:                                         \
    UnpackBlock<W, true>(src, grid, bias);        \
    break;

#define LANE_DELTA_WIDTH_CASES(M)                                                     \
  M(1) M(2) M(3) M(4) M(5) M(6) M(7) M(8) M(9) M(10) M(11) M(12) M(13) M(14) M(15)    \
  M(16) M(17) M(18) M(19) M(20) M(21) M(22) M(23) M(24) M(25) M(26) M(27) M(28) M(29) \
  M(30) M(31) M(32)

// Returns bytes written, or 0 if some block's delta span exceeds 32 bits and
// the block therefore cannot be represented. Callers must check.
template <LaneDeltaOrder kOrder>
inline size_t LaneDeltaEncode(const int32_t* in, size_t n, uint8_t* out) {
  const size_t nblocks = n / kBlockSize;
  const size_t header = LaneDeltaHeaderSize(n);

  uint32_t* seeds = reinterpret_cast<uint32_t*>(out);
  uint8_t* widths = out + kLanes * sizeof(uint32_t);
  int32_t* mins = reinterpret_cast<int32_t*>(
      out + kLanes * sizeof(uint32_t) + ((nblocks + 3) & ~size_t(3)));
  uint32_t* dst = reinterpret_cast<uint32_t*>(out + header);

  uint32_t carry[kLanes] = {};
  uint32_t vals[kBlockSize];
  int64_t deltas[kBlockSize];
  uint32_t grid[kBlockSize];

  for (size_t b = 0; b < nblocks; ++b) {
    const int32_t* blk = in + b * kBlockSize;
    for (size_t t = 0; t < kBlockSize; ++t) {
      vals[t] = static_cast<uint32_t>(blk[LaneDeltaSource<kOrder>(t)]);
    }
    if (b == 0) {
      // The first row has no predecessor; seed each lane from it so that its
      // delta is zero, and hand the seeds to the decoder.
      std::memcpy(carry, vals, sizeof(carry));
      std::memcpy(seeds, vals, sizeof(carry));
    }

    int64_t min_delta = std::numeric_limits<int64_t>::max();
    for (size_t lane = 0; lane < kLanes; ++lane) {
      int64_t prev = static_cast<int32_t>(carry[lane]);
      for (size_t row = 0; row < kRowsPerBlock; ++row) {
        const int64_t cur = static_cast<int32_t>(vals[row * kLanes + lane]);
        const int64_t d = cur - prev;
        deltas[row * kLanes + lane] = d;
        if (d < min_delta) min_delta = d;
        prev = cur;
      }
      carry[lane] = vals[(kRowsPerBlock - 1) * kLanes + lane];
    }

    uint64_t span = 0;
    for (size_t t = 0; t < kBlockSize; ++t) {
      const uint64_t adj = static_cast<uint64_t>(deltas[t] - min_delta);
      if (adj > span) span = adj;
    }
    // A delta of two int32 spans up to 33 bits, so subtracting the minimum
    // does not always fit. It does not have to: reconstruction is a chain of
    // uint32 adds, so deltas truncated to 32 bits round-trip exactly. Pay the
    // full width and drop the minimum in that case.
    const bool modular = span > 0xFFFFFFFFull;
    if (modular) min_delta = 0;

    const uint32_t w = modular ? 32 : LaneDeltaBitsFor(span);
    widths[b] = static_cast<uint8_t>(w);
    mins[b] = static_cast<int32_t>(min_delta);

    if (w > 0) {
      for (size_t t = 0; t < kBlockSize; ++t) {
        grid[t] = static_cast<uint32_t>(deltas[t] - min_delta);
      }
      switch (w) {
        LANE_DELTA_WIDTH_CASES(LANE_DELTA_PACK_CASE)
        default:
          return 0;
      }
      dst += w * kLanes;
    }
  }

  for (size_t b = nblocks; b < ((nblocks + 3) & ~size_t(3)); ++b) {
    widths[b] = 0;
  }

  uint8_t* end = reinterpret_cast<uint8_t*>(dst);
  const size_t tail = n % kBlockSize;
  if (tail > 0) {
    std::memcpy(end, in + nblocks * kBlockSize, tail * sizeof(int32_t));
    end += tail * sizeof(int32_t);
  }
  return static_cast<size_t>(end - out);
}

template <LaneDeltaOrder kOrder, bool kRepair = true>
inline void LaneDeltaDecode(const uint8_t* in, size_t n, int32_t* out) {
  const size_t nblocks = n / kBlockSize;
  const size_t header = LaneDeltaHeaderSize(n);

  const uint32_t* seeds = reinterpret_cast<const uint32_t*>(in);
  const uint8_t* widths = in + kLanes * sizeof(uint32_t);
  const int32_t* mins = reinterpret_cast<const int32_t*>(
      in + kLanes * sizeof(uint32_t) + ((nblocks + 3) & ~size_t(3)));
  const uint32_t* src = reinterpret_cast<const uint32_t*>(in + header);

  uint32_t carry[kLanes];
  std::memcpy(carry, seeds, sizeof(carry));

  uint32_t grid[kBlockSize];

  for (size_t b = 0; b < nblocks; ++b) {
    const uint32_t w = widths[b];
    const uint32_t bias = static_cast<uint32_t>(mins[b]);

    if (w == 0) {
      for (size_t t = 0; t < kBlockSize; ++t) grid[t] = bias;
    } else {
      switch (w) {
        LANE_DELTA_WIDTH_CASES(LANE_DELTA_UNPACK_CASE)
        default:
          break;
      }
      src += w * kLanes;
    }

    // 32 independent prefix sums, one row at a time. Each of these inner loops
    // is 32 lanes of pure vertical add with no carry between them, which is
    // what the serial version cannot do.
    for (size_t lane = 0; lane < kLanes; ++lane) {
      grid[lane] += carry[lane];
    }
    for (size_t row = 1; row < kRowsPerBlock; ++row) {
      uint32_t* cur = grid + row * kLanes;
      const uint32_t* prev = cur - kLanes;
      for (size_t lane = 0; lane < kLanes; ++lane) {
        cur[lane] += prev[lane];
      }
    }
    std::memcpy(carry, grid + (kRowsPerBlock - 1) * kLanes, sizeof(carry));

    std::memcpy(out + b * kBlockSize, grid, sizeof(grid));
  }

  const size_t tail = n % kBlockSize;
  if (tail > 0) {
    std::memcpy(out + nblocks * kBlockSize, reinterpret_cast<const uint8_t*>(src),
                tail * sizeof(int32_t));
  }
}

#undef LANE_DELTA_PACK_CASE
#undef LANE_DELTA_UNPACK_CASE
#undef LANE_DELTA_WIDTH_CASES

}  // namespace fastlanes
}  // namespace util
}  // namespace arrow
