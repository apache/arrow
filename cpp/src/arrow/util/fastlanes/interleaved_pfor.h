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
// Plain (non-delta) PFOR through the FastLanes container, in two orders:
//
//   kFileOrder  per-block frame-of-reference subtract, then bit-pack with the
//               interleaved container in input order. This is Building Block
//               1 alone -- lane_delta.h and transposed_delta.h are Building
//               Block 1 applied to a delta chain; this file applies it to
//               plain PFOR, which has no chain to break. Decode returns
//               values in file order with no permutation, matching the note
//               in fastlanes_kernels_internal.h.
//
//   kFlOrder    the same, but the residuals are placed with the paper's lane
//               assignment (reused from transposed_delta.h's Transpose32x32:
//               lane l holds the contiguous run [32l, 32l+32)) before
//               packing, and gathered back to file order after unpacking.
//               PFOR has no chain for this ordering to help, so this arm
//               exists to price the gather it forces rather than to recommend
//               it -- see fastlanes_kernels_internal.h's header comment.
//
// Both orders are otherwise identical: same per-block FOR, same bit width
// choice, same container. Comparing their decode cost against BM_PforDecode
// (Arrow's shipped sequential decoder) on the same real dataset columns is
// what isolates the layout question from the value-ordering question.
//
// Wire layout for n values (n a multiple of 1024; any tail is stored raw):
//
//   [0, nblocks)                 uint8 bit width per block
//   padded to a 4-byte boundary
//   next 4 * nblocks bytes       int32 min (frame of reference) per block
//   remainder                    per block, bit_width * 32 uint32 packed words
// ---------------------------------------------------------------------------

#pragma once

#include <cstddef>
#include <cstdint>
#include <cstring>
#include <limits>

#include "arrow/util/fastlanes/fastlanes_kernels_internal.h"
#include "arrow/util/fastlanes/transposed_delta.h"

namespace arrow {
namespace util {
namespace fastlanes {

enum class InterleavedPforOrder { kFileOrder, kFlOrder };

inline uint32_t InterleavedPforBitsFor(uint64_t v) {
  uint32_t bits = 0;
  while (v != 0) {
    ++bits;
    v >>= 1;
  }
  return bits;
}

inline size_t InterleavedPforHeaderSize(size_t n) {
  const size_t nblocks = n / kBlockSize;
  const size_t widths = (nblocks + 3) & ~size_t(3);
  return widths + nblocks * sizeof(int32_t);
}

inline size_t InterleavedPforMaxEncodedSize(size_t n) {
  return InterleavedPforHeaderSize(n) + n * sizeof(uint32_t) +
         (n % kBlockSize) * sizeof(int32_t);
}

#define ILP_PACK_CASE(W)     \
  case W:                    \
    PackBlock<W>(grid, dst); \
    break;

#define ILP_UNPACK_CASE(W)                 \
  case W:                                  \
    UnpackBlock<W, true>(src, grid, bias); \
    break;

#define ILP_WIDTH_CASES(M)                                                          \
  M(1)                                                                              \
  M(2) M(3) M(4) M(5) M(6) M(7) M(8) M(9) M(10) M(11) M(12) M(13) M(14) M(15) M(16) \
      M(17) M(18) M(19) M(20) M(21) M(22) M(23) M(24) M(25) M(26) M(27) M(28) M(29) \
          M(30) M(31) M(32)

// Returns bytes written, or 0 if some block's span exceeds 32 bits (it never
// does for plain int32 values: max - min fits in 32 bits unconditionally).
template <InterleavedPforOrder kOrder>
inline size_t InterleavedPforEncode(const int32_t* in, size_t n, uint8_t* out) {
  const size_t nblocks = n / kBlockSize;

  uint8_t* widths = out;
  int32_t* mins = reinterpret_cast<int32_t*>(out + ((nblocks + 3) & ~size_t(3)));
  uint32_t* dst = reinterpret_cast<uint32_t*>(out + InterleavedPforHeaderSize(n));

  uint32_t grid[kBlockSize];

  for (size_t b = 0; b < nblocks; ++b) {
    const int32_t* blk = in + b * kBlockSize;

    int64_t vmin = std::numeric_limits<int64_t>::max();
    int64_t vmax = std::numeric_limits<int64_t>::min();
    for (size_t t = 0; t < kBlockSize; ++t) {
      const int64_t v = static_cast<int64_t>(blk[t]);
      if (v < vmin) vmin = v;
      if (v > vmax) vmax = v;
    }
    const uint32_t w = InterleavedPforBitsFor(static_cast<uint64_t>(vmax - vmin));
    widths[b] = static_cast<uint8_t>(w);
    mins[b] = static_cast<int32_t>(vmin);

    if (w > 0) {
      if constexpr (kOrder == InterleavedPforOrder::kFileOrder) {
        for (size_t t = 0; t < kBlockSize; ++t) {
          grid[t] = static_cast<uint32_t>(static_cast<int64_t>(blk[t]) - vmin);
        }
      } else {
        // grid[row * kLanes + lane] takes the residual of the value at
        // original position lane * kRowsPerBlock + row, the same placement
        // TransposedDeltaEncode uses -- so lane l ends up holding the
        // contiguous run [32l, 32l+32) once packed.
        for (size_t lane = 0; lane < kLanes; ++lane) {
          const size_t run = lane * kRowsPerBlock;
          for (size_t row = 0; row < kRowsPerBlock; ++row) {
            grid[row * kLanes + lane] =
                static_cast<uint32_t>(static_cast<int64_t>(blk[run + row]) - vmin);
          }
        }
      }
      switch (w) {
        ILP_WIDTH_CASES(ILP_PACK_CASE)
        default:
          return 0;
      }
      dst += w * kLanes;
    }
  }

  for (size_t b = nblocks; b < ((nblocks + 3) & ~size_t(3)); ++b) widths[b] = 0;

  uint8_t* end = reinterpret_cast<uint8_t*>(dst);
  const size_t tail = n % kBlockSize;
  if (tail > 0) {
    std::memcpy(end, in + nblocks * kBlockSize, tail * sizeof(int32_t));
    end += tail * sizeof(int32_t);
  }
  return static_cast<size_t>(end - out);
}

template <InterleavedPforOrder kOrder>
inline void InterleavedPforDecode(const uint8_t* in, size_t n, int32_t* out) {
  const size_t nblocks = n / kBlockSize;

  const uint8_t* widths = in;
  const int32_t* mins =
      reinterpret_cast<const int32_t*>(in + ((nblocks + 3) & ~size_t(3)));
  const uint32_t* src =
      reinterpret_cast<const uint32_t*>(in + InterleavedPforHeaderSize(n));

  uint32_t grid[kBlockSize];

  for (size_t b = 0; b < nblocks; ++b) {
    const uint32_t w = widths[b];
    const uint32_t bias = static_cast<uint32_t>(mins[b]);
    int32_t* out_blk = out + b * kBlockSize;

    if (w == 0) {
      if constexpr (kOrder == InterleavedPforOrder::kFileOrder) {
        for (size_t t = 0; t < kBlockSize; ++t) out_blk[t] = static_cast<int32_t>(bias);
      } else {
        for (size_t t = 0; t < kBlockSize; ++t) grid[t] = bias;
        Transpose32x32(grid, out_blk);
      }
      continue;
    }

    switch (w) {
      ILP_WIDTH_CASES(ILP_UNPACK_CASE)
      default:
        break;
    }
    if constexpr (kOrder == InterleavedPforOrder::kFileOrder) {
      // File order in, file order out: the container already returns values
      // at the position they were packed from, so no permutation is owed --
      // just the one copy out of the block-sized scratch grid.
      std::memcpy(out_blk, grid, sizeof(grid));
    } else {
      // Undo the lane assignment applied at encode: Transpose32x32 is its own
      // inverse for a square grid, so the same call used to build the FL_ORDER
      // layout also reads it back into file order.
      Transpose32x32(grid, out_blk);
    }
    src += w * kLanes;
  }

  const size_t tail = n % kBlockSize;
  if (tail > 0) {
    std::memcpy(out + nblocks * kBlockSize, src, tail * sizeof(int32_t));
  }
}

#undef ILP_PACK_CASE
#undef ILP_UNPACK_CASE
#undef ILP_WIDTH_CASES

}  // namespace fastlanes
}  // namespace util
}  // namespace arrow
