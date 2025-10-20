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

#pragma once

#include <array>
#include <cstdint>
#include <cstring>
#include <utility>

#include <xsimd/xsimd.hpp>

#include "arrow/util/bit_util.h"

namespace arrow::internal {

// https://github.com/fast-pack/LittleIntPacker/blob/master/src/horizontalpacking32.c
// TODO
// - No zero and full size unpack here
// - _mm_cvtepi8_epi32
// - var rshifts no avail on SSE
// -  no need for while loop (for up to 8 is sufficient)
// -  no need for the top functions
// - Shifts per swizzle can be improved when self.packed_max_byte_spread == 1 and the
//   byte can be reused (when val_bit_width divides packed_max_byte_spread).

/// Compute the maximum spread in bytes that a packed integer can cover.
///
/// This is assuming contiguous packed integer starting on a byte aligned boundary.
/// This function is non-monotonic, for instance three bit integers will be split on the
/// first byte boundary (hence having a spread of two bytes) while four bit integer will
/// be well behaved and never spread over byte boundary (hence having a spread of one).
constexpr int PackedMaxSpreadBytes(int width) {
  int max = static_cast<int>(bit_util::BytesForBits(width));
  int start = width;
  while (start % 8 != 0) {
    const int byte_start = start / 8;
    const int byte_end = (start + width - 1) / 8;  // inclusive end bit
    const int spread = byte_end - byte_start + 1;
    max = spread > max ? spread : max;
    start += width;
  }
  return max;
}

struct KernelShape {
  const int simd_bit_size_;
  const int unpacked_bit_size_;
  const int packed_bit_size_;
  const int packed_max_spread_bytes_ = PackedMaxSpreadBytes(packed_bit_size_);

  /// Properties of an SIMD batch
  constexpr int simd_bit_size() const { return simd_bit_size_; }
  constexpr int simd_byte_size() const { return simd_bit_size_ / 8; }

  /// Properties of the unpacked integers
  constexpr int unpacked_bit_size() const { return unpacked_bit_size_; }
  constexpr int unpacked_byte_size() const { return unpacked_bit_size_ / 8; }
  constexpr int unpacked_per_simd() const { return simd_bit_size_ / unpacked_bit_size_; }

  /// Properties of the packed integers
  constexpr int packed_bit_size() const { return packed_bit_size_; }
  constexpr int packed_max_spread_bytes() const { return packed_max_spread_bytes_; }
};

template <typename UnpackedUint, int kPackedBitSize, int kSimdBitSize>
struct KernelTraits {
  static constexpr KernelShape kShape = {
      /* .simd_bit_size_= */ kSimdBitSize,
      /* .unpacked_bit_size= */ 8 * sizeof(UnpackedUint),
      /* .packed_bit_size_= */ kPackedBitSize,
  };

  using unpacked_type = UnpackedUint;
  using simd_batch = xsimd::make_sized_batch_t<unpacked_type, kShape.unpacked_per_simd()>;
  using simd_bytes = xsimd::make_sized_batch_t<uint8_t, kShape.simd_byte_size()>;
  using arch_type = typename simd_batch::arch_type;
};

struct KernelPlanSize {
  int reads_per_kernel_;
  int swizzles_per_read_;
  int shifts_per_swizzle_;

  constexpr int reads_per_kernel() const { return reads_per_kernel_; }

  constexpr int swizzles_per_read() const { return swizzles_per_read_; }
  constexpr int swizzles_per_kernel() const {
    return swizzles_per_read_ * reads_per_kernel();
  }

  constexpr int shifts_per_swizzle() const { return shifts_per_swizzle_; }
  constexpr int shifts_per_read() const {
    return shifts_per_swizzle_ * swizzles_per_read();
  }
  constexpr int shifts_per_kernel() const {
    return shifts_per_read() * reads_per_kernel();
  }
};

constexpr KernelPlanSize BuildPlanSize(const KernelShape& shape) {
  const int shifts_per_swizzle =
      shape.unpacked_byte_size() / shape.packed_max_spread_bytes();

  const int vals_per_swizzle = shifts_per_swizzle * shape.unpacked_per_simd();

  const auto swizzles_per_read_for_offset = [&](int bit_offset) -> int {
    const int vals_per_simd =
        (shape.simd_bit_size() - bit_offset) / shape.packed_bit_size();
    return vals_per_simd / vals_per_swizzle;
  };

  // If after a whole swizzle reading iteration we fall unaligned, the remaining
  // iterations will start with an aligned first value, reducing the effective capacity of
  // the SIMD batch.
  // We must check that our read iteration size still works with subsequent misalignment
  // by looping until aligned.
  // One may think that using such large reading iterations risks overshooting an aligned
  // byte and increasing the total number of values extracted, but in practice reading
  // iterations increase by factors of 2 and are quickly multiple of 8 (and aligned after
  // the first one).
  int swizzles_per_read = swizzles_per_read_for_offset(0);
  int reads_per_kernel = 0;
  int packed_start_bit = 0;
  do {
    int new_swizzles_per_read = swizzles_per_read_for_offset(packed_start_bit % 8);
    if (new_swizzles_per_read <= swizzles_per_read) {
      swizzles_per_read = new_swizzles_per_read;
      packed_start_bit = 0;
      reads_per_kernel = 0;
    }
    int bits_per_read = swizzles_per_read * vals_per_swizzle * shape.packed_bit_size();
    packed_start_bit += bits_per_read;
    reads_per_kernel += 1;
  } while (packed_start_bit % 8 != 0);

  return {
      /* .reads_per_kernel_= */ reads_per_kernel,
      /* .swizzles_per_read_= */ swizzles_per_read,
      /* .shifts_per_swizzle_= */ shifts_per_swizzle,
  };
}

template <typename UnpackedUint, int kPackedBitSize, int kSimdBitSize>
struct KernelPlan {
  using Traits = KernelTraits<UnpackedUint, kPackedBitSize, kSimdBitSize>;
  static constexpr auto kShape = Traits::kShape;
  static constexpr auto kPlanSize = BuildPlanSize(kShape);

  using ReadsPerKernel = std::array<int, kPlanSize.reads_per_kernel()>;

  using Swizzle = std::array<int, kShape.simd_byte_size()>;
  using SwizzlesPerRead = std::array<Swizzle, kPlanSize.swizzles_per_read()>;
  using SwizzlesPerKernel = std::array<SwizzlesPerRead, kPlanSize.reads_per_kernel()>;

  using Shift = std::array<UnpackedUint, kShape.unpacked_per_simd()>;
  using ShiftsPerSwizzle = std::array<Shift, kPlanSize.shifts_per_swizzle()>;
  using ShiftsPerRead = std::array<ShiftsPerSwizzle, kPlanSize.swizzles_per_read()>;
  using ShitsPerKernel = std::array<ShiftsPerRead, kPlanSize.reads_per_kernel()>;

  static constexpr int unpacked_per_shifts() { return kShape.unpacked_per_simd(); }
  static constexpr int unpacked_per_swizzle() {
    return unpacked_per_shifts() * kPlanSize.shifts_per_swizzle();
  }
  static constexpr int unpacked_per_read() {
    return unpacked_per_swizzle() * kPlanSize.swizzle_per_read();
  }
  static constexpr int unpacked_per_kernel() {
    return unpacked_per_read() * kPlanSize.reads_per_kernel();
  }

  ReadsPerKernel reads;
  SwizzlesPerKernel swizzles;
  ShitsPerKernel shifts;
  UnpackedUint mask = bit_util::LeastSignificantBitMask<UnpackedUint>(kPackedBitSize);
};

template <typename UnpackedUint, int kPackedBitSize, int kSimdBitSize>
constexpr KernelPlan<UnpackedUint, kPackedBitSize, kSimdBitSize> BuildPlan() {
  using Plan = KernelPlan<UnpackedUint, kPackedBitSize, kSimdBitSize>;
  constexpr auto kShape = Plan::kShape;
  constexpr auto kPlanSize = Plan::kPlanSize;

  Plan plan = {};

  int packed_start_bit = 0;
  for (int r = 0; r < kPlanSize.reads_per_kernel(); ++r) {
    plan.reads.at(r) = packed_start_bit / 8;
    packed_start_bit = packed_start_bit % 8;

    for (int sw = 0; sw < kPlanSize.swizzles_per_read(); ++sw) {
      for (int sh = 0; sh < kPlanSize.shifts_per_swizzle(); ++sh) {
        const int sh_offset_bytes = sh * kShape.packed_max_spread_bytes();
        const int sh_offset_bits = 8 * sh_offset_bytes;

        for (int u = 0; u < kShape.unpacked_per_simd(); ++u) {
          const int packed_start_byte = packed_start_bit / 8;
          const int u_offset_byte = u * kShape.unpacked_byte_size();
          const int sw_offset_byte = sh_offset_bytes + u_offset_byte;

          // Looping over the multiple bytes needed for current values
          for (int b = 0; b < kShape.packed_max_spread_bytes(); ++b) {
            plan.swizzles.at(r).at(sw).at(sw_offset_byte + b) = packed_start_byte + b;
          }
          // Shift is a single value but many packed values may be swizzles to a sing
          // unpacked value
          plan.shifts.at(r).at(sw).at(sh).at(u) = sh_offset_bits + packed_start_bit % 8;

          packed_start_bit += kShape.packed_bit_size();
        }
      }
    }
  }

  return plan;
}

}  // namespace arrow::internal
