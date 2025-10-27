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
#include "arrow/util/bpacking_dispatch_internal.h"

namespace arrow::internal {

// https://github.com/fast-pack/LittleIntPacker/blob/master/src/horizontalpacking32.c
// TODO
// - _mm_cvtepi8_epi32
// - no _mm_srlv_epi32 (128bit) in xsimd with AVX2 required arch
// -  no need for while loop (for up to 8 is sufficient)
// - Shifts per swizzle can be improved when self.packed_max_byte_spread == 1 and the
//   byte can be reused (when val_bit_width divides packed_max_byte_spread).

struct KernelShape {
  const int simd_bit_size_;
  const int unpacked_bit_size_;
  const int packed_bit_size_;
  const int packed_max_spread_bytes_ = PackedMaxSpreadBytes(packed_bit_size_, 0);

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
    if (new_swizzles_per_read < swizzles_per_read) {
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
    return unpacked_per_swizzle() * kPlanSize.swizzles_per_read();
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
    const int read_start_byte = packed_start_bit / 8;
    plan.reads.at(r) = read_start_byte;

    for (int sw = 0; sw < kPlanSize.swizzles_per_read(); ++sw) {
      for (int sh = 0; sh < kPlanSize.shifts_per_swizzle(); ++sh) {
        const int sh_offset_bytes = sh * kShape.packed_max_spread_bytes();
        const int sh_offset_bits = 8 * sh_offset_bytes;

        for (int u = 0; u < kShape.unpacked_per_simd(); ++u) {
          const int packed_start_byte = packed_start_bit / 8;
          const int packed_byte_in_read = packed_start_byte - read_start_byte;
          const int u_offset_byte = u * kShape.unpacked_byte_size();
          const int sw_offset_byte = sh_offset_bytes + u_offset_byte;

          // Looping over the multiple bytes needed for current values
          for (int b = 0; b < kShape.packed_max_spread_bytes(); ++b) {
            plan.swizzles.at(r).at(sw).at(sw_offset_byte + b) = packed_byte_in_read + b;
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

/// Simple constexpr maximum element suited for non empty arrays.
template <typename T, std::size_t N>
constexpr T max_value(const std::array<T, N>& arr) {
  static_assert(N > 0);
  T out = 0;
  for (const T& v : arr) {
    if (v > out) {
      out = v;
    }
  }
  return out;
}

template <typename UnpackedUint, int kPackedBitSize, int kSimdBitSize>
struct Kernel {
  static constexpr auto kPlan = BuildPlan<UnpackedUint, kPackedBitSize, kSimdBitSize>();
  static constexpr auto kPlanSize = kPlan.kPlanSize;
  static constexpr auto kShape = kPlan.kShape;
  using Traits = typename decltype(kPlan)::Traits;
  using unpacked_type = typename Traits::unpacked_type;
  using simd_batch = typename Traits::simd_batch;
  using simd_bytes = typename Traits::simd_bytes;
  using arch_type = typename Traits::arch_type;

  static constexpr int kValuesUnpacked = kPlan.unpacked_per_kernel();

  template <int kReadIdx, int kSwizzleIdx, int kShiftIdx>
  static void unpack_one_shift_impl(const simd_batch& words, unpacked_type* out) {
    static constexpr auto kRightShiftsArr =
        kPlan.shifts.at(kReadIdx).at(kSwizzleIdx).at(kShiftIdx);

    constexpr bool kHasSse2 = xsimd::supported_architectures::contains<xsimd::sse2>();
    constexpr bool kHasAvx2 = xsimd::supported_architectures::contains<xsimd::avx2>();

    // Intel x86-64 does not have variable right shifts before AVX2.
    // Instead, since we know the packed value can safely be left shifted up to the
    // maximum already in the batch, we use a multiplication to emulate a left shits,
    // followed by a static right shift.
    // Trick from Daniel Lemire and Leonid Boytsov, Decoding billions of integers per
    // second through vectorization, Software Practice & Experience 45 (1), 2015.
    // http://arxiv.org/abs/1209.2137
    simd_batch shifted;
    if constexpr (kHasSse2 && !kHasAvx2) {
      static constexpr unpacked_type kMaxRightShift = max_value(kRightShiftsArr);

      struct MakeMults {
        static constexpr unpacked_type get(int i, int n) {
          // Equivalent to left shift of kMaxRightShift - kRightShifts.at(i).
          return unpacked_type{1} << (kMaxRightShift - kRightShiftsArr.at(i));
        }
      };

      constexpr auto kMults =
          xsimd::make_batch_constant<unpacked_type, arch_type, MakeMults>();

      shifted = (words * kMults) >> kMaxRightShift;
    } else {
      struct MakeRightShifts {
        static constexpr unpacked_type get(int i, int n) { return kRightShiftsArr.at(i); }
      };

      constexpr auto kRightShifts =
          xsimd::make_batch_constant<unpacked_type, arch_type, MakeRightShifts>();

      shifted = words >> kRightShifts;
    }

    constexpr auto kMask = kPlan.mask;
    constexpr auto kOutOffset = (kReadIdx * kPlan.unpacked_per_read() +
                                 kSwizzleIdx * kPlan.unpacked_per_swizzle() +
                                 kShiftIdx * kPlan.unpacked_per_shifts());

    const auto vals = shifted & kMask;
    xsimd::store_unaligned(out + kOutOffset, vals);
  }

  template <int kReadIdx, int kSwizzleIdx, int... kShiftIds>
  static void unpack_one_swizzle_impl(const simd_bytes& bytes, unpacked_type* out,
                                      std::integer_sequence<int, kShiftIds...>) {
    struct MakeSwizzles {
      static constexpr int get(int i, int n) {
        return kPlan.swizzles.at(kReadIdx).at(kSwizzleIdx).at(i);
      }
    };

    constexpr auto kSwizzles =
        xsimd::make_batch_constant<uint8_t, arch_type, MakeSwizzles>();

    const auto swizzled = xsimd::swizzle(bytes, kSwizzles);
    const auto words = xsimd::bitwise_cast<unpacked_type>(swizzled);
    (unpack_one_shift_impl<kReadIdx, kSwizzleIdx, kShiftIds>(words, out), ...);
  }

  template <int kReadIdx, int... kSwizzleIds>
  static void unpack_one_read_impl(const uint8_t* in, unpacked_type* out,
                                   std::integer_sequence<int, kSwizzleIds...>) {
    using ShiftSeq = std::make_integer_sequence<int, kPlanSize.shifts_per_swizzle()>;
    const auto bytes = simd_bytes::load_unaligned(in + kPlan.reads.at(kReadIdx));
    (unpack_one_swizzle_impl<kReadIdx, kSwizzleIds>(bytes, out, ShiftSeq{}), ...);
  }

  template <int... kReadIds>
  static void unpack_all_impl(const uint8_t* in, unpacked_type* out,
                              std::integer_sequence<int, kReadIds...>) {
    using SwizzleSeq = std::make_integer_sequence<int, kPlanSize.swizzles_per_read()>;
    (unpack_one_read_impl<kReadIds>(in, out, SwizzleSeq{}), ...);
  }

  static const uint8_t* unpack(const uint8_t* in, unpacked_type* out) {
    using ReadSeq = std::make_integer_sequence<int, kPlanSize.reads_per_kernel()>;
    unpack_all_impl(in, out, ReadSeq{});
    return in + (kPlan.unpacked_per_kernel() * kShape.packed_bit_size()) / 8;
  }
};

}  // namespace arrow::internal
