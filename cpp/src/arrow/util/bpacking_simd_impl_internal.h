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
#include <numeric>
#include <utility>

#include <xsimd/xsimd.hpp>

#include "arrow/util/bit_util.h"
#include "arrow/util/bpacking_dispatch_internal.h"

namespace arrow::internal {

// https://github.com/fast-pack/LittleIntPacker/blob/master/src/horizontalpacking32.c
// TODO
// - _mm_cvtepi8_epi32
// - no _mm_srlv_epi32 (128bit) in xsimd with AVX2 required arch
// - no need for while loop (for up to 8 is sufficient)
// - upstream var lshift to xsimd
// - array to batch constant to xsimd
// - Shifts per swizzle can be improved when self.packed_max_byte_spread == 1 and the
//   byte can be reused (when val_bit_width divides packed_max_byte_spread).
// - Try for uint16_t and uint8_t and bool (currently copy)
// - For Avx2:
//   - Inspect how swizzle across lanes are handled: _mm256_shuffle_epi8 not used?
//   - Investigate AVX2 with 128 bit register

template <typename Arr>
constexpr Arr BuildConstantArray(typename Arr::value_type val) {
  Arr out = {};
  for (auto& v : out) {
    v = val;
  }
  return out;
}

constexpr bool PackedIsOversizedForSimd(int simd_bit_size, int unpacked_bit_size,
                                        int packed_bit_size) {
  const int unpacked_per_simd = simd_bit_size / unpacked_bit_size;

  const auto packed_per_read_for_offset = [&](int bit_offset) -> int {
    return (simd_bit_size - bit_offset) / packed_bit_size;
  };

  int packed_start_bit = 0;
  do {
    int packed_per_read = packed_per_read_for_offset(packed_start_bit % 8);
    if (packed_per_read < unpacked_per_simd) {
      return true;
    }
    packed_start_bit += unpacked_per_simd * packed_bit_size;
  } while (packed_start_bit % 8 != 0);

  return false;
}

struct KernelShape {
  const int simd_bit_size_;
  const int unpacked_bit_size_;
  const int packed_bit_size_;
  const int packed_max_spread_bytes_ = PackedMaxSpreadBytes(packed_bit_size_, 0);
  const bool is_oversized_ =
      PackedIsOversizedForSimd(simd_bit_size_, unpacked_bit_size_, packed_bit_size_);

  constexpr bool is_medium() const {
    return packed_max_spread_bytes() <= unpacked_byte_size();
  }
  constexpr bool is_large() const { return !is_medium() && !is_oversized(); }
  constexpr bool is_oversized() const { return is_oversized_; }

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

struct MediumKernelPlanSize {
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

constexpr MediumKernelPlanSize BuildMediumPlanSize(const KernelShape& shape) {
  const int shifts_per_swizzle =
      shape.unpacked_byte_size() / shape.packed_max_spread_bytes();

  const int vals_per_swizzle = shifts_per_swizzle * shape.unpacked_per_simd();

  const auto swizzles_per_read_for_offset = [&](int bit_offset) -> int {
    const int vals_per_simd =
        (shape.simd_bit_size() - bit_offset) / shape.packed_bit_size();
    return vals_per_simd / vals_per_swizzle;
  };

  // If after a whole swizzle reading iteration we fall unaligned, the remaining
  // iterations will start with an unaligned first value, reducing the effective capacity
  // of the SIMD batch.
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
struct MediumKernelPlan {
  using Traits = KernelTraits<UnpackedUint, kPackedBitSize, kSimdBitSize>;
  static constexpr auto kShape = Traits::kShape;
  static constexpr auto kPlanSize = BuildMediumPlanSize(kShape);

  using ReadsPerKernel = std::array<int, kPlanSize.reads_per_kernel()>;

  using Swizzle = std::array<uint8_t, kShape.simd_byte_size()>;
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
constexpr MediumKernelPlan<UnpackedUint, kPackedBitSize, kSimdBitSize> BuildMediumPlan() {
  using Plan = MediumKernelPlan<UnpackedUint, kPackedBitSize, kSimdBitSize>;
  constexpr auto kShape = Plan::kShape;
  constexpr auto kPlanSize = Plan::kPlanSize;
  static_assert(kShape.is_medium());

  Plan plan = {};

  int packed_start_bit = 0;
  for (int r = 0; r < kPlanSize.reads_per_kernel(); ++r) {
    const int read_start_byte = packed_start_bit / 8;
    plan.reads.at(r) = read_start_byte;

    for (int sw = 0; sw < kPlanSize.swizzles_per_read(); ++sw) {
      constexpr int kUndefined = -1;
      plan.swizzles.at(r).at(sw) = BuildConstantArray<typename Plan::Swizzle>(kUndefined);
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
            plan.swizzles.at(r).at(sw).at(sw_offset_byte + b) =
                static_cast<uint8_t>(packed_byte_in_read + b);
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

template <const auto& kArr, typename Arch, std::size_t... Is>
constexpr auto make_batch_constant_impl(std::index_sequence<Is...>) {
  using Array = std::decay_t<decltype(kArr)>;
  using value_type = typename Array::value_type;

  return xsimd::batch_constant<value_type, Arch, kArr[Is]...>{};
}

template <const auto& kArr, typename Arch>
constexpr auto make_batch_constant() {
  return make_batch_constant_impl<kArr, Arch>(std::make_index_sequence<kArr.size()>());
}

// Intel x86-64 does not have variable left shifts before AVX2.
//
// We replace the variable left shift by a variable multiply with a power of two.
//
// This trick is borrowed from Daniel Lemire and Leonid Boytsov, Decoding billions of
// integers per second through vectorization, Software Practice & Experience 45 (1), 2015.
// http://arxiv.org/abs/1209.2137
template <typename Arch, typename Int, Int... kShifts>
auto left_shift(const xsimd::batch<Int, Arch>& batch,
                xsimd::batch_constant<Int, Arch, kShifts...> shifts) {
  constexpr bool kHasSse2 = xsimd::supported_architectures::contains<xsimd::sse2>();
  constexpr bool kHasAvx2 = xsimd::supported_architectures::contains<xsimd::avx2>();

  if constexpr (kHasSse2 && !kHasAvx2) {
    static constexpr auto kShiftsArr = std::array{kShifts...};

    struct MakeMults {
      static constexpr Int get(int i, int n) { return Int{1} << kShiftsArr.at(i); }
    };

    constexpr auto kMults = xsimd::make_batch_constant<Int, Arch, MakeMults>();
    return batch * kMults;

  } else {
    return batch << shifts;
  }
}

// Intel x86-64 does not have variable right shifts before AVX2.
//
// When we know that the relevant bits will not overflow, we can instead shift left all
// values to align them with the one with the largest right shifts followed by a constant
// shift on all values.
// In doing so, we replace the variable left shift by a variable multiply with a power of
// two.
//
// This trick is borrowed from Daniel Lemire and Leonid Boytsov, Decoding billions of
// integers per second through vectorization, Software Practice & Experience 45 (1), 2015.
// http://arxiv.org/abs/1209.2137
template <typename Arch, typename Int, Int... kShifts>
auto overflow_right_shift(const xsimd::batch<Int, Arch>& batch,
                          xsimd::batch_constant<Int, Arch, kShifts...> shifts) {
  constexpr bool kHasSse2 = xsimd::supported_architectures::contains<xsimd::sse2>();
  constexpr bool kHasAvx2 = xsimd::supported_architectures::contains<xsimd::avx2>();

  if constexpr (kHasSse2 && !kHasAvx2) {
    static constexpr auto kShiftsArr = std::array{kShifts...};
    static constexpr Int kMaxRightShift = max_value(kShiftsArr);

    struct MakeMults {
      static constexpr Int get(int i, int n) {
        // Equivalent to left shift of kMaxRightShift - kRightShifts.at(i).
        return Int{1} << (kMaxRightShift - kShiftsArr.at(i));
      }
    };

    constexpr auto kMults = xsimd::make_batch_constant<Int, Arch, MakeMults>();
    return (batch * kMults) >> kMaxRightShift;

  } else {
    return batch >> shifts;
  }
}

template <typename UnpackedUint, int kPackedBitSize, int kSimdBitSize>
struct MediumKernel {
  static constexpr auto kPlan =
      BuildMediumPlan<UnpackedUint, kPackedBitSize, kSimdBitSize>();
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
    constexpr auto kRightShifts = make_batch_constant<kRightShiftsArr, arch_type>();
    constexpr auto kMask = kPlan.mask;
    constexpr auto kOutOffset = (kReadIdx * kPlan.unpacked_per_read() +
                                 kSwizzleIdx * kPlan.unpacked_per_swizzle() +
                                 kShiftIdx * kPlan.unpacked_per_shifts());

    // Intel x86-64 does not have variable right shifts before AVX2.
    // We know the packed value can safely be left shifted up to the largest offset so we
    // can use the fallback on these platforms.
    const auto shifted = overflow_right_shift(words, kRightShifts);
    const auto vals = shifted & kMask;
    xsimd::store_unaligned(out + kOutOffset, vals);
  }

  template <int kReadIdx, int kSwizzleIdx, int... kShiftIds>
  static void unpack_one_swizzle_impl(const simd_bytes& bytes, unpacked_type* out,
                                      std::integer_sequence<int, kShiftIds...>) {
    static constexpr auto kSwizzlesArr = kPlan.swizzles.at(kReadIdx).at(kSwizzleIdx);
    constexpr auto kSwizzles = make_batch_constant<kSwizzlesArr, arch_type>();

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

template <typename UnpackedUint, int kPackedBitSize, int kSimdBitSize>
struct LargeKernelPlan {
  using Traits = KernelTraits<UnpackedUint, kPackedBitSize, kSimdBitSize>;
  static constexpr auto kShape = Traits::kShape;

  static constexpr int kUnpackedPerkernel = std::lcm(kShape.unpacked_per_simd(), 8);
  static constexpr int kReadsPerKernel = static_cast<int>(bit_util::CeilDiv(
      kUnpackedPerkernel * kShape.packed_bit_size(), kShape.simd_bit_size()));

  using ReadsPerKernel = std::array<int, kReadsPerKernel>;

  using Swizzle = std::array<uint8_t, kShape.simd_byte_size()>;
  using SwizzlesPerKernel = std::array<Swizzle, kReadsPerKernel>;

  using Shift = std::array<UnpackedUint, kShape.unpacked_per_simd()>;
  using ShitsPerKernel = std::array<Shift, kReadsPerKernel>;

  ReadsPerKernel reads;
  SwizzlesPerKernel low_swizzles;
  SwizzlesPerKernel high_swizzles;
  ShitsPerKernel low_rshifts;
  ShitsPerKernel high_lshifts;
  UnpackedUint low_mask;
  UnpackedUint high_mask;
};

template <typename UnpackedUint, int kPackedBitSize, int kSimdBitSize>
constexpr LargeKernelPlan<UnpackedUint, kPackedBitSize, kSimdBitSize> BuildLargePlan() {
  using Plan = LargeKernelPlan<UnpackedUint, kPackedBitSize, kSimdBitSize>;
  constexpr auto kShape = Plan::kShape;
  static_assert(kShape.is_large());
  constexpr int kOverBytes =
      kShape.packed_max_spread_bytes() - kShape.unpacked_byte_size();

  Plan plan = {};

  int packed_start_bit = 0;
  for (int r = 0; r < Plan::kReadsPerKernel; ++r) {
    const int read_start_byte = packed_start_bit / 8;
    plan.reads.at(r) = read_start_byte;

    constexpr int kUndefined = -1;
    plan.low_swizzles.at(r) = BuildConstantArray<typename Plan::Swizzle>(kUndefined);
    plan.high_swizzles.at(r) = BuildConstantArray<typename Plan::Swizzle>(kUndefined);

    for (int u = 0; u < kShape.unpacked_per_simd(); ++u) {
      const int packed_start_byte = packed_start_bit / 8;
      const int packed_byte_in_read = packed_start_byte - read_start_byte;

      // Looping over maximum number of bytes that can fit a value
      // We fill more than necessary in the high swizzle because in the absence of
      // variable right shifts, we will erase some bits from the low sizzled values.
      for (int b = 0; b < kShape.unpacked_byte_size(); ++b) {
        const auto idx = u * kShape.unpacked_byte_size() + b;
        plan.low_swizzles.at(r).at(idx) = packed_byte_in_read + b;
        plan.high_swizzles.at(r).at(idx) = packed_byte_in_read + b + kOverBytes;
      }

      // low and high swizzles need to be rshifted but the oversized bytes created a
      // larger lshift for high values.
      plan.low_rshifts.at(r).at(u) = packed_start_bit % 8;
      plan.high_lshifts.at(r).at(u) = 8 * kOverBytes - (packed_start_bit % 8);

      packed_start_bit += kShape.packed_bit_size();
    }
  }

  constexpr auto mask = bit_util::LeastSignificantBitMask<UnpackedUint>(kPackedBitSize);
  constexpr auto half_low_bit_mask =
      bit_util::LeastSignificantBitMask<UnpackedUint>(kShape.unpacked_bit_size() / 2);
  plan.low_mask = mask & half_low_bit_mask;
  plan.high_mask = mask & (~half_low_bit_mask);

  return plan;
}

template <typename UnpackedUint, int kPackedBitSize, int kSimdBitSize>
struct LargeKernel {
  static constexpr auto kPlan =
      BuildLargePlan<UnpackedUint, kPackedBitSize, kSimdBitSize>();
  static constexpr auto kShape = kPlan.kShape;
  using Traits = typename decltype(kPlan)::Traits;
  using unpacked_type = typename Traits::unpacked_type;
  using simd_batch = typename Traits::simd_batch;
  using simd_bytes = typename Traits::simd_bytes;
  using arch_type = typename Traits::arch_type;

  static constexpr int kValuesUnpacked = kPlan.kUnpackedPerkernel;

  template <int kReadIdx, int kSwizzleIdx, int kShiftIdx>
  static void unpack_one_shift_impl(const simd_batch& words, unpacked_type* out) {
    static constexpr auto kRightShiftsArr =
        kPlan.shifts.at(kReadIdx).at(kSwizzleIdx).at(kShiftIdx);
    constexpr auto kRightShifts = make_batch_constant<kRightShiftsArr, arch_type>();
    constexpr auto kMask = kPlan.mask;
    constexpr auto kOutOffset = (kReadIdx * kPlan.unpacked_per_read() +
                                 kSwizzleIdx * kPlan.unpacked_per_swizzle() +
                                 kShiftIdx * kPlan.unpacked_per_shifts());

    // Intel x86-64 does not have variable right shifts before AVX2.
    // We know the packed value can safely be left shifted up to the largest offset so we
    // can use the fallback on these platforms.
    const auto shifted = overflow_right_shift(words, kRightShifts);
    const auto vals = shifted & kMask;
    xsimd::store_unaligned(out + kOutOffset, vals);
  }

  template <int kReadIdx, int kSwizzleIdx, int... kShiftIds>
  static void unpack_one_swizzle_impl(const simd_bytes& bytes, unpacked_type* out,
                                      std::integer_sequence<int, kShiftIds...>) {
    static constexpr auto kSwizzlesArr = kPlan.swizzles.at(kReadIdx).at(kSwizzleIdx);
    constexpr auto kSwizzles = make_batch_constant<kSwizzlesArr, arch_type>();

    const auto swizzled = xsimd::swizzle(bytes, kSwizzles);
    const auto words = xsimd::bitwise_cast<unpacked_type>(swizzled);
    (unpack_one_shift_impl<kReadIdx, kSwizzleIdx, kShiftIds>(words, out), ...);
  }

  template <int kReadIdx>
  static void unpack_one_read_impl(const uint8_t* in, unpacked_type* out) {
    static constexpr auto kLowSwizzlesArr = kPlan.low_swizzles.at(kReadIdx);
    constexpr auto kLowSwizzles = make_batch_constant<kLowSwizzlesArr, arch_type>();
    static constexpr auto kLowRShiftsArr = kPlan.low_rshifts.at(kReadIdx);
    constexpr auto kLowRShifts = make_batch_constant<kLowRShiftsArr, arch_type>();

    static constexpr auto kHighSwizzlesArr = kPlan.high_swizzles.at(kReadIdx);
    constexpr auto kHighSwizzles = make_batch_constant<kHighSwizzlesArr, arch_type>();
    static constexpr auto kHighLShiftsArr = kPlan.high_lshifts.at(kReadIdx);
    constexpr auto kHighLShifts = make_batch_constant<kHighLShiftsArr, arch_type>();

    const auto bytes = simd_bytes::load_unaligned(in + kPlan.reads.at(kReadIdx));

    const auto low_swizzled = xsimd::swizzle(bytes, kLowSwizzles);
    const auto low_words = xsimd::bitwise_cast<unpacked_type>(low_swizzled);
    const auto low_shifted = overflow_right_shift(low_words, kLowRShifts);
    const auto low_half_vals = low_shifted & kPlan.low_mask;

    const auto high_swizzled = xsimd::swizzle(bytes, kHighSwizzles);
    const auto high_words = xsimd::bitwise_cast<unpacked_type>(high_swizzled);
    const auto high_shifted = left_shift(high_words, kHighLShifts);
    const auto high_half_vals = high_shifted & kPlan.high_mask;

    const auto vals = low_half_vals | high_half_vals;
    xsimd::store_unaligned(out + kReadIdx * kShape.unpacked_per_simd(), vals);
  }

  template <int... kReadIds>
  static void unpack_all_impl(const uint8_t* in, unpacked_type* out,
                              std::integer_sequence<int, kReadIds...>) {
    (unpack_one_read_impl<kReadIds>(in, out), ...);
  }

  static const uint8_t* unpack(const uint8_t* in, unpacked_type* out) {
    using ReadSeq = std::make_integer_sequence<int, kPlan.kReadsPerKernel>;
    unpack_all_impl(in, out, ReadSeq{});
    return in + (kPlan.kUnpackedPerkernel * kShape.packed_bit_size()) / 8;
  }
};

template <typename UnpackedUint, int kPackedBitSize, int kSimdBitSize>
struct OversizedKernel {
  using unpacked_type = UnpackedUint;

  static constexpr int kValuesUnpacked = 0;

  static const uint8_t* unpack(const uint8_t* in, unpacked_type* out) { return in; }
};

template <typename UnpackedUint, int kPackedBitSize, int kSimdBitSize>
constexpr auto DispatchKernel() {
  using kTraits = KernelTraits<UnpackedUint, kPackedBitSize, kSimdBitSize>;
  if constexpr (kTraits::kShape.is_medium()) {
    return MediumKernel<UnpackedUint, kPackedBitSize, kSimdBitSize>{};
  } else if constexpr (kTraits::kShape.is_large()) {
    return LargeKernel<UnpackedUint, kPackedBitSize, kSimdBitSize>{};
  } else {
    return OversizedKernel<UnpackedUint, kPackedBitSize, kSimdBitSize>{};
  }
}

template <typename UnpackedUint, int kPackedBitSize, int kSimdBitSize>
using DispatchKernelType =
    decltype(DispatchKernel<UnpackedUint, kPackedBitSize, kSimdBitSize>());

template <typename UnpackedUint, int kPackedBitSize, int kSimdBitSize>
struct Kernel : DispatchKernelType<UnpackedUint, kPackedBitSize, kSimdBitSize> {
  using Base = DispatchKernelType<UnpackedUint, kPackedBitSize, kSimdBitSize>;
  using Base::kValuesUnpacked;
  using Base::unpack;
};

template <int kPackedBitSize, int kSimdBitSize>
struct Kernel<uint8_t, kPackedBitSize, kSimdBitSize>
    : Kernel<uint16_t, kPackedBitSize, kSimdBitSize> {
  using Base = DispatchKernelType<uint16_t, kPackedBitSize, kSimdBitSize>;
  using Base::kValuesUnpacked;
  using unpacked_type = uint8_t;

  static const uint8_t* unpack(const uint8_t* in, unpacked_type* out) {
    uint16_t buffer[kValuesUnpacked] = {};
    in = Base::unpack(in, buffer);
    for (int k = 0; k < kValuesUnpacked; ++k) {
      out[k] = static_cast<unpacked_type>(buffer[k]);
    }
    return in;
  }
};

template <int kPackedBitSize, int kSimdBitSize>
struct Kernel<bool, kPackedBitSize, kSimdBitSize>
    : Kernel<uint16_t, kPackedBitSize, kSimdBitSize> {
  using Base = DispatchKernelType<uint16_t, kPackedBitSize, kSimdBitSize>;
  using Base::kValuesUnpacked;
  using unpacked_type = bool;

  static const uint8_t* unpack(const uint8_t* in, unpacked_type* out) {
    uint16_t buffer[kValuesUnpacked] = {};
    in = Base::unpack(in, buffer);
    for (int k = 0; k < kValuesUnpacked; ++k) {
      out[k] = static_cast<unpacked_type>(buffer[k]);
    }
    return in;
  }
};

}  // namespace arrow::internal
