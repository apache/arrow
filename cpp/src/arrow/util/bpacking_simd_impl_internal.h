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
#include "arrow/util/type_traits.h"

namespace arrow::internal {

// https://github.com/fast-pack/LittleIntPacker/blob/master/src/horizontalpacking32.c
// TODO
// - _mm_cvtepi8_epi32
// - no need for while loop (for up to 8 is sufficient)
// - Shifts per swizzle can be improved when self.packed_max_byte_spread == 1 and the
//   byte can be reused (when val_bit_width divides packed_max_byte_spread).
// - Reduce input size on small bit width using a broadcast.
// - Fix overreading problem
// - Improve Swizzle by computing which bigger swapable slots are free

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
  // The integer type to work with, `unpacked_type` or an appropriate type for bool.
  using uint_type = std::conditional_t<std::is_same_v<unpacked_type, bool>,
                                       SizedUint<sizeof(bool)>, unpacked_type>;
  using simd_batch = xsimd::make_sized_batch_t<uint_type, kShape.unpacked_per_simd()>;
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

struct MediumKernelOptions {
  /// An indicative limit on the number of values unpacked by the kernel.
  /// This is a heuristic setting: other constraints such as alignment may not always make
  /// small values feasibles. Must be a power of two.
  int unpacked_per_kernel_limit = -1;
};

constexpr MediumKernelPlanSize BuildMediumPlanSize(const KernelShape& shape,
                                                   const MediumKernelOptions& options) {
  const int shifts_per_swizzle =
      shape.unpacked_byte_size() / shape.packed_max_spread_bytes();

  const int vals_per_swizzle = shifts_per_swizzle * shape.unpacked_per_simd();

  // Using `unpacked_per_kernel_limit` to influence the number of swizzles per reads.
  const auto packed_per_read_for_offset = [&](int bit_offset) -> int {
    const int best = (shape.simd_bit_size() - bit_offset) / shape.packed_bit_size();
    const int limit = options.unpacked_per_kernel_limit;
    return (best > limit) && (limit > 0) ? limit : best;
  };

  const auto swizzles_per_read_for_offset = [&](int bit_offset) -> int {
    return packed_per_read_for_offset(bit_offset) / vals_per_swizzle;
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

// TODO(C++20) Non type template parameter for MediumKernelOptions.
template <typename UnpackedUint, int kPackedBitSize, int kSimdBitSize,
          int kUnpackedPerKernelLimit>
struct MediumKernelPlan {
  using Traits = KernelTraits<UnpackedUint, kPackedBitSize, kSimdBitSize>;
  using uint_type = typename Traits::uint_type;
  static constexpr auto kShape = Traits::kShape;
  static constexpr auto kPlanSize = BuildMediumPlanSize(
      kShape, {/* .unpacked_per_kernel_limit= */ kUnpackedPerKernelLimit});

  using ReadsPerKernel = std::array<int, kPlanSize.reads_per_kernel()>;

  using Swizzle = std::array<uint8_t, kShape.simd_byte_size()>;
  using SwizzlesPerRead = std::array<Swizzle, kPlanSize.swizzles_per_read()>;
  using SwizzlesPerKernel = std::array<SwizzlesPerRead, kPlanSize.reads_per_kernel()>;

  using Shift = std::array<uint_type, kShape.unpacked_per_simd()>;
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

  static constexpr int bytes_per_read() {
    return unpacked_per_read() * kShape.packed_bit_size();
  }

  ReadsPerKernel reads;
  SwizzlesPerKernel swizzles;
  ShitsPerKernel shifts;
  uint_type mask = bit_util::LeastSignificantBitMask<uint_type>(kPackedBitSize);
};

// TODO(C++20) Non type template parameter for MediumKernelOptions.
template <typename UnpackedUint, int kPackedBitSize, int kSimdBitSize,
          int kUnpackedPerKernelLimit>
constexpr auto BuildMediumPlan() {
  using Plan = MediumKernelPlan<UnpackedUint, kPackedBitSize, kSimdBitSize,
                                kUnpackedPerKernelLimit>;
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

template <typename T, std::size_t N>
struct SwizzleBiLaneGenericPlan {
  using ByteSwizzle = std::array<T, N>;

  ByteSwizzle self_lane;
  ByteSwizzle cross_lane;
};

template <typename T, std::size_t N>
constexpr SwizzleBiLaneGenericPlan<T, N> BuildSwizzleBiLaneGenericPlan(
    std::array<T, N> mask) {
  constexpr T kAsZero = 0x80;  // Most significant bit of the byte must be 1
  constexpr std::size_t kSize = N;
  constexpr std::size_t kSizeHalf = kSize / 2;

  SwizzleBiLaneGenericPlan<T, N> plan = {};

  for (std::size_t k = 0; k < kSize; ++k) {
    const bool is_defined = (0 <= mask[k]) && (mask[k] < kSize);
    const bool is_first_lane_idx = k < kSizeHalf;
    const bool is_first_lane_mask = mask[k] < kSizeHalf;

    if (!is_defined) {
      plan.self_lane[k] = kAsZero;
      plan.cross_lane[k] = kAsZero;
    } else {
      if (is_first_lane_idx == is_first_lane_mask) {
        plan.self_lane[k] = mask[k] % kSizeHalf;
        plan.cross_lane[k] = kAsZero;
      } else {
        plan.self_lane[k] = kAsZero;
        plan.cross_lane[k] = mask[k] % kSizeHalf;
      }
    }
  }

  return plan;
}

template <typename T, typename A, T... Vals>
constexpr bool isOnlyFromHigh(xsimd::batch_constant<T, A, Vals...>) {
  return ((Vals >= (sizeof...(Vals) / 2)) && ...);
}

template <typename T, typename A, T... Vals>
constexpr bool isOnlyFromLow(xsimd::batch_constant<T, A, Vals...>) {
  return ((Vals < (sizeof...(Vals) / 2)) && ...);
}

/// Merged in xsimd 14.0, simply use swizzle
template <typename Arch, uint8_t... kIdx>
auto swizzle_bytes(const xsimd::batch<uint8_t, Arch>& batch,
                   xsimd::batch_constant<uint8_t, Arch, kIdx...> mask)
    -> xsimd::batch<uint8_t, Arch> {
  if constexpr (std::is_base_of_v<xsimd::avx2, Arch>) {
    static constexpr auto kPlan = BuildSwizzleBiLaneGenericPlan(std::array{kIdx...});
    static constexpr auto kSelfSwizzleArr = kPlan.self_lane;
    constexpr auto kSelfSwizzle = make_batch_constant<kSelfSwizzleArr, Arch>();
    static constexpr auto kCrossSwizzleArr = kPlan.cross_lane;
    constexpr auto kCrossSwizzle = make_batch_constant<kCrossSwizzleArr, Arch>();

    struct LaneMask {
      static constexpr uint8_t get(uint8_t i, uint8_t n) {
        constexpr auto kMask = std::array{kIdx...};
        return kMask[i] % (kMask.size() / 2);
      }
    };

    constexpr auto kLaneMask = xsimd::make_batch_constant<uint8_t, Arch, LaneMask>();
    if constexpr (isOnlyFromLow(mask)) {
      auto broadcast = _mm256_permute2x128_si256(batch, batch, 0x00);  // [low | low]
      return _mm256_shuffle_epi8(broadcast, kLaneMask.as_batch());
    }
    if constexpr (isOnlyFromHigh(mask)) {
      auto broadcast = _mm256_permute2x128_si256(batch, batch, 0x11);  // [high | high]
      return _mm256_shuffle_epi8(broadcast, kLaneMask.as_batch());
    }

    auto self = _mm256_shuffle_epi8(batch, kSelfSwizzle.as_batch());
    auto swapped = _mm256_permute2x128_si256(batch, batch, 0x01);
    auto cross = _mm256_shuffle_epi8(swapped, kCrossSwizzle.as_batch());
    return _mm256_or_si256(self, cross);
  } else {
    return xsimd::swizzle(batch, mask);
  }
}

template <typename Int, typename Arch, Int... kShifts>
constexpr auto make_mult(xsimd::batch_constant<Int, Arch, kShifts...>) {
  return xsimd::batch_constant<Int, Arch, static_cast<Int>(1u << kShifts)...>();
}

template <typename Int, int kOffset, int kLength, Int... kVals>
struct SelectStride {
  static constexpr auto kShiftsArr = std::array{kVals...};

  static constexpr Int get(int i, int n) { return kShiftsArr[kLength * i + kOffset]; }
};

template <typename ToInt, int kOffset, typename Int, typename Arch, Int... kShifts>
constexpr auto select_stride(xsimd::batch_constant<Int, Arch, kShifts...>) {
  return xsimd::make_batch_constant<
      ToInt, Arch, SelectStride<Int, kOffset, sizeof(ToInt) / sizeof(Int), kShifts...>>();
}

// Fallback for variable shift left.
//
// We replace the variable left shift by a variable multiply with a power of two.
//
// This trick is borrowed from Daniel Lemire and Leonid Boytsov, Decoding billions of
// integers per second through vectorization, Software Practice & Experience 45 (1), 2015.
// http://arxiv.org/abs/1209.2137
template <typename Arch, typename Int, Int... kShifts>
auto left_shift(const xsimd::batch<Int, Arch>& batch,
                xsimd::batch_constant<Int, Arch, kShifts...> shifts)
    -> xsimd::batch<Int, Arch> {
  constexpr bool kHasSse2 = std::is_base_of_v<xsimd::sse2, Arch>;
  constexpr bool kHasAvx2 = std::is_base_of_v<xsimd::avx2, Arch>;
  static_assert(!(kHasSse2 && kHasAvx2), "The hierarchy are different in xsimd");

  // TODO(xsimd-14) this can be simplified to
  // constexpr auto kMults = xsimd::make_batch_constant<Int, 1, Arch>() << shits;
  constexpr auto kMults = make_mult(shifts);

  constexpr auto IntSize = sizeof(Int);

  // Sizes and architecture for which there is no variable left shift and there is a
  // multiplication
  if constexpr (                                                                  //
      (kHasSse2 && (IntSize == sizeof(uint16_t) || IntSize == sizeof(uint32_t)))  //
      || (kHasAvx2 && (IntSize == sizeof(uint16_t)))                              //
  ) {
    return batch * kMults;
  }

  // Architecture for which there is no variable left shift on uint8_t but a fallback
  // exists for uint16_t.
  if constexpr ((kHasSse2 || kHasAvx2) && (IntSize == sizeof(uint8_t))) {
    const auto batch16 = xsimd::bitwise_cast<uint16_t>(batch);

    constexpr auto kShifts0 = select_stride<uint16_t, 0>(shifts);
    const auto shifted0 = left_shift(batch16, kShifts0) & 0x00FF;

    constexpr auto kShifts1 = select_stride<uint16_t, 1>(shifts);
    const auto shifted1 = left_shift(batch16 & 0xFF00, kShifts1);

    return xsimd::bitwise_cast<Int>(shifted0 | shifted1);
  }

  return batch << shifts;
}

// Fallback for variable shift right.
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
auto right_shift_by_excess(const xsimd::batch<Int, Arch>& batch,
                           xsimd::batch_constant<Int, Arch, kShifts...> shifts) {
  constexpr bool kHasSse2 = std::is_base_of_v<xsimd::sse2, Arch>;
  constexpr bool kHasAvx2 = std::is_base_of_v<xsimd::avx2, Arch>;
  static_assert(!(kHasSse2 && kHasAvx2), "The hierarchy are different in xsimd");

  constexpr auto IntSize = sizeof(Int);

  // Architecture for which there is no variable right shift but a larger fallback exists.
  if constexpr (kHasAvx2 && (IntSize == sizeof(uint8_t) || IntSize == sizeof(uint16_t))) {
    using twice_uint = SizedUint<2 * IntSize>;

    const auto batch2 = xsimd::bitwise_cast<twice_uint>(batch);

    constexpr auto kShifts0 = select_stride<twice_uint, 0>(shifts);
    constexpr auto kMask0 = bit_util::LeastSignificantBitMask<twice_uint>(8 * IntSize);
    const auto shifted0 = right_shift_by_excess(batch2 & kMask0, kShifts0);

    constexpr auto kShifts1 = select_stride<twice_uint, 1>(shifts);
    constexpr auto kMask1 = kMask0 << (8 * IntSize);
    const auto shifted1 = right_shift_by_excess(batch2, kShifts1) & kMask1;

    return xsimd::bitwise_cast<Int>(shifted0 | shifted1);
  }

  // These conditions are the ones matched in `left_shift`, i.e. the ones where variable
  // shift right will not be available but a left shift (fallback) exists.
  if constexpr (kHasSse2 && (IntSize != sizeof(uint64_t))) {
    static constexpr auto kShiftsArr = std::array{kShifts...};
    static constexpr Int kMaxRightShift = max_value(kShiftsArr);

    struct MakeShifts {
      static constexpr Int get(int i, int n) { return kMaxRightShift - kShiftsArr.at(i); }
    };

    // TODO(xsimd-14) this can be simplified to
    // constexpr auto kRShifts = xsimd::make_batch_constant<Int, kMaxRightShift, Arch>() -
    // shifts;
    constexpr auto kLShifts = xsimd::make_batch_constant<Int, Arch, MakeShifts>();

    const auto lshifted = left_shift(batch, kLShifts);
    // TODO(xsimd-14)
    // return xsimd::bitwise_rshift<kMaxRightShift>(lshifted);
    return xsimd::batch<Int, Arch>(lshifted) >> kMaxRightShift;
  }

  return batch >> shifts;
}

// TODO(C++20) Non type template parameter for MediumKernelOptions.
template <typename UnpackedUint, int kPackedBitSize, int kSimdBitSize,
          int kUnpackedPerKernelLimit = 32>
struct MediumKernel {
  static constexpr auto kPlan = BuildMediumPlan<UnpackedUint, kPackedBitSize,
                                                kSimdBitSize, kUnpackedPerKernelLimit>();
  static constexpr auto kPlanSize = kPlan.kPlanSize;
  static constexpr auto kShape = kPlan.kShape;
  using Traits = typename decltype(kPlan)::Traits;
  using unpacked_type = typename Traits::unpacked_type;
  using uint_type = typename Traits::uint_type;
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
    const auto shifted = right_shift_by_excess(words, kRightShifts);
    const auto vals = shifted & kMask;
    if constexpr (std::is_same_v<unpacked_type, bool>) {
      const xsimd::batch_bool<uint_type, arch_type> bools(vals);
      bools.store_unaligned(out + kOutOffset);
    } else {
      vals.store_unaligned(out + kOutOffset);
    }
  }

  template <int kReadIdx, int kSwizzleIdx, int... kShiftIds>
  static void unpack_one_swizzle_impl(const simd_bytes& bytes, unpacked_type* out,
                                      std::integer_sequence<int, kShiftIds...>) {
    static constexpr auto kSwizzlesArr = kPlan.swizzles.at(kReadIdx).at(kSwizzleIdx);
    constexpr auto kSwizzles = make_batch_constant<kSwizzlesArr, arch_type>();

    const auto swizzled = swizzle_bytes(bytes, kSwizzles);
    const auto words = xsimd::bitwise_cast<uint_type>(swizzled);
    (unpack_one_shift_impl<kReadIdx, kSwizzleIdx, kShiftIds>(words, out), ...);
  }

  template <typename Uint>
  static simd_bytes load_bytes_as(const uint8_t* in) {
    const Uint val = util::SafeLoadAs<Uint>(in);
    const auto batch = xsimd::batch<Uint, arch_type>(val);
    return xsimd::bitwise_cast<uint8_t>(batch);
  }

  static simd_bytes load_bytes(const uint8_t* in) {
    if constexpr (kPlan.bytes_per_read() > static_cast<int>(sizeof(uint64_t))) {
      return simd_bytes::load_unaligned(in);
    } else if constexpr (kPlan.bytes_per_read() > static_cast<int>(sizeof(uint32_t))) {
      return load_bytes_as<uint64_t>(in);
    }
    return load_bytes_as<uint32_t>(in);
  }

  template <int kReadIdx, int... kSwizzleIds>
  static void unpack_one_read_impl(const uint8_t* in, unpacked_type* out,
                                   std::integer_sequence<int, kSwizzleIds...>) {
    using ShiftSeq = std::make_integer_sequence<int, kPlanSize.shifts_per_swizzle()>;
    const auto bytes = load_bytes(in + kPlan.reads.at(kReadIdx));
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
  using uint_type = typename Traits::uint_type;
  static constexpr auto kShape = Traits::kShape;

  static constexpr int kUnpackedPerkernel = std::lcm(kShape.unpacked_per_simd(), 8);
  static constexpr int kReadsPerKernel = static_cast<int>(bit_util::CeilDiv(
      kUnpackedPerkernel * kShape.packed_bit_size(), kShape.simd_bit_size()));

  using ReadsPerKernel = std::array<int, kReadsPerKernel>;

  using Swizzle = std::array<uint8_t, kShape.simd_byte_size()>;
  using SwizzlesPerKernel = std::array<Swizzle, kReadsPerKernel>;

  using Shift = std::array<uint_type, kShape.unpacked_per_simd()>;
  using ShitsPerKernel = std::array<Shift, kReadsPerKernel>;

  ReadsPerKernel reads;
  SwizzlesPerKernel low_swizzles;
  SwizzlesPerKernel high_swizzles;
  ShitsPerKernel low_rshifts;
  ShitsPerKernel high_lshifts;
  uint_type mask;
};

template <typename UnpackedUint, int kPackedBitSize, int kSimdBitSize>
constexpr LargeKernelPlan<UnpackedUint, kPackedBitSize, kSimdBitSize> BuildLargePlan() {
  using Plan = LargeKernelPlan<UnpackedUint, kPackedBitSize, kSimdBitSize>;
  using uint_type = typename Plan::Traits::uint_type;
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

  plan.mask = bit_util::LeastSignificantBitMask<uint_type>(kPackedBitSize);

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

    const auto low_swizzled = swizzle_bytes(bytes, kLowSwizzles);
    const auto low_words = xsimd::bitwise_cast<unpacked_type>(low_swizzled);
    simd_batch low_shifted;
    if constexpr (kShape.unpacked_byte_size() == 1) {
      // The logic of the fallback in right_shift_by_excess does not work for this single
      // byte case case, so we use directly xsimd and its scalar fallback.
      low_shifted = low_words >> kLowRShifts;
    } else {
      low_shifted = right_shift_by_excess(low_words, kLowRShifts);
    }

    const auto high_swizzled = swizzle_bytes(bytes, kHighSwizzles);
    const auto high_words = xsimd::bitwise_cast<unpacked_type>(high_swizzled);
    const auto high_shifted = left_shift(high_words, kHighLShifts);

    // We can have a single mask and apply it after OR because the shifts will ensure that
    // there are zeros where the high/low values are incomplete.
    const auto vals = (low_shifted | high_shifted) & kPlan.mask;
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

/// A Kernel that does not extract anything, leaving all work to the naive implementation.
template <typename UnpackedUint, int kPackedBitSize, int kSimdBitSize>
struct NoOpKernel {
  using unpacked_type = UnpackedUint;

  static constexpr int kValuesUnpacked = 0;

  static const uint8_t* unpack(const uint8_t* in, unpacked_type* out) { return in; }
};

template <typename UnpackedUint, int kPackedBitSize, int kSimdBitSize,
          typename WorkingKernel>
struct ForwardToKernel : WorkingKernel {
  using unpacked_type = UnpackedUint;

  static constexpr int kValuesUnpacked = WorkingKernel::kValuesUnpacked;

  static const uint8_t* unpack(const uint8_t* in, unpacked_type* out) {
    using working_type = typename WorkingKernel::unpacked_type;

    working_type buffer[kValuesUnpacked] = {};
    in = WorkingKernel::unpack(in, buffer);
    for (int k = 0; k < kValuesUnpacked; ++k) {
      out[k] = static_cast<unpacked_type>(buffer[k]);
    }
    return in;
  }
};

/*******************************
 *  Kernel static dispatching  *
 *******************************/

/// The public kernel exposed for any size.
template <typename UnpackedUint, int kPackedBitSize, int kSimdBitSize, typename = void>
struct Kernel;

template <typename UnpackedUint, int kPackedBitSize, int kSimdBitSize>
struct Kernel<  //
    UnpackedUint, kPackedBitSize, kSimdBitSize,
    std::enable_if_t<
        KernelTraits<UnpackedUint, kPackedBitSize, kSimdBitSize>::kShape.is_medium()>>
    : MediumKernel<UnpackedUint, kPackedBitSize, kSimdBitSize> {};

template <typename UnpackedUint, int kPackedBitSize, int kSimdBitSize>
struct Kernel<  //
    UnpackedUint, kPackedBitSize, kSimdBitSize,
    std::enable_if_t<
        KernelTraits<UnpackedUint, kPackedBitSize, kSimdBitSize>::kShape.is_large()>>
    : LargeKernel<UnpackedUint, kPackedBitSize, kSimdBitSize> {};

template <typename UnpackedUint, int kPackedBitSize, int kSimdBitSize>
struct Kernel<  //
    UnpackedUint, kPackedBitSize, kSimdBitSize,
    std::enable_if_t<
        KernelTraits<UnpackedUint, kPackedBitSize, kSimdBitSize>::kShape.is_oversized() &&
        sizeof(UnpackedUint) < sizeof(uint64_t)>>
    : ForwardToKernel<UnpackedUint, kPackedBitSize, kSimdBitSize,
                      MediumKernel<SizedUint<2 * sizeof(UnpackedUint)>, kPackedBitSize,
                                   kSimdBitSize>> {};

template <typename UnpackedUint, int kPackedBitSize, int kSimdBitSize>
struct Kernel<  //
    UnpackedUint, kPackedBitSize, kSimdBitSize,
    std::enable_if_t<
        KernelTraits<UnpackedUint, kPackedBitSize, kSimdBitSize>::kShape.is_oversized() &&
        sizeof(UnpackedUint) == sizeof(uint64_t)>>
    : NoOpKernel<UnpackedUint, kPackedBitSize, kSimdBitSize> {};

}  // namespace arrow::internal
