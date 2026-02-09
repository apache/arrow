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

/// Simd integer unpacking kernels, that is small functions that efficiently operate over
/// a fixed input size.
///
/// This is a generalization of the algorithm from Daniel Lemire and Leonid Boytsov,
/// Decoding billions of integers per second through vectorization, Software Practice &
/// Experience 45 (1), 2015.
/// http://arxiv.org/abs/1209.2137
/// https://github.com/fast-pack/LittleIntPacker/blob/master/src/horizontalpacking32.c

#pragma once

#include <array>
#include <concepts>
#include <cstdint>
#include <cstring>
#include <numeric>
#include <utility>

#include <xsimd/xsimd.hpp>

#include "arrow/util/bit_util.h"
#include "arrow/util/bpacking_dispatch_internal.h"
#include "arrow/util/type_traits.h"

namespace arrow::internal::bpacking {

template <typename T, std::size_t N>
constexpr std::array<T, N> BuildConstantArray(T val) {
  std::array<T, N> out = {};
  for (auto& v : out) {
    v = val;
  }
  return out;
}

template <typename Arr>
constexpr Arr BuildConstantArrayLike(typename Arr::value_type val) {
  return BuildConstantArray<typename Arr::value_type, std::tuple_size_v<Arr>>(val);
}

/*********************
 *  xsimd utilities  *
 *********************/

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

template <std::array kArr, typename Arch, std::size_t... Is>
constexpr auto array_to_batch_constant_impl(std::index_sequence<Is...>) {
  using Array = std::decay_t<decltype(kArr)>;
  using value_type = Array::value_type;

  return xsimd::batch_constant<value_type, Arch, kArr[Is]...>{};
}

/// Make a ``xsimd::batch_constant`` from a static constexpr array.
template <std::array kArr, typename Arch>
constexpr auto array_to_batch_constant() {
  return array_to_batch_constant_impl<kArr, Arch>(
      std::make_index_sequence<kArr.size()>());
}

template <std::unsigned_integral Uint, typename Arch>
xsimd::batch<uint8_t, Arch> load_val_as(const uint8_t* in) {
  const Uint val = util::SafeLoadAs<Uint>(in);
  const auto batch = xsimd::batch<Uint, Arch>(val);
  return xsimd::bitwise_cast<uint8_t>(batch);
}

template <int kBytes, typename Arch>
xsimd::batch<uint8_t, Arch> safe_load_bytes(const uint8_t* in) {
  if constexpr (kBytes <= sizeof(uint64_t)) {
    return load_val_as<SizedUint<kBytes>, Arch>(in);
  }
  using simd_bytes = xsimd::batch<uint8_t, Arch>;
  return simd_bytes::load_unaligned(in);
}

template <std::integral Int, int kOffset, int kLength, typename Arr>
constexpr auto select_stride_impl(Arr shifts) {
  std::array<Int, shifts.size() / kLength> out{};
  for (std::size_t i = 0; i < out.size(); ++i) {
    out[i] = shifts[kLength * i + kOffset];
  }
  return out;
}

/// Select indices in a batch constant according to a given stride.
///
/// Given a ``xsimd::batch_constant`` over a given integer type ``Int``, return a new
/// batch constant over a larger integer type ``ToInt`` that select one of every "stride"
/// element from the input.
/// Because batch constants can only contain a predetermined number of values, the
/// "stride" is determined as the ratio of the sizes of the output and input integer
/// types.
/// The ``kOffset`` is used to determine which sequence of values picked by the stride.
/// Equivalently, it is the index of the first value selected.
///
/// For instance given an input with the following values
///         |0|1|2|3|4|5|6|7|
/// and a stride of 2 (e.g. sizeof(uint16_t)/sizeof(uint8_t)), an offset of 0 would
/// return the values:
///         |0|2|4|6|
/// while an offset of 1 would return the values:
///         |1|3|5|7|
template <typename ToInt, int kOffset, typename Int, typename Arch, Int... kShifts>
constexpr auto select_stride(xsimd::batch_constant<Int, Arch, kShifts...>) {
  constexpr auto kStridesArr =
      select_stride_impl<ToInt, kOffset, sizeof(ToInt) / sizeof(Int)>(
          std::array{kShifts...});
  return array_to_batch_constant<kStridesArr, Arch>();
}

template <typename Arch>
constexpr bool HasSse2 = std::is_base_of_v<xsimd::sse2, Arch>;

template <typename Arch>
constexpr bool HasAvx2 = std::is_base_of_v<xsimd::avx2, Arch>;

/// Wrapper around ``xsimd::bitwise_lshift`` with optimizations for non implemented sizes.
//
// We replace the variable left shift by a variable multiply with a power of two.
//
// This trick is borrowed from Daniel Lemire and Leonid Boytsov, Decoding billions of
// integers per second through vectorization, Software Practice & Experience 45 (1), 2015.
// http://arxiv.org/abs/1209.2137
//
/// TODO(xsimd) Tracking in https://github.com/xtensor-stack/xsimd/pull/1220
template <typename Arch, typename Int, Int... kShifts>
auto left_shift(const xsimd::batch<Int, Arch>& batch,
                xsimd::batch_constant<Int, Arch, kShifts...> shifts)
    -> xsimd::batch<Int, Arch> {
  constexpr bool kHasSse2 = HasSse2<Arch>;
  constexpr bool kHasAvx2 = HasAvx2<Arch>;
  static_assert(
      !(kHasSse2 && kHasAvx2),
      "In xsimd, an x86 arch is either part of the SSE family or of the AVX family,"
      "not both. If this check fails, it means the assumptions made here to detect SSE "
      "and AVX are out of date.");

  constexpr auto kMults = xsimd::make_batch_constant<Int, 1, Arch>() << shifts;

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

/// Fallback for variable shift right.
///
/// When we know that the relevant bits will not overflow, we can instead shift left all
/// values to align them with the one with the largest right shifts followed by a constant
/// shift on all values.
/// In doing so, we replace the variable left shift by a variable multiply with a power of
/// two.
///
/// This trick is borrowed from Daniel Lemire and Leonid Boytsov, Decoding billions of
/// integers per second through vectorization, Software Practice & Experience 45 (1),
/// 2015. http://arxiv.org/abs/1209.2137
template <typename Arch, typename Int, Int... kShifts>
auto right_shift_by_excess(const xsimd::batch<Int, Arch>& batch,
                           xsimd::batch_constant<Int, Arch, kShifts...> shifts) {
  constexpr bool kHasSse2 = HasSse2<Arch>;
  constexpr bool kHasAvx2 = HasAvx2<Arch>;
  static_assert(!(kHasSse2 && kHasAvx2), "The hierarchy are different in xsimd");

  constexpr auto IntSize = sizeof(Int);

  // Architecture for which there is no variable right shift but a larger fallback exists.
  /// TODO(xsimd) Tracking for Avx2 in https://github.com/xtensor-stack/xsimd/pull/1220
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
    constexpr Int kMaxRShift = max_value(std::array{kShifts...});

    constexpr auto kLShifts =
        xsimd::make_batch_constant<Int, kMaxRShift, Arch>() - shifts;

    return xsimd::bitwise_rshift<kMaxRShift>(left_shift(batch, kLShifts));
  }

  return batch >> shifts;
}

/****************************
 *  Properties of a kernel  *
 ****************************/

/// Compute whether packed values may overflow SIMD register.
///
/// Due to packing misalignment, we may not be able to fit in a SIMD register as many
/// packed values as unpacked values.
/// This happens because of the packing bit alignment creates spare bits we are forced to
/// read when reading whole bytes. This is mostly known to be the case with 63 bits values
/// in a 128 bit SIMD register.
///
/// @see KernelShape
/// @see PackedMaxSpreadBytes
constexpr bool PackedIsOversizedForSimd(int simd_bit_size, int unpacked_bit_size,
                                        int packed_bit_size) {
  const int unpacked_per_simd = simd_bit_size / unpacked_bit_size;

  const auto packed_per_read_for_offset = [&](int bit_offset) -> int {
    return (simd_bit_size - bit_offset) / packed_bit_size;
  };

  int packed_start_bit = 0;
  // Try all possible bit offsets for this packed size
  do {
    int packed_per_read = packed_per_read_for_offset(packed_start_bit % 8);
    if (packed_per_read < unpacked_per_simd) {
      // A single SIMD batch isn't enough to unpack from this bit offset,
      // so the "medium" model cannot work.
      return true;
    }
    packed_start_bit += unpacked_per_simd * packed_bit_size;
  } while (packed_start_bit % 8 != 0);

  return false;
}

/// Different sizes of a given kernel.
///
/// When integers are bit-packed, they can spread over multiple bytes.
/// For instance, integers packed over three bits quickly spread over two bytes (on the
/// value `C` below) despite three bits being much smaller than a single byte.
///
/// ```
/// |A|A|A|B|B|B|C|C|  |C|D|D|D|E|E|E|F| ...
/// ```
///
/// When the spread is smaller or equal to the unsigned integer to unpack to, we classify
/// if as "medium".
///
/// When it is strictly larger, it classifies as "large".
///
/// On rare occasions, due to offsets in reading a subsequent batch, we may not even be
/// able to read as many packed values as we should extract in a batch. We classify this
/// as "oversized". For instance unpacking 63 bits values to uint64_t on 128 bits SIMD
/// register (possibly the only such case), we need to write 2 uint64_t on each output
/// register that we flush to memory. The first two value spread over bits [0, 126[,
/// fitting in 16 bytes (the full SIMD register). However, the next two values on input
/// bits [126, 252[, are not byte aligned and would require reading the whole [120, 256[
/// range, or 17 bytes. We would need to read more than one SIMD register to create
/// a single output register with two values. Such pathological cases are not handled with
/// SIMD but rather fully falling back to the exact scalar loop.
///
/// There is currently no "small" classification, as medium kernels can handle even 1-bit
/// packed values. Future work may investigate a dedicated kernel for when a single byte
/// can fit more values than is being unpack in a single register. For instance, unpacking
/// 1 bit values to uint32_t on 128 bits SIMD register, we can fit 8 values in a byte, or
/// twice the number of uint32_t that fit in an SIMD register. This can be use to further
/// reduce the number of swizzles done in a kernel run. Other optimizations of interest
/// for small input sizes on BMI2 is to use ``_pdep_u64`` directly to perform unpacking.
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

/// Packing all useful and derived information about a kernel in a single type.
template <std::unsigned_integral UnpackedUint, int kPackedBitSize, int kSimdBitSize>
struct KernelTraits {
  static constexpr KernelShape kShape = {
      .simd_bit_size_ = kSimdBitSize,
      .unpacked_bit_size_ = 8 * sizeof(UnpackedUint),
      .packed_bit_size_ = kPackedBitSize,
  };

  static_assert(kShape.simd_bit_size() % kShape.unpacked_bit_size() == 0);
  static_assert(0 < kShape.packed_bit_size());
  static_assert(kShape.packed_bit_size() < kShape.simd_bit_size());

  using unpacked_type = UnpackedUint;
  /// The integer type to work with, `unpacked_type` or an appropriate type for bool.
  using uint_type = std::conditional_t<std::is_same_v<unpacked_type, bool>,
                                       SizedUint<sizeof(bool)>, unpacked_type>;
  using simd_batch = xsimd::make_sized_batch_t<uint_type, kShape.unpacked_per_simd()>;
  using simd_bytes = xsimd::make_sized_batch_t<uint8_t, kShape.simd_byte_size()>;
  using arch_type = simd_batch::arch_type;
};

/// Return similar kernel traits but with a different integer unpacking type.
template <typename KerTraits, std::unsigned_integral Uint>
using KernelTraitsWithUnpackUint = KernelTraits<Uint, KerTraits::kShape.packed_bit_size(),
                                                KerTraits::kShape.simd_bit_size()>;

/******************
 *  MediumKernel  *
 ******************/

/// Compile time options of a Medium kernel.
struct MediumKernelOptions {
  /// An indicative limit on the number of values unpacked by the kernel.
  /// This is a heuristic setting: other constraints such as alignment may not always make
  /// small values feasibles. Must be a power of two.
  int unpacked_per_kernel_limit_;
};

/// Compile time dimensions of a Medium kernel.
///
/// @see MediumKernel for explanation of the algorithm.
struct MediumKernelPlanSize {
  int reads_per_kernel_;
  int swizzles_per_read_;
  int shifts_per_swizzle_;

  constexpr static MediumKernelPlanSize Build(const KernelShape& shape,
                                              const MediumKernelOptions& options);

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

constexpr MediumKernelPlanSize MediumKernelPlanSize::Build(
    const KernelShape& shape, const MediumKernelOptions& options) {
  const int shifts_per_swizzle =
      shape.unpacked_byte_size() / shape.packed_max_spread_bytes();

  const int vals_per_swizzle = shifts_per_swizzle * shape.unpacked_per_simd();

  // Using `unpacked_per_kernel_limit` to influence the number of swizzles per reads.
  const auto packed_per_read_for_offset = [&](int bit_offset) -> int {
    const int best = (shape.simd_bit_size() - bit_offset) / shape.packed_bit_size();
    const int limit = options.unpacked_per_kernel_limit_;
    return (best > limit) && (limit > 0) ? limit : best;
  };

  const auto swizzles_per_read_for_offset = [&](int bit_offset) -> int {
    return packed_per_read_for_offset(bit_offset) / vals_per_swizzle;
  };

  // If after a whole swizzle reading iteration we fall byte-unaligned, the remaining
  // iterations will start with an byte-unaligned first value, reducing the effective
  // capacity of the SIMD batch.
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
      .reads_per_kernel_ = reads_per_kernel,
      .swizzles_per_read_ = swizzles_per_read,
      .shifts_per_swizzle_ = shifts_per_swizzle,
  };
}

/// Utility to map a desired number of bits to the smallest feasible size.
///
/// When needing to read @p bits_per_read, find the next possible size that can be read in
/// one operation. This is used  to adjust the number of input bytes to kernel reads.
/// For instance if a kernel plan needs to read 47 bits, instead of reading a whole SIMD
/// register of data (which would restrict it to larger input memory buffers), this
/// function advise kernel plans to read only read 64 bits.
/// This limits restrictions set by the plan on the input memory reads built to avoid
/// reading overflow.
constexpr int adjust_bytes_per_read(int bits_per_read, int simd_byte_size) {
  if (bits_per_read <= static_cast<int>(8 * sizeof(uint32_t))) {
    return sizeof(uint32_t);
  } else if (bits_per_read <= static_cast<int>(8 * sizeof(uint64_t))) {
    return sizeof(uint64_t);
  }
  return simd_byte_size;
}

/// Compile time constants of a Medium kernel.
///
/// This contains most of the information about the medium kernel algorithm stored
/// as index offsets and other constants (shift amounts...).
///
/// @see MediumKernel for an explanation of the algorithm.
template <typename KerTraits, MediumKernelOptions kOptions>
struct MediumKernelPlan {
  using Traits = KerTraits;
  using uint_type = Traits::uint_type;
  static constexpr auto kShape = Traits::kShape;
  static constexpr auto kPlanSize = MediumKernelPlanSize::Build(kShape, kOptions);

  /// Array of byte offsets relative to the Kernel input memory address.
  using ReadsPerKernel = std::array<int, kPlanSize.reads_per_kernel()>;

  /// Array of indices used for byte swizzle/shuffle in an SIMD register.
  using Swizzle = std::array<uint8_t, kShape.simd_byte_size()>;
  using SwizzlesPerRead = std::array<Swizzle, kPlanSize.swizzles_per_read()>;
  using SwizzlesPerKernel = std::array<SwizzlesPerRead, kPlanSize.reads_per_kernel()>;

  /// Array of integer shifts for each unpacked integer in an SIMD register.
  using Shifts = std::array<uint_type, kShape.unpacked_per_simd()>;
  using ShiftsPerSwizzle = std::array<Shifts, kPlanSize.shifts_per_swizzle()>;
  using ShiftsPerRead = std::array<ShiftsPerSwizzle, kPlanSize.swizzles_per_read()>;
  using ShiftsPerKernel = std::array<ShiftsPerRead, kPlanSize.reads_per_kernel()>;

  constexpr static MediumKernelPlan Build();

  static constexpr int unpacked_per_shift() { return kShape.unpacked_per_simd(); }
  static constexpr int unpacked_per_swizzle() {
    return unpacked_per_shift() * kPlanSize.shifts_per_swizzle();
  }
  static constexpr int unpacked_per_read() {
    return unpacked_per_swizzle() * kPlanSize.swizzles_per_read();
  }
  static constexpr int unpacked_per_kernel() {
    return unpacked_per_read() * kPlanSize.reads_per_kernel();
  }

  static constexpr int bytes_per_read() {
    const auto bits_per_read = unpacked_per_read() * kShape.packed_bit_size();
    return adjust_bytes_per_read(bits_per_read, kShape.simd_byte_size());
  }

  constexpr int total_bytes_read() const { return reads.back() + bytes_per_read(); }

  ReadsPerKernel reads;
  SwizzlesPerKernel swizzles;
  ShiftsPerKernel shifts;
  uint_type mask = bit_util::LeastSignificantBitMask<uint_type>(kShape.packed_bit_size());
};

template <typename KerTraits, MediumKernelOptions kOptions>
constexpr auto MediumKernelPlan<KerTraits, kOptions>::Build()
    -> MediumKernelPlan<KerTraits, kOptions> {
  using Plan = MediumKernelPlan<KerTraits, kOptions>;
  constexpr auto kShape = Plan::kShape;
  constexpr auto kPlanSize = Plan::kPlanSize;
  static_assert(kShape.is_medium());

  Plan plan = {};

  int packed_start_bit = 0;
  for (int r = 0; r < kPlanSize.reads_per_kernel(); ++r) {
    const int read_start_byte = packed_start_bit / 8;
    plan.reads.at(r) = read_start_byte;

    for (int sw = 0; sw < kPlanSize.swizzles_per_read(); ++sw) {
      // Not all swizzle values are defined. This is not an issue as these bits do not
      // influence the input (they will be masked away).
      constexpr int kUndefined = -1;
      plan.swizzles.at(r).at(sw) = BuildConstantArrayLike<Plan::Swizzle>(kUndefined);
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

/// Unpack a fixed number of packed integer using SIMD operations.
///
/// This is only valid for medium sized inputs where packed values spread over fewer bytes
/// than the unpacked integer size.
/// All dimensions and constants are computed at compile time to reduce run-time
/// computations and improve xsimd fallback possibilities (with
/// ``xsimd::batch_constant``).
///
/// Let us give an example of the algorithm for values packed over 3 bits, extracted to
/// uint32_t, using a fictive 64 bits SIMD size.
/// The input register is as follow, where 3 consecutive letter represents the 3 bits of a
/// packed value in little endian representation.
///
///     |AAABBBCC|CDDDEEEF|FFGGGHHH|IIIJJJKK||KLLLMMMN|NNOOOPPP|QQQRRRSS|STTTUUUV|
///
/// The algorithm generates SIMD registers of unpacked values ready to be written to
/// memory. Here we can fit 64/32 = 2 unpacked values per output register.
/// The first step is to put the values in the 32 bit slots corresponding to the unpacked
/// values using a swizzle (shuffle/indexing) operation. The spread for 3 bit value is of
/// 2 bytes (at worst bit alignment) so we move 2 bytes at the time.
/// We care about creating a register as follows:
///
///     |   ~ Val AAA anywhere here         ||   ~ Val BBB anywhere here         |
///
/// Since we have unused bytes in this register, we will also use them to fit the next
/// values:
///
///     | ~ Val AAA       | ~ Val CCC       || ~ Val BBB       | ~ Val DDD       |
///
/// Resulting in the following swizzled register.
///
///     |AAABBBCC|CDDDEEEF|AAABBBCC|CDDDEEEF||AAABBBCC|CDDDEEEF|CDDDEEEF|FFGGGHHH|
///
/// Now we will apply shift and mask operations to properly align the values.
/// Shifting has a different value for each uint32_t in the SIMD register.
/// Note that because of the figure is little endian, right shift operator >> actually
/// shifts the bits representation, to the left.
///
///     | Align AAA with >> 0               || Align BBB with >> 3               |
///     |AAABBBCC|CDDDEEEF|AAABBBCC|CDDDEEEF||BBBCCCDD|DEEEFCDD|DEEEFFFG|GGHHH000|
///
/// Masking (bitewise AND) uses the same mask for all uint32_t
///
///     |11100000000000000000000000000000000||11100000000000000000000000000000000|
///   & |AAABBBCC|CDDDEEEF|AAABBBCC|CDDDEEEF||BBBCCCDD|DEEEFCDD|DEEEFFFG|GGHHH000|
///   = |AAA00000|00000000|00000000|00000000||BBB00000|00000000|00000000|00000000|
///
/// We have created a SIMD register with the first two unpacked values A and B that we can
/// write to memory.
/// Now we can do the same with C and D without having to go to another swizzle (doing
/// multiple shifts per swizzle).
/// The next value to unpack is E, which starts on bit 12 (not byte aligned).
/// We therefore cannot end here and reapply the same kernel on the same input.
/// Instead, we run another similar iteration to unpack E, F, G, H, this time taking into
/// account that we have an extra offset of 12 bits (this will change all the constants
/// used for swizzle and shift).
///
/// Note that in this example, we could have swizzled more than two values in each slot.
/// In practice, there may be situations where a more complex algorithm could fit more
/// shifts per swizzles, but that tend to not be the case when the SIMD register size
/// increases.
///
/// @see KernelShape
/// @see MediumKernelPlan
template <typename KerTraits, MediumKernelOptions kOptions>
struct MediumKernel {
  static constexpr auto kPlan = MediumKernelPlan<KerTraits, kOptions>::Build();
  static constexpr auto kPlanSize = kPlan.kPlanSize;
  static constexpr auto kShape = kPlan.kShape;
  using Traits = decltype(kPlan)::Traits;
  using unpacked_type = Traits::unpacked_type;
  using uint_type = Traits::uint_type;
  using simd_batch = Traits::simd_batch;
  using simd_bytes = Traits::simd_bytes;
  using arch_type = Traits::arch_type;

  static constexpr int kValuesUnpacked = kPlan.unpacked_per_kernel();
  static constexpr int kBytesRead = kPlan.total_bytes_read();

  template <int kReadIdx, int kSwizzleIdx, int kShiftIdx>
  static void unpack_one_shift_impl(const simd_batch& words, unpacked_type* out) {
    constexpr auto kRightShiftsArr =
        kPlan.shifts.at(kReadIdx).at(kSwizzleIdx).at(kShiftIdx);
    constexpr auto kRightShifts = array_to_batch_constant<kRightShiftsArr, arch_type>();
    constexpr auto kMask = kPlan.mask;
    constexpr auto kOutOffset = (kReadIdx * kPlan.unpacked_per_read() +
                                 kSwizzleIdx * kPlan.unpacked_per_swizzle() +
                                 kShiftIdx * kPlan.unpacked_per_shift());

    // Intel x86-64 does not have variable right shifts before AVX2.
    // We know the packed value can safely be left shifted up to the largest offset so we
    // can use the fallback on these platforms.
    const auto shifted = right_shift_by_excess(words, kRightShifts);
    const auto vals = shifted & kMask;
    if constexpr (std::is_same_v<unpacked_type, bool>) {
      const xsimd::batch_bool<uint_type, arch_type> bools = vals != 0;
      bools.store_unaligned(out + kOutOffset);
    } else {
      vals.store_unaligned(out + kOutOffset);
    }
  }

  template <int kReadIdx, int kSwizzleIdx, int... kShiftIds>
  static void unpack_one_swizzle_impl(const simd_bytes& bytes, unpacked_type* out,
                                      std::integer_sequence<int, kShiftIds...>) {
    constexpr auto kSwizzlesArr = kPlan.swizzles.at(kReadIdx).at(kSwizzleIdx);
    constexpr auto kSwizzles = array_to_batch_constant<kSwizzlesArr, arch_type>();

    const auto swizzled = xsimd::swizzle(bytes, kSwizzles);
    const auto words = xsimd::bitwise_cast<uint_type>(swizzled);
    (unpack_one_shift_impl<kReadIdx, kSwizzleIdx, kShiftIds>(words, out), ...);
  }

  template <int kReadIdx, int... kSwizzleIds>
  static void unpack_one_read_impl(const uint8_t* in, unpacked_type* out,
                                   std::integer_sequence<int, kSwizzleIds...>) {
    using ShiftSeq = std::make_integer_sequence<int, kPlanSize.shifts_per_swizzle()>;
    const auto bytes =
        safe_load_bytes<kPlan.bytes_per_read(), arch_type>(in + kPlan.reads.at(kReadIdx));
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

/*****************
 *  LargeKernel  *
 *****************/

/// Compile time dimensions of a Large kernel.
///
/// @see LargeKernel for explanation of the algorithm.
struct LargeKernelPlanSize {
  int reads_per_kernel_;
  int unpacked_per_kernel_;

  constexpr static LargeKernelPlanSize Build(const KernelShape& shape) {
    const auto unpacked_per_kernel = std::lcm(shape.unpacked_per_simd(), 8);
    const auto packed_bits_per_kernel = unpacked_per_kernel * shape.packed_bit_size();
    const auto reads_per_kernel =
        bit_util::CeilDiv(packed_bits_per_kernel, shape.simd_bit_size());

    return {
        .reads_per_kernel_ = static_cast<int>(reads_per_kernel),
        .unpacked_per_kernel_ = unpacked_per_kernel,
    };
  }

  constexpr int reads_per_kernel() const { return reads_per_kernel_; }

  constexpr int unpacked_per_read() const {
    return unpacked_per_kernel_ / reads_per_kernel_;
  }
  constexpr int unpacked_per_kernel() const { return unpacked_per_kernel_; }
};

/// Compile time constants of a Large kernel.
///
/// This contains most of the information about the large kernel algorithm stored
/// as index offsets and other constants (shift amounts...).
///
/// @see LargeKernel for an explanation of the algorithm.
template <typename KerTraits>
struct LargeKernelPlan {
  using Traits = KerTraits;
  using uint_type = Traits::uint_type;
  static constexpr auto kShape = Traits::kShape;
  static constexpr auto kPlanSize = LargeKernelPlanSize::Build(kShape);

  /// Array of byte offsets relative to the Kernel input memory address.
  using ReadsPerKernel = std::array<int, kPlanSize.reads_per_kernel()>;

  /// Array of indices used for byte swizzle/shuffle in an SIMD register.
  using Swizzle = std::array<uint8_t, kShape.simd_byte_size()>;
  using SwizzlesPerKernel = std::array<Swizzle, kPlanSize.reads_per_kernel()>;

  /// Array of integer shifts for each unpacked integer in an SIMD register.
  using Shifts = std::array<uint_type, kShape.unpacked_per_simd()>;
  using ShitsPerKernel = std::array<Shifts, kPlanSize.reads_per_kernel()>;

  static constexpr LargeKernelPlan Build();

  static constexpr int bytes_per_read() {
    const auto bits_per_read = kPlanSize.unpacked_per_read() * kShape.packed_bit_size();
    return adjust_bytes_per_read(bits_per_read, kShape.simd_byte_size());
  }

  constexpr int total_bytes_read() const { return reads.back() + bytes_per_read(); }

  ReadsPerKernel reads;
  SwizzlesPerKernel low_swizzles;
  SwizzlesPerKernel high_swizzles;
  ShitsPerKernel low_rshifts;
  ShitsPerKernel high_lshifts;
  uint_type mask;
};

template <typename KerTraits>
constexpr auto LargeKernelPlan<KerTraits>::Build() -> LargeKernelPlan<KerTraits> {
  using Plan = LargeKernelPlan<KerTraits>;
  using uint_type = Plan::Traits::uint_type;
  constexpr auto kShape = Plan::kShape;
  constexpr auto kPlanSize = Plan::kPlanSize;
  static_assert(kShape.is_large());
  constexpr int kOverBytes =
      kShape.packed_max_spread_bytes() - kShape.unpacked_byte_size();

  Plan plan = {};

  int packed_start_bit = 0;
  for (int r = 0; r < kPlanSize.reads_per_kernel(); ++r) {
    const int read_start_byte = packed_start_bit / 8;
    plan.reads.at(r) = read_start_byte;

    // Not all swizzle values are defined. This is not an issue as these bits do not
    // influence the input (they will be masked away).
    constexpr int kUndefined = -1;
    plan.low_swizzles.at(r) = BuildConstantArrayLike<Plan::Swizzle>(kUndefined);
    plan.high_swizzles.at(r) = BuildConstantArrayLike<Plan::Swizzle>(kUndefined);

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

      // low and high swizzles need to be rshifted but the oversized bytes create a
      // larger lshift for high values.
      plan.low_rshifts.at(r).at(u) = packed_start_bit % 8;
      plan.high_lshifts.at(r).at(u) = 8 * kOverBytes - (packed_start_bit % 8);

      packed_start_bit += kShape.packed_bit_size();
    }
  }

  plan.mask = bit_util::LeastSignificantBitMask<uint_type>(kShape.packed_bit_size());

  return plan;
}

/// Unpack a fixed number of packed integer using SIMD operations.
///
/// This is only valid for large sized inputs, where packed values spread over more bytes
/// than the unpacked integer size.
/// All dimensions and constants are computed at compile time to reduce run-time
/// computations and improve xsimd fallback possibilities (with
/// ``xsimd::batch_constant``).
///
/// Let us give an example of the algorithm for values packed over 13 bits, extracted to
/// uint16_t, using a fictive 32 bits SIMD size.
/// The input register is as follow, where 13 consecutive letters represent the 13 bits of
/// a packed value in little endian representation.
///
///         |AAAAAAAA|AAAAABBB||BBBBBBBB|BBCCCCCC|
///
/// The algorithm generates SIMD registers of unpacked values ready to be written to
/// memory. Here we can fit 32/16 = 2 unpacked values per output register.
/// Since 13-bit values can spread over 3 bytes (at worst bit alignment) but we're
/// unpacking to uint16_t (2 bytes).
///
/// The core idea is to perform two separate swizzles: one for the "low" bits and one
/// for the "high" bits of each value, then combine them with shifts and OR operations.
///
///         | Low bits from A || Low bits from B |
///   low:  |AAAAAAAA|AAAAABBB||AAAAABBB|BBBBBBBB|
///
///         | High bits frm A || High bits frm B |
///   high: |AAAAABBB|BBBBBBBB||BBBBBBBB|BBCCCCCC|
///
/// We then apply different shifts to align the values bits. The low values are
/// right-shifted by the bit offset, and the high values are left-shifted to fill the gap.
/// Note that because of the figure is little endian, right (>>) and left (<<) shift
/// operators actually shifts the bits representation in the oppostite directions.
///
///         | Align A w. >> 0 || Align B w. >> 5 |
///   low:  |AAAAAAAA|AAAAABBB||BBBBBBBBB|BB00000|
///
///         | Align A w. << 8 || Align B w. << 3 |
///   high: |00000000|AAAAABBB||000BBBBB|BBBBBCCC|
///
/// We then proceed to merge both inputs. The idea is to mask out irrelevant bits from the
/// high and low parts and merge with bitwise OR. However, in practice we can merge first
/// and apply the mask only once afterwards (because we shifted zeros in). Below is the
/// bitwise OR of both parts:
///
///         |AAAAAAAA|AAAAABBB||BBBBBBBBB|BB00000|
///       & |00000000|AAAAABBB||000BBBBB|BBBBBCCC|
///       = |AAAAAAAA|AAAAABBB||BBBBBBBB|BBBBBCCC|
///
/// Followed by a bitwise AND mask using the same values on both uint16_t:
///
///         |AAAAAAAA|AAAAABBB||BBBBBBBB|BBBBBCCC|
///       & |11111111|11111000||11111111|11111000|
///       = |AAAAAAAA|AAAAA000||BBBBBBBB|BBBBB000|
///
/// We have created a SIMD register with the two unpacked values A and B that we can
/// write to memory.
/// The next value to unpack is C, which starts on bit 26 (not byte aligned).
/// We therefore cannot end here and reapply the same kernel on the same input.
/// Instead, we run further similar iterations to unpack the next batch of values, this
/// time taking into account that we have an extra bit offset (this will change all the
/// constants used for swizzle and shift).
///
/// Note that in this example, the value A did not require the low/high merge operations.
/// However, because this is an SIMD algorithm we must apply the same operations to all
/// values, but we are also not paying an extra cost for doing so.
///
/// @see KernelShape
/// @see LargeKernelPlan
template <typename KerTraits>
struct LargeKernel {
  static constexpr auto kPlan = LargeKernelPlan<KerTraits>::Build();
  static constexpr auto kPlanSize = kPlan.kPlanSize;
  static constexpr auto kShape = kPlan.kShape;
  using Traits = typename decltype(kPlan)::Traits;
  using unpacked_type = Traits::unpacked_type;
  using simd_batch = Traits::simd_batch;
  using simd_bytes = Traits::simd_bytes;
  using arch_type = Traits::arch_type;

  static constexpr int kValuesUnpacked = kPlanSize.unpacked_per_kernel();
  static constexpr int kBytesRead = kPlan.total_bytes_read();

  template <int kReadIdx>
  static void unpack_one_read_impl(const uint8_t* in, unpacked_type* out) {
    constexpr auto kLowSwizzles =
        array_to_batch_constant<kPlan.low_swizzles.at(kReadIdx), arch_type>();
    constexpr auto kLowRShifts =
        array_to_batch_constant<kPlan.low_rshifts.at(kReadIdx), arch_type>();
    constexpr auto kHighSwizzles =
        array_to_batch_constant<kPlan.high_swizzles.at(kReadIdx), arch_type>();
    constexpr auto kHighLShifts =
        array_to_batch_constant<kPlan.high_lshifts.at(kReadIdx), arch_type>();

    const auto bytes =
        safe_load_bytes<kPlan.bytes_per_read(), arch_type>(in + kPlan.reads.at(kReadIdx));

    const auto low_swizzled = xsimd::swizzle(bytes, kLowSwizzles);
    const auto low_words = xsimd::bitwise_cast<unpacked_type>(low_swizzled);
    simd_batch low_shifted;
    if constexpr (kShape.unpacked_byte_size() == 1 && HasSse2<arch_type>) {
      // The logic of the fallback in right_shift_by_excess does not work for this single
      // byte case case, so we use directly xsimd and its scalar fallback.
      low_shifted = low_words >> kLowRShifts;
    } else {
      low_shifted = right_shift_by_excess(low_words, kLowRShifts);
    }

    const auto high_swizzled = xsimd::swizzle(bytes, kHighSwizzles);
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
    using ReadSeq = std::make_integer_sequence<int, kPlanSize.reads_per_kernel()>;
    unpack_all_impl(in, out, ReadSeq{});
    return in + (kPlan.kPlanSize.unpacked_per_kernel() * kShape.packed_bit_size()) / 8;
  }
};

/*********************
 *  Utility Kernels  *
 *********************/

/// A Kernel that does not extract anything, leaving all work to the naive implementation.
template <typename KernelTraits>
struct NoOpKernel {
  using unpacked_type = KernelTraits::unpacked_type;

  static constexpr int kValuesUnpacked = 0;
  static constexpr int kBytesRead = 0;

  static const uint8_t* unpack(const uint8_t* in, unpacked_type* out) { return in; }
};

template <typename KernelTraits, typename WorkingKernel>
struct ForwardToKernel : WorkingKernel {
  using unpacked_type = KernelTraits::unpacked_type;

  static constexpr int kValuesUnpacked = WorkingKernel::kValuesUnpacked;

  static const uint8_t* unpack(const uint8_t* in, unpacked_type* out) {
    using working_type = WorkingKernel::unpacked_type;

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

// Benchmarking show unpack to uint64_t is underperforming on SSE4.2 and Avx2
template <typename KerTraits, typename Arch = KerTraits::arch_type>
constexpr bool kMediumShouldUseUint32 =
    (HasSse2<Arch> || HasSse2<Arch>) &&  //
    (KerTraits::kShape.unpacked_byte_size() == sizeof(uint64_t)) &&
    (KerTraits::kShape.packed_bit_size() < 32) &&
    KernelTraitsWithUnpackUint<KerTraits, uint32_t>::kShape.is_medium();

// Benchmarking show large unpack to uint8_t is underperforming on SSE4.2
template <typename KerTraits, typename Arch = KerTraits::arch_type>
constexpr bool kLargeShouldUseUint16 =
    HasSse2<Arch> && (KerTraits::kShape.unpacked_byte_size() == sizeof(uint8_t));

// A ``std::enable_if`` that works on MSVC
template <typename KerTraits>
constexpr auto KernelDispatchImpl() {
  constexpr MediumKernelOptions kMedKernelOpts = {.unpacked_per_kernel_limit_ = 32};
  if constexpr (KerTraits::kShape.is_medium()) {
    if constexpr (kMediumShouldUseUint32<KerTraits>) {
      using Kernel32 =
          MediumKernel<KernelTraitsWithUnpackUint<KerTraits, uint32_t>, kMedKernelOpts>;
      return ForwardToKernel<KerTraits, Kernel32>{};
    } else {
      return MediumKernel<KerTraits, kMedKernelOpts>{};
    }
  } else if constexpr (KerTraits::kShape.is_large()) {
    if constexpr (kLargeShouldUseUint16<KerTraits>) {
      using Kernel16 =
          MediumKernel<KernelTraitsWithUnpackUint<KerTraits, uint16_t>, kMedKernelOpts>;
      return ForwardToKernel<KerTraits, Kernel16>{};
    } else {
      return LargeKernel<KerTraits>{};
    }
  } else if constexpr (KerTraits::kShape.is_oversized()) {
    return NoOpKernel<KerTraits>{};
  }
}

template <typename Traits>
using KernelDispatch = decltype(KernelDispatchImpl<Traits>());

/// The public kernel exposed for any size.
template <std::unsigned_integral UnpackedUint, int kPackedBitSize, int kSimdBitSize>
struct Kernel : KernelDispatch<KernelTraits<UnpackedUint, kPackedBitSize, kSimdBitSize>> {
};

}  // namespace arrow::internal::bpacking
