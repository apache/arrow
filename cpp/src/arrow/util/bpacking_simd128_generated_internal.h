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

// WARNING: this file is generated, DO NOT EDIT.
// Usage:
//   python cpp/src/arrow/util/bpacking_simd_codegen.py 128

#pragma once

#include <cstdint>
#include <cstring>

#include <xsimd/xsimd.hpp>

#include "arrow/util/bit_util.h"
#include "arrow/util/ubsan.h"

namespace arrow::internal {

// https://github.com/fast-pack/LittleIntPacker/blob/master/src/horizontalpacking32.c
// TODO
// - No zero and full size unpack here
// - _mm_cvtepi8_epi32
// - var rshifts no avail on SSE

template <typename Uint, int SimdByteSize, int BitWidth>
struct SimdUnpackerForWidthTraits {
  static constexpr int kOutByteSize = sizeof(Uint);
  static constexpr int kOutBitSize = 8 * kOutByteSize;
  static constexpr int kSimdByteSize = SimdByteSize;
  static constexpr int kSimdBitSize = 8 * kSimdByteSize;
  static constexpr int kSimdOutCount = kSimdByteSize / kOutByteSize;
  static constexpr int kPackedBitSize = BitWidth;
  static constexpr int kPackedMinByteSize = bit_util::BytesForBits(kPackedBitSize);
  // TODO should not be here
  static constexpr int kOutCountUnpacked = 32;
  static constexpr int kOutBytesUnpacked = kOutCountUnpacked * kOutByteSize;

  using out_type = Uint;
  using simd_batch = xsimd::make_sized_batch_t<out_type, kSimdOutCount>;
  using simd_bytes = xsimd::make_sized_batch_t<uint8_t, kSimdByteSize>;
};

template <typename Uint, int SimdByteSize, int BitWidth>
struct SimdUnpackerForWidth;

template <typename Uint, int SimdByteSize>
struct SimdUnpackerForWidth<Uint, SimdByteSize, 0> {
  using Traits = SimdUnpackerForWidthTraits<Uint, SimdByteSize, 0>;

  static const uint8_t* unpack(const uint8_t* in, typename Traits::out_type* out) {
    std::memset(out, 0, Traits::kOutBytesUnpacked);
    return in;
  }
};

template <typename Uint, int SimdByteSize>
struct SimdUnpackerForWidth<Uint, SimdByteSize, SimdByteSize> {
  using Traits = SimdUnpackerForWidthTraits<Uint, SimdByteSize, SimdByteSize>;

  static const uint8_t* unpack(const uint8_t* in, typename Traits::out_type* out) {
    std::memcpy(out, in, Traits::kOutBytesUnpacked);
    return in + Traits::kOutBytesUnpacked;
  }
};

template <typename Int>
constexpr auto LowBitMask(Int count) {
  if (count == 8 * sizeof(Int)) {
    return ~Int{0};
  }
  return (Int{1} << count) - Int{1};
}

template <typename Uint, int SimdByteSize, int BitWidth>
struct SimdUnpackerForWidth {
  using Traits = SimdUnpackerForWidthTraits<Uint, SimdByteSize, BitWidth>;
  using out_type = typename Traits::out_type;
  using simd_batch = typename Traits::simd_batch;
  using simd_bytes = typename Traits::simd_bytes;

  static_assert(Traits::kOutBitSize >= 1);

  template <typename T, typename G>
  static constexpr auto make_batch_constant() {
    return xsimd::make_batch_constant<T, typename simd_batch::arch_type, G>();
  }

  template <int kIteration>
  struct ByteSwizzle {
    static constexpr int get(int byte_idx, int byte_count) {
      // The byte index as if all simd batch iterations were contiguous.
      const int out_iter_byte_idx = byte_idx + kIteration * Traits::kSimdByteSize;
      // The index of the value we are unpacking in this byte.
      const int out_iter_idx = out_iter_byte_idx / Traits::kOutByteSize;
      // The index within the unpacked of the byte we are unpacking.
      const int out_byte_offset = out_iter_byte_idx % Traits::kOutByteSize;
      // Where the packed value starts in the input.
      const int in_bit_start = out_iter_idx * Traits::kPackedBitSize;
      // At which byte the packed value starts in the input.
      const int in_byte_start = in_bit_start / 8;

      // This is the LSB byte there is always data
      if (out_byte_offset == 0) {
        return in_byte_start;
      }

      // Number of bits in the LSB byte of the output value that actually contain data
      // about the current value.
      const int bits_in_offset_0 = (in_byte_start + 1) * 8 - in_bit_start;
      // Bit capacity in all lesser output bytes, accounting for shit
      const int bits_in_smaller_offset = bits_in_offset_0 + (out_byte_offset - 1) * 8;

      // No more data to extract for this output value, fill with zero.
      if (bits_in_smaller_offset >= Traits::kPackedBitSize) {
        // X86_64 looks at bit 8 and Arm for oversized index.
        return LowBitMask(8U);
      }

      return in_byte_start + out_byte_offset;
    }
  };

  template <int kIteration>
  struct UnpackedShift {
    static constexpr int get(int out_idx, int out_count) {
      // The out value index as if all simd batch iterations were contiguous.
      const int out_iter_idx = out_idx + kIteration * Traits::kSimdOutCount;
      // The bit index in the input where the associated output values starts.
      const int in_bit_start = out_iter_idx * Traits::kPackedBitSize;
      // The bit index in the input where the value starts
      const int in_byte_start = in_bit_start / 8;

      return in_bit_start - (8 * in_byte_start);
    }
  };

  static const uint8_t* unpack(const uint8_t* in, out_type* out) {
    constexpr out_type kMask = LowBitMask<out_type>(Traits::kPackedBitSize);

    constexpr auto kShifts0 = make_batch_constant<out_type, UnpackedShift<0>>();
    constexpr auto kShifts1 = make_batch_constant<out_type, UnpackedShift<1>>();

    auto bytes = simd_bytes::load_unaligned(in + Traits::kSimdOutCount * 0);
    {
      constexpr auto kReorder = make_batch_constant<uint8_t, ByteSwizzle<0>>();
      auto numbers = xsimd::bitwise_cast<out_type>(xsimd::swizzle(bytes, kReorder));
      ((numbers >> kShifts0) & kMask).store_unaligned(out + 0 * Traits::kSimdOutCount);
      ((numbers >> kShifts1) & kMask).store_unaligned(out + 1 * Traits::kSimdOutCount);
    }
    {
      constexpr auto kReorder = make_batch_constant<uint8_t, ByteSwizzle<2>>();
      auto numbers = xsimd::bitwise_cast<out_type>(xsimd::swizzle(bytes, kReorder));
      ((numbers >> kShifts0) & kMask).store_unaligned(out + 2 * Traits::kSimdOutCount);
      ((numbers >> kShifts1) & kMask).store_unaligned(out + 3 * Traits::kSimdOutCount);
    }
    {
      constexpr auto kReorder = make_batch_constant<uint8_t, ByteSwizzle<4>>();
      auto numbers = xsimd::bitwise_cast<out_type>(xsimd::swizzle(bytes, kReorder));
      ((numbers >> kShifts0) & kMask).store_unaligned(out + 4 * Traits::kSimdOutCount);
      ((numbers >> kShifts1) & kMask).store_unaligned(out + 5 * Traits::kSimdOutCount);
    }
    {
      constexpr auto kReorder = make_batch_constant<uint8_t, ByteSwizzle<6>>();
      auto numbers = xsimd::bitwise_cast<out_type>(xsimd::swizzle(bytes, kReorder));
      ((numbers >> kShifts0) & kMask).store_unaligned(out + 6 * Traits::kSimdOutCount);
      ((numbers >> kShifts1) & kMask).store_unaligned(out + 7 * Traits::kSimdOutCount);
    }

    return in + 4;
  }
};

// static_assert(SimdUnpackerForWidth<uint32_t, 16, 3>::ByteSwizzle<0>::get(9, 16) == 1);

template <typename Uint>
struct Simd128Unpacker {
  static constexpr int kValuesUnpacked = 32;
  using out_type = Uint;

  template <int kBit>
  static const uint8_t* unpack(const uint8_t* in, out_type* out) {
    return SimdUnpackerForWidth<Uint, 16, kBit>::unpack(in, out);
  }
};

}  // namespace arrow::internal
