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

#include <algorithm>
#include <cstring>
#include <type_traits>

#include "arrow/util/bit_util.h"
#include "arrow/util/endian.h"
#include "arrow/util/logging.h"
#include "arrow/util/macros.h"
#include "arrow/util/ubsan.h"

namespace arrow::internal {

/// Unpack a zero bit packed array.
template <typename Uint>
void unpack_null(const uint8_t* in, Uint* out, int batch_size) {
  std::memset(out, 0, batch_size * sizeof(Uint));
}

/// Unpack a packed array where packed and unpacked values have exactly the same number of
/// bits.
template <typename Uint>
void unpack_full(const uint8_t* in, Uint* out, int batch_size) {
  if constexpr (ARROW_LITTLE_ENDIAN == 1) {
    std::memcpy(out, in, batch_size * sizeof(Uint));
  } else {
    using bit_util::FromLittleEndian;
    using util::SafeLoadAs;

    for (int k = 0; k < batch_size; k += 1) {
      out[k] = FromLittleEndian(SafeLoadAs<Uint>(in + (k * sizeof(Uint))));
    }
  }
}

/// Compute the maximum spread in bytes that a packed integer can cover.
///
/// This is assuming contiguous packed integer starting with the given bit offset away
/// from a byte boundary.
/// This function is non-monotonic, for instance with zero offset, three bit integers
/// will be split on the first byte boundary (hence having a spread of two bytes) while
/// four bit integer will be well behaved and never spread over byte boundary (hence
/// having a spread of one).
constexpr int PackedMaxSpreadBytes(int width, int bit_offset) {
  int max = static_cast<int>(bit_util::BytesForBits(width));
  int start = bit_offset;
  do {
    const int byte_start = start / 8;
    const int byte_end = (start + width - 1) / 8;  // inclusive end bit
    const int spread = byte_end - byte_start + 1;
    max = spread > max ? spread : max;
    start += width;
  } while (start % 8 != bit_offset);
  return max;
}

/// Compute the maximum spread in bytes that a packed integer can cover across all bit
/// offsets.
constexpr int PackedMaxSpreadBytes(int width) {
  int max = 0;
  for (int offset = 0; offset < 8; ++offset) {
    const int spread = PackedMaxSpreadBytes(width, offset);
    max = spread > max ? spread : max;
  }
  return max;
}

// Integer type that tries to contain as much as the spread as possible.
template <int kSpreadBytes>
using SpreadBufferUint = std::conditional_t<
    (kSpreadBytes <= sizeof(uint8_t)), uint_fast8_t,
    std::conditional_t<(kSpreadBytes <= sizeof(uint16_t)), uint_fast16_t,
                       std::conditional_t<(kSpreadBytes <= sizeof(uint32_t)),
                                          uint_fast32_t, uint_fast64_t>>>;

/// Unpack integers.
/// This function works for all input batch sizes but is not the fastest.
/// In prolog mode, instead of unpacking all required element, the function will
/// stop if it finds a byte aligned value start.
template <int kPackedBitWidth, bool kIsProlog, typename Uint>
int unpack_exact(const uint8_t* in, Uint* out, int batch_size, int bit_offset) {
  // For the epilog we adapt the max spread since better alignment give shorter spreads
  ARROW_DCHECK(kIsProlog || bit_offset == 0);
  ARROW_DCHECK(bit_offset >= 0 && bit_offset < 8);
  constexpr int kMaxSpreadBytes = kIsProlog ? PackedMaxSpreadBytes(kPackedBitWidth)
                                            : PackedMaxSpreadBytes(kPackedBitWidth, 0);
  using buffer_uint = SpreadBufferUint<kMaxSpreadBytes>;
  constexpr int kBufferSize = sizeof(buffer_uint);
  // Due to misalignment, on large bit width, the spread can be larger than the maximum
  // size integer. For instance a 63 bit width misaligned packed integer can spread over 9
  // aligned bytes.
  constexpr bool kOversized = kBufferSize < kMaxSpreadBytes;
  constexpr buffer_uint kLowMask =
      bit_util::LeastSignificantBitMask<buffer_uint, true>(kPackedBitWidth);

  ARROW_DCHECK_GE(bit_offset, 0);
  ARROW_DCHECK_LE(bit_offset, 8);

  // Looping over values one by one
  const int start_bit_term = batch_size * kPackedBitWidth + bit_offset;
  int start_bit = bit_offset;
  while ((start_bit < start_bit_term) && (!kIsProlog || (start_bit % 8 != 0))) {
    const int start_byte = start_bit / 8;
    const int spread_bytes = ((start_bit + kPackedBitWidth - 1) / 8) - start_byte + 1;
    ARROW_COMPILER_ASSUME(spread_bytes <= kMaxSpreadBytes);

    // Reading the bytes for the current value.
    // Must be careful not to read out of input bounds.
    buffer_uint buffer = 0;
    if constexpr (kOversized) {
      // We read the max possible bytes in the first pass and handle the rest after.
      // Even though the worst spread does not happen on all iterations we can still read
      // all bytes because we will mask them.
      std::memcpy(&buffer, in + start_byte, std::min(kBufferSize, spread_bytes));
    } else {
      std::memcpy(&buffer, in + start_byte, spread_bytes);
    }

    buffer = bit_util::FromLittleEndian(buffer);
    const int bit_offset = start_bit % 8;
    buffer >>= bit_offset;
    Uint val = static_cast<Uint>(buffer & kLowMask);

    // Handle the oversized bytes
    if constexpr (kOversized) {
      // The oversized bytes do not happen at all iterations
      if (spread_bytes > kBufferSize) {
        std::memcpy(&buffer, in + start_byte + kBufferSize, spread_bytes - kBufferSize);
        buffer = bit_util::FromLittleEndian(buffer);
        buffer <<= 8 * kBufferSize - bit_offset;
        val |= static_cast<Uint>(buffer & kLowMask);
      }
    }

    *out = val;
    out++;
    start_bit += kPackedBitWidth;
  }

  ARROW_DCHECK((start_bit - bit_offset) % kPackedBitWidth == 0);
  return (start_bit - bit_offset) / kPackedBitWidth;
}

/// Unpack a packed array, delegating to a Unpacker struct.
///
/// @tparam kPackedBitWidth The width in bits of the values in the packed array.
/// @tparam Unpacker The struct providing information and an ``unpack`` method to unpack a
///                  fixed amount of values (usually constrained by SIMD batch sizes and
///                  byte alignment).
/// @tparam UnpackedUInt The type in which we unpack the values.
template <int kPackedBitWidth, template <typename, int> typename Unpacker,
          typename UnpackedUInt>
void unpack_width(const uint8_t* in, UnpackedUInt* out, int batch_size, int bit_offset) {
  if constexpr (kPackedBitWidth == 0) {
    // Easy case to handle, simply setting memory to zero.
    return unpack_null(in, out, batch_size);
  } else {
    // In case of misalignment, we need to run the prolog until aligned.
    int extracted = unpack_exact<kPackedBitWidth, true>(in, out, batch_size, bit_offset);
    // We either extracted everything or found a alignment
    const int start_bit = extracted * kPackedBitWidth + bit_offset;
    ARROW_DCHECK((extracted == batch_size) || ((start_bit) % 8 == 0));
    batch_size -= extracted;
    ARROW_DCHECK_GE(batch_size, 0);
    in += start_bit / 8;
    out += extracted;

    if constexpr (kPackedBitWidth == 8 * sizeof(UnpackedUInt)) {
      // Only memcpy / static_cast
      return unpack_full(in, out, batch_size);
    } else {
      using UnpackerForWidth = Unpacker<UnpackedUInt, kPackedBitWidth>;
      constexpr auto kValuesUnpacked = UnpackerForWidth::kValuesUnpacked;

      // Running the optimized kernel for batch extraction
      const int unpacker_iter_count = batch_size / kValuesUnpacked;
      for (int i = 0; i < unpacker_iter_count; ++i) {
        in = UnpackerForWidth::unpack(in, out);
        out += kValuesUnpacked;
      }
      batch_size -= unpacker_iter_count * kValuesUnpacked;

      // Running the epilog for the remaining values that don't fit in a kernel
      ARROW_DCHECK_LT(batch_size, kValuesUnpacked);
      ARROW_DCHECK_GE(batch_size, 0);
      ARROW_COMPILER_ASSUME(batch_size < kValuesUnpacked);
      ARROW_COMPILER_ASSUME(batch_size >= 0);
      unpack_exact<kPackedBitWidth, false>(in, out, batch_size, /* bit_offset= */ 0);
    }
  }
}

template <template <typename, int> typename Unpacker, typename UnpackedUint>
static void unpack_jump(const uint8_t* in, UnpackedUint* out, int batch_size,
                        int num_bits, int bit_offset) {
  if constexpr (std::is_same_v<UnpackedUint, bool>) {
    switch (num_bits) {
      case 0:
        return unpack_width<0, Unpacker>(in, out, batch_size, bit_offset);
      case 1:
        return unpack_width<1, Unpacker>(in, out, batch_size, bit_offset);
    }
  } else if constexpr (sizeof(UnpackedUint) == 1) {
    switch (num_bits) {
      case 0:
        return unpack_width<0, Unpacker>(in, out, batch_size, bit_offset);
      case 1:
        return unpack_width<1, Unpacker>(in, out, batch_size, bit_offset);
      case 2:
        return unpack_width<2, Unpacker>(in, out, batch_size, bit_offset);
      case 3:
        return unpack_width<3, Unpacker>(in, out, batch_size, bit_offset);
      case 4:
        return unpack_width<4, Unpacker>(in, out, batch_size, bit_offset);
      case 5:
        return unpack_width<5, Unpacker>(in, out, batch_size, bit_offset);
      case 6:
        return unpack_width<6, Unpacker>(in, out, batch_size, bit_offset);
      case 7:
        return unpack_width<7, Unpacker>(in, out, batch_size, bit_offset);
      case 8:
        return unpack_width<8, Unpacker>(in, out, batch_size, bit_offset);
    }
  } else if constexpr (sizeof(UnpackedUint) == 2) {
    switch (num_bits) {
      case 0:
        return unpack_width<0, Unpacker>(in, out, batch_size, bit_offset);
      case 1:
        return unpack_width<1, Unpacker>(in, out, batch_size, bit_offset);
      case 2:
        return unpack_width<2, Unpacker>(in, out, batch_size, bit_offset);
      case 3:
        return unpack_width<3, Unpacker>(in, out, batch_size, bit_offset);
      case 4:
        return unpack_width<4, Unpacker>(in, out, batch_size, bit_offset);
      case 5:
        return unpack_width<5, Unpacker>(in, out, batch_size, bit_offset);
      case 6:
        return unpack_width<6, Unpacker>(in, out, batch_size, bit_offset);
      case 7:
        return unpack_width<7, Unpacker>(in, out, batch_size, bit_offset);
      case 8:
        return unpack_width<8, Unpacker>(in, out, batch_size, bit_offset);
      case 9:
        return unpack_width<9, Unpacker>(in, out, batch_size, bit_offset);
      case 10:
        return unpack_width<10, Unpacker>(in, out, batch_size, bit_offset);
      case 11:
        return unpack_width<11, Unpacker>(in, out, batch_size, bit_offset);
      case 12:
        return unpack_width<12, Unpacker>(in, out, batch_size, bit_offset);
      case 13:
        return unpack_width<13, Unpacker>(in, out, batch_size, bit_offset);
      case 14:
        return unpack_width<14, Unpacker>(in, out, batch_size, bit_offset);
      case 15:
        return unpack_width<15, Unpacker>(in, out, batch_size, bit_offset);
      case 16:
        return unpack_width<16, Unpacker>(in, out, batch_size, bit_offset);
    }
  } else if constexpr (sizeof(UnpackedUint) == 4) {
    switch (num_bits) {
      case 0:
        return unpack_width<0, Unpacker>(in, out, batch_size, bit_offset);
      case 1:
        return unpack_width<1, Unpacker>(in, out, batch_size, bit_offset);
      case 2:
        return unpack_width<2, Unpacker>(in, out, batch_size, bit_offset);
      case 3:
        return unpack_width<3, Unpacker>(in, out, batch_size, bit_offset);
      case 4:
        return unpack_width<4, Unpacker>(in, out, batch_size, bit_offset);
      case 5:
        return unpack_width<5, Unpacker>(in, out, batch_size, bit_offset);
      case 6:
        return unpack_width<6, Unpacker>(in, out, batch_size, bit_offset);
      case 7:
        return unpack_width<7, Unpacker>(in, out, batch_size, bit_offset);
      case 8:
        return unpack_width<8, Unpacker>(in, out, batch_size, bit_offset);
      case 9:
        return unpack_width<9, Unpacker>(in, out, batch_size, bit_offset);
      case 10:
        return unpack_width<10, Unpacker>(in, out, batch_size, bit_offset);
      case 11:
        return unpack_width<11, Unpacker>(in, out, batch_size, bit_offset);
      case 12:
        return unpack_width<12, Unpacker>(in, out, batch_size, bit_offset);
      case 13:
        return unpack_width<13, Unpacker>(in, out, batch_size, bit_offset);
      case 14:
        return unpack_width<14, Unpacker>(in, out, batch_size, bit_offset);
      case 15:
        return unpack_width<15, Unpacker>(in, out, batch_size, bit_offset);
      case 16:
        return unpack_width<16, Unpacker>(in, out, batch_size, bit_offset);
      case 17:
        return unpack_width<17, Unpacker>(in, out, batch_size, bit_offset);
      case 18:
        return unpack_width<18, Unpacker>(in, out, batch_size, bit_offset);
      case 19:
        return unpack_width<19, Unpacker>(in, out, batch_size, bit_offset);
      case 20:
        return unpack_width<20, Unpacker>(in, out, batch_size, bit_offset);
      case 21:
        return unpack_width<21, Unpacker>(in, out, batch_size, bit_offset);
      case 22:
        return unpack_width<22, Unpacker>(in, out, batch_size, bit_offset);
      case 23:
        return unpack_width<23, Unpacker>(in, out, batch_size, bit_offset);
      case 24:
        return unpack_width<24, Unpacker>(in, out, batch_size, bit_offset);
      case 25:
        return unpack_width<25, Unpacker>(in, out, batch_size, bit_offset);
      case 26:
        return unpack_width<26, Unpacker>(in, out, batch_size, bit_offset);
      case 27:
        return unpack_width<27, Unpacker>(in, out, batch_size, bit_offset);
      case 28:
        return unpack_width<28, Unpacker>(in, out, batch_size, bit_offset);
      case 29:
        return unpack_width<29, Unpacker>(in, out, batch_size, bit_offset);
      case 30:
        return unpack_width<30, Unpacker>(in, out, batch_size, bit_offset);
      case 31:
        return unpack_width<31, Unpacker>(in, out, batch_size, bit_offset);
      case 32:
        return unpack_width<32, Unpacker>(in, out, batch_size, bit_offset);
    }
  } else if constexpr (sizeof(UnpackedUint) == 8) {
    switch (num_bits) {
      case 0:
        return unpack_width<0, Unpacker>(in, out, batch_size, bit_offset);
      case 1:
        return unpack_width<1, Unpacker>(in, out, batch_size, bit_offset);
      case 2:
        return unpack_width<2, Unpacker>(in, out, batch_size, bit_offset);
      case 3:
        return unpack_width<3, Unpacker>(in, out, batch_size, bit_offset);
      case 4:
        return unpack_width<4, Unpacker>(in, out, batch_size, bit_offset);
      case 5:
        return unpack_width<5, Unpacker>(in, out, batch_size, bit_offset);
      case 6:
        return unpack_width<6, Unpacker>(in, out, batch_size, bit_offset);
      case 7:
        return unpack_width<7, Unpacker>(in, out, batch_size, bit_offset);
      case 8:
        return unpack_width<8, Unpacker>(in, out, batch_size, bit_offset);
      case 9:
        return unpack_width<9, Unpacker>(in, out, batch_size, bit_offset);
      case 10:
        return unpack_width<10, Unpacker>(in, out, batch_size, bit_offset);
      case 11:
        return unpack_width<11, Unpacker>(in, out, batch_size, bit_offset);
      case 12:
        return unpack_width<12, Unpacker>(in, out, batch_size, bit_offset);
      case 13:
        return unpack_width<13, Unpacker>(in, out, batch_size, bit_offset);
      case 14:
        return unpack_width<14, Unpacker>(in, out, batch_size, bit_offset);
      case 15:
        return unpack_width<15, Unpacker>(in, out, batch_size, bit_offset);
      case 16:
        return unpack_width<16, Unpacker>(in, out, batch_size, bit_offset);
      case 17:
        return unpack_width<17, Unpacker>(in, out, batch_size, bit_offset);
      case 18:
        return unpack_width<18, Unpacker>(in, out, batch_size, bit_offset);
      case 19:
        return unpack_width<19, Unpacker>(in, out, batch_size, bit_offset);
      case 20:
        return unpack_width<20, Unpacker>(in, out, batch_size, bit_offset);
      case 21:
        return unpack_width<21, Unpacker>(in, out, batch_size, bit_offset);
      case 22:
        return unpack_width<22, Unpacker>(in, out, batch_size, bit_offset);
      case 23:
        return unpack_width<23, Unpacker>(in, out, batch_size, bit_offset);
      case 24:
        return unpack_width<24, Unpacker>(in, out, batch_size, bit_offset);
      case 25:
        return unpack_width<25, Unpacker>(in, out, batch_size, bit_offset);
      case 26:
        return unpack_width<26, Unpacker>(in, out, batch_size, bit_offset);
      case 27:
        return unpack_width<27, Unpacker>(in, out, batch_size, bit_offset);
      case 28:
        return unpack_width<28, Unpacker>(in, out, batch_size, bit_offset);
      case 29:
        return unpack_width<29, Unpacker>(in, out, batch_size, bit_offset);
      case 30:
        return unpack_width<30, Unpacker>(in, out, batch_size, bit_offset);
      case 31:
        return unpack_width<31, Unpacker>(in, out, batch_size, bit_offset);
      case 32:
        return unpack_width<32, Unpacker>(in, out, batch_size, bit_offset);
      case 33:
        return unpack_width<33, Unpacker>(in, out, batch_size, bit_offset);
      case 34:
        return unpack_width<34, Unpacker>(in, out, batch_size, bit_offset);
      case 35:
        return unpack_width<35, Unpacker>(in, out, batch_size, bit_offset);
      case 36:
        return unpack_width<36, Unpacker>(in, out, batch_size, bit_offset);
      case 37:
        return unpack_width<37, Unpacker>(in, out, batch_size, bit_offset);
      case 38:
        return unpack_width<38, Unpacker>(in, out, batch_size, bit_offset);
      case 39:
        return unpack_width<39, Unpacker>(in, out, batch_size, bit_offset);
      case 40:
        return unpack_width<40, Unpacker>(in, out, batch_size, bit_offset);
      case 41:
        return unpack_width<41, Unpacker>(in, out, batch_size, bit_offset);
      case 42:
        return unpack_width<42, Unpacker>(in, out, batch_size, bit_offset);
      case 43:
        return unpack_width<43, Unpacker>(in, out, batch_size, bit_offset);
      case 44:
        return unpack_width<44, Unpacker>(in, out, batch_size, bit_offset);
      case 45:
        return unpack_width<45, Unpacker>(in, out, batch_size, bit_offset);
      case 46:
        return unpack_width<46, Unpacker>(in, out, batch_size, bit_offset);
      case 47:
        return unpack_width<47, Unpacker>(in, out, batch_size, bit_offset);
      case 48:
        return unpack_width<48, Unpacker>(in, out, batch_size, bit_offset);
      case 49:
        return unpack_width<49, Unpacker>(in, out, batch_size, bit_offset);
      case 50:
        return unpack_width<50, Unpacker>(in, out, batch_size, bit_offset);
      case 51:
        return unpack_width<51, Unpacker>(in, out, batch_size, bit_offset);
      case 52:
        return unpack_width<52, Unpacker>(in, out, batch_size, bit_offset);
      case 53:
        return unpack_width<53, Unpacker>(in, out, batch_size, bit_offset);
      case 54:
        return unpack_width<54, Unpacker>(in, out, batch_size, bit_offset);
      case 55:
        return unpack_width<55, Unpacker>(in, out, batch_size, bit_offset);
      case 56:
        return unpack_width<56, Unpacker>(in, out, batch_size, bit_offset);
      case 57:
        return unpack_width<57, Unpacker>(in, out, batch_size, bit_offset);
      case 58:
        return unpack_width<58, Unpacker>(in, out, batch_size, bit_offset);
      case 59:
        return unpack_width<59, Unpacker>(in, out, batch_size, bit_offset);
      case 60:
        return unpack_width<60, Unpacker>(in, out, batch_size, bit_offset);
      case 61:
        return unpack_width<61, Unpacker>(in, out, batch_size, bit_offset);
      case 62:
        return unpack_width<62, Unpacker>(in, out, batch_size, bit_offset);
      case 63:
        return unpack_width<63, Unpacker>(in, out, batch_size, bit_offset);
      case 64:
        return unpack_width<64, Unpacker>(in, out, batch_size, bit_offset);
    }
  }
  ARROW_DCHECK(false) << "Unsupported num_bits " << num_bits;
}
}  // namespace arrow::internal
