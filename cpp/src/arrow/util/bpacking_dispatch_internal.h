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
#include "arrow/util/bpacking_internal.h"
#include "arrow/util/endian.h"
#include "arrow/util/logging.h"
#include "arrow/util/macros.h"
#include "arrow/util/ubsan.h"

namespace arrow::internal::bpacking {

/// Unpack a zero bit packed array.
template <bool kHasBias = false, typename Uint>
ARROW_FORCE_INLINE void unpack_null(const uint8_t* in, Uint* out, int batch_size,
                                    Uint bias = Uint{}) {
  if constexpr (kHasBias) {
    // Every unpacked value is zero, so every output value is the bias.
    std::fill(out, out + batch_size, bias);
  } else {
    std::memset(out, 0, batch_size * sizeof(Uint));
  }
}

/// Unpack a packed array where packed and unpacked values have exactly the same number of
/// bits.
template <bool kHasBias = false, typename Uint>
ARROW_FORCE_INLINE void unpack_full(const uint8_t* in, Uint* out, int batch_size,
                                    Uint bias = Uint{}) {
  if constexpr (ARROW_LITTLE_ENDIAN == 1) {
    if constexpr (kHasBias) {
      // Two things are needed for this loop to reach memcpy speed, and it is
      // 10.8x slower than the memcpy below without them -- far worse than the
      // second add pass the bias exists to remove.
      //   1. A constant-size memcpy for the load, not SafeLoadAs: SafeLoadAs
      //      builds an AlignedStorage per element and the vectorizer refuses it,
      //      while a fixed-size memcpy is just an unaligned load.
      //   2. A restrict qualifier: `in` is a uint8_t*, so it may alias
      //      anything, including `out`. Without restating that they are
      //      distinct the compiler has to assume overlap and emits a scalar
      //      loop.
      const uint8_t* ARROW_RESTRICT src = in;
      Uint* ARROW_RESTRICT dst = out;
      for (int k = 0; k < batch_size; k += 1) {
        Uint val;
        std::memcpy(&val, src + (k * sizeof(Uint)), sizeof(Uint));
        dst[k] = static_cast<Uint>(val + bias);
      }
    } else {
      std::memcpy(out, in, batch_size * sizeof(Uint));
    }
  } else {
    using bit_util::FromLittleEndian;
    using util::SafeLoadAs;

    for (int k = 0; k < batch_size; k += 1) {
      Uint val = FromLittleEndian(SafeLoadAs<Uint>(in + (k * sizeof(Uint))));
      if constexpr (kHasBias) {
        val = static_cast<Uint>(val + bias);
      }
      out[k] = val;
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
ARROW_FORCE_INLINE constexpr int PackedMaxSpreadBytes(int width, int bit_offset) {
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
ARROW_FORCE_INLINE constexpr int PackedMaxSpreadBytes(int width) {
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
template <int kPackedBitWidth, bool kIsProlog, bool kHasBias = false, typename Uint>
ARROW_FORCE_INLINE int unpack_exact(const uint8_t* in, const uint8_t* in_end, Uint* out,
                                    int batch_size, int bit_offset, Uint bias = Uint{}) {
  static_assert(kPackedBitWidth > 0);

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
  constexpr bool kLarge = kBufferSize < kMaxSpreadBytes;
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
    ARROW_DCHECK_LE(spread_bytes, kMaxSpreadBytes);
    ARROW_COMPILER_ASSUME(spread_bytes <= kMaxSpreadBytes);

    // Reading the bytes for the current value.
    buffer_uint buffer = 0;
    if (ARROW_PREDICT_TRUE(in + start_byte + kBufferSize < in_end)) {
      // Fast path we read the whole buffer. In all but few last reads (plural!) we will
      // always have enough bytes left in the buffer to avoid out-of-bounds reads. On top
      // of this, in Arrow, `unpack` is always called with `max_read_bytes` set, meaning
      // this will often be the *only* path taken.
      // We added this special case because `std::memcpy` without a compile time constant
      // was not inlined and optimized properly by the compiler, resulting to a function
      // call on each iteration.
      // This also handles the `kLarge` case detailed below.
      std::memcpy(&buffer, in + start_byte, kBufferSize);
    } else {
      // Slow path, we need to read exactly the correct number of bytes to avoid
      // out-of-bounds reads.
      if constexpr (kLarge) {
        // We read the max possible bytes in the first pass and handle the rest after.
        // Even though the worst spread does not happen on all iterations we can still
        // read all bytes because we will mask them.
        std::memcpy(&buffer, in + start_byte, std::min(kBufferSize, spread_bytes));
      } else {
        std::memcpy(&buffer, in + start_byte, spread_bytes);
      }
    }

    buffer = bit_util::FromLittleEndian(buffer);
    const int bit_offset = start_bit % 8;
    buffer >>= bit_offset;
    Uint val = static_cast<Uint>(buffer & kLowMask);

    // Handle the oversized bytes
    if constexpr (kLarge) {
      // The oversized bytes do not happen at all iterations
      if (spread_bytes > kBufferSize) {
        std::memcpy(&buffer, in + start_byte + kBufferSize, spread_bytes - kBufferSize);
        buffer = bit_util::FromLittleEndian(buffer);
        buffer <<= 8 * kBufferSize - bit_offset;
        val |= static_cast<Uint>(buffer & kLowMask);
      }
    }

    if constexpr (kHasBias) {
      val = static_cast<Uint>(val + bias);
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
/// @param batch_size The number of values to unpack.
/// @param bit_offset The bit offset of the first value in the first input byte.
/// @param max_read_bytes The maximum size of the input byte array that can be read.
///                       This is used to safely overread.
///                       Negative value to deduce from batch_size.
template <int kPackedBitWidth, template <typename, int> typename Unpacker,
          bool kHasBias = false, typename UnpackedUInt>
void unpack_width(const uint8_t* in, UnpackedUInt* out, int batch_size, int bit_offset,
                  int max_read_bytes, UnpackedUInt bias = UnpackedUInt{}) {
  if constexpr (kPackedBitWidth == 0) {
    // Easy case to handle, simply setting memory to zero.
    return unpack_null<kHasBias>(in, out, batch_size, bias);
  } else {
    // Number of bytes to read according to batch_size.
    const int bytes_batch = static_cast<int>(
        bit_util::BytesForBits(batch_size * kPackedBitWidth + bit_offset));
    // If specified, max_read_bytes must be greater that the bytes needed to extract the
    // number of desired values.
    ARROW_DCHECK(max_read_bytes < 0 || bytes_batch <= max_read_bytes);
    const uint8_t* in_end = in + (max_read_bytes >= 0 ? max_read_bytes : bytes_batch);

    // In case of misalignment, we need to run the prolog until aligned.
    int extracted = unpack_exact<kPackedBitWidth, true, kHasBias>(
        in, in_end, out, batch_size, bit_offset, bias);
    // We either extracted everything or found a alignment
    const int start_bit = extracted * kPackedBitWidth + bit_offset;
    ARROW_DCHECK((extracted == batch_size) || ((start_bit) % 8 == 0));
    batch_size -= extracted;
    ARROW_DCHECK_GE(batch_size, 0);
    in += start_bit / 8;
    out += extracted;

    if constexpr (kPackedBitWidth == 8 * sizeof(UnpackedUInt)) {
      // Only memcpy / static_cast
      return unpack_full<kHasBias>(in, out, batch_size, bias);
    } else {
      using UnpackerForWidth = Unpacker<UnpackedUInt, kPackedBitWidth>;
      // Number of values extracted by one iteration of the kernel
      constexpr auto kValuesUnpacked = UnpackerForWidth::kValuesUnpacked;
      // Number of bytes read, but not necessarily unpacked, by one iteration of the
      // kernel. This constant prevent reading past buffer end.
      constexpr auto kBytesRead = UnpackerForWidth::kBytesRead;

      if constexpr (kValuesUnpacked > 0) {
        const uint8_t* in_last = in_end - kBytesRead;
        // Whether this unpacker family folds the bias into its own stores. The
        // xsimd kernels do; the generated scalar and AVX-512 families do not, so
        // they get a second pass over the values the kernel just wrote. That
        // pass is over kValuesUnpacked elements still in L1, not over the whole
        // output, but it is a second pass all the same and the measured cost of
        // one is 1.47-2.40x the unpack -- so the fallback is for correctness on
        // those targets, not a substitute for folding it in.
        constexpr bool kUnpackerTakesBias =
            requires(const uint8_t* i, UnpackedUInt* o, UnpackedUInt b) {
              UnpackerForWidth::unpack(i, o, b);
            };

        // Running the optimized kernel for batch extraction
        while ((batch_size >= kValuesUnpacked) && (in <= in_last)) {
          if constexpr (kHasBias && kUnpackerTakesBias) {
            in = UnpackerForWidth::unpack(in, out, bias);
          } else {
            in = UnpackerForWidth::unpack(in, out);
            if constexpr (kHasBias) {
              for (int k = 0; k < kValuesUnpacked; ++k) {
                out[k] = static_cast<UnpackedUInt>(out[k] + bias);
              }
            }
          }
          out += kValuesUnpacked;
          batch_size -= kValuesUnpacked;
        }

        // Performance check making sure we ran the kernel loop as much as possible:
        // Either we ran out because we could not pack enough values, or because we would
        // overread.
        ARROW_DCHECK((batch_size < kValuesUnpacked) || (in_end - in) < kBytesRead);
      }

      // Running the epilog for the remaining values that don't fit in a kernel
      ARROW_DCHECK_GE(batch_size, 0);
      ARROW_COMPILER_ASSUME(batch_size >= 0);
      unpack_exact<kPackedBitWidth, false, kHasBias>(in, in_end, out, batch_size,
                                                     /* bit_offset= */ 0, bias);
    }
  }
}

template <template <typename, int> typename Unpacker, bool kHasBias = false,
          typename UnpackedUint>
static void unpack_jump(const uint8_t* in, UnpackedUint* out, const UnpackOptions& opt,
                        UnpackedUint bias = UnpackedUint{}) {
  // A bias on a bool output would have to saturate rather than add, which is not
  // what any caller means by it.
  static_assert(!(kHasBias && std::is_same_v<UnpackedUint, bool>),
                "unpack with a bias is not defined for bool output");
  if constexpr (std::is_same_v<UnpackedUint, bool>) {
    switch (opt.bit_width) {
      case 0:
        return unpack_width<0, Unpacker, kHasBias>(
            in, out, opt.batch_size, opt.bit_offset, opt.max_read_bytes, bias);
      case 1:
        return unpack_width<1, Unpacker, kHasBias>(
            in, out, opt.batch_size, opt.bit_offset, opt.max_read_bytes, bias);
    }
  } else if constexpr (sizeof(UnpackedUint) == 1) {
    switch (opt.bit_width) {
      case 0:
        return unpack_width<0, Unpacker, kHasBias>(
            in, out, opt.batch_size, opt.bit_offset, opt.max_read_bytes, bias);
      case 1:
        return unpack_width<1, Unpacker, kHasBias>(
            in, out, opt.batch_size, opt.bit_offset, opt.max_read_bytes, bias);
      case 2:
        return unpack_width<2, Unpacker, kHasBias>(
            in, out, opt.batch_size, opt.bit_offset, opt.max_read_bytes, bias);
      case 3:
        return unpack_width<3, Unpacker, kHasBias>(
            in, out, opt.batch_size, opt.bit_offset, opt.max_read_bytes, bias);
      case 4:
        return unpack_width<4, Unpacker, kHasBias>(
            in, out, opt.batch_size, opt.bit_offset, opt.max_read_bytes, bias);
      case 5:
        return unpack_width<5, Unpacker, kHasBias>(
            in, out, opt.batch_size, opt.bit_offset, opt.max_read_bytes, bias);
      case 6:
        return unpack_width<6, Unpacker, kHasBias>(
            in, out, opt.batch_size, opt.bit_offset, opt.max_read_bytes, bias);
      case 7:
        return unpack_width<7, Unpacker, kHasBias>(
            in, out, opt.batch_size, opt.bit_offset, opt.max_read_bytes, bias);
      case 8:
        return unpack_width<8, Unpacker, kHasBias>(
            in, out, opt.batch_size, opt.bit_offset, opt.max_read_bytes, bias);
    }
  } else if constexpr (sizeof(UnpackedUint) == 2) {
    switch (opt.bit_width) {
      case 0:
        return unpack_width<0, Unpacker, kHasBias>(
            in, out, opt.batch_size, opt.bit_offset, opt.max_read_bytes, bias);
      case 1:
        return unpack_width<1, Unpacker, kHasBias>(
            in, out, opt.batch_size, opt.bit_offset, opt.max_read_bytes, bias);
      case 2:
        return unpack_width<2, Unpacker, kHasBias>(
            in, out, opt.batch_size, opt.bit_offset, opt.max_read_bytes, bias);
      case 3:
        return unpack_width<3, Unpacker, kHasBias>(
            in, out, opt.batch_size, opt.bit_offset, opt.max_read_bytes, bias);
      case 4:
        return unpack_width<4, Unpacker, kHasBias>(
            in, out, opt.batch_size, opt.bit_offset, opt.max_read_bytes, bias);
      case 5:
        return unpack_width<5, Unpacker, kHasBias>(
            in, out, opt.batch_size, opt.bit_offset, opt.max_read_bytes, bias);
      case 6:
        return unpack_width<6, Unpacker, kHasBias>(
            in, out, opt.batch_size, opt.bit_offset, opt.max_read_bytes, bias);
      case 7:
        return unpack_width<7, Unpacker, kHasBias>(
            in, out, opt.batch_size, opt.bit_offset, opt.max_read_bytes, bias);
      case 8:
        return unpack_width<8, Unpacker, kHasBias>(
            in, out, opt.batch_size, opt.bit_offset, opt.max_read_bytes, bias);
      case 9:
        return unpack_width<9, Unpacker, kHasBias>(
            in, out, opt.batch_size, opt.bit_offset, opt.max_read_bytes, bias);
      case 10:
        return unpack_width<10, Unpacker, kHasBias>(
            in, out, opt.batch_size, opt.bit_offset, opt.max_read_bytes, bias);
      case 11:
        return unpack_width<11, Unpacker, kHasBias>(
            in, out, opt.batch_size, opt.bit_offset, opt.max_read_bytes, bias);
      case 12:
        return unpack_width<12, Unpacker, kHasBias>(
            in, out, opt.batch_size, opt.bit_offset, opt.max_read_bytes, bias);
      case 13:
        return unpack_width<13, Unpacker, kHasBias>(
            in, out, opt.batch_size, opt.bit_offset, opt.max_read_bytes, bias);
      case 14:
        return unpack_width<14, Unpacker, kHasBias>(
            in, out, opt.batch_size, opt.bit_offset, opt.max_read_bytes, bias);
      case 15:
        return unpack_width<15, Unpacker, kHasBias>(
            in, out, opt.batch_size, opt.bit_offset, opt.max_read_bytes, bias);
      case 16:
        return unpack_width<16, Unpacker, kHasBias>(
            in, out, opt.batch_size, opt.bit_offset, opt.max_read_bytes, bias);
    }
  } else if constexpr (sizeof(UnpackedUint) == 4) {
    switch (opt.bit_width) {
      case 0:
        return unpack_width<0, Unpacker, kHasBias>(
            in, out, opt.batch_size, opt.bit_offset, opt.max_read_bytes, bias);
      case 1:
        return unpack_width<1, Unpacker, kHasBias>(
            in, out, opt.batch_size, opt.bit_offset, opt.max_read_bytes, bias);
      case 2:
        return unpack_width<2, Unpacker, kHasBias>(
            in, out, opt.batch_size, opt.bit_offset, opt.max_read_bytes, bias);
      case 3:
        return unpack_width<3, Unpacker, kHasBias>(
            in, out, opt.batch_size, opt.bit_offset, opt.max_read_bytes, bias);
      case 4:
        return unpack_width<4, Unpacker, kHasBias>(
            in, out, opt.batch_size, opt.bit_offset, opt.max_read_bytes, bias);
      case 5:
        return unpack_width<5, Unpacker, kHasBias>(
            in, out, opt.batch_size, opt.bit_offset, opt.max_read_bytes, bias);
      case 6:
        return unpack_width<6, Unpacker, kHasBias>(
            in, out, opt.batch_size, opt.bit_offset, opt.max_read_bytes, bias);
      case 7:
        return unpack_width<7, Unpacker, kHasBias>(
            in, out, opt.batch_size, opt.bit_offset, opt.max_read_bytes, bias);
      case 8:
        return unpack_width<8, Unpacker, kHasBias>(
            in, out, opt.batch_size, opt.bit_offset, opt.max_read_bytes, bias);
      case 9:
        return unpack_width<9, Unpacker, kHasBias>(
            in, out, opt.batch_size, opt.bit_offset, opt.max_read_bytes, bias);
      case 10:
        return unpack_width<10, Unpacker, kHasBias>(
            in, out, opt.batch_size, opt.bit_offset, opt.max_read_bytes, bias);
      case 11:
        return unpack_width<11, Unpacker, kHasBias>(
            in, out, opt.batch_size, opt.bit_offset, opt.max_read_bytes, bias);
      case 12:
        return unpack_width<12, Unpacker, kHasBias>(
            in, out, opt.batch_size, opt.bit_offset, opt.max_read_bytes, bias);
      case 13:
        return unpack_width<13, Unpacker, kHasBias>(
            in, out, opt.batch_size, opt.bit_offset, opt.max_read_bytes, bias);
      case 14:
        return unpack_width<14, Unpacker, kHasBias>(
            in, out, opt.batch_size, opt.bit_offset, opt.max_read_bytes, bias);
      case 15:
        return unpack_width<15, Unpacker, kHasBias>(
            in, out, opt.batch_size, opt.bit_offset, opt.max_read_bytes, bias);
      case 16:
        return unpack_width<16, Unpacker, kHasBias>(
            in, out, opt.batch_size, opt.bit_offset, opt.max_read_bytes, bias);
      case 17:
        return unpack_width<17, Unpacker, kHasBias>(
            in, out, opt.batch_size, opt.bit_offset, opt.max_read_bytes, bias);
      case 18:
        return unpack_width<18, Unpacker, kHasBias>(
            in, out, opt.batch_size, opt.bit_offset, opt.max_read_bytes, bias);
      case 19:
        return unpack_width<19, Unpacker, kHasBias>(
            in, out, opt.batch_size, opt.bit_offset, opt.max_read_bytes, bias);
      case 20:
        return unpack_width<20, Unpacker, kHasBias>(
            in, out, opt.batch_size, opt.bit_offset, opt.max_read_bytes, bias);
      case 21:
        return unpack_width<21, Unpacker, kHasBias>(
            in, out, opt.batch_size, opt.bit_offset, opt.max_read_bytes, bias);
      case 22:
        return unpack_width<22, Unpacker, kHasBias>(
            in, out, opt.batch_size, opt.bit_offset, opt.max_read_bytes, bias);
      case 23:
        return unpack_width<23, Unpacker, kHasBias>(
            in, out, opt.batch_size, opt.bit_offset, opt.max_read_bytes, bias);
      case 24:
        return unpack_width<24, Unpacker, kHasBias>(
            in, out, opt.batch_size, opt.bit_offset, opt.max_read_bytes, bias);
      case 25:
        return unpack_width<25, Unpacker, kHasBias>(
            in, out, opt.batch_size, opt.bit_offset, opt.max_read_bytes, bias);
      case 26:
        return unpack_width<26, Unpacker, kHasBias>(
            in, out, opt.batch_size, opt.bit_offset, opt.max_read_bytes, bias);
      case 27:
        return unpack_width<27, Unpacker, kHasBias>(
            in, out, opt.batch_size, opt.bit_offset, opt.max_read_bytes, bias);
      case 28:
        return unpack_width<28, Unpacker, kHasBias>(
            in, out, opt.batch_size, opt.bit_offset, opt.max_read_bytes, bias);
      case 29:
        return unpack_width<29, Unpacker, kHasBias>(
            in, out, opt.batch_size, opt.bit_offset, opt.max_read_bytes, bias);
      case 30:
        return unpack_width<30, Unpacker, kHasBias>(
            in, out, opt.batch_size, opt.bit_offset, opt.max_read_bytes, bias);
      case 31:
        return unpack_width<31, Unpacker, kHasBias>(
            in, out, opt.batch_size, opt.bit_offset, opt.max_read_bytes, bias);
      case 32:
        return unpack_width<32, Unpacker, kHasBias>(
            in, out, opt.batch_size, opt.bit_offset, opt.max_read_bytes, bias);
    }
  } else if constexpr (sizeof(UnpackedUint) == 8) {
    switch (opt.bit_width) {
      case 0:
        return unpack_width<0, Unpacker, kHasBias>(
            in, out, opt.batch_size, opt.bit_offset, opt.max_read_bytes, bias);
      case 1:
        return unpack_width<1, Unpacker, kHasBias>(
            in, out, opt.batch_size, opt.bit_offset, opt.max_read_bytes, bias);
      case 2:
        return unpack_width<2, Unpacker, kHasBias>(
            in, out, opt.batch_size, opt.bit_offset, opt.max_read_bytes, bias);
      case 3:
        return unpack_width<3, Unpacker, kHasBias>(
            in, out, opt.batch_size, opt.bit_offset, opt.max_read_bytes, bias);
      case 4:
        return unpack_width<4, Unpacker, kHasBias>(
            in, out, opt.batch_size, opt.bit_offset, opt.max_read_bytes, bias);
      case 5:
        return unpack_width<5, Unpacker, kHasBias>(
            in, out, opt.batch_size, opt.bit_offset, opt.max_read_bytes, bias);
      case 6:
        return unpack_width<6, Unpacker, kHasBias>(
            in, out, opt.batch_size, opt.bit_offset, opt.max_read_bytes, bias);
      case 7:
        return unpack_width<7, Unpacker, kHasBias>(
            in, out, opt.batch_size, opt.bit_offset, opt.max_read_bytes, bias);
      case 8:
        return unpack_width<8, Unpacker, kHasBias>(
            in, out, opt.batch_size, opt.bit_offset, opt.max_read_bytes, bias);
      case 9:
        return unpack_width<9, Unpacker, kHasBias>(
            in, out, opt.batch_size, opt.bit_offset, opt.max_read_bytes, bias);
      case 10:
        return unpack_width<10, Unpacker, kHasBias>(
            in, out, opt.batch_size, opt.bit_offset, opt.max_read_bytes, bias);
      case 11:
        return unpack_width<11, Unpacker, kHasBias>(
            in, out, opt.batch_size, opt.bit_offset, opt.max_read_bytes, bias);
      case 12:
        return unpack_width<12, Unpacker, kHasBias>(
            in, out, opt.batch_size, opt.bit_offset, opt.max_read_bytes, bias);
      case 13:
        return unpack_width<13, Unpacker, kHasBias>(
            in, out, opt.batch_size, opt.bit_offset, opt.max_read_bytes, bias);
      case 14:
        return unpack_width<14, Unpacker, kHasBias>(
            in, out, opt.batch_size, opt.bit_offset, opt.max_read_bytes, bias);
      case 15:
        return unpack_width<15, Unpacker, kHasBias>(
            in, out, opt.batch_size, opt.bit_offset, opt.max_read_bytes, bias);
      case 16:
        return unpack_width<16, Unpacker, kHasBias>(
            in, out, opt.batch_size, opt.bit_offset, opt.max_read_bytes, bias);
      case 17:
        return unpack_width<17, Unpacker, kHasBias>(
            in, out, opt.batch_size, opt.bit_offset, opt.max_read_bytes, bias);
      case 18:
        return unpack_width<18, Unpacker, kHasBias>(
            in, out, opt.batch_size, opt.bit_offset, opt.max_read_bytes, bias);
      case 19:
        return unpack_width<19, Unpacker, kHasBias>(
            in, out, opt.batch_size, opt.bit_offset, opt.max_read_bytes, bias);
      case 20:
        return unpack_width<20, Unpacker, kHasBias>(
            in, out, opt.batch_size, opt.bit_offset, opt.max_read_bytes, bias);
      case 21:
        return unpack_width<21, Unpacker, kHasBias>(
            in, out, opt.batch_size, opt.bit_offset, opt.max_read_bytes, bias);
      case 22:
        return unpack_width<22, Unpacker, kHasBias>(
            in, out, opt.batch_size, opt.bit_offset, opt.max_read_bytes, bias);
      case 23:
        return unpack_width<23, Unpacker, kHasBias>(
            in, out, opt.batch_size, opt.bit_offset, opt.max_read_bytes, bias);
      case 24:
        return unpack_width<24, Unpacker, kHasBias>(
            in, out, opt.batch_size, opt.bit_offset, opt.max_read_bytes, bias);
      case 25:
        return unpack_width<25, Unpacker, kHasBias>(
            in, out, opt.batch_size, opt.bit_offset, opt.max_read_bytes, bias);
      case 26:
        return unpack_width<26, Unpacker, kHasBias>(
            in, out, opt.batch_size, opt.bit_offset, opt.max_read_bytes, bias);
      case 27:
        return unpack_width<27, Unpacker, kHasBias>(
            in, out, opt.batch_size, opt.bit_offset, opt.max_read_bytes, bias);
      case 28:
        return unpack_width<28, Unpacker, kHasBias>(
            in, out, opt.batch_size, opt.bit_offset, opt.max_read_bytes, bias);
      case 29:
        return unpack_width<29, Unpacker, kHasBias>(
            in, out, opt.batch_size, opt.bit_offset, opt.max_read_bytes, bias);
      case 30:
        return unpack_width<30, Unpacker, kHasBias>(
            in, out, opt.batch_size, opt.bit_offset, opt.max_read_bytes, bias);
      case 31:
        return unpack_width<31, Unpacker, kHasBias>(
            in, out, opt.batch_size, opt.bit_offset, opt.max_read_bytes, bias);
      case 32:
        return unpack_width<32, Unpacker, kHasBias>(
            in, out, opt.batch_size, opt.bit_offset, opt.max_read_bytes, bias);
      case 33:
        return unpack_width<33, Unpacker, kHasBias>(
            in, out, opt.batch_size, opt.bit_offset, opt.max_read_bytes, bias);
      case 34:
        return unpack_width<34, Unpacker, kHasBias>(
            in, out, opt.batch_size, opt.bit_offset, opt.max_read_bytes, bias);
      case 35:
        return unpack_width<35, Unpacker, kHasBias>(
            in, out, opt.batch_size, opt.bit_offset, opt.max_read_bytes, bias);
      case 36:
        return unpack_width<36, Unpacker, kHasBias>(
            in, out, opt.batch_size, opt.bit_offset, opt.max_read_bytes, bias);
      case 37:
        return unpack_width<37, Unpacker, kHasBias>(
            in, out, opt.batch_size, opt.bit_offset, opt.max_read_bytes, bias);
      case 38:
        return unpack_width<38, Unpacker, kHasBias>(
            in, out, opt.batch_size, opt.bit_offset, opt.max_read_bytes, bias);
      case 39:
        return unpack_width<39, Unpacker, kHasBias>(
            in, out, opt.batch_size, opt.bit_offset, opt.max_read_bytes, bias);
      case 40:
        return unpack_width<40, Unpacker, kHasBias>(
            in, out, opt.batch_size, opt.bit_offset, opt.max_read_bytes, bias);
      case 41:
        return unpack_width<41, Unpacker, kHasBias>(
            in, out, opt.batch_size, opt.bit_offset, opt.max_read_bytes, bias);
      case 42:
        return unpack_width<42, Unpacker, kHasBias>(
            in, out, opt.batch_size, opt.bit_offset, opt.max_read_bytes, bias);
      case 43:
        return unpack_width<43, Unpacker, kHasBias>(
            in, out, opt.batch_size, opt.bit_offset, opt.max_read_bytes, bias);
      case 44:
        return unpack_width<44, Unpacker, kHasBias>(
            in, out, opt.batch_size, opt.bit_offset, opt.max_read_bytes, bias);
      case 45:
        return unpack_width<45, Unpacker, kHasBias>(
            in, out, opt.batch_size, opt.bit_offset, opt.max_read_bytes, bias);
      case 46:
        return unpack_width<46, Unpacker, kHasBias>(
            in, out, opt.batch_size, opt.bit_offset, opt.max_read_bytes, bias);
      case 47:
        return unpack_width<47, Unpacker, kHasBias>(
            in, out, opt.batch_size, opt.bit_offset, opt.max_read_bytes, bias);
      case 48:
        return unpack_width<48, Unpacker, kHasBias>(
            in, out, opt.batch_size, opt.bit_offset, opt.max_read_bytes, bias);
      case 49:
        return unpack_width<49, Unpacker, kHasBias>(
            in, out, opt.batch_size, opt.bit_offset, opt.max_read_bytes, bias);
      case 50:
        return unpack_width<50, Unpacker, kHasBias>(
            in, out, opt.batch_size, opt.bit_offset, opt.max_read_bytes, bias);
      case 51:
        return unpack_width<51, Unpacker, kHasBias>(
            in, out, opt.batch_size, opt.bit_offset, opt.max_read_bytes, bias);
      case 52:
        return unpack_width<52, Unpacker, kHasBias>(
            in, out, opt.batch_size, opt.bit_offset, opt.max_read_bytes, bias);
      case 53:
        return unpack_width<53, Unpacker, kHasBias>(
            in, out, opt.batch_size, opt.bit_offset, opt.max_read_bytes, bias);
      case 54:
        return unpack_width<54, Unpacker, kHasBias>(
            in, out, opt.batch_size, opt.bit_offset, opt.max_read_bytes, bias);
      case 55:
        return unpack_width<55, Unpacker, kHasBias>(
            in, out, opt.batch_size, opt.bit_offset, opt.max_read_bytes, bias);
      case 56:
        return unpack_width<56, Unpacker, kHasBias>(
            in, out, opt.batch_size, opt.bit_offset, opt.max_read_bytes, bias);
      case 57:
        return unpack_width<57, Unpacker, kHasBias>(
            in, out, opt.batch_size, opt.bit_offset, opt.max_read_bytes, bias);
      case 58:
        return unpack_width<58, Unpacker, kHasBias>(
            in, out, opt.batch_size, opt.bit_offset, opt.max_read_bytes, bias);
      case 59:
        return unpack_width<59, Unpacker, kHasBias>(
            in, out, opt.batch_size, opt.bit_offset, opt.max_read_bytes, bias);
      case 60:
        return unpack_width<60, Unpacker, kHasBias>(
            in, out, opt.batch_size, opt.bit_offset, opt.max_read_bytes, bias);
      case 61:
        return unpack_width<61, Unpacker, kHasBias>(
            in, out, opt.batch_size, opt.bit_offset, opt.max_read_bytes, bias);
      case 62:
        return unpack_width<62, Unpacker, kHasBias>(
            in, out, opt.batch_size, opt.bit_offset, opt.max_read_bytes, bias);
      case 63:
        return unpack_width<63, Unpacker, kHasBias>(
            in, out, opt.batch_size, opt.bit_offset, opt.max_read_bytes, bias);
      case 64:
        return unpack_width<64, Unpacker, kHasBias>(
            in, out, opt.batch_size, opt.bit_offset, opt.max_read_bytes, bias);
    }
  }
  ARROW_DCHECK(false) << "Unsupported num_bits " << opt.bit_width;
}
}  // namespace arrow::internal::bpacking
