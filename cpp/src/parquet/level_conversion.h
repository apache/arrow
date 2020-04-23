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

#include <cstdint>
#include "arrow/util/bit_util.h"

#if defined(ARROW_HAVE_BMI2)
#include "x86intrin.h"
#endif

namespace parquet {
namespace internal {
// These APIs are likely to be revised as part of ARROW-8494 to reduce duplicate code.
// They currently represent minimal functionality for vectorized computation of definition
// levels.

/// Builds a bitmap by applying predicate to the level vector provided.
///
/// \param[in] levels Rep or def level array.
/// \param[in] num_levels The number of levels to process (must be [0, 64])
/// \param[in] predicate The predicate to apply (must have the signature `bool
/// predicate(int16_t)`.
/// \returns The bitmap using least significant "bit" ordering.
///
/// N.B. Correct byte ordering is dependent on little-endian architectures.
///
template <typename Predicate>
uint64_t LevelsToBitmap(const int16_t* levels, int64_t num_levels, Predicate predicate) {
  // Both clang and GCC can vectorize this automatically with AVX2.
  uint64_t mask = 0;
  for (int x = 0; x < num_levels; x++) {
    mask |= static_cast<int64_t>(predicate(levels[x]) ? 1 : 0) << x;
  }
  return mask;
}

/// Builds a  bitmap where each set bit indicates the correspond level is greater
/// than rhs.
static inline int64_t GreaterThanBitmap(const int16_t* levels, int64_t num_levels,
                                        int16_t rhs) {
  return LevelsToBitmap(levels, num_levels, [&](int16_t value) { return value > rhs; });
}

/// Append bits number_of_bits from new_bits to valid_bits and valid_bits_offset.
///
/// \param[in] new_bits The zero-padded bitmap to append.
/// \param[in] number_of_bits The number of bits to append from new_bits.
/// \param[in] valid_bits_length The number of bytes allocated in valid_bits.
/// \param[in] valid_bits_offset The bit-offset at which to start appending new bits.
/// \param[in,out] valid_bits The validity bitmap to append to.
/// \returns The new bit offset inside of valid_bits.
static inline int64_t AppendBitmap(uint64_t new_bits, int64_t number_of_bits,
                                   int64_t valid_bits_length, int64_t valid_bits_offset,
                                   uint8_t* valid_bits) {
  // Selection masks to retrieve all low order bits for each bytes.
  constexpr uint64_t kLsbSelectionMasks[] = {
      0,  // unused.
      0x0101010101010101,
      0x0303030303030303,
      0x0707070707070707,
      0x0F0F0F0F0F0F0F0F,
      0x1F1F1F1F1F1F1F1F,
      0x3F3F3F3F3F3F3F3F,
      0x7F7F7F7F7F7F7F7F,
  };
  int64_t valid_byte_offset = valid_bits_offset / 8;
  int64_t bit_offset = valid_bits_offset % 8;

  int64_t new_offset = valid_bits_offset + number_of_bits;
  union ByteAddressableBitmap {
    explicit ByteAddressableBitmap(uint64_t mask) : mask(mask) {}
    uint64_t mask;
    uint8_t bytes[8];
  };

  if (bit_offset != 0) {
    int64_t bits_to_carry = 8 - bit_offset;
    // Get the mask the will select the lower order bits  (the ones to carry
    // over to the existing byte and shift up.
    const ByteAddressableBitmap carry_bits(kLsbSelectionMasks[bits_to_carry]);
    // Mask to select non-carried bits.
    const ByteAddressableBitmap inverse_selection(~carry_bits.mask);
    // Fill out the last incomplete byte in the output, by extracting the least
    // siginficant bits from the first byte.
    const ByteAddressableBitmap new_bitmap(new_bits);
    // valid bits should be a valid bitmask so all trailing bytes hsould be unset
    // so no mask is need to start.
    valid_bits[valid_byte_offset] =
        valid_bits[valid_byte_offset] |  // See above the
        (((new_bitmap.bytes[0] & carry_bits.bytes[0])) << bit_offset);

    // We illustrate logic with a 3-byte example in little endian/LSB order.
    // Note this ordering is the reversed from HEX masks above with are expressed
    // big-endian/MSB and shifts right move the bits to the left (division).
    // 0  1  2  3  4  5  6  7   8  9  10 11 12 13 14 15   16 17 18 19 20 21 22 23
    // Shifted mask should look like this assuming bit offset = 6:
    // 2  3  4  5  6  7  N  N   10 11 12 13 14 15  N  N   18 19 20 21 22 23  N  N
    // clang-format on
    uint64_t shifted_new_bits = (new_bits & inverse_selection.mask) >> bits_to_carry;
    // captured_carry:
    // 0  1  N  N  N  N  N  N   8  9  N  N  N   N  N  N   16 17  N  N  N  N  N  N
    uint64_t captured_carry = carry_bits.mask & new_bits;
    // mask_cary_bits:
    // N  N  N  N  N  N  8  9   N  N  N  N  N   N 16 17    N  N   N  N  N  N  N  N
    uint64_t mask_carry_bits = (captured_carry >> 8) << bit_offset;

    new_bits = shifted_new_bits | mask_carry_bits;
    // Don't overwrite the first byte
    valid_byte_offset += 1;
    number_of_bits -= bits_to_carry;
  }

  int64_t bytes_for_new_bits = ::arrow::BitUtil::BytesForBits(number_of_bits);
  if (valid_bits_length - ::arrow::BitUtil::BytesForBits(valid_bits_offset) >=
      static_cast<int64_t>(sizeof(new_bits))) {
    // This should be the common case and  inlined as a single instruction which
    // should be cheaper then the general case of calling mempcy, so it is likely
    // worth the extra branch.
    std::memcpy(valid_bits + valid_byte_offset, &new_bits, sizeof(new_bits));
  } else {
    std::memcpy(valid_bits + valid_byte_offset, &new_bits, bytes_for_new_bits);
  }
  return new_offset;
}

/// \brief Appends bit values to the validitdy bimap_valid bits, based on bitmaps
/// generated by GreaterThanBitmap, and the appropriate treshold definition_leve.
///
/// \param[in] new_bits Bitmap to append (intrepreted as Little-endian/LSB).
/// \param[in] new_bit_count The number of bits to append  from new_bits.
/// \param[in,out] validity_bitmap The validity bitmap to update.
/// \param[in,out] validity_bitmap_offset The offset to start appending bits to in
/// valid_bits (updated to latest bitmap).
/// \param[in,out] set_bit_count The number of set bits appended is added to
/// set_bit_count.
void AppendToValidityBitmap(uint64_t new_bits, int64_t new_bit_count,
                            uint8_t* validity_bitmap, int64_t* validity_bitmap_offset,
                            int64_t* set_bit_count) {
  int64_t min_valid_bits_size =
      ::arrow::BitUtil::BytesForBits(new_bit_count + *validity_bitmap_offset);

  *set_bit_count += ::arrow::BitUtil::PopCount(new_bits);
  *validity_bitmap_offset = AppendBitmap(new_bits, new_bit_count, min_valid_bits_size,
                                         *validity_bitmap_offset, validity_bitmap);
}

/// The same as AppendToValidityBitmap but only appends bits from bitmap that have
/// a corresponding bit set in selection_bitmap.
///
/// \returns The number of bits appended.
///
/// N.B. This is only implemented for archiectures that suppor the BMI2 instruction
/// set.
int64_t AppendSelectedBitsToValidityBitmap(uint64_t new_bits, uint64_t selection_bitmap,
                                           uint8_t* validity_bitmap,
                                           int64_t* validity_bitmap_offset,
                                           int64_t* set_bit_count) {
#if defined(ARROW_HAVE_BMI2)
  // If the parent list was empty at for the given slot it should not be added to the
  // bitmap.
  uint64_t selected_bits = _pext_u64(new_bits, selection_bitmap);
  int64_t selected_count =
      static_cast<int64_t>(::arrow::BitUtil::PopCount(selection_bitmap));

  AppendToValidityBitmap(selected_bits, selected_count, validity_bitmap,
                         validity_bitmap_offset, set_bit_count);
  return selected_count;
#else
  // We shouldn't get here.
  std::abort();
#endif
}

}  // namespace internal
}  // namespace parquet
