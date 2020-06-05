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
#include <memory>

#include "arrow/buffer.h"
#include "arrow/memory_pool.h"
#include "arrow/result.h"
#include "arrow/util/bit_util.h"
#include "arrow/util/bitmap_reader.h"
#include "arrow/util/visibility.h"

namespace arrow {
namespace internal {

// A function that visits each bit in a bitmap and calls a visitor function with a
// boolean representation of that bit. This is intended to be analogous to
// GenerateBits.
template <class Visitor>
void VisitBits(const uint8_t* bitmap, int64_t start_offset, int64_t length,
               Visitor&& visit) {
  BitmapReader reader(bitmap, start_offset, length);
  for (int64_t index = 0; index < length; ++index) {
    visit(reader.IsSet());
    reader.Next();
  }
}

// Like VisitBits(), but unrolls its main loop for better performance.
template <class Visitor>
void VisitBitsUnrolled(const uint8_t* bitmap, int64_t start_offset, int64_t length,
                       Visitor&& visit) {
  if (length == 0) {
    return;
  }

  // Start by visiting any bits preceding the first full byte.
  int64_t num_bits_before_full_bytes =
      BitUtil::RoundUpToMultipleOf8(start_offset) - start_offset;
  // Truncate num_bits_before_full_bytes if it is greater than length.
  if (num_bits_before_full_bytes > length) {
    num_bits_before_full_bytes = length;
  }
  // Use the non loop-unrolled VisitBits since we don't want to add branches
  VisitBits<Visitor>(bitmap, start_offset, num_bits_before_full_bytes, visit);

  // Shift the start pointer to the first full byte and compute the
  // number of full bytes to be read.
  const uint8_t* first_full_byte = bitmap + BitUtil::CeilDiv(start_offset, 8);
  const int64_t num_full_bytes = (length - num_bits_before_full_bytes) / 8;

  // Iterate over each full byte of the input bitmap and call the visitor in
  // a loop-unrolled manner.
  for (int64_t byte_index = 0; byte_index < num_full_bytes; ++byte_index) {
    // Get the current bit-packed byte value from the bitmap.
    const uint8_t byte = *(first_full_byte + byte_index);

    // Execute the visitor function on each bit of the current byte.
    visit(BitUtil::GetBitFromByte(byte, 0));
    visit(BitUtil::GetBitFromByte(byte, 1));
    visit(BitUtil::GetBitFromByte(byte, 2));
    visit(BitUtil::GetBitFromByte(byte, 3));
    visit(BitUtil::GetBitFromByte(byte, 4));
    visit(BitUtil::GetBitFromByte(byte, 5));
    visit(BitUtil::GetBitFromByte(byte, 6));
    visit(BitUtil::GetBitFromByte(byte, 7));
  }

  // Write any leftover bits in the last byte.
  const int64_t num_bits_after_full_bytes = (length - num_bits_before_full_bytes) % 8;
  VisitBits<Visitor>(first_full_byte + num_full_bytes, 0, num_bits_after_full_bytes,
                     visit);
}

// ----------------------------------------------------------------------
// Bitmap utilities

/// Copy a bit range of an existing bitmap
///
/// \param[in] pool memory pool to allocate memory from
/// \param[in] bitmap source data
/// \param[in] offset bit offset into the source data
/// \param[in] length number of bits to copy
///
/// \return Status message
ARROW_EXPORT
Result<std::shared_ptr<Buffer>> CopyBitmap(MemoryPool* pool, const uint8_t* bitmap,
                                           int64_t offset, int64_t length);

/// Copy a bit range of an existing bitmap into an existing bitmap
///
/// \param[in] bitmap source data
/// \param[in] offset bit offset into the source data
/// \param[in] length number of bits to copy
/// \param[in] dest_offset bit offset into the destination
/// \param[in] restore_trailing_bits don't clobber bits outside the destination range
/// \param[out] dest the destination buffer, must have at least space for
/// (offset + length) bits
ARROW_EXPORT
void CopyBitmap(const uint8_t* bitmap, int64_t offset, int64_t length, uint8_t* dest,
                int64_t dest_offset, bool restore_trailing_bits = true);

/// Invert a bit range of an existing bitmap into an existing bitmap
///
/// \param[in] bitmap source data
/// \param[in] offset bit offset into the source data
/// \param[in] length number of bits to copy
/// \param[in] dest_offset bit offset into the destination
/// \param[out] dest the destination buffer, must have at least space for
/// (offset + length) bits
ARROW_EXPORT
void InvertBitmap(const uint8_t* bitmap, int64_t offset, int64_t length, uint8_t* dest,
                  int64_t dest_offset);

/// Invert a bit range of an existing bitmap
///
/// \param[in] pool memory pool to allocate memory from
/// \param[in] bitmap source data
/// \param[in] offset bit offset into the source data
/// \param[in] length number of bits to copy
///
/// \return Status message
ARROW_EXPORT
Result<std::shared_ptr<Buffer>> InvertBitmap(MemoryPool* pool, const uint8_t* bitmap,
                                             int64_t offset, int64_t length);

/// Compute the number of 1's in the given data array
///
/// \param[in] data a packed LSB-ordered bitmap as a byte array
/// \param[in] bit_offset a bitwise offset into the bitmap
/// \param[in] length the number of bits to inspect in the bitmap relative to
/// the offset
///
/// \return The number of set (1) bits in the range
ARROW_EXPORT
int64_t CountSetBits(const uint8_t* data, int64_t bit_offset, int64_t length);

ARROW_EXPORT
bool BitmapEquals(const uint8_t* left, int64_t left_offset, const uint8_t* right,
                  int64_t right_offset, int64_t bit_length);

/// \brief Do a "bitmap and" on right and left buffers starting at
/// their respective bit-offsets for the given bit-length and put
/// the results in out_buffer starting at the given bit-offset.
///
/// out_buffer will be allocated and initialized to zeros using pool before
/// the operation.
ARROW_EXPORT
Result<std::shared_ptr<Buffer>> BitmapAnd(MemoryPool* pool, const uint8_t* left,
                                          int64_t left_offset, const uint8_t* right,
                                          int64_t right_offset, int64_t length,
                                          int64_t out_offset);

/// \brief Do a "bitmap and" on right and left buffers starting at
/// their respective bit-offsets for the given bit-length and put
/// the results in out starting at the given bit-offset.
ARROW_EXPORT
void BitmapAnd(const uint8_t* left, int64_t left_offset, const uint8_t* right,
               int64_t right_offset, int64_t length, int64_t out_offset, uint8_t* out);

/// \brief Do a "bitmap or" for the given bit length on right and left buffers
/// starting at their respective bit-offsets and put the results in out_buffer
/// starting at the given bit-offset.
///
/// out_buffer will be allocated and initialized to zeros using pool before
/// the operation.
ARROW_EXPORT
Result<std::shared_ptr<Buffer>> BitmapOr(MemoryPool* pool, const uint8_t* left,
                                         int64_t left_offset, const uint8_t* right,
                                         int64_t right_offset, int64_t length,
                                         int64_t out_offset);

/// \brief Do a "bitmap or" for the given bit length on right and left buffers
/// starting at their respective bit-offsets and put the results in out
/// starting at the given bit-offset.
ARROW_EXPORT
void BitmapOr(const uint8_t* left, int64_t left_offset, const uint8_t* right,
              int64_t right_offset, int64_t length, int64_t out_offset, uint8_t* out);

/// \brief Do a "bitmap xor" for the given bit-length on right and left
/// buffers starting at their respective bit-offsets and put the results in
/// out_buffer starting at the given bit offset.
///
/// out_buffer will be allocated and initialized to zeros using pool before
/// the operation.
ARROW_EXPORT
Result<std::shared_ptr<Buffer>> BitmapXor(MemoryPool* pool, const uint8_t* left,
                                          int64_t left_offset, const uint8_t* right,
                                          int64_t right_offset, int64_t length,
                                          int64_t out_offset);

/// \brief Do a "bitmap xor" for the given bit-length on right and left
/// buffers starting at their respective bit-offsets and put the results in
/// out starting at the given bit offset.
ARROW_EXPORT
void BitmapXor(const uint8_t* left, int64_t left_offset, const uint8_t* right,
               int64_t right_offset, int64_t length, int64_t out_offset, uint8_t* out);

/// \brief Generate Bitmap with all position to `value` except for one found
/// at `straggler_pos`.
ARROW_EXPORT
Result<std::shared_ptr<Buffer>> BitmapAllButOne(MemoryPool* pool, int64_t length,
                                                int64_t straggler_pos, bool value = true);

/// \brief Convert vector of bytes to bitmap buffer
ARROW_EXPORT
Result<std::shared_ptr<Buffer>> BytesToBits(const std::vector<uint8_t>&,
                                            MemoryPool* pool = default_memory_pool());

}  // namespace internal
}  // namespace arrow
