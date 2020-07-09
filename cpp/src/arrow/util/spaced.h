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

#include "arrow/util/align_util.h"
#include "arrow/util/bit_block_counter.h"
#include "arrow/util/bitmap_reader.h"

namespace arrow {
namespace util {
namespace internal {

/// \brief Compress the buffer to spaced, excluding the null entries.
///
/// \param[in] src the source buffer
/// \param[in] num_values the size of source buffer
/// \param[in] valid_bits bitmap data indicating position of valid slots
/// \param[in] valid_bits_offset offset into valid_bits
/// \param[out] output the output buffer spaced
/// \return The size of spaced buffer.
template <typename T>
inline int SpacedCompress(const T* src, int num_values, const uint8_t* valid_bits,
                          int64_t valid_bits_offset, T* output) {
  int num_valid_values = 0;
  int idx_src = 0;

  const auto p =
      arrow::internal::BitmapWordAlign<1>(valid_bits, valid_bits_offset, num_values);
  // First handle the leading bits
  const int leading_bits = static_cast<int>(p.leading_bits);
  while (idx_src < leading_bits) {
    if (BitUtil::GetBit(valid_bits, valid_bits_offset)) {
      output[num_valid_values] = src[idx_src];
      num_valid_values++;
    }
    idx_src++;
    valid_bits_offset++;
  }

  // The aligned parts scanned with BitBlockCounter
  arrow::internal::BitBlockCounter data_counter(valid_bits, valid_bits_offset,
                                                num_values - leading_bits);
  auto current_block = data_counter.NextWord();
  while (idx_src < num_values) {
    if (current_block.AllSet()) {  // All true values
      int run_length = 0;
      // Scan forward until a block that has some false values (or the end)
      while (current_block.length > 0 && current_block.AllSet()) {
        run_length += current_block.length;
        current_block = data_counter.NextWord();
      }
      // Fill all valid values of this scan
      std::memcpy(&output[num_valid_values], &src[idx_src], run_length * sizeof(T));
      num_valid_values += run_length;
      idx_src += run_length;
      valid_bits_offset += run_length;
      // The current_block already computed, advance to next loop
      continue;
    } else if (!current_block.NoneSet()) {  // Some values are null
      arrow::internal::BitmapReader valid_bits_reader(valid_bits, valid_bits_offset,
                                                      current_block.length);
      for (int64_t i = 0; i < current_block.length; i++) {
        if (valid_bits_reader.IsSet()) {
          output[num_valid_values] = src[idx_src];
          num_valid_values++;
        }
        idx_src++;
        valid_bits_reader.Next();
      }
      valid_bits_offset += current_block.length;
    } else {  // All null values
      idx_src += current_block.length;
      valid_bits_offset += current_block.length;
    }
    current_block = data_counter.NextWord();
  }

  return num_valid_values;
}

/// \brief Relocate values in buffer into positions of non-null values as indicated by
/// a validity bitmap.
///
/// \param[in, out] buffer the in-place buffer
/// \param[in] num_values total size of buffer including null slots
/// \param[in] null_count number of null slots
/// \param[in] valid_bits bitmap data indicating position of valid slots
/// \param[in] valid_bits_offset offset into valid_bits
/// \return The number of values expanded, including nulls.
template <typename T>
inline int SpacedExpand(T* buffer, int num_values, int null_count,
                        const uint8_t* valid_bits, int64_t valid_bits_offset) {
  constexpr int64_t kBatchSize = arrow::internal::BitBlockCounter::kWordBits;
  constexpr int64_t kBatchByteSize = kBatchSize / 8;

  // Point to end as we add the spacing from the back.
  int idx_decode = num_values - null_count - 1;
  int idx_buffer = num_values - 1;
  int64_t idx_valid_bits = valid_bits_offset + idx_buffer;

  // Depending on the number of nulls, some of the value slots in buffer may
  // be uninitialized, and this will cause valgrind warnings / potentially UB
  std::memset(static_cast<void*>(&buffer[idx_decode + 1]), 0, null_count * sizeof(T));
  if (idx_decode < 0) {
    return num_values;
  }

  // Access the bitmap by aligned way
  const auto p = arrow::internal::BitmapWordAlign<kBatchByteSize>(
      valid_bits, valid_bits_offset, num_values);

  // The trailing bits
  auto trailing_bits = p.trailing_bits;
  while (trailing_bits > 0) {
    if (BitUtil::GetBit(valid_bits, idx_valid_bits)) {
      buffer[idx_buffer] = buffer[idx_decode];
      idx_decode--;
    }

    idx_buffer--;
    idx_valid_bits--;
    trailing_bits--;
  }

  // The aligned parts scanned from the back with BitBlockCounter
  auto aligned_words = p.aligned_words;
  T tmp[kBatchSize];
  std::memset(&tmp, 0, kBatchSize * sizeof(T));
  while (aligned_words > 0 && idx_decode < idx_buffer) {
    const uint8_t* aligned_bits = p.aligned_start + (aligned_words - 1) * kBatchByteSize;
    arrow::internal::BitBlockCounter data_counter(aligned_bits, 0, kBatchSize);
    const auto current_block = data_counter.NextWord();
    if (current_block.AllSet()) {  // All true values
      idx_buffer -= kBatchSize;
      idx_decode -= kBatchSize;
      if (idx_buffer >= idx_decode + static_cast<int>(kBatchSize)) {
        std::memcpy(&buffer[idx_buffer + 1], &buffer[idx_decode + 1],
                    kBatchSize * sizeof(T));
      } else {
        // Exchange the data to avoid the buffer overlap
        std::memcpy(tmp, &buffer[idx_decode + 1], kBatchSize * sizeof(T));
        std::memcpy(&buffer[idx_buffer + 1], tmp, kBatchSize * sizeof(T));
      }
    } else if (!current_block.NoneSet()) {  // Some values are null
      arrow::internal::BitmapReader valid_bits_reader(aligned_bits, 0, kBatchSize);
      idx_buffer -= kBatchSize;
      idx_decode -= current_block.popcount;

      // Foward scan and pack the target data to temp
      int idx = idx_decode + 1;
      for (uint64_t i = 0; i < kBatchSize; i++) {
        if (valid_bits_reader.IsSet()) {
          tmp[i] = buffer[idx];
          idx++;
        }
        valid_bits_reader.Next();
      }
      std::memcpy(&buffer[idx_buffer + 1], tmp, kBatchSize * sizeof(T));
    } else {  // All null values
      idx_buffer -= kBatchSize;
    }

    idx_valid_bits -= kBatchSize;
    aligned_words--;
  }

  // The remaining leading bits
  auto leading_bits = p.leading_bits;
  while (leading_bits > 0) {
    if (BitUtil::GetBit(valid_bits, idx_valid_bits)) {
      buffer[idx_buffer] = buffer[idx_decode];
      idx_decode--;
    }
    leading_bits--;
    idx_buffer--;
    idx_valid_bits--;
  }

  return num_values;
}

}  // namespace internal
}  // namespace util
}  // namespace arrow
