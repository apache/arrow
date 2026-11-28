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

#include <cassert>
#include <cstdint>
#include <cstring>
#include <type_traits>

#include "arrow/util/bit_run_reader.h"
#include "arrow/util/visibility.h"

namespace arrow::util::internal {

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

  arrow::internal::SetBitRunReader reader(valid_bits, valid_bits_offset, num_values);
  while (true) {
    const auto run = reader.NextRun();
    if (run.length == 0) {
      break;
    }
    std::memcpy(output + num_valid_values, src + run.position, run.length * sizeof(T));
    num_valid_values += static_cast<int32_t>(run.length);
  }

  return num_valid_values;
}

/// \brief Relocate values according to a validity bitmap, to the right
///
/// Run based implementation, see SpacedExpandRightward. Only the trailing
/// `null_count` slots of the buffer are zeroed, the other null slots keep
/// whatever value they held.
template <typename T>
inline void SpacedExpandRightwardScalar(T* buffer, int num_values, int null_count,
                                        const uint8_t* valid_bits,
                                        int64_t valid_bits_offset) {
  // Point to end as we add the spacing from the back.
  int idx_decode = num_values - null_count;

  // Depending on the number of nulls, some of the value slots in buffer may
  // be uninitialized, and this will cause valgrind warnings / potentially UB
  memset(static_cast<void*>(buffer + idx_decode), 0, null_count * sizeof(T));
  if (idx_decode == 0) {
    // All nulls, nothing more to do
    return;
  }

  arrow::internal::ReverseSetBitRunReader reader(valid_bits, valid_bits_offset,
                                                 num_values);
  while (true) {
    const auto run = reader.NextRun();
    if (run.length == 0) {
      break;
    }
    idx_decode -= static_cast<int32_t>(run.length);
    assert(idx_decode >= 0);
    if (idx_decode == run.position) {
      // We have come to the point where no more expansion is required: the remaining
      // values are already in their final position.
      return;
    }
    // Source and destination may overlap if run.length > 1
    memmove(buffer + run.position, buffer + idx_decode, run.length * sizeof(T));
  }

  // Otherwise caller gave an incorrect null_count
  assert(idx_decode == 0);
}

/// \brief SpacedExpandRightward for 4 byte values, dispatched on the CPU
ARROW_EXPORT void SpacedExpandRightward32(void* buffer, int num_values, int null_count,
                                          const uint8_t* valid_bits,
                                          int64_t valid_bits_offset);

/// \brief SpacedExpandRightward for 8 byte values, dispatched on the CPU
ARROW_EXPORT void SpacedExpandRightward64(void* buffer, int num_values, int null_count,
                                          const uint8_t* valid_bits,
                                          int64_t valid_bits_offset);

/// \brief Relocate values according to a validity bitmap, to the right
///
/// Non-null values should initially be densely packed at the left of the buffer.
/// This method spreads the values out according to the given validity bitmap.
///
/// 4 and 8 byte values go through an implementation dispatched on the CPU,
/// which zeroes every null slot. Other widths use SpacedExpandRightwardScalar,
/// which only zeroes the trailing `null_count` slots.
///
/// \param[in, out] buffer the in-place buffer
/// \param[in] num_values total size of buffer including null slots
/// \param[in] null_count number of null slots
/// \param[in] valid_bits bitmap data indicating position of valid slots
/// \param[in] valid_bits_offset offset into valid_bits
template <typename T>
inline void SpacedExpandRightward(T* buffer, int num_values, int null_count,
                                  const uint8_t* valid_bits, int64_t valid_bits_offset) {
  if constexpr (std::is_trivially_copyable_v<T> && sizeof(T) == 4) {
    SpacedExpandRightward32(buffer, num_values, null_count, valid_bits,
                            valid_bits_offset);
  } else if constexpr (std::is_trivially_copyable_v<T> && sizeof(T) == 8) {
    SpacedExpandRightward64(buffer, num_values, null_count, valid_bits,
                            valid_bits_offset);
  } else {
    SpacedExpandRightwardScalar(buffer, num_values, null_count, valid_bits,
                                valid_bits_offset);
  }
}

/// \brief Spread values out according to a validity bitmap
///
/// Run based implementation of SpacedExpand.
template <typename T>
inline void SpacedExpandScalar(const T* src, int num_values, int null_count,
                               const uint8_t* valid_bits, int64_t valid_bits_offset,
                               T* output) {
  // End of the last run: the slots between it and the next run are nulls.
  int64_t nulls_begin = 0;
  int64_t idx_src = 0;
  arrow::internal::SetBitRunReader reader(valid_bits, valid_bits_offset, num_values);
  while (true) {
    const auto run = reader.NextRun();
    if (run.length == 0) {
      break;
    }
    memset(static_cast<void*>(output + nulls_begin), 0,
           (run.position - nulls_begin) * sizeof(T));
    memcpy(static_cast<void*>(output + run.position), src + idx_src,
           run.length * sizeof(T));
    idx_src += run.length;
    nulls_begin = run.position + run.length;
  }
  assert(idx_src == num_values - null_count);
  memset(static_cast<void*>(output + nulls_begin), 0,
         (num_values - nulls_begin) * sizeof(T));
}

/// \brief SpacedExpand for 4 byte values, dispatched on the CPU
ARROW_EXPORT void SpacedExpand32(const void* src, int num_values, int null_count,
                                 const uint8_t* valid_bits, int64_t valid_bits_offset,
                                 void* output);

/// \brief SpacedExpand for 8 byte values, dispatched on the CPU
ARROW_EXPORT void SpacedExpand64(const void* src, int num_values, int null_count,
                                 const uint8_t* valid_bits, int64_t valid_bits_offset,
                                 void* output);

/// \brief Spread values out according to a validity bitmap
///
/// Out of place version of SpacedExpandRightward: the `num_values - null_count`
/// values of `src` go to the valid slots of `output`, in order, and every null
/// slot is zeroed. `src` needs not be aligned, and must not overlap `output`.
///
/// \param[in] src the dense values
/// \param[in] num_values total size of output including null slots
/// \param[in] null_count number of null slots
/// \param[in] valid_bits bitmap data indicating position of valid slots
/// \param[in] valid_bits_offset offset into valid_bits
/// \param[out] output the spaced values
template <typename T>
inline void SpacedExpand(const T* src, int num_values, int null_count,
                         const uint8_t* valid_bits, int64_t valid_bits_offset,
                         T* output) {
  if constexpr (std::is_trivially_copyable_v<T> && sizeof(T) == 4) {
    SpacedExpand32(src, num_values, null_count, valid_bits, valid_bits_offset, output);
  } else if constexpr (std::is_trivially_copyable_v<T> && sizeof(T) == 8) {
    SpacedExpand64(src, num_values, null_count, valid_bits, valid_bits_offset, output);
  } else {
    SpacedExpandScalar(src, num_values, null_count, valid_bits, valid_bits_offset,
                       output);
  }
}

/// \brief Relocate values according to a validity bitmap, to the left
///
/// Non-null values should initially be densely packed at the right of the buffer.
/// This method spreads the values out according to the given validity bitmap.
/// Null entries are zero-initialized.
///
/// \param[in, out] buffer the in-place buffer
/// \param[in] byte_width the byte width of values
/// \param[in] length total length of buffer including null slots
/// \param[in] null_count number of null slots
/// \param[in] valid_bits bitmap data indicating position of valid slots
/// \param[in] valid_bits_offset offset into valid_bits
inline void SpacedExpandLeftward(uint8_t* buffer, int byte_width, int64_t length,
                                 int64_t null_count, const uint8_t* valid_bits,
                                 int64_t valid_bits_offset) {
  // Point to start of values.
  int64_t idx_decode = byte_width * null_count;

  // Depending on the number of nulls, some of the value slots in buffer may
  // be uninitialized, and this will cause valgrind warnings / potentially UB
  memset(buffer, 0, idx_decode);

  arrow::internal::SetBitRunReader reader(valid_bits, valid_bits_offset, length);
  while (true) {
    const auto run = reader.NextRun();
    if (run.length == 0) {
      break;
    }
    if (idx_decode == run.position * byte_width) {
      // We have come to the point where no more expansion is required: the remaining
      // values are already in their final position.
      return;
    }
    // Source and destination may overlap if run.length > 1
    memmove(buffer + run.position * byte_width, buffer + idx_decode,
            run.length * byte_width);
    idx_decode += run.length * byte_width;
  }

  // Otherwise caller gave an incorrect null_count
  assert(idx_decode == length * byte_width);
}

}  // namespace arrow::util::internal
