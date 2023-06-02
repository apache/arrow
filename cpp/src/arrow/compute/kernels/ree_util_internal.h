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

// Useful operations to implement kernels handling run-end encoded arrays

#include <algorithm>
#include <cstdint>
#include <limits>
#include <memory>

#include "arrow/array/data.h"
#include "arrow/compute/exec.h"
#include "arrow/compute/kernel.h"
#include "arrow/result.h"
#include "arrow/status.h"
#include "arrow/type_traits.h"
#include "arrow/util/bit_util.h"
#include "arrow/util/logging.h"

namespace arrow {
namespace compute {
namespace internal {
namespace ree_util {

template <typename ArrowType, bool in_has_validity_buffer,
          bool out_has_validity_buffer = in_has_validity_buffer, typename Enable = void>
struct ReadWriteValue {};

// Numeric and primitive C-compatible types
template <typename ArrowType, bool in_has_validity_buffer, bool out_has_validity_buffer>
class ReadWriteValue<ArrowType, in_has_validity_buffer, out_has_validity_buffer,
                     enable_if_has_c_type<ArrowType>> {
 public:
  using ValueRepr = typename ArrowType::c_type;

 private:
  const uint8_t* input_validity_;
  const uint8_t* input_values_;

  // Needed only by the writing functions
  uint8_t* output_validity_;
  uint8_t* output_values_;

 public:
  explicit ReadWriteValue(const ArraySpan& input_values_array,
                          ArrayData* output_values_array_data)
      : input_validity_(in_has_validity_buffer ? input_values_array.buffers[0].data
                                               : NULLPTR),
        input_values_(input_values_array.buffers[1].data),
        output_validity_((out_has_validity_buffer && output_values_array_data)
                             ? output_values_array_data->buffers[0]->mutable_data()
                             : NULLPTR),
        output_values_(output_values_array_data
                           ? output_values_array_data->buffers[1]->mutable_data()
                           : NULLPTR) {}

  [[nodiscard]] bool ReadValue(ValueRepr* out, int64_t read_offset) const {
    bool valid = true;
    if constexpr (in_has_validity_buffer) {
      valid = bit_util::GetBit(input_validity_, read_offset);
    }
    if constexpr (std::is_same_v<ArrowType, BooleanType>) {
      *out = bit_util::GetBit(input_values_, read_offset);
    } else {
      *out = (reinterpret_cast<const ValueRepr*>(input_values_))[read_offset];
    }
    return valid;
  }

  /// \brief Ensure padding is zeroed in validity bitmap.
  void ZeroValidityPadding(int64_t length) const {
    DCHECK(output_values_);
    if constexpr (out_has_validity_buffer) {
      DCHECK(output_validity_);
      const int64_t validity_buffer_size = bit_util::BytesForBits(length);
      output_validity_[validity_buffer_size - 1] = 0;
    }
  }

  void WriteValue(int64_t write_offset, bool valid, ValueRepr value) const {
    if constexpr (out_has_validity_buffer) {
      bit_util::SetBitTo(output_validity_, write_offset, valid);
    }
    if (valid) {
      if constexpr (std::is_same_v<ArrowType, BooleanType>) {
        bit_util::SetBitTo(output_values_, write_offset, value);
      } else {
        (reinterpret_cast<ValueRepr*>(output_values_))[write_offset] = value;
      }
    }
  }

  void WriteRun(int64_t write_offset, int64_t run_length, bool valid,
                ValueRepr value) const {
    if constexpr (out_has_validity_buffer) {
      bit_util::SetBitsTo(output_validity_, write_offset, run_length, valid);
    }
    if (valid) {
      if constexpr (std::is_same_v<ArrowType, BooleanType>) {
        bit_util::SetBitsTo(reinterpret_cast<uint8_t*>(output_values_), write_offset,
                            run_length, value);
      } else {
        auto* output_values_c = reinterpret_cast<ValueRepr*>(output_values_);
        std::fill(output_values_c + write_offset,
                  output_values_c + write_offset + run_length, value);
      }
    }
  }

  bool Compare(ValueRepr lhs, ValueRepr rhs) const { return lhs == rhs; }
};

// FixedSizeBinary, Decimal128
template <typename ArrowType, bool in_has_validity_buffer, bool out_has_validity_buffer>
class ReadWriteValue<ArrowType, in_has_validity_buffer, out_has_validity_buffer,
                     enable_if_fixed_size_binary<ArrowType>> {
 public:
  // Every value is represented as a pointer to byte_width_ bytes
  using ValueRepr = uint8_t const*;

 private:
  const uint8_t* input_validity_;
  const uint8_t* input_values_;

  // Needed only by the writing functions
  uint8_t* output_validity_;
  uint8_t* output_values_;

  const size_t byte_width_;

 public:
  ReadWriteValue(const ArraySpan& input_values_array, ArrayData* output_values_array_data)
      : input_validity_(in_has_validity_buffer ? input_values_array.buffers[0].data
                                               : NULLPTR),
        input_values_(input_values_array.buffers[1].data),
        output_validity_((out_has_validity_buffer && output_values_array_data)
                             ? output_values_array_data->buffers[0]->mutable_data()
                             : NULLPTR),
        output_values_(output_values_array_data
                           ? output_values_array_data->buffers[1]->mutable_data()
                           : NULLPTR),
        byte_width_(input_values_array.type->byte_width()) {}

  [[nodiscard]] bool ReadValue(ValueRepr* out, int64_t read_offset) const {
    bool valid = true;
    if constexpr (in_has_validity_buffer) {
      valid = bit_util::GetBit(input_validity_, read_offset);
    }
    *out = input_values_ + (read_offset * byte_width_);
    return valid;
  }

  /// \brief Ensure padding is zeroed in validity bitmap.
  void ZeroValidityPadding(int64_t length) const {
    DCHECK(output_values_);
    if constexpr (out_has_validity_buffer) {
      DCHECK(output_validity_);
      const int64_t validity_buffer_size = bit_util::BytesForBits(length);
      output_validity_[validity_buffer_size - 1] = 0;
    }
  }

  void WriteValue(int64_t write_offset, bool valid, ValueRepr value) const {
    if constexpr (out_has_validity_buffer) {
      bit_util::SetBitTo(output_validity_, write_offset, valid);
    }
    if (valid) {
      memcpy(output_values_ + (write_offset * byte_width_), value, byte_width_);
    }
  }

  void WriteRun(int64_t write_offset, int64_t run_length, bool valid,
                ValueRepr value) const {
    if constexpr (out_has_validity_buffer) {
      bit_util::SetBitsTo(output_validity_, write_offset, run_length, valid);
    }
    if (valid) {
      uint8_t* ptr = output_values_ + (write_offset * byte_width_);
      for (int64_t i = 0; i < run_length; ++i) {
        memcpy(ptr, value, byte_width_);
        ptr += byte_width_;
      }
    }
  }

  bool Compare(ValueRepr lhs, ValueRepr rhs) const {
    return memcmp(lhs, rhs, byte_width_) == 0;
  }
};

// Binary, String...
template <typename ArrowType, bool in_has_validity_buffer, bool out_has_validity_buffer>
class ReadWriteValue<ArrowType, in_has_validity_buffer, out_has_validity_buffer,
                     enable_if_base_binary<ArrowType>> {
 public:
  using ValueRepr = std::string_view;
  using offset_type = typename ArrowType::offset_type;

 private:
  const uint8_t* input_validity_;
  const offset_type* input_offsets_;
  const uint8_t* input_values_;

  // Needed only by the writing functions
  uint8_t* output_validity_;
  offset_type* output_offsets_;
  uint8_t* output_values_;

 public:
  ReadWriteValue(const ArraySpan& input_values_array, ArrayData* output_values_array_data)
      : input_validity_(in_has_validity_buffer ? input_values_array.buffers[0].data
                                               : NULLPTR),
        input_offsets_(input_values_array.template GetValues<offset_type>(1, 0)),
        input_values_(input_values_array.buffers[2].data),
        output_validity_((out_has_validity_buffer && output_values_array_data)
                             ? output_values_array_data->buffers[0]->mutable_data()
                             : NULLPTR),
        output_offsets_(
            output_values_array_data
                ? output_values_array_data->template GetMutableValues<offset_type>(1, 0)
                : NULLPTR),
        output_values_(output_values_array_data
                           ? output_values_array_data->buffers[2]->mutable_data()
                           : NULLPTR) {}

  [[nodiscard]] bool ReadValue(ValueRepr* out, int64_t read_offset) const {
    bool valid = true;
    if constexpr (in_has_validity_buffer) {
      valid = bit_util::GetBit(input_validity_, read_offset);
    }
    if (valid) {
      const offset_type offset0 = input_offsets_[read_offset];
      const offset_type offset1 = input_offsets_[read_offset + 1];
      *out = std::string_view(reinterpret_cast<const char*>(input_values_ + offset0),
                              offset1 - offset0);
    }
    return valid;
  }

  /// \brief Ensure padding is zeroed in validity bitmap.
  void ZeroValidityPadding(int64_t length) const {
    DCHECK(output_values_);
    if constexpr (out_has_validity_buffer) {
      DCHECK(output_validity_);
      const int64_t validity_buffer_size = bit_util::BytesForBits(length);
      output_validity_[validity_buffer_size - 1] = 0;
    }
  }

  void WriteValue(int64_t write_offset, bool valid, ValueRepr value) const {
    if constexpr (out_has_validity_buffer) {
      bit_util::SetBitTo(output_validity_, write_offset, valid);
    }
    const offset_type offset0 = output_offsets_[write_offset];
    const offset_type offset1 =
        offset0 + (valid ? static_cast<offset_type>(value.size()) : 0);
    output_offsets_[write_offset + 1] = offset1;
    if (valid) {
      memcpy(output_values_ + offset0, value.data(), value.size());
    }
  }

  void WriteRun(int64_t write_offset, int64_t run_length, bool valid,
                ValueRepr value) const {
    if constexpr (out_has_validity_buffer) {
      bit_util::SetBitsTo(output_validity_, write_offset, run_length, valid);
    }
    if (valid) {
      int64_t i = write_offset;
      offset_type offset = output_offsets_[i];
      while (i < write_offset + run_length) {
        memcpy(output_values_ + offset, value.data(), value.size());
        offset += static_cast<offset_type>(value.size());
        i += 1;
        output_offsets_[i] = offset;
      }
    } else {
      offset_type offset = output_offsets_[write_offset];
      offset_type* begin = output_offsets_ + write_offset + 1;
      std::fill(begin, begin + run_length, offset);
    }
  }

  bool Compare(ValueRepr lhs, ValueRepr rhs) const { return lhs == rhs; }
};

Result<std::shared_ptr<Buffer>> AllocateValuesBuffer(int64_t length, const DataType& type,
                                                     MemoryPool* pool,
                                                     int64_t data_buffer_size);

Result<std::shared_ptr<ArrayData>> PreallocateRunEndsArray(
    const std::shared_ptr<DataType>& run_end_type, int64_t physical_length,
    MemoryPool* pool);

Result<std::shared_ptr<ArrayData>> PreallocateValuesArray(
    const std::shared_ptr<DataType>& value_type, bool has_validity_buffer, int64_t length,
    int64_t null_count, MemoryPool* pool, int64_t data_buffer_size);

/// \brief Preallocate the ArrayData for the run-end encoded version
/// of the flat input array
///
/// \param data_buffer_size the size of the data buffer for string and binary types
Result<std::shared_ptr<ArrayData>> PreallocateREEArray(
    std::shared_ptr<RunEndEncodedType> ree_type, bool has_validity_buffer,
    int64_t logical_length, int64_t physical_length, int64_t physical_null_count,
    MemoryPool* pool, int64_t data_buffer_size);

/// \brief Writes a single run-end to the first slot of the pre-allocated
/// run-end encoded array in out
///
/// Pre-conditions:
/// - run_ends_data is of a valid run-ends type
/// - run_ends_data has at least one slot
/// - run_end > 0
/// - run_ends fits in the run-end type without overflow
void WriteSingleRunEnd(ArrayData* run_ends_data, int64_t run_end);

Result<std::shared_ptr<ArrayData>> MakeNullREEArray(
    const std::shared_ptr<DataType>& run_end_type, int64_t logical_length,
    MemoryPool* pool);

}  // namespace ree_util
}  // namespace internal
}  // namespace compute
}  // namespace arrow
