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

#include "arrow/array/builder_base.h"

#include <algorithm>  // IWYU pragma: keep
#include <array>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <iterator>
#include <limits>
#include <memory>
#include <string>
#include <type_traits>
#include <vector>

#include "arrow/buffer.h"
#include "arrow/memory_pool.h"
#include "arrow/status.h"
#include "arrow/type.h"
#include "arrow/type_traits.h"
#include "arrow/util/bit-util.h"
#include "arrow/util/macros.h"
#include "arrow/util/string_view.h"
#include "arrow/util/type_traits.h"
#include "arrow/util/visibility.h"

namespace arrow {

class Array;
struct ArrayData;

constexpr int64_t kMinBuilderCapacity = 1 << 5;
constexpr int64_t kListMaximumElements = std::numeric_limits<int32_t>::max() - 1;

/// Base class for all data array builders.
///
/// This class provides a facilities for incrementally building the null bitmap
/// (see Append methods) and as a side effect the current number of slots and
/// the null count.
///
/// \note Users are expected to use builders as one of the concrete types below.
/// For example, ArrayBuilder* pointing to BinaryBuilder should be downcast before use.
class ARROW_EXPORT ArrayBuilder {
 public:
  explicit ArrayBuilder(const std::shared_ptr<DataType>& type, MemoryPool* pool)
      : type_(type),
        pool_(pool),
        null_bitmap_(NULLPTR),
        null_count_(0),
        null_bitmap_data_(NULLPTR),
        length_(0),
        capacity_(0) {}

  virtual ~ArrayBuilder() = default;

  /// For nested types. Since the objects are owned by this class instance, we
  /// skip shared pointers and just return a raw pointer
  ArrayBuilder* child(int i) { return children_[i].get(); }

  int num_children() const { return static_cast<int>(children_.size()); }

  int64_t length() const { return length_; }
  int64_t null_count() const { return null_count_; }
  int64_t capacity() const { return capacity_; }

  /// \brief Ensure that enough memory has been allocated to fit the indicated
  /// number of total elements in the builder, including any that have already
  /// been appended. Does not account for reallocations that may be due to
  /// variable size data, like binary values. To make space for incremental
  /// appends, use Reserve instead.
  ///
  /// \param[in] capacity the minimum number of total array values to
  ///            accommodate. Must be greater than the current capacity.
  /// \return Status
  virtual Status Resize(int64_t capacity);

  /// \brief Ensure that there is enough space allocated to add the indicated
  /// number of elements without any further calls to Resize. The memory
  /// allocated is rounded up to the next highest power of 2 similar to memory
  /// allocations in STL containers like std::vector
  /// \param[in] additional_capacity the number of additional array values
  /// \return Status
  Status Reserve(int64_t additional_capacity);

  /// Reset the builder.
  virtual void Reset();

  /// For cases where raw data was memcpy'd into the internal buffers, allows us
  /// to advance the length of the builder. It is your responsibility to use
  /// this function responsibly.
  Status Advance(int64_t elements);

  /// \brief Return result of builder as an internal generic ArrayData
  /// object. Resets builder except for dictionary builder
  ///
  /// \param[out] out the finalized ArrayData object
  /// \return Status
  virtual Status FinishInternal(std::shared_ptr<ArrayData>* out) = 0;

  /// \brief Return result of builder as an Array object.
  ///
  /// The builder is reset except for DictionaryBuilder.
  ///
  /// \param[out] out the finalized Array object
  /// \return Status
  Status Finish(std::shared_ptr<Array>* out);

  std::shared_ptr<DataType> type() const { return type_; }

 protected:
  ArrayBuilder() {}

  /// Append to null bitmap
  Status AppendToBitmap(bool is_valid);

  /// Vector append. Treat each zero byte as a null.   If valid_bytes is null
  /// assume all of length bits are valid.
  Status AppendToBitmap(const uint8_t* valid_bytes, int64_t length);

  /// Set the next length bits to not null (i.e. valid).
  Status SetNotNull(int64_t length);

  // Unsafe operations (don't check capacity/don't resize)

  void UnsafeAppendNull() { UnsafeAppendToBitmap(false); }

  // Append to null bitmap, update the length
  void UnsafeAppendToBitmap(bool is_valid) {
    if (is_valid) {
      BitUtil::SetBit(null_bitmap_data_, length_);
    } else {
      ++null_count_;
    }
    ++length_;
  }

  template <typename IterType>
  void UnsafeAppendToBitmap(const IterType& begin, const IterType& end) {
    int64_t byte_offset = length_ / 8;
    int64_t bit_offset = length_ % 8;
    uint8_t bitset = null_bitmap_data_[byte_offset];

    for (auto iter = begin; iter != end; ++iter) {
      if (bit_offset == 8) {
        bit_offset = 0;
        null_bitmap_data_[byte_offset] = bitset;
        byte_offset++;
        // TODO: Except for the last byte, this shouldn't be needed
        bitset = null_bitmap_data_[byte_offset];
      }

      if (*iter) {
        bitset |= BitUtil::kBitmask[bit_offset];
      } else {
        bitset &= BitUtil::kFlippedBitmask[bit_offset];
        ++null_count_;
      }

      bit_offset++;
    }

    if (bit_offset != 0) {
      null_bitmap_data_[byte_offset] = bitset;
    }

    length_ += std::distance(begin, end);
  }

  // Vector append. Treat each zero byte as a nullzero. If valid_bytes is null
  // assume all of length bits are valid.
  void UnsafeAppendToBitmap(const uint8_t* valid_bytes, int64_t length);

  void UnsafeAppendToBitmap(const std::vector<bool>& is_valid);

  // Set the next length bits to not null (i.e. valid).
  void UnsafeSetNotNull(int64_t length);

  static Status TrimBuffer(const int64_t bytes_filled, ResizableBuffer* buffer);

  static Status CheckCapacity(int64_t new_capacity, int64_t old_capacity) {
    if (new_capacity < 0) {
      return Status::Invalid("Resize capacity must be positive");
    }
    if (new_capacity < old_capacity) {
      return Status::Invalid("Resize cannot downsize");
    }
    return Status::OK();
  }

  std::shared_ptr<DataType> type_;
  MemoryPool* pool_;

  // When null_bitmap are first appended to the builder, the null bitmap is allocated
  std::shared_ptr<ResizableBuffer> null_bitmap_;
  int64_t null_count_;
  uint8_t* null_bitmap_data_;

  // Array length, so far. Also, the index of the next element to be added
  int64_t length_;
  int64_t capacity_;

  // Child value array builders. These are owned by this class
  std::vector<std::shared_ptr<ArrayBuilder>> children_;

 private:
  ARROW_DISALLOW_COPY_AND_ASSIGN(ArrayBuilder);
};

}  // namespace arrow
