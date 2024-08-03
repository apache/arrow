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

#include <limits>
#include <memory>

#include "arrow/status.h"
#include "arrow/util/macros.h"

#include "arrow/util/logging.h"
#include "gandiva/arrow.h"
#include "gandiva/selection_vector.h"

namespace gandiva {

/// \brief template implementation of selection vector with a specific ctype and arrow
/// type.
template <typename C_TYPE, typename A_TYPE, SelectionVector::Mode mode>
class SelectionVectorImpl : public SelectionVector {
 public:
  SelectionVectorImpl(int64_t max_slots, std::shared_ptr<arrow::Buffer> buffer)
      : max_slots_(max_slots), num_slots_(0), buffer_(buffer), mode_(mode) {
    raw_data_ = reinterpret_cast<C_TYPE*>(buffer->mutable_data());
  }

  SelectionVectorImpl(int64_t max_slots, int64_t num_slots,
                      std::shared_ptr<arrow::Buffer> buffer)
      : max_slots_(max_slots), num_slots_(num_slots), buffer_(buffer), mode_(mode) {
    if (buffer) {
      raw_data_ = const_cast<C_TYPE*>(reinterpret_cast<const C_TYPE*>(buffer->data()));
    }
  }

  uint64_t GetIndex(int64_t index) const override { return raw_data_[index]; }

  void SetIndex(int64_t index, uint64_t value) override {
    raw_data_[index] = static_cast<C_TYPE>(value);
  }

  ArrayPtr ToArray() const override;

  int64_t GetMaxSlots() const override { return max_slots_; }

  int64_t GetNumSlots() const override { return num_slots_; }

  void SetNumSlots(int64_t num_slots) override {
    ARROW_DCHECK_LE(num_slots, max_slots_);
    num_slots_ = num_slots;
  }

  uint64_t GetMaxSupportedValue() const override {
    return std::numeric_limits<C_TYPE>::max();
  }

  Mode GetMode() const override { return mode_; }

  arrow::Buffer& GetBuffer() const override { return *buffer_; }

  static Status AllocateBuffer(int64_t max_slots, arrow::MemoryPool* pool,
                               std::shared_ptr<arrow::Buffer>* buffer);

  static Status ValidateBuffer(int64_t max_slots, std::shared_ptr<arrow::Buffer> buffer);

 protected:
  /// maximum slots in the vector
  int64_t max_slots_;

  /// number of slots in the vector
  int64_t num_slots_;

  std::shared_ptr<arrow::Buffer> buffer_;
  C_TYPE* raw_data_;

  /// SelectionVector mode
  Mode mode_;
};

template <typename C_TYPE, typename A_TYPE, SelectionVector::Mode mode>
ArrayPtr SelectionVectorImpl<C_TYPE, A_TYPE, mode>::ToArray() const {
  auto data_type = arrow::TypeTraits<A_TYPE>::type_singleton();
  auto array_data = arrow::ArrayData::Make(data_type, num_slots_, {NULLPTR, buffer_});
  return arrow::MakeArray(array_data);
}

using SelectionVectorInt16 =
    SelectionVectorImpl<uint16_t, arrow::UInt16Type, SelectionVector::MODE_UINT16>;
using SelectionVectorInt32 =
    SelectionVectorImpl<uint32_t, arrow::UInt32Type, SelectionVector::MODE_UINT32>;
using SelectionVectorInt64 =
    SelectionVectorImpl<uint64_t, arrow::UInt64Type, SelectionVector::MODE_UINT64>;

}  // namespace gandiva
