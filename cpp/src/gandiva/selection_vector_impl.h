// Copyright (C) 2017-2018 Dremio Corporation
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#ifndef GANDIVA_SELECTION_VECTOR_IMP_H
#define GANDIVA_SELECTION_VECTOR_IMP_H

#include "gandiva/arrow.h"
#include "gandiva/logging.h"
#include "gandiva/selection_vector.h"
#include "gandiva/status.h"

namespace gandiva {

/// \brief template implementation of selection vector with a specific ctype and arrow
/// type.
template <typename C_TYPE, typename A_TYPE>
class SelectionVectorImpl : public SelectionVector {
 public:
  SelectionVectorImpl(int max_slots, std::shared_ptr<arrow::Buffer> buffer)
      : max_slots_(max_slots), num_slots_(0), buffer_(buffer) {
    raw_data_ = reinterpret_cast<C_TYPE *>(buffer->mutable_data());
  }

  int GetIndex(int index) const override {
    DCHECK_LE(index, max_slots_);
    return raw_data_[index];
  }

  void SetIndex(int index, int value) override {
    DCHECK_LE(index, max_slots_);
    DCHECK_LE(value, GetMaxSupportedValue());

    raw_data_[index] = value;
  }

  ArrayPtr ToArray() const override;

  int GetMaxSlots() const override { return max_slots_; }

  int GetNumSlots() const override { return num_slots_; }

  void SetNumSlots(int num_slots) override {
    DCHECK_LE(num_slots, max_slots_);
    num_slots_ = num_slots;
  }

  int GetMaxSupportedValue() const override { return std::numeric_limits<C_TYPE>::max(); }

  static Status AllocateBuffer(int max_slots, arrow::MemoryPool *pool,
                               std::shared_ptr<arrow::Buffer> *buffer);

  static Status ValidateBuffer(int max_slots, std::shared_ptr<arrow::Buffer> buffer);

 protected:
  /// maximum slots in the vector
  int max_slots_;

  /// number of slots in the vector
  int num_slots_;

  std::shared_ptr<arrow::Buffer> buffer_;
  C_TYPE *raw_data_;
};

template <typename C_TYPE, typename A_TYPE>
ArrayPtr SelectionVectorImpl<C_TYPE, A_TYPE>::ToArray() const {
  auto data_type = arrow::TypeTraits<A_TYPE>::type_singleton();
  auto array_data = arrow::ArrayData::Make(data_type, num_slots_, {nullptr, buffer_});
  return arrow::MakeArray(array_data);
}

using SelectionVectorInt16 = SelectionVectorImpl<int16_t, arrow::Int16Type>;
using SelectionVectorInt32 = SelectionVectorImpl<int32_t, arrow::Int32Type>;

}  // namespace gandiva

#endif  // GANDIVA_SELECTION_VECTOR_IMPL_H
