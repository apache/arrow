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

#ifndef GANDIVA_SELECTION_VECTOR__H
#define GANDIVA_SELECTION_VECTOR__H

#include "gandiva/arrow.h"
#include "gandiva/logging.h"
#include "gandiva/status.h"

namespace gandiva {

/// \brief Selection Vector : vector of indices in a row-batch for a selection,
/// backed by an arrow-array.
class SelectionVector {
 public:
  ~SelectionVector() = default;

  /// Get the value at a given index.
  virtual int GetIndex(int index) const = 0;

  /// Set the value at a given index.
  virtual void SetIndex(int index, int value) = 0;

  // Get the max supported value in the selection vector.
  virtual int GetMaxSupportedValue() const = 0;

  /// The maximum slots (capacity) of the selection vector.
  virtual int GetMaxSlots() const = 0;

  /// The number of slots (size) of the selection vector.
  virtual int GetNumSlots() const = 0;

  /// Set the number of slots in the selection vector.
  virtual void SetNumSlots(int num_slots) = 0;

  /// Convert to arrow-array.
  virtual ArrayPtr ToArray() const = 0;

  /// populate selection vector for all the set bits in the bitmap.
  ///
  /// \param[in] : bitmap the bitmap
  /// \param[in] : bitmap_size size of the bitmap in bytes
  /// \param[in] : max_bitmap_index max valid index in bitmap (can be lesser than
  ///              capacity in the bitmap, due to alignment/padding).
  Status PopulateFromBitMap(const uint8_t *bitmap, int bitmap_size, int max_bitmap_index);
};

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

 protected:
  static Status AllocateBuffer(int max_slots, arrow::MemoryPool *pool,
                               std::shared_ptr<arrow::Buffer> *buffer);

  static Status ValidateBuffer(int max_slots, std::shared_ptr<arrow::Buffer> buffer);

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

class SelectionVectorInt16 : public SelectionVectorImpl<int16_t, arrow::Int16Type> {
 public:
  SelectionVectorInt16(int max_slots, std::shared_ptr<arrow::Buffer> buffer)
      : SelectionVectorImpl(max_slots, buffer) {}

  int GetMaxSupportedValue() const override { return INT16_MAX; }

  /// \param[in] : max_slots max number of slots
  /// \param[in] : buffer buffer sized to accomodate max_slots
  /// \param[out]: selection_vector selection vector backed by 'buffer'
  static Status Make(int max_slots, std::shared_ptr<arrow::Buffer> buffer,
                     std::shared_ptr<SelectionVectorInt16> *selection_vector);

  /// \param[in] : max_slots max number of slots
  /// \param[in] : pool memory pool to allocate buffer
  /// \param[out]: selection_vector selection vector backed by a buffer allocated from the
  ///              pool.
  static Status Make(int max_slots, arrow::MemoryPool *pool,
                     std::shared_ptr<SelectionVectorInt16> *selection_vector);
};

class SelectionVectorInt32 : public SelectionVectorImpl<int32_t, arrow::Int32Type> {
 public:
  SelectionVectorInt32(int max_slots, std::shared_ptr<arrow::Buffer> buffer)
      : SelectionVectorImpl(max_slots, buffer) {}

  int GetMaxSupportedValue() const override { return INT32_MAX; }

  /// \param[in] : max_slots max number of slots
  /// \param[in] : buffer buffer sized to accomodate max_slots
  /// \param[out]: selection_vector selection vector backed by 'buffer'
  static Status Make(int max_slots, std::shared_ptr<arrow::Buffer> buffer,
                     std::shared_ptr<SelectionVectorInt32> *selection_vector);

  /// \param[in] : max_slots max number of slots
  /// \param[in] : pool memory pool to allocate buffer
  /// \param[out]: selection_vector selection vector backed by a buffer allocated from the
  ///              pool.
  static Status Make(int max_slots, arrow::MemoryPool *pool,
                     std::shared_ptr<SelectionVectorInt32> *selection_vector);
};

}  // namespace gandiva

#endif  // GANDIVA_SELECTION_VECTOR__H
