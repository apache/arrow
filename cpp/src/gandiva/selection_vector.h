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

#ifndef GANDIVA_SELECTION_VECTOR__H
#define GANDIVA_SELECTION_VECTOR__H

#include <memory>

#include "gandiva/arrow.h"
#include "gandiva/logging.h"
#include "gandiva/status.h"

namespace gandiva {

/// \brief Selection Vector : vector of indices in a row-batch for a selection,
/// backed by an arrow-array.
class SelectionVector {
 public:
  virtual ~SelectionVector() = default;

  /// Get the value at a given index.
  virtual uint GetIndex(int index) const = 0;

  /// Set the value at a given index.
  virtual void SetIndex(int index, uint value) = 0;

  // Get the max supported value in the selection vector.
  virtual uint GetMaxSupportedValue() const = 0;

  /// The maximum slots (capacity) of the selection vector.
  virtual int GetMaxSlots() const = 0;

  /// The number of slots (size) of the selection vector.
  virtual int GetNumSlots() const = 0;

  /// Set the number of slots in the selection vector.
  virtual void SetNumSlots(int num_slots) = 0;

  /// Convert to arrow-array.
  virtual ArrayPtr ToArray() const = 0;

  /// \brief populate selection vector for all the set bits in the bitmap.
  ///
  /// \param[in] : bitmap the bitmap
  /// \param[in] : bitmap_size size of the bitmap in bytes
  /// \param[in] : max_bitmap_index max valid index in bitmap (can be lesser than
  ///              capacity in the bitmap, due to alignment/padding).
  Status PopulateFromBitMap(const uint8_t* bitmap, int bitmap_size, int max_bitmap_index);

  /// \brief make selection vector with int16 type records.
  ///
  /// \param[in] : max_slots max number of slots
  /// \param[in] : buffer buffer sized to accomodate max_slots
  /// \param[out]: selection_vector selection vector backed by 'buffer'
  static Status MakeInt16(int max_slots, std::shared_ptr<arrow::Buffer> buffer,
                          std::shared_ptr<SelectionVector>* selection_vector);

  /// \param[in] : max_slots max number of slots
  /// \param[in] : pool memory pool to allocate buffer
  /// \param[out]: selection_vector selection vector backed by a buffer allocated from the
  ///              pool.
  static Status MakeInt16(int max_slots, arrow::MemoryPool* pool,
                          std::shared_ptr<SelectionVector>* selection_vector);

  /// \brief make selection vector with int32 type records.
  ///
  /// \param[in] : max_slots max number of slots
  /// \param[in] : buffer buffer sized to accomodate max_slots
  /// \param[out]: selection_vector selection vector backed by 'buffer'
  static Status MakeInt32(int max_slots, std::shared_ptr<arrow::Buffer> buffer,
                          std::shared_ptr<SelectionVector>* selection_vector);

  /// \brief make selection vector with int32 type records.
  ///
  /// \param[in] : max_slots max number of slots
  /// \param[in] : pool memory pool to allocate buffer
  /// \param[out]: selection_vector selection vector backed by a buffer allocated from the
  ///              pool.
  static Status MakeInt32(int max_slots, arrow::MemoryPool* pool,
                          std::shared_ptr<SelectionVector>* selection_vector);
};

}  // namespace gandiva

#endif  // GANDIVA_SELECTION_VECTOR__H
