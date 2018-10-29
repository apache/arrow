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

#include "gandiva/selection_vector.h"

#include <memory>
#include <sstream>
#include <utility>
#include <vector>

#include "gandiva/selection_vector_impl.h"

namespace gandiva {

Status SelectionVector::PopulateFromBitMap(const uint8_t* bitmap, int bitmap_size,
                                           int max_bitmap_index) {
  if (bitmap_size % 8 != 0) {
    std::stringstream ss;
    ss << "bitmap size " << bitmap_size << " must be padded to 64-bit size";
    return Status::Invalid(ss.str());
  }
  if (static_cast<unsigned int>(max_bitmap_index) > GetMaxSupportedValue()) {
    std::stringstream ss;
    ss << "max_bitmap_index " << max_bitmap_index << " must be <= maxSupportedValue "
       << GetMaxSupportedValue() << " in selection vector";
    return Status::Invalid(ss.str());
  }

  int max_slots = GetMaxSlots();

  // jump  8-bytes at a time, add the index corresponding to each valid bit to the
  // the selection vector.
  int selection_idx = 0;
  const uint64_t* bitmap_64 = reinterpret_cast<const uint64_t*>(bitmap);
  for (int bitmap_idx = 0; bitmap_idx < bitmap_size / 8; ++bitmap_idx) {
    uint64_t current_word = bitmap_64[bitmap_idx];

    while (current_word != 0) {
      uint64_t highest_only = current_word & -current_word;
      int pos_in_word = __builtin_ctzl(highest_only);

      int pos_in_bitmap = bitmap_idx * 64 + pos_in_word;
      if (pos_in_bitmap > max_bitmap_index) {
        // the bitmap may be slighly larger for alignment/padding.
        break;
      }

      if (selection_idx >= max_slots) {
        return Status::Invalid("selection vector has no remaining slots");
      }
      SetIndex(selection_idx, pos_in_bitmap);
      ++selection_idx;

      current_word ^= highest_only;
    }
  }

  SetNumSlots(selection_idx);
  return Status::OK();
}

Status SelectionVector::MakeInt16(int max_slots, std::shared_ptr<arrow::Buffer> buffer,
                                  std::shared_ptr<SelectionVector>* selection_vector) {
  auto status = SelectionVectorInt16::ValidateBuffer(max_slots, buffer);
  ARROW_RETURN_NOT_OK(status);

  *selection_vector = std::make_shared<SelectionVectorInt16>(max_slots, buffer);
  return Status::OK();
}

Status SelectionVector::MakeInt16(int max_slots, arrow::MemoryPool* pool,
                                  std::shared_ptr<SelectionVector>* selection_vector) {
  std::shared_ptr<arrow::Buffer> buffer;
  auto status = SelectionVectorInt16::AllocateBuffer(max_slots, pool, &buffer);
  ARROW_RETURN_NOT_OK(status);

  *selection_vector = std::make_shared<SelectionVectorInt16>(max_slots, buffer);
  return Status::OK();
}

Status SelectionVector::MakeInt32(int max_slots, std::shared_ptr<arrow::Buffer> buffer,
                                  std::shared_ptr<SelectionVector>* selection_vector) {
  auto status = SelectionVectorInt32::ValidateBuffer(max_slots, buffer);
  ARROW_RETURN_NOT_OK(status);

  *selection_vector = std::make_shared<SelectionVectorInt32>(max_slots, buffer);
  return Status::OK();
}

Status SelectionVector::MakeInt32(int max_slots, arrow::MemoryPool* pool,
                                  std::shared_ptr<SelectionVector>* selection_vector) {
  std::shared_ptr<arrow::Buffer> buffer;
  auto status = SelectionVectorInt32::AllocateBuffer(max_slots, pool, &buffer);
  ARROW_RETURN_NOT_OK(status);

  *selection_vector = std::make_shared<SelectionVectorInt32>(max_slots, buffer);
  return Status::OK();
}

template <typename C_TYPE, typename A_TYPE>
Status SelectionVectorImpl<C_TYPE, A_TYPE>::AllocateBuffer(
    int max_slots, arrow::MemoryPool* pool, std::shared_ptr<arrow::Buffer>* buffer) {
  auto buffer_len = max_slots * sizeof(C_TYPE);
  auto astatus = arrow::AllocateBuffer(pool, buffer_len, buffer);
  ARROW_RETURN_NOT_OK(astatus);

  return Status::OK();
}

template <typename C_TYPE, typename A_TYPE>
Status SelectionVectorImpl<C_TYPE, A_TYPE>::ValidateBuffer(
    int max_slots, std::shared_ptr<arrow::Buffer> buffer) {
  // verify buffer is mutable
  if (!buffer->is_mutable()) {
    return Status::Invalid("buffer for selection vector must be mutable");
  }

  // verify size of buffer.
  int64_t min_len = max_slots * sizeof(C_TYPE);
  if (buffer->size() < min_len) {
    std::stringstream ss;
    ss << "buffer for selection_data has size " << buffer->size()
       << ", must have minimum size " << min_len;
    return Status::Invalid(ss.str());
  }
  return Status::OK();
}

}  // namespace gandiva
