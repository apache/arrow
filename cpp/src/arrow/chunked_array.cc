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

#include "arrow/chunked_array.h"

#include <algorithm>
#include <cstdlib>
#include <memory>
#include <sstream>
#include <utility>

#include "arrow/array/array_base.h"
#include "arrow/array/array_nested.h"
#include "arrow/array/validate.h"
#include "arrow/pretty_print.h"
#include "arrow/status.h"
#include "arrow/type.h"
#include "arrow/util/checked_cast.h"
#include "arrow/util/logging.h"

namespace arrow {

using internal::checked_cast;

class MemoryPool;

// ----------------------------------------------------------------------
// ChunkedArray methods

ChunkedArray::ChunkedArray(ArrayVector chunks) : chunks_(std::move(chunks)) {
  length_ = 0;
  null_count_ = 0;

  ARROW_CHECK_GT(chunks_.size(), 0)
      << "cannot construct ChunkedArray from empty vector and omitted type";
  type_ = chunks_[0]->type();
  for (const std::shared_ptr<Array>& chunk : chunks_) {
    length_ += chunk->length();
    null_count_ += chunk->null_count();
  }
}

ChunkedArray::ChunkedArray(ArrayVector chunks, std::shared_ptr<DataType> type)
    : chunks_(std::move(chunks)), type_(std::move(type)) {
  length_ = 0;
  null_count_ = 0;
  for (const std::shared_ptr<Array>& chunk : chunks_) {
    length_ += chunk->length();
    null_count_ += chunk->null_count();
  }
}

Result<std::shared_ptr<ChunkedArray>> ChunkedArray::Make(ArrayVector chunks,
                                                         std::shared_ptr<DataType> type) {
  if (type == nullptr) {
    if (chunks.size() == 0) {
      return Status::Invalid(
          "cannot construct ChunkedArray from empty vector "
          "and omitted type");
    }
    type = chunks[0]->type();
  }
  for (size_t i = 0; i < chunks.size(); ++i) {
    if (!chunks[i]->type()->Equals(*type)) {
      return Status::Invalid("Array chunks must all be same type");
    }
  }
  return std::make_shared<ChunkedArray>(std::move(chunks), std::move(type));
}

bool ChunkedArray::Equals(const ChunkedArray& other) const {
  if (length_ != other.length()) {
    return false;
  }
  if (null_count_ != other.null_count()) {
    return false;
  }
  // We cannot toggle check_metadata here yet, so we don't check it
  if (!type_->Equals(*other.type_, /*check_metadata=*/false)) {
    return false;
  }

  // Check contents of the underlying arrays. This checks for equality of
  // the underlying data independently of the chunk size.
  return internal::ApplyBinaryChunked(
             *this, other,
             [](const Array& left_piece, const Array& right_piece,
                int64_t ARROW_ARG_UNUSED(position)) {
               if (!left_piece.Equals(right_piece)) {
                 return Status::Invalid("Unequal piece");
               }
               return Status::OK();
             })
      .ok();
}

bool ChunkedArray::Equals(const std::shared_ptr<ChunkedArray>& other) const {
  if (this == other.get()) {
    return true;
  }
  if (!other) {
    return false;
  }
  return Equals(*other.get());
}

bool ChunkedArray::ApproxEquals(const ChunkedArray& other,
                                const EqualOptions& equal_options) const {
  if (length_ != other.length()) {
    return false;
  }
  if (null_count_ != other.null_count()) {
    return false;
  }
  // We cannot toggle check_metadata here yet, so we don't check it
  if (!type_->Equals(*other.type_, /*check_metadata=*/false)) {
    return false;
  }

  // Check contents of the underlying arrays. This checks for equality of
  // the underlying data independently of the chunk size.
  return internal::ApplyBinaryChunked(
             *this, other,
             [&](const Array& left_piece, const Array& right_piece,
                 int64_t ARROW_ARG_UNUSED(position)) {
               if (!left_piece.ApproxEquals(right_piece, equal_options)) {
                 return Status::Invalid("Unequal piece");
               }
               return Status::OK();
             })
      .ok();
}

std::shared_ptr<ChunkedArray> ChunkedArray::Slice(int64_t offset, int64_t length) const {
  ARROW_CHECK_LE(offset, length_) << "Slice offset greater than array length";
  bool offset_equals_length = offset == length_;
  int curr_chunk = 0;
  while (curr_chunk < num_chunks() && offset >= chunk(curr_chunk)->length()) {
    offset -= chunk(curr_chunk)->length();
    curr_chunk++;
  }

  ArrayVector new_chunks;
  if (num_chunks() > 0 && (offset_equals_length || length == 0)) {
    // Special case the zero-length slice to make sure there is at least 1 Array
    // in the result. When there are zero chunks we return zero chunks
    new_chunks.push_back(chunk(std::min(curr_chunk, num_chunks() - 1))->Slice(0, 0));
  } else {
    while (curr_chunk < num_chunks() && length > 0) {
      new_chunks.push_back(chunk(curr_chunk)->Slice(offset, length));
      length -= chunk(curr_chunk)->length() - offset;
      offset = 0;
      curr_chunk++;
    }
  }

  return std::make_shared<ChunkedArray>(new_chunks, type_);
}

std::shared_ptr<ChunkedArray> ChunkedArray::Slice(int64_t offset) const {
  return Slice(offset, length_);
}

Result<std::vector<std::shared_ptr<ChunkedArray>>> ChunkedArray::Flatten(
    MemoryPool* pool) const {
  if (type()->id() != Type::STRUCT) {
    // Emulate nonexistent copy constructor
    return std::vector<std::shared_ptr<ChunkedArray>>{
        std::make_shared<ChunkedArray>(chunks_, type_)};
  }

  std::vector<ArrayVector> flattened_chunks(type()->num_fields());
  for (const auto& chunk : chunks_) {
    ARROW_ASSIGN_OR_RAISE(auto arrays,
                          checked_cast<const StructArray&>(*chunk).Flatten(pool));
    DCHECK_EQ(arrays.size(), flattened_chunks.size());
    for (size_t i = 0; i < arrays.size(); ++i) {
      flattened_chunks[i].push_back(arrays[i]);
    }
  }

  std::vector<std::shared_ptr<ChunkedArray>> flattened(type()->num_fields());
  for (size_t i = 0; i < flattened.size(); ++i) {
    auto child_type = type()->field(static_cast<int>(i))->type();
    flattened[i] =
        std::make_shared<ChunkedArray>(std::move(flattened_chunks[i]), child_type);
  }
  return flattened;
}

Result<std::shared_ptr<ChunkedArray>> ChunkedArray::View(
    const std::shared_ptr<DataType>& type) const {
  ArrayVector out_chunks(this->num_chunks());
  for (int i = 0; i < this->num_chunks(); ++i) {
    ARROW_ASSIGN_OR_RAISE(out_chunks[i], chunks_[i]->View(type));
  }
  return std::make_shared<ChunkedArray>(out_chunks, type);
}

std::string ChunkedArray::ToString() const {
  std::stringstream ss;
  ARROW_CHECK_OK(PrettyPrint(*this, 0, &ss));
  return ss.str();
}

Status ChunkedArray::Validate() const {
  if (chunks_.size() == 0) {
    return Status::OK();
  }

  const auto& type = *chunks_[0]->type();
  // Make sure chunks all have the same type
  for (size_t i = 1; i < chunks_.size(); ++i) {
    const Array& chunk = *chunks_[i];
    if (!chunk.type()->Equals(type)) {
      return Status::Invalid("In chunk ", i, " expected type ", type.ToString(),
                             " but saw ", chunk.type()->ToString());
    }
  }
  // Validate the chunks themselves
  for (size_t i = 0; i < chunks_.size(); ++i) {
    const Array& chunk = *chunks_[i];
    const Status st = internal::ValidateArray(chunk);
    if (!st.ok()) {
      return Status::Invalid("In chunk ", i, ": ", st.ToString());
    }
  }
  return Status::OK();
}

Status ChunkedArray::ValidateFull() const {
  RETURN_NOT_OK(Validate());
  for (size_t i = 0; i < chunks_.size(); ++i) {
    const Array& chunk = *chunks_[i];
    const Status st = internal::ValidateArrayFull(chunk);
    if (!st.ok()) {
      return Status::Invalid("In chunk ", i, ": ", st.ToString());
    }
  }
  return Status::OK();
}

namespace internal {

bool MultipleChunkIterator::Next(std::shared_ptr<Array>* next_left,
                                 std::shared_ptr<Array>* next_right) {
  if (pos_ == length_) return false;

  // Find non-empty chunk
  std::shared_ptr<Array> chunk_left, chunk_right;
  while (true) {
    chunk_left = left_.chunk(chunk_idx_left_);
    chunk_right = right_.chunk(chunk_idx_right_);
    if (chunk_pos_left_ == chunk_left->length()) {
      chunk_pos_left_ = 0;
      ++chunk_idx_left_;
      continue;
    }
    if (chunk_pos_right_ == chunk_right->length()) {
      chunk_pos_right_ = 0;
      ++chunk_idx_right_;
      continue;
    }
    break;
  }
  // Determine how big of a section to return
  int64_t iteration_size = std::min(chunk_left->length() - chunk_pos_left_,
                                    chunk_right->length() - chunk_pos_right_);

  *next_left = chunk_left->Slice(chunk_pos_left_, iteration_size);
  *next_right = chunk_right->Slice(chunk_pos_right_, iteration_size);

  pos_ += iteration_size;
  chunk_pos_left_ += iteration_size;
  chunk_pos_right_ += iteration_size;
  return true;
}

}  // namespace internal
}  // namespace arrow
