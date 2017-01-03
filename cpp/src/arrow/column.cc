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

#include "arrow/column.h"

#include <memory>
#include <sstream>

#include "arrow/array.h"
#include "arrow/status.h"
#include "arrow/type.h"

namespace arrow {

ChunkedArray::ChunkedArray(const ArrayVector& chunks) : chunks_(chunks) {
  length_ = 0;
  null_count_ = 0;
  for (const std::shared_ptr<Array>& chunk : chunks) {
    length_ += chunk->length();
    null_count_ += chunk->null_count();
  }
}

bool ChunkedArray::Equals(const ChunkedArray& other) const {
  if (length_ != other.length()) { return false; }
  if (null_count_ != other.null_count()) { return false; }

  // Check contents of the underlying arrays. This checks for equality of
  // the underlying data independently of the chunk size.
  int this_chunk_idx = 0;
  int32_t this_start_idx = 0;
  int other_chunk_idx = 0;
  int32_t other_start_idx = 0;
  while (this_chunk_idx < static_cast<int32_t>(chunks_.size())) {
    const std::shared_ptr<Array> this_array = chunks_[this_chunk_idx];
    const std::shared_ptr<Array> other_array = other.chunk(other_chunk_idx);
    int32_t common_length = std::min(
        this_array->length() - this_start_idx, other_array->length() - other_start_idx);
    if (!this_array->RangeEquals(this_start_idx, this_start_idx + common_length,
            other_start_idx, other_array)) {
      return false;
    }

    // If we have exhausted the current chunk, proceed to the next one individually.
    if (this_start_idx + common_length == this_array->length()) {
      this_chunk_idx++;
      this_start_idx = 0;
    }
    if (other_start_idx + common_length == other_array->length()) {
      other_chunk_idx++;
      other_start_idx = 0;
    }
  }
  return true;
}

bool ChunkedArray::Equals(const std::shared_ptr<ChunkedArray>& other) const {
  if (this == other.get()) { return true; }
  if (!other) { return false; }
  return Equals(*other.get());
}

Column::Column(const std::shared_ptr<Field>& field, const ArrayVector& chunks)
    : field_(field) {
  data_ = std::make_shared<ChunkedArray>(chunks);
}

Column::Column(const std::shared_ptr<Field>& field, const std::shared_ptr<Array>& data)
    : field_(field) {
  data_ = std::make_shared<ChunkedArray>(ArrayVector({data}));
}

Column::Column(
    const std::shared_ptr<Field>& field, const std::shared_ptr<ChunkedArray>& data)
    : field_(field), data_(data) {}

bool Column::Equals(const Column& other) const {
  if (!field_->Equals(other.field())) { return false; }
  return data_->Equals(other.data());
}

bool Column::Equals(const std::shared_ptr<Column>& other) const {
  if (this == other.get()) { return true; }
  if (!other) { return false; }

  return Equals(*other.get());
}

Status Column::ValidateData() {
  for (int i = 0; i < data_->num_chunks(); ++i) {
    std::shared_ptr<DataType> type = data_->chunk(i)->type();
    if (!this->type()->Equals(type)) {
      std::stringstream ss;
      ss << "In chunk " << i << " expected type " << this->type()->ToString()
         << " but saw " << type->ToString();
      return Status::Invalid(ss.str());
    }
  }
  return Status::OK();
}

}  // namespace arrow
