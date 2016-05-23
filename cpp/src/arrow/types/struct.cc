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

#include "arrow/types/struct.h"

#include <sstream>

namespace arrow {

bool StructArray::EqualsExact(const StructArray& other) const {
  if (this == &other) { return true; }
  if (field_arrays_.size() != other.fields().size()) { return false; }
  if (null_count_ != other.null_count_) { return false; }

  if (null_count_ > 0) {
    bool equal_null_bitmap =
        null_bitmap_->Equals(*other.null_bitmap_, util::bytes_for_bits(length_));
    if (!equal_null_bitmap) { return false; }

    bool fields_equal = true;
    for (size_t i = 0; i < field_arrays_.size(); ++i) {
      fields_equal = field_arrays_[i].get()->Equals(other.field(i));
      if (!fields_equal) { return false; }
    }
  }
  return true;
}

bool StructArray::Equals(const std::shared_ptr<Array>& arr) const {
  if (this == arr.get()) { return true; }
  if (!arr) { return false; }
  if (this->type_enum() != arr->type_enum()) { return false; }
  return EqualsExact(*static_cast<const StructArray*>(arr.get()));
}

Status StructArray::Validate() const {
  if (length_ < 0) { return Status::Invalid("Length was negative"); }

  if (null_count() > length_) {
    return Status::Invalid("Null count exceeds the length of this struct");
  }

  if (field_arrays_.size() > 0) {
    // Validate fields
    int32_t array_length = field_arrays_[0]->length();
    for (auto it : field_arrays_) {
      if (!(it->length() == array_length)) {
        std::stringstream ss;
        ss << "Length is not equal from field " << it->type()->ToString();
        return Status::Invalid(ss.str());
      }

      const Status child_valid = it->Validate();
      if (!child_valid.ok()) {
        std::stringstream ss;
        ss << "Child array invalid: " << child_valid.ToString();
        return Status::Invalid(ss.str());
      }
    }

    // Validate null bitmap
    // TODO: No checks for the consistency of bitmaps. Its cost maybe expensive.
    if (array_length > 0 && array_length != length_) {
      return Status::Invalid("Struct's length is not equal to its child arrays");
    }
  }
  return Status::OK();
}

}  // namespace arrow
