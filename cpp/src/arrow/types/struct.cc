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
  if (null_count_ != other.null_count_) { return false; }

  bool equal_null_bitmap = true;
  if (null_count_ > 0) {
    equal_null_bitmap =
        null_bitmap_->Equals(*other.null_bitmap_, util::bytes_for_bits(length_));
  }

  if (!equal_null_bitmap) { return false; }
  bool values_equal = false;
  for (size_t i = 0; i < field_arrays_.size(); ++i) {
    values_equal = field_arrays_.at(i).get()->Equals(other.fields().at(i));
  }

  return values_equal;
}

bool StructArray::Equals(const std::shared_ptr<Array>& arr) const {
  if (this == arr.get()) { return true; }
  if (this->type_enum() != arr->type_enum()) { return false; }
  return EqualsExact(*static_cast<const StructArray*>(arr.get()));
}

Status StructArray::Validate() const {
  if (length_ < 0) { return Status::Invalid("Length was negative"); }

  if (null_count() > length_) {
    return Status::Invalid("Null count exceeds the length of this struct");
  }

  if (fields().size() <= 0 && length_ > 0) {
    return Status::Invalid("Fields are not existed, but length is not zero");
  }

  int32_t struct_length = length();
  if (struct_length > 0) {
    // Validate fields
    bool fields_equal = true;
    for (auto it : field_arrays_) {
      fields_equal = (it->length() == struct_length);
      if (fields_equal == false) {
        std::stringstream ss;
        ss << "The length of field [ " << it->type()->ToString() << " ] "
           << "is not equal to this StructArray's length";
        return Status::Invalid(ss.str());
      }
      fields_equal = true;

      const Status child_valid = it->Validate();
      if (!child_valid.ok()) {
        std::stringstream ss;
        ss << "Child array invalid: " << child_valid.ToString();
        return Status::Invalid(ss.str());
      }
    }

    // Validate bitmap
    // If struct is null at pos i, then its children should be null
    // at the same position.
    bool children_null = true;
    for (int i = 0; i < length_; ++i) {
      for (size_t j = 0; j < field_arrays_.size(); ++j) {
        children_null &= field_arrays_[j]->IsNull(i);
      }

      if (children_null == IsNull(i)) {
        children_null = true;
        continue;
      }

      std::stringstream ss;
      if (true == IsNull(i)) {
        ss << type()->ToString() << "is null at position " << i
           << ", but some of its child fields are not null at the same position";
      } else {
        ss << type()->ToString() << "is not null at position " << i
           << ", but some of its child fields are null at the same position";
      }
      return Status::Invalid(ss.str());
    }
  }
  return Status::OK();
}

}  // namespace arrow
