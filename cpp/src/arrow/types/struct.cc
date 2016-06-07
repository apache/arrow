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

bool StructArray::Equals(const std::shared_ptr<Array>& arr) const {
  if (this == arr.get()) { return true; }
  if (!arr) { return false; }
  if (this->type_enum() != arr->type_enum()) { return false; }
  if (null_count_ != arr->null_count()) { return false; }
  return RangeEquals(0, length_, 0, arr);
}

bool StructArray::RangeEquals(int32_t start_idx, int32_t end_idx, int32_t other_start_idx,
    const std::shared_ptr<Array>& arr) const {
  if (this == arr.get()) { return true; }
  if (!arr) { return false; }
  if (Type::STRUCT != arr->type_enum()) { return false; }
  const auto other = static_cast<StructArray*>(arr.get());

  bool equal_fields = true;
  for (int32_t i = start_idx, o_i = other_start_idx; i < end_idx; ++i, ++o_i) {
    if (IsNull(i) != arr->IsNull(o_i)) { return false; }
    if (IsNull(i)) continue;
    for (size_t j = 0; j < field_arrays_.size(); ++j) {
      // TODO: really we should be comparing stretches of non-null data rather
      // than looking at one value at a time.
      equal_fields = field(j)->RangeEquals(i, i + 1, o_i, other->field(j));
      if (!equal_fields) { return false; }
    }
  }

  return true;
}

Status StructArray::Validate() const {
  if (length_ < 0) { return Status::Invalid("Length was negative"); }

  if (null_count() > length_) {
    return Status::Invalid("Null count exceeds the length of this struct");
  }

  if (field_arrays_.size() > 0) {
    // Validate fields
    int32_t array_length = field_arrays_[0]->length();
    size_t idx = 0;
    for (auto it : field_arrays_) {
      if (it->length() != array_length) {
        std::stringstream ss;
        ss << "Length is not equal from field " << it->type()->ToString()
           << " at position {" << idx << "}";
        return Status::Invalid(ss.str());
      }

      const Status child_valid = it->Validate();
      if (!child_valid.ok()) {
        std::stringstream ss;
        ss << "Child array invalid: " << child_valid.ToString() << " at position {" << idx
           << "}";
        return Status::Invalid(ss.str());
      }
      ++idx;
    }

    if (array_length > 0 && array_length != length_) {
      return Status::Invalid("Struct's length is not equal to its child arrays");
    }
  }
  return Status::OK();
}

}  // namespace arrow
