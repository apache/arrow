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

#include "arrow/types/primitive.h"

#include <memory>

#include "arrow/util/buffer.h"

namespace arrow {

// ----------------------------------------------------------------------
// Primitive array base

PrimitiveArray::PrimitiveArray(const TypePtr& type, int32_t length,
    const std::shared_ptr<Buffer>& data,
    int32_t null_count,
    const std::shared_ptr<Buffer>& nulls) :
    Array(type, length, null_count, nulls) {
  data_ = data;
  raw_data_ = data == nullptr? nullptr : data_->data();
}

bool PrimitiveArray::EqualsExact(const PrimitiveArray& other) const {
  if (this == &other) return true;
  if (null_count_ != other.null_count_) {
    return false;
  }

  bool equal_data = data_->Equals(*other.data_, length_);
  if (null_count_ > 0) {
    return equal_data &&
      nulls_->Equals(*other.nulls_, util::ceil_byte(length_) / 8);
  } else {
    return equal_data;
  }
}

bool PrimitiveArray::Equals(const std::shared_ptr<Array>& arr) const {
  if (this == arr.get()) return true;
  if (this->type_enum() != arr->type_enum()) {
    return false;
  }
  return EqualsExact(*static_cast<const PrimitiveArray*>(arr.get()));
}

} // namespace arrow
