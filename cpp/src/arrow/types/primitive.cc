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

void PrimitiveArray::Init(const TypePtr& type, int64_t length,
    const std::shared_ptr<Buffer>& data,
    const std::shared_ptr<Buffer>& nulls) {
  Array::Init(type, length, nulls);
  data_ = data;
  raw_data_ = data == nullptr? nullptr : data_->data();
}

bool PrimitiveArray::Equals(const PrimitiveArray& other) const {
  if (this == &other) return true;
  if (type_->nullable != other.type_->nullable) return false;

  bool equal_data = data_->Equals(*other.data_, length_);
  if (type_->nullable) {
    return equal_data &&
      nulls_->Equals(*other.nulls_, util::ceil_byte(length_) / 8);
  } else {
    return equal_data;
  }
}

} // namespace arrow
