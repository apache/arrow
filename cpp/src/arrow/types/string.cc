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

#include "arrow/types/string.h"

#include <sstream>
#include <string>

#include "arrow/type.h"

namespace arrow {

const std::shared_ptr<DataType> BINARY(new BinaryType());
const std::shared_ptr<DataType> STRING(new StringType());

BinaryArray::BinaryArray(int32_t length, const std::shared_ptr<Buffer>& offsets,
    const ArrayPtr& values, int32_t null_count,
    const std::shared_ptr<Buffer>& null_bitmap)
    : BinaryArray(BINARY, length, offsets, values, null_count, null_bitmap) {}

BinaryArray::BinaryArray(const TypePtr& type, int32_t length,
    const std::shared_ptr<Buffer>& offsets, const ArrayPtr& values, int32_t null_count,
    const std::shared_ptr<Buffer>& null_bitmap)
    : ListArray(type, length, offsets, values, null_count, null_bitmap),
      bytes_(std::dynamic_pointer_cast<UInt8Array>(values).get()),
      raw_bytes_(bytes_->raw_data()) {
  // Check in case the dynamic cast fails.
  DCHECK(bytes_);
}

Status BinaryArray::Validate() const {
  if (values()->null_count() > 0) {
    std::stringstream ss;
    ss << type()->ToString() << " can have null values in the value array";
    Status::Invalid(ss.str());
  }
  return ListArray::Validate();
}

StringArray::StringArray(int32_t length, const std::shared_ptr<Buffer>& offsets,
    const ArrayPtr& values, int32_t null_count,
    const std::shared_ptr<Buffer>& null_bitmap)
    : StringArray(STRING, length, offsets, values, null_count, null_bitmap) {}

Status StringArray::Validate() const {
  // TODO(emkornfield) Validate proper UTF8 code points?
  return BinaryArray::Validate();
}

TypePtr BinaryBuilder::value_type_ = TypePtr(new UInt8Type());

}  // namespace arrow
