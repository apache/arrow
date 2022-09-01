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

#include "arrow/array/array_encoded.h"
#include "arrow/array/util.h"
#include "arrow/util/logging.h"
#include "arrow/util/rle_util.h"

namespace arrow {

// ----------------------------------------------------------------------
// RunLengthEncodedArray

RunLengthEncodedArray::RunLengthEncodedArray(const std::shared_ptr<ArrayData>& data) {
  ARROW_CHECK_EQ(data->type->id(), Type::RUN_LENGTH_ENCODED);
  SetData(data);
}

RunLengthEncodedArray::RunLengthEncodedArray(const std::shared_ptr<DataType>& type,
                                             int64_t length,
                                             const std::shared_ptr<Array>& run_ends_array,
                                             const std::shared_ptr<Array>& values_array,
                                             int64_t offset) {
  ARROW_CHECK_EQ(type->id(), Type::RUN_LENGTH_ENCODED);
  SetData(ArrayData::Make(type, length, {NULLPTR}, 0, offset));
  data_->child_data.push_back(std::move(run_ends_array->data()));
  data_->child_data.push_back(std::move(values_array->data()));
}

Result<std::shared_ptr<RunLengthEncodedArray>> RunLengthEncodedArray::Make(
    const std::shared_ptr<Array>& run_ends_array,
    const std::shared_ptr<Array>& values_array, int64_t logical_length, int64_t offset) {
  if (run_ends_array->type_id() != Type::INT32) {
    return Status::Invalid("Run ends array must be int32 type");
  }
  if (run_ends_array->null_count() != 0) {
    return Status::Invalid("Run ends array cannot contain null values");
  }

  return std::make_shared<RunLengthEncodedArray>(run_length_encoded(values_array->type()),
                                                 logical_length, run_ends_array,
                                                 values_array, offset);
}

std::shared_ptr<Array> RunLengthEncodedArray::values_array() const {
  return MakeArray(data()->child_data[1]);
}

std::shared_ptr<Array> RunLengthEncodedArray::run_ends_array() const {
  return MakeArray(data()->child_data[0]);
}

int64_t RunLengthEncodedArray::GetPhysicalOffset() const {
  const ArraySpan span(*this->data_);
  return rle_util::GetPhysicalOffset(span);
}

int64_t RunLengthEncodedArray::GetPhysicalLength() const {
  const ArraySpan span(*this->data_);
  return rle_util::GetPhysicalLength(span);
}

}  // namespace arrow
