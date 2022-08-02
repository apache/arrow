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

namespace arrow {

// ----------------------------------------------------------------------
// RunLengthEncodedArray

RunLengthEncodedArray::RunLengthEncodedArray(const std::shared_ptr<ArrayData>& data) {
  ARROW_CHECK_EQ(data->type->id(), Type::RUN_LENGTH_ENCODED);
  SetData(data);
}

RunLengthEncodedArray::RunLengthEncodedArray(const std::shared_ptr<DataType>& type,
                                             int64_t length,
                                             std::shared_ptr<Array>& values_array,
                                             std::shared_ptr<Buffer> run_ends_buffer,
                                             int64_t offset) {
  ARROW_CHECK_EQ(type->id(), Type::RUN_LENGTH_ENCODED);
  SetData(ArrayData::Make(type, length, {run_ends_buffer}, 0, offset));
  data_->child_data.push_back(std::move(values_array->data()));
}

Result<std::shared_ptr<RunLengthEncodedArray>> RunLengthEncodedArray::Make(
    std::shared_ptr<Array>& values_array, std::shared_ptr<Array>& run_ends_array,
    int64_t logical_length, int64_t offset) {
  if (run_ends_array->type_id() != Type::INT32) {
    return Status::Invalid("Run ends array must be int32 type");
  }
  int64_t physical_length = values_array->length();
  if (run_ends_array->null_count() != 0) {
    return Status::Invalid("Run ends array cannot contain null values");
  }

  std::shared_ptr<Buffer> run_ends_buffer = SliceBuffer(
      run_ends_array->data()->buffers[1], run_ends_array->offset(), physical_length);

  return std::make_shared<RunLengthEncodedArray>(run_length_encoded(values_array->type()),
                                                 logical_length, values_array,
                                                 std::move(run_ends_buffer), offset);
}

std::shared_ptr<Array> RunLengthEncodedArray::values_array() const {
  return MakeArray(data()->child_data[0]);
}

std::shared_ptr<Buffer> RunLengthEncodedArray::run_ends_buffer() const {
  return data()->buffers[0];
}

const int32_t* RunLengthEncodedArray::run_ends() const {
  return data()->GetValuesSafe<int32_t>(0, 0);
}

}  // namespace arrow
