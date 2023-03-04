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

#include "arrow/array/array_run_end.h"
#include "arrow/array/util.h"
#include "arrow/util/logging.h"
#include "arrow/util/ree_util.h"

namespace arrow {

// ----------------------------------------------------------------------
// RunEndEncodedArray

RunEndEncodedArray::RunEndEncodedArray(const std::shared_ptr<ArrayData>& data) {
  this->SetData(data);
}

RunEndEncodedArray::RunEndEncodedArray(const std::shared_ptr<DataType>& type,
                                       int64_t length,
                                       const std::shared_ptr<Array>& run_ends,
                                       const std::shared_ptr<Array>& values,
                                       int64_t offset) {
  this->SetData(ArrayData::Make(type, length,
                                /*buffers=*/{NULLPTR},
                                /*child_data=*/{run_ends->data(), values->data()},
                                /*null_count=*/0, offset));
}

Result<std::shared_ptr<RunEndEncodedArray>> RunEndEncodedArray::Make(
    int64_t logical_length, const std::shared_ptr<Array>& run_ends,
    const std::shared_ptr<Array>& values, int64_t logical_offset) {
  auto run_end_type = run_ends->type();
  auto values_type = values->type();
  if (!RunEndEncodedType::RunEndTypeValid(*run_end_type)) {
    return Status::Invalid("Run end type must be int16, int32 or int64");
  }
  if (run_ends->null_count() != 0) {
    return Status::Invalid("Run ends array cannot contain null values");
  }
  if (values->length() < run_ends->length()) {
    return Status::Invalid("Values array has to be at least as long as run ends array");
  }

  return std::make_shared<RunEndEncodedArray>(
      run_end_encoded(std::move(run_end_type), std::move(values_type)), logical_length,
      run_ends, values, logical_offset);
}

void RunEndEncodedArray::SetData(const std::shared_ptr<ArrayData>& data) {
  ARROW_CHECK_EQ(data->type->id(), Type::RUN_END_ENCODED);
  const auto* ree_type =
      internal::checked_cast<const RunEndEncodedType*>(data->type.get());
  ARROW_CHECK_EQ(ree_type->run_end_type()->id(), data->child_data[0]->type->id());
  ARROW_CHECK_EQ(ree_type->value_type()->id(), data->child_data[1]->type->id());

  DCHECK_EQ(data->child_data.size(), 2);

  // A non-zero number of logical values in this array (offset + length) implies
  // a non-zero number of runs and values.
  DCHECK(data->offset + data->length == 0 || data->child_data[0]->length > 0);
  DCHECK(data->offset + data->length == 0 || data->child_data[1]->length > 0);
  // At least as many values as run_ends
  DCHECK_GE(data->child_data[1]->length, data->child_data[0]->length);

  // The null count for run-end encoded arrays is always 0. Actual number of
  // nulls needs to be calculated through other means.
  DCHECK_EQ(data->null_count, 0);

  Array::SetData(data);
  run_ends_array_ = MakeArray(this->data()->child_data[0]);
  values_array_ = MakeArray(this->data()->child_data[1]);
}

int64_t RunEndEncodedArray::FindPhysicalOffset() const {
  const ArraySpan span(*this->data_);
  return ree_util::FindPhysicalIndex(span, 0, span.offset);
}

int64_t RunEndEncodedArray::FindPhysicalLength() const {
  const ArraySpan span(*this->data_);
  return ree_util::FindPhysicalLength(span);
}

}  // namespace arrow
