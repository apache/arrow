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
#include "arrow/array/builder_primitive.h"
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

namespace {

template <typename RunEndType>
Result<std::shared_ptr<Array>> MakeLogicalRunEnds(const RunEndEncodedArray& self,
                                                  MemoryPool* pool) {
  using RunEndCType = typename RunEndType::c_type;
  if (self.offset() == 0) {
    const auto& run_ends = *self.run_ends();
    if (self.length() == 0) {
      return run_ends.Slice(0, 0);
    }

    // If offset==0 and the non-zero logical length aligns perfectly with a
    // physical run-end, we can return a slice of the run-ends array.
    const int64_t physical_length = self.FindPhysicalLength();
    const auto* run_end_values = self.data()->child_data[0]->GetValues<RunEndCType>(1);
    if (run_end_values[physical_length - 1] == self.length()) {
      return run_ends.Slice(0, physical_length);
    }

    // Otherwise we need to copy the run-ends array and adjust only the very
    // last run-end.
    auto new_run_ends_data = ArrayData::Make(run_ends.type(), physical_length, 0, 0);
    {
      ARROW_ASSIGN_OR_RAISE(auto buffer,
                            AllocateBuffer(physical_length * sizeof(RunEndCType), pool));
      new_run_ends_data->buffers = {NULLPTR, std::move(buffer)};
    }
    auto* new_run_end_values = new_run_ends_data->GetMutableValues<RunEndCType>(1);
    memcpy(new_run_end_values, run_end_values,
           (physical_length - 1) * sizeof(RunEndCType));
    new_run_end_values[physical_length - 1] = static_cast<RunEndCType>(self.length());
    return MakeArray(std::move(new_run_ends_data));
  }

  // When the logical offset is non-zero, all run-end values need to be adjusted.
  int64_t physical_offset = self.FindPhysicalOffset();
  int64_t physical_length = self.FindPhysicalLength();

  const auto* run_end_values = self.data()->child_data[0]->GetValues<RunEndCType>(1);
  NumericBuilder<RunEndType> builder(pool);
  RETURN_NOT_OK(builder.Resize(physical_length));
  if (physical_length > 0) {
    for (int64_t i = 0; i < physical_length - 1; i++) {
      const auto run_end = run_end_values[physical_offset + i] - self.offset();
      DCHECK_LT(run_end, self.length());
      RETURN_NOT_OK(builder.Append(static_cast<RunEndCType>(run_end)));
    }
    DCHECK_GE(run_end_values[physical_offset + physical_length - 1] - self.offset(),
              self.length());
    RETURN_NOT_OK(builder.Append(static_cast<RunEndCType>(self.length())));
  }
  return builder.Finish();
}

}  // namespace

Result<std::shared_ptr<Array>> RunEndEncodedArray::LogicalRunEnds(
    MemoryPool* pool) const {
  DCHECK(data()->child_data[0]->buffers[1]->is_cpu());
  switch (run_ends_array_->type_id()) {
    case Type::INT16:
      return MakeLogicalRunEnds<Int16Type>(*this, pool);
    case Type::INT32:
      return MakeLogicalRunEnds<Int32Type>(*this, pool);
    default:
      DCHECK_EQ(run_ends_array_->type_id(), Type::INT64);
      return MakeLogicalRunEnds<Int64Type>(*this, pool);
  }
}

std::shared_ptr<Array> RunEndEncodedArray::LogicalValues() const {
  const int64_t physical_offset = FindPhysicalOffset();
  const int64_t physical_length = FindPhysicalLength();
  return MakeArray(data()->child_data[1]->Slice(physical_offset, physical_length));
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
