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

#include "arrow/array/builder_run_end.h"
#include "arrow/array/builder_primitive.h"

#include <cstddef>
#include <cstdint>
#include <utility>
#include <vector>

#include "arrow/scalar.h"
#include "arrow/util/checked_cast.h"
#include "arrow/util/int_util_overflow.h"
#include "arrow/util/logging.h"
#include "arrow/util/ree_util.h"

namespace arrow {
namespace internal {

RunCompressorBuilder::RunCompressorBuilder(MemoryPool* pool,
                                           std::shared_ptr<ArrayBuilder> inner_builder,
                                           std::shared_ptr<DataType> type)
    : ArrayBuilder(pool), inner_builder_(std::move(inner_builder)) {}

RunCompressorBuilder::~RunCompressorBuilder() = default;

void RunCompressorBuilder::Reset() {
  current_run_length_ = 0;
  current_value_.reset();
  inner_builder_->Reset();
  UpdateDimensions();
}

Status RunCompressorBuilder::ResizePhysical(int64_t capacity) {
  RETURN_NOT_OK(inner_builder_->Resize(capacity));
  UpdateDimensions();
  return Status::OK();
}

Status RunCompressorBuilder::AppendNulls(int64_t length) {
  if (ARROW_PREDICT_FALSE(length == 0)) {
    return Status::OK();
  }
  if (ARROW_PREDICT_FALSE(current_run_length_ == 0)) {
    // Open a new NULL run
    DCHECK_EQ(current_value_, NULLPTR);
    current_run_length_ = length;
  } else if (current_value_ == NULLPTR) {
    // Extend the currently open NULL run
    current_run_length_ += length;
  } else {
    // Close then non-NULL run
    ARROW_RETURN_NOT_OK(WillCloseRun(current_value_, current_run_length_));
    ARROW_RETURN_NOT_OK(inner_builder_->AppendScalar(*current_value_));
    UpdateDimensions();
    // Open a new NULL run
    current_value_.reset();
    current_run_length_ = length;
  }
  return Status::OK();
}

Status RunCompressorBuilder::AppendEmptyValues(int64_t length) {
  if (ARROW_PREDICT_FALSE(length == 0)) {
    return Status::OK();
  }
  // Empty values are usually appended as placeholders for future values, so
  // we make no attempt at making the empty values appended now part of the
  // current run. Each AppendEmptyValues() creates its own run of the given length.
  ARROW_RETURN_NOT_OK(FinishCurrentRun());
  {
    ARROW_RETURN_NOT_OK(WillCloseRunOfEmptyValues(length));
    ARROW_RETURN_NOT_OK(inner_builder_->AppendEmptyValue());
    UpdateDimensions();
  }
  // Current run remains cleared after FinishCurrentRun() as we don't want to
  // extend it with empty values potentially coming in the future.
  return Status::OK();
}

Status RunCompressorBuilder::AppendScalar(const Scalar& scalar, int64_t n_repeats) {
  if (ARROW_PREDICT_FALSE(n_repeats == 0)) {
    return Status::OK();
  }
  if (ARROW_PREDICT_FALSE(current_run_length_ == 0)) {
    // Open a new run
    current_value_ = scalar.is_valid ? scalar.shared_from_this() : NULLPTR;
    current_run_length_ = n_repeats;
  } else if ((current_value_ == NULLPTR && !scalar.is_valid) ||
             (current_value_ != NULLPTR && current_value_->Equals(scalar))) {
    // Extend the currently open run
    current_run_length_ += n_repeats;
  } else {
    // Close the current run
    ARROW_RETURN_NOT_OK(WillCloseRun(current_value_, current_run_length_));
    ARROW_RETURN_NOT_OK(current_value_ ? inner_builder_->AppendScalar(*current_value_)
                                       : inner_builder_->AppendNull());
    UpdateDimensions();
    // Open a new run
    current_value_ = scalar.is_valid ? scalar.shared_from_this() : NULLPTR;
    current_run_length_ = n_repeats;
  }
  return Status::OK();
}

Status RunCompressorBuilder::AppendScalars(const ScalarVector& scalars) {
  if (scalars.empty()) {
    return Status::OK();
  }
  RETURN_NOT_OK(ArrayBuilder::AppendScalars(scalars));
  UpdateDimensions();
  return Status::OK();
}

Status RunCompressorBuilder::AppendRunCompressedArraySlice(
    const ArraySpan& run_compressed_array, int64_t offset, int64_t length) {
  DCHECK(!has_open_run());
  RETURN_NOT_OK(inner_builder_->AppendArraySlice(run_compressed_array, offset, length));
  UpdateDimensions();
  return Status::OK();
}

Status RunCompressorBuilder::FinishCurrentRun() {
  if (current_run_length_ > 0) {
    // Close the current run
    ARROW_RETURN_NOT_OK(WillCloseRun(current_value_, current_run_length_));
    ARROW_RETURN_NOT_OK(current_value_ ? inner_builder_->AppendScalar(*current_value_)
                                       : inner_builder_->AppendNull());
    UpdateDimensions();
    // Clear the current run
    current_value_.reset();
    current_run_length_ = 0;
  }
  return Status::OK();
}

Status RunCompressorBuilder::FinishInternal(std::shared_ptr<ArrayData>* out) {
  ARROW_RETURN_NOT_OK(FinishCurrentRun());
  return inner_builder_->FinishInternal(out);
}

}  // namespace internal

// ----------------------------------------------------------------------
// RunEndEncodedBuilder

RunEndEncodedBuilder::ValueRunBuilder::ValueRunBuilder(
    MemoryPool* pool, const std::shared_ptr<ArrayBuilder>& value_builder,
    const std::shared_ptr<DataType>& value_type, RunEndEncodedBuilder& ree_builder)
    : RunCompressorBuilder(pool, std::move(value_builder), std::move(value_type)),
      ree_builder_(ree_builder) {}

RunEndEncodedBuilder::RunEndEncodedBuilder(
    MemoryPool* pool, const std::shared_ptr<ArrayBuilder>& run_end_builder,
    const std::shared_ptr<ArrayBuilder>& value_builder, std::shared_ptr<DataType> type)
    : ArrayBuilder(pool), type_(internal::checked_pointer_cast<RunEndEncodedType>(type)) {
  auto value_run_builder =
      std::make_shared<ValueRunBuilder>(pool, value_builder, type_->value_type(), *this);
  value_run_builder_ = value_run_builder.get();
  children_ = {run_end_builder, std::move(value_run_builder)};
  UpdateDimensions(0, 0);
  null_count_ = 0;
}

Status RunEndEncodedBuilder::ResizePhysical(int64_t capacity) {
  RETURN_NOT_OK(value_run_builder_->ResizePhysical(capacity));
  RETURN_NOT_OK(run_end_builder().Resize(capacity));
  UpdateDimensions(committed_logical_length_, 0);
  return Status::OK();
}

void RunEndEncodedBuilder::Reset() {
  value_run_builder_->Reset();
  run_end_builder().Reset();
  UpdateDimensions(0, 0);
}

Status RunEndEncodedBuilder::AppendNulls(int64_t length) {
  RETURN_NOT_OK(value_run_builder_->AppendNulls(length));
  UpdateDimensions(committed_logical_length_, value_run_builder_->open_run_length());
  return Status::OK();
}

Status RunEndEncodedBuilder::AppendEmptyValues(int64_t length) {
  RETURN_NOT_OK(value_run_builder_->AppendEmptyValues(length));
  DCHECK_EQ(value_run_builder_->open_run_length(), 0);
  UpdateDimensions(committed_logical_length_, 0);
  return Status::OK();
}

Status RunEndEncodedBuilder::AppendScalar(const Scalar& scalar, int64_t n_repeats) {
  if (scalar.type->id() == Type::RUN_END_ENCODED) {
    return AppendScalar(*internal::checked_cast<const RunEndEncodedScalar&>(scalar).value,
                        n_repeats);
  }
  RETURN_NOT_OK(value_run_builder_->AppendScalar(scalar, n_repeats));
  UpdateDimensions(committed_logical_length_, value_run_builder_->open_run_length());
  return Status::OK();
}

Status RunEndEncodedBuilder::AppendScalars(const ScalarVector& scalars) {
  RETURN_NOT_OK(this->ArrayBuilder::AppendScalars(scalars));
  UpdateDimensions(committed_logical_length_, value_run_builder_->open_run_length());
  return Status::OK();
}

template <typename RunEndCType>
Status RunEndEncodedBuilder::DoAppendArraySlice(const ArraySpan& array, int64_t offset,
                                                int64_t length) {
  ARROW_DCHECK(offset + length <= array.length);
  DCHECK_GT(length, 0);
  DCHECK(!value_run_builder_->has_open_run());

  ree_util::RunEndEncodedArraySpan<RunEndCType> ree_span(array, array.offset + offset,
                                                         length);
  const int64_t physical_offset = ree_span.PhysicalIndex(0);
  const int64_t physical_length =
      ree_span.PhysicalIndex(ree_span.length() - 1) + 1 - physical_offset;

  RETURN_NOT_OK(ReservePhysical(physical_length));

  // Append all the run ends from array sliced by offset and length
  for (auto it = ree_span.iterator(0, physical_offset); !it.is_end(ree_span); ++it) {
    const int64_t run_end = committed_logical_length_ + it.run_length();
    RETURN_NOT_OK(DoAppendRunEnd<RunEndCType>(run_end));
    UpdateDimensions(run_end, 0);
  }

  // Append all the values directly
  RETURN_NOT_OK(value_run_builder_->AppendRunCompressedArraySlice(
      ree_util::ValuesArray(array), physical_offset, physical_length));

  return Status::OK();
}

Status RunEndEncodedBuilder::AppendArraySlice(const ArraySpan& array, int64_t offset,
                                              int64_t length) {
  ARROW_DCHECK(array.type->Equals(type_));

  // Ensure any open run is closed before appending the array slice.
  RETURN_NOT_OK(value_run_builder_->FinishCurrentRun());

  if (length == 0) {
    return Status::OK();
  }

  switch (type_->run_end_type()->id()) {
    case Type::INT16:
      RETURN_NOT_OK(DoAppendArraySlice<int16_t>(array, offset, length));
      break;
    case Type::INT32:
      RETURN_NOT_OK(DoAppendArraySlice<int32_t>(array, offset, length));
      break;
    case Type::INT64:
      RETURN_NOT_OK(DoAppendArraySlice<int64_t>(array, offset, length));
      break;
    default:
      return Status::Invalid("Invalid type for run ends array: ", type_->run_end_type());
  }

  return Status::OK();
}

std::shared_ptr<DataType> RunEndEncodedBuilder::type() const { return type_; }

Status RunEndEncodedBuilder::FinishInternal(std::shared_ptr<ArrayData>* out) {
  // Finish the values array before so we can close the current run and append
  // the last run-end.
  std::shared_ptr<ArrayData> values_data;
  RETURN_NOT_OK(value_run_builder_->FinishInternal(&values_data));
  auto values_array = MakeArray(values_data);

  ARROW_ASSIGN_OR_RAISE(auto run_ends_array, run_end_builder().Finish());

  ARROW_ASSIGN_OR_RAISE(auto ree_array,
                        RunEndEncodedArray::Make(length_, run_ends_array, values_array));
  *out = std::move(ree_array->data());
  return Status::OK();
}

Status RunEndEncodedBuilder::FinishCurrentRun() {
  RETURN_NOT_OK(value_run_builder_->FinishCurrentRun());
  UpdateDimensions(length_, 0);
  return Status::OK();
}

template <typename RunEndCType>
Status RunEndEncodedBuilder::DoAppendRunEnd(int64_t run_end) {
  constexpr auto max = std::numeric_limits<RunEndCType>::max();
  if (ARROW_PREDICT_FALSE(run_end > max)) {
    return Status::Invalid("Run end value must fit on run ends type but ", run_end, " > ",
                           max, ".");
  }
  return internal::checked_cast<typename CTypeTraits<RunEndCType>::BuilderType*>(
             children_[0].get())
      ->Append(static_cast<RunEndCType>(run_end));
}

Status RunEndEncodedBuilder::AppendRunEnd(int64_t run_end) {
  switch (type_->run_end_type()->id()) {
    case Type::INT16:
      RETURN_NOT_OK(DoAppendRunEnd<int16_t>(run_end));
      break;
    case Type::INT32:
      RETURN_NOT_OK(DoAppendRunEnd<int32_t>(run_end));
      break;
    case Type::INT64:
      RETURN_NOT_OK(DoAppendRunEnd<int64_t>(run_end));
      break;
    default:
      return Status::Invalid("Invalid type for run ends array: ", type_->run_end_type());
  }
  return Status::OK();
}

Status RunEndEncodedBuilder::CloseRun(int64_t run_length) {
  // TODO(felipecrv): gracefully fragment runs bigger than INT32_MAX
  if (ARROW_PREDICT_FALSE(run_length > std::numeric_limits<int32_t>::max())) {
    return Status::Invalid(
        "Run-length of run-encoded arrays must fit in a 32-bit signed integer.");
  }
  int64_t run_end;
  if (internal::AddWithOverflow(committed_logical_length_, run_length, &run_end)) {
    return Status::Invalid("Run end value must fit on run ends type.");
  }
  RETURN_NOT_OK(AppendRunEnd(/*run_end=*/run_end));
  UpdateDimensions(run_end, 0);
  return Status::OK();
}

ArrayBuilder& RunEndEncodedBuilder::run_end_builder() { return *children_[0]; }
ArrayBuilder& RunEndEncodedBuilder::value_builder() { return *children_[1]; }

}  // namespace arrow
