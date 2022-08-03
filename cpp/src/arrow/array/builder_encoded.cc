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

#include "arrow/array/builder_encoded.h"

#include <cstddef>
#include <cstdint>
#include <utility>
#include <vector>

#include "arrow/scalar.h"
#include "arrow/util/logging.h"
#include "arrow/util/checked_cast.h"
#include "arrow/util/rle

namespace arrow {

// ----------------------------------------------------------------------
// RunLengthEncodedBuilder

RunLengthEncodedBuilder::RunLengthEncodedBuilder(std::shared_ptr<ArrayBuilder> values_builder, MemoryPool *pool) :
  ArrayBuilder(pool), type_(internal::checked_pointer_cast<RunLengthEncodedType>(values_builder->type())), values_builder_(*values_builder) {
  children_ = {std::move(values_builder)};
}

void RunLengthEncodedBuilder::Reset() {
  capacity_ = length_ = null_count_ = 0;
  values_builder_.Reset();
  run_ends_builder_.Reset();
  current_value_.reset();
  run_start_ = 0;
}

Status RunLengthEncodedBuilder::AppendNull() {
  return AppendNulls(1);
}

Status RunLengthEncodedBuilder::AppendNulls(int64_t length) {
  if (length == 0) {
    return Status::OK();
  }
  if (current_value_) {
    RETURN_NOT_OK(FinishRun());
    current_value_ = {};
  }
  return AddLength(length);
}

Status RunLengthEncodedBuilder::AppendEmptyValue() {
  return Status::NotImplemented("Appending empty values to RLE builder");
}

Status RunLengthEncodedBuilder::AppendEmptyValues(int64_t length) {
  return Status::NotImplemented("Appending empty values to RLE builder");
}

Status RunLengthEncodedBuilder::AppendScalar(const Scalar& scalar, int64_t n_repeats) {
  if (n_repeats == 0) {
    return Status::OK();
  }
  if (!current_value_ || !current_value_->Equals(scalar)) {
    RETURN_NOT_OK(FinishRun());
    current_value_ = scalar.shared_from_this();
  }
  return AddLength(n_repeats);
}

Status RunLengthEncodedBuilder::AppendScalars(const ScalarVector& scalars) {
  for (auto scalar: scalars) {
    RETURN_NOT_OK(AppendScalar(*scalar, 1));
  }
  return Status::OK();
}

Status RunLengthEncodedBuilder::AppendArraySlice(const ArraySpan& array, int64_t offset,
                                                 int64_t length) {
  auto full_array = array.ToArray();
  if (array.type->Equals(type_)) {
    // array is also run-length encoded
    for (size_t index = 0; index < length; index++) {
      // TODO: rle_util
      const int32_t *array_run_ends = array.GetValues<int32_t>(0, 1);
      ARROW_ASSIGN_OR_RAISE(auto scalar, full_array->GetScalar(offset + index));
      RETURN_NOT_OK(AppendScalar(*scalar, 1));
    }
  } else if (array.type->Equals(type_->encoded_type())) {
    // array is not encoded
    for (size_t index = 0; index < length; index++) {
      ARROW_ASSIGN_OR_RAISE(auto scalar, full_array->GetScalar(offset + index));
      RETURN_NOT_OK(AppendScalar(*scalar, 1));
    }
  } else {
    return Status::Invalid("adding ", array.type," array slice to ", type_, " builder");
  }
}


Status RunLengthEncodedBuilder::FinishRun() {
  if (length_ - run_start_ == 0) {
    return Status::OK();
  }
  if (current_value_) {
    RETURN_NOT_OK(values_builder_.AppendScalar(*current_value_));
  } else {
    RETURN_NOT_OK(values_builder_.AppendNull());
  }
  RETURN_NOT_OK(run_ends_builder_.Append(length_));
  current_value_.reset();
  run_start_ = 0;
  return Status::OK();
}

Status RunLengthEncodedBuilder::ResizePhysical(int64_t capacity) {
  RETURN_NOT_OK(values_builder_.Resize(capacity));
  return run_ends_builder_.Resize(capacity);
}

Status RunLengthEncodedBuilder::AddLength(int64_t added_length) {
  if (ARROW_PREDICT_FALSE(added_length < 0)) {
    return Status::Invalid(
        "Added length must be positive (requested: ", added_length, ")");
  }
  if (ARROW_PREDICT_FALSE(added_length > std::numeric_limits<int32_t>::max()
                          || length_ + added_length > std::numeric_limits<int32_t>::max())) {
    return Status::Invalid(
        "Run-length encoded array length must fit in a 32-bit signed integer (adding ",
          added_length, " items to array of length ", length_, ")");
  }

  length_ += added_length;
  return Status::OK();
}


}  // namespace arrow
