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

#include <algorithm>
#include <cstdint>

#include "arrow/builder.h"
#include "arrow/util/bit_util.h"
#include "arrow/util/logging.h"
#include "arrow/util/ree_util.h"

namespace arrow {
namespace ree_util {

namespace {

template <typename RunEndCType>
int64_t LogicalNullCount(const ArraySpan& span) {
  const auto& values = ValuesArray(span);
  const auto& values_bitmap = values.buffers[0].data;
  int64_t null_count = 0;

  RunEndEncodedArraySpan<RunEndCType> ree_span(span);
  auto end = ree_span.end();
  for (auto it = ree_span.begin(); it != end; ++it) {
    const bool is_null =
        values_bitmap &&
        !bit_util::GetBit(values_bitmap, values.offset + it.index_into_array());
    if (is_null) {
      null_count += it.run_length();
    }
  }
  return null_count;
}

}  // namespace

int64_t LogicalNullCount(const ArraySpan& span) {
  const auto type_id = RunEndsArray(span).type->id();
  if (type_id == Type::INT16) {
    return LogicalNullCount<int16_t>(span);
  }
  if (type_id == Type::INT32) {
    return LogicalNullCount<int32_t>(span);
  }
  DCHECK_EQ(type_id, Type::INT64);
  return LogicalNullCount<int64_t>(span);
}

int64_t FindPhysicalIndex(const ArraySpan& span, int64_t i, int64_t absolute_offset) {
  const auto type_id = RunEndsArray(span).type->id();
  if (type_id == Type::INT16) {
    return internal::FindPhysicalIndex<int16_t>(span, i, absolute_offset);
  }
  if (type_id == Type::INT32) {
    return internal::FindPhysicalIndex<int32_t>(span, i, absolute_offset);
  }
  DCHECK_EQ(type_id, Type::INT64);
  return internal::FindPhysicalIndex<int64_t>(span, i, absolute_offset);
}

int64_t FindPhysicalLength(const ArraySpan& span) {
  auto type_id = RunEndsArray(span).type->id();
  if (type_id == Type::INT16) {
    return internal::FindPhysicalLength<int16_t>(span);
  }
  if (type_id == Type::INT32) {
    return internal::FindPhysicalLength<int32_t>(span);
  }
  DCHECK_EQ(type_id, Type::INT64);
  return internal::FindPhysicalLength<int64_t>(span);
}

namespace {

template <typename RunEndCType>
Status ValidateRunEndEncodedChildren(const RunEndEncodedType& type,
                                     int64_t logical_length,
                                     const std::shared_ptr<ArrayData>& run_ends_data,
                                     const std::shared_ptr<ArrayData>& values_data,
                                     int64_t null_count, int64_t logical_offset) {
  // Overflow was already checked at this point
  if (logical_offset + logical_length > std::numeric_limits<RunEndCType>::max()) {
    return Status::Invalid(
        "Offset + length of a run-end encoded array must fit in a value"
        " of the run end type ",
        *type.run_end_type(), ", but offset + length is ",
        logical_offset + logical_length, " while the allowed maximum is ",
        std::numeric_limits<RunEndCType>::max());
  }
  if (!run_ends_data) {
    return Status::Invalid("Run ends array is null pointer");
  }
  if (!values_data) {
    return Status::Invalid("Values array is null pointer");
  }
  if (*run_ends_data->type != *type.run_end_type()) {
    return Status::Invalid("Run ends array of ", type, " must be ", *type.run_end_type(),
                           ", but run end type is ", *run_ends_data->type);
  }
  if (*values_data->type != *type.value_type()) {
    return Status::Invalid("Parent type says this array encodes ", *type.value_type(),
                           " values, but value type is ", *values_data->type);
  }
  if (run_ends_data->GetNullCount() != 0) {
    return Status::Invalid("Null count must be 0 for run ends array, but is ",
                           run_ends_data->GetNullCount());
  }
  if (run_ends_data->length > values_data->length) {
    return Status::Invalid("Length of run_ends is greater than the length of values: ",
                           run_ends_data->length, " > ", values_data->length);
  }
  if (run_ends_data->length == 0) {
    if (logical_length == 0) {
      return Status::OK();
    }
    return Status::Invalid("Run-end encoded array has non-zero length ", logical_length,
                           ", but run ends array has zero length");
  }
  if (null_count != 0) {
    return Status::Invalid("Null count must be 0 for run-end encoded array, but is ",
                           null_count);
  }
  if (!run_ends_data->buffers[1]->is_cpu()) {
    return Status::OK();
  }
  const auto* run_ends = run_ends_data->GetValues<RunEndCType>(1);
  // The last run-end is the logical offset + the logical length.
  if (run_ends[run_ends_data->length - 1] < logical_offset + logical_length) {
    return Status::Invalid("Last run end is ", run_ends[run_ends_data->length - 1],
                           " but it should match ", logical_offset + logical_length,
                           " (offset: ", logical_offset, ", length: ", logical_length,
                           ")");
  }
  return Status::OK();
}

}  // namespace

Status ValidateRunEndEncodedChildren(const RunEndEncodedType& type,
                                     int64_t logical_length,
                                     const std::shared_ptr<ArrayData>& run_ends_data,
                                     const std::shared_ptr<ArrayData>& values_data,
                                     int64_t null_count, int64_t logical_offset) {
  switch (type.run_end_type()->id()) {
    case Type::INT16:
      return ValidateRunEndEncodedChildren<int16_t>(
          type, logical_length, run_ends_data, values_data, null_count, logical_offset);
    case Type::INT32:
      return ValidateRunEndEncodedChildren<int32_t>(
          type, logical_length, run_ends_data, values_data, null_count, logical_offset);
    default:
      DCHECK_EQ(type.run_end_type()->id(), Type::INT64);
      return ValidateRunEndEncodedChildren<int64_t>(
          type, logical_length, run_ends_data, values_data, null_count, logical_offset);
  }
}

}  // namespace ree_util
}  // namespace arrow
