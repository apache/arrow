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

namespace internal {

/// \pre 0 <= i < array_span.length()
template <typename RunEndCType>
int64_t FindPhysicalIndexImpl(PhysicalIndexFinder<RunEndCType>& self, int64_t i) {
  DCHECK_LT(i, self.array_span.length);
  const int64_t run_ends_size = ree_util::RunEndsArray(self.array_span).length;
  DCHECK_LT(self.last_physical_index, run_ends_size);
  // This access to self.run_ends[last_physical_index] is alwas safe because:
  // 1. 0 <= i < array_span.length() implies there is at least one run and the initial
  //    value 0 will be safe to index with.
  // 2. last_physical_index > 0 is always the result of a valid call to
  //    internal::FindPhysicalIndex.
  if (ARROW_PREDICT_TRUE(self.array_span.offset + i <
                         self.run_ends[self.last_physical_index])) {
    // The cached value is an upper-bound, but is it the least upper-bound?
    if (self.last_physical_index == 0 ||
        self.array_span.offset + i >= self.run_ends[self.last_physical_index - 1]) {
      return self.last_physical_index;
    }
    // last_physical_index - 1 is a candidate for the least upper-bound,
    // so search for the least upper-bound in the range that includes it.
    const int64_t j = ree_util::internal::FindPhysicalIndex<RunEndCType>(
        self.run_ends, /*run_ends_size=*/self.last_physical_index, i,
        self.array_span.offset);
    DCHECK_LT(j, self.last_physical_index);
    return self.last_physical_index = j;
  }

  // last_physical_index is not an upper-bound, and the logical index i MUST be
  // in the runs that follow it. Since i is a valid logical index, we know that at least
  // one extra run is present.
  DCHECK_LT(self.last_physical_index + 1, run_ends_size);
  const int64_t min_physical_index = self.last_physical_index + 1;

  const int64_t j = ree_util::internal::FindPhysicalIndex<RunEndCType>(
      /*run_ends=*/self.run_ends + min_physical_index,
      /*run_ends_size=*/run_ends_size - min_physical_index, i, self.array_span.offset);
  DCHECK_LT(min_physical_index + j, run_ends_size);
  return self.last_physical_index = min_physical_index + j;
}

int64_t FindPhysicalIndexImpl16(PhysicalIndexFinder<int16_t>& self, int64_t i) {
  return FindPhysicalIndexImpl(self, i);
}

int64_t FindPhysicalIndexImpl32(PhysicalIndexFinder<int32_t>& self, int64_t i) {
  return FindPhysicalIndexImpl(self, i);
}

int64_t FindPhysicalIndexImpl64(PhysicalIndexFinder<int64_t>& self, int64_t i) {
  return FindPhysicalIndexImpl(self, i);
}

}  // namespace internal

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

std::pair<int64_t, int64_t> FindPhysicalRange(const ArraySpan& span, int64_t offset,
                                              int64_t length) {
  const auto& run_ends_span = RunEndsArray(span);
  auto type_id = run_ends_span.type->id();
  if (type_id == Type::INT16) {
    auto* run_ends = run_ends_span.GetValues<int16_t>(1);
    return internal::FindPhysicalRange<int16_t>(run_ends, run_ends_span.length, length,
                                                offset);
  }
  if (type_id == Type::INT32) {
    auto* run_ends = run_ends_span.GetValues<int32_t>(1);
    return internal::FindPhysicalRange<int32_t>(run_ends, run_ends_span.length, length,
                                                offset);
  }
  DCHECK_EQ(type_id, Type::INT64);
  auto* run_ends = run_ends_span.GetValues<int64_t>(1);
  return internal::FindPhysicalRange<int64_t>(run_ends, run_ends_span.length, length,
                                              offset);
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
