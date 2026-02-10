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

#include "arrow/util/ree_util.h"

#include <cstring>
#include <limits>
#include <memory>
#include <utility>

#include "arrow/buffer.h"
#include "arrow/memory_pool.h"
#include "arrow/result.h"
#include "arrow/status.h"
#include "arrow/type.h"
#include "arrow/type_traits.h"
#include "arrow/util/logging.h"

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

int64_t LogicalNullCount(const ArraySpan& span) {
  const auto type_id = RunEndsArray(span).type->id();
  if (type_id == Type::INT16) {
    return LogicalNullCount<int16_t>(span);
  }
  if (type_id == Type::INT32) {
    return LogicalNullCount<int32_t>(span);
  }
  ARROW_DCHECK_EQ(type_id, Type::INT64);
  return LogicalNullCount<int64_t>(span);
}

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
      ARROW_DCHECK_EQ(type.run_end_type()->id(), Type::INT64);
      return ValidateRunEndEncodedChildren<int64_t>(
          type, logical_length, run_ends_data, values_data, null_count, logical_offset);
  }
}

int64_t FindPhysicalIndex(const ArraySpan& span, int64_t i, int64_t absolute_offset) {
  const auto type_id = RunEndsArray(span).type->id();
  if (type_id == Type::INT16) {
    return internal::FindPhysicalIndex<int16_t>(span, i, absolute_offset);
  }
  if (type_id == Type::INT32) {
    return internal::FindPhysicalIndex<int32_t>(span, i, absolute_offset);
  }
  ARROW_DCHECK_EQ(type_id, Type::INT64);
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
  ARROW_DCHECK_EQ(type_id, Type::INT64);
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
  ARROW_DCHECK_EQ(type_id, Type::INT64);
  auto* run_ends = run_ends_span.GetValues<int64_t>(1);
  return internal::FindPhysicalRange<int64_t>(run_ends, run_ends_span.length, length,
                                              offset);
}

namespace {

template <typename RunEndType, typename ValueType, bool has_validity_buffer>
Result<std::shared_ptr<ArrayData>> RunEndEncodeImplExec(const ArraySpan& input_array,
                                                        MemoryPool* pool) {
  using RunEndCType = typename RunEndType::c_type;
  const int64_t input_length = input_array.length;

  auto run_end_type = TypeTraits<RunEndType>::type_singleton();
  auto ree_type =
      std::make_shared<RunEndEncodedType>(run_end_type, input_array.type->GetSharedPtr());

  if (input_length == 0) {
    return internal::PreallocateREEArray(std::move(ree_type), has_validity_buffer,
                                         /*logical_length=*/input_length,
                                         /*physical_length=*/0, pool,
                                         /*data_buffer_size=*/0);
  }

  // First pass: count the number of runs
  int64_t num_valid_runs = 0;
  int64_t num_output_runs = 0;
  int64_t data_buffer_size = 0;  // for string and binary types
  RETURN_NOT_OK(internal::ValidateRunEndType(run_end_type, input_length));

  internal::RunEndEncodingLoop<RunEndType, ValueType, has_validity_buffer> counting_loop(
      input_array,
      /*output_values_array_data=*/NULLPTR,
      /*output_run_ends=*/NULLPTR);
  std::tie(num_valid_runs, num_output_runs, data_buffer_size) =
      counting_loop.CountNumberOfRuns();
  const auto physical_null_count = num_output_runs - num_valid_runs;
  ARROW_DCHECK(!has_validity_buffer || physical_null_count > 0)
      << "has_validity_buffer is expected to imply physical_null_count > 0";

  ARROW_ASSIGN_OR_RAISE(auto output_array_data,
                        internal::PreallocateREEArray(
                            std::move(ree_type), has_validity_buffer,
                            /*logical_length=*/input_length,
                            /*physical_length=*/num_output_runs, pool, data_buffer_size));

  // Initialize the output pointers
  auto* output_run_ends =
      output_array_data->child_data[0]->template GetMutableValues<RunEndCType>(1, 0);
  auto* output_values_array_data = output_array_data->child_data[1].get();
  // Set the null_count on the physical array
  output_values_array_data->null_count = physical_null_count;

  // Second pass: write the runs
  internal::RunEndEncodingLoop<RunEndType, ValueType, has_validity_buffer> writing_loop(
      input_array, output_values_array_data, output_run_ends);
  [[maybe_unused]] int64_t num_written_runs = writing_loop.WriteEncodedRuns();
  ARROW_DCHECK_EQ(num_written_runs, num_output_runs);

  return output_array_data;
}

template <typename RunEndType>
Result<std::shared_ptr<ArrayData>> RunEndEncodeDispatchValueType(const ArraySpan& input,
                                                                 MemoryPool* pool) {
  const bool has_validity_buffer = input.GetNullCount() > 0;
  switch (input.type->id()) {
    case Type::NA:
      if (input.length == 0) {
        return internal::MakeNullREEArray(TypeTraits<RunEndType>::type_singleton(), 0,
                                          pool);
      }
      RETURN_NOT_OK(internal::ValidateRunEndType(TypeTraits<RunEndType>::type_singleton(),
                                                 input.length));
      return internal::MakeNullREEArray(TypeTraits<RunEndType>::type_singleton(),
                                        input.length, pool);

#define REE_ENCODE_CASE(TYPE_CLASS, TYPE_ENUM)                                         \
  case Type::TYPE_ENUM:                                                                \
    return has_validity_buffer                                                         \
               ? RunEndEncodeImplExec<RunEndType, TYPE_CLASS##Type, true>(input, pool) \
               : RunEndEncodeImplExec<RunEndType, TYPE_CLASS##Type, false>(input, pool);

      ARROW_REE_SUPPORTED_TYPES(REE_ENCODE_CASE)

#undef REE_ENCODE_CASE

    default:
      return Status::NotImplemented("RunEndEncode not implemented for type ",
                                    input.type->ToString());
  }
}

}  // namespace

Result<std::shared_ptr<ArrayData>> RunEndEncodeArray(
    const ArraySpan& input, const std::shared_ptr<DataType>& run_end_type,
    MemoryPool* pool) {
  switch (run_end_type->id()) {
    case Type::INT16:
      return RunEndEncodeDispatchValueType<Int16Type>(input, pool);
    case Type::INT32:
      return RunEndEncodeDispatchValueType<Int32Type>(input, pool);
    case Type::INT64:
      return RunEndEncodeDispatchValueType<Int64Type>(input, pool);
    default:
      return Status::Invalid("Invalid run end type: ", run_end_type->ToString());
  }
}

Result<std::shared_ptr<ArrayData>> RunEndEncodeArray(
    const std::shared_ptr<ArrayData>& input,
    const std::shared_ptr<DataType>& run_end_type, MemoryPool* pool) {
  ArraySpan span(*input);
  return RunEndEncodeArray(span, run_end_type, pool);
}

namespace internal {

/// \pre 0 <= i < array_span.length()
template <typename RunEndCType>
int64_t FindPhysicalIndexImpl(PhysicalIndexFinder<RunEndCType>& self, int64_t i) {
  ARROW_DCHECK_LT(i, self.array_span.length);
  const int64_t run_ends_size = ree_util::RunEndsArray(self.array_span).length;
  ARROW_DCHECK_LT(self.last_physical_index, run_ends_size);
  // This access to self.run_ends[last_physical_index] is always safe because:
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
    ARROW_DCHECK_LT(j, self.last_physical_index);
    return self.last_physical_index = j;
  }

  // last_physical_index is not an upper-bound, and the logical index i MUST be
  // in the runs that follow it. Since i is a valid logical index, we know that at least
  // one extra run is present.
  ARROW_DCHECK_LT(self.last_physical_index + 1, run_ends_size);
  const int64_t min_physical_index = self.last_physical_index + 1;

  const int64_t j = ree_util::internal::FindPhysicalIndex<RunEndCType>(
      /*run_ends=*/self.run_ends + min_physical_index,
      /*run_ends_size=*/run_ends_size - min_physical_index, i, self.array_span.offset);
  ARROW_DCHECK_LT(min_physical_index + j, run_ends_size);
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

Result<std::shared_ptr<Buffer>> AllocateValuesBuffer(int64_t length, const DataType& type,
                                                     MemoryPool* pool,
                                                     int64_t data_buffer_size) {
  if (type.bit_width() == 1) {
    return AllocateEmptyBitmap(length, pool);
  } else if (is_fixed_width(type.id())) {
    return AllocateBuffer(length * type.byte_width(), pool);
  } else {
    ARROW_DCHECK(is_base_binary_like(type.id()));
    return AllocateBuffer(data_buffer_size, pool);
  }
}

Result<std::shared_ptr<ArrayData>> PreallocateRunEndsArray(
    const std::shared_ptr<DataType>& run_end_type, int64_t physical_length,
    MemoryPool* pool) {
  ARROW_DCHECK(is_run_end_type(run_end_type->id()));
  ARROW_ASSIGN_OR_RAISE(
      auto run_ends_buffer,
      AllocateBuffer(physical_length * run_end_type->byte_width(), pool));
  return ArrayData::Make(run_end_type, physical_length,
                         {NULLPTR, std::move(run_ends_buffer)}, /*null_count=*/0);
}

Result<std::shared_ptr<ArrayData>> PreallocateValuesArray(
    const std::shared_ptr<DataType>& value_type, bool has_validity_buffer, int64_t length,
    MemoryPool* pool, int64_t data_buffer_size) {
  std::vector<std::shared_ptr<Buffer>> values_data_buffers;
  std::shared_ptr<Buffer> validity_buffer = NULLPTR;
  if (has_validity_buffer) {
    ARROW_ASSIGN_OR_RAISE(validity_buffer, AllocateEmptyBitmap(length, pool));
  }
  ARROW_ASSIGN_OR_RAISE(auto values_buffer, AllocateValuesBuffer(length, *value_type,
                                                                 pool, data_buffer_size));
  if (is_base_binary_like(value_type->id())) {
    const int offset_byte_width = offset_bit_width(value_type->id()) / 8;
    ARROW_ASSIGN_OR_RAISE(auto offsets_buffer,
                          AllocateBuffer((length + 1) * offset_byte_width, pool));
    memset(offsets_buffer->mutable_data(), 0, offset_byte_width);
    offsets_buffer->ZeroPadding();
    values_data_buffers = {std::move(validity_buffer), std::move(offsets_buffer),
                           std::move(values_buffer)};
  } else {
    values_data_buffers = {std::move(validity_buffer), std::move(values_buffer)};
  }
  auto data = ArrayData::Make(value_type, length, std::move(values_data_buffers),
                              kUnknownNullCount);
  ARROW_DCHECK(!(has_validity_buffer && length > 0) || data->buffers[0]);
  return data;
}

Result<std::shared_ptr<ArrayData>> PreallocateREEArray(
    std::shared_ptr<RunEndEncodedType> ree_type, bool has_validity_buffer,
    int64_t logical_length, int64_t physical_length, MemoryPool* pool,
    int64_t data_buffer_size) {
  ARROW_ASSIGN_OR_RAISE(
      auto run_ends_data,
      PreallocateRunEndsArray(ree_type->run_end_type(), physical_length, pool));
  ARROW_ASSIGN_OR_RAISE(auto values_data, PreallocateValuesArray(
                                              ree_type->value_type(), has_validity_buffer,
                                              physical_length, pool, data_buffer_size));
  return ArrayData::Make(std::move(ree_type), logical_length, {NULLPTR},
                         {std::move(run_ends_data), std::move(values_data)},
                         /*null_count=*/0);
}

void WriteSingleRunEnd(ArrayData* run_ends_data, int64_t run_end) {
  ARROW_DCHECK_GT(run_end, 0);
  ARROW_DCHECK(is_run_end_type(run_ends_data->type->id()));
  auto* output_run_ends = run_ends_data->template GetMutableValues<uint8_t>(1);
  switch (run_ends_data->type->id()) {
    case Type::INT16:
      *reinterpret_cast<int16_t*>(output_run_ends) = static_cast<int16_t>(run_end);
      break;
    case Type::INT32:
      *reinterpret_cast<int32_t*>(output_run_ends) = static_cast<int32_t>(run_end);
      break;
    default:
      ARROW_DCHECK_EQ(run_ends_data->type->id(), Type::INT64);
      *reinterpret_cast<int64_t*>(output_run_ends) = static_cast<int64_t>(run_end);
      break;
  }
}

Result<std::shared_ptr<ArrayData>> MakeNullREEArray(
    const std::shared_ptr<DataType>& run_end_type, int64_t logical_length,
    MemoryPool* pool) {
  auto ree_type = std::make_shared<RunEndEncodedType>(run_end_type, null());
  const int64_t physical_length = logical_length > 0 ? 1 : 0;
  ARROW_ASSIGN_OR_RAISE(auto run_ends_data,
                        PreallocateRunEndsArray(run_end_type, physical_length, pool));
  if (logical_length > 0) {
    WriteSingleRunEnd(run_ends_data.get(), logical_length);
  }
  auto values_data = ArrayData::Make(null(), physical_length, {NULLPTR},
                                     /*null_count=*/physical_length);
  return ArrayData::Make(std::move(ree_type), logical_length, {NULLPTR},
                         {std::move(run_ends_data), std::move(values_data)},
                         /*null_count=*/0);
}

Status ValidateRunEndType(const std::shared_ptr<DataType>& run_end_type,
                          int64_t input_length) {
  int64_t run_end_max = std::numeric_limits<int64_t>::max();
  switch (run_end_type->id()) {
    case Type::INT16:
      run_end_max = std::numeric_limits<int16_t>::max();
      break;
    case Type::INT32:
      run_end_max = std::numeric_limits<int32_t>::max();
      break;
    default:
      ARROW_DCHECK_EQ(run_end_type->id(), Type::INT64);
      break;
  }
  if (input_length < 0 || input_length > run_end_max) {
    return Status::Invalid(
        "Cannot run-end encode Arrays with more elements than the "
        "run end type can hold: ",
        run_end_max);
  }
  return Status::OK();
}

}  // namespace internal
}  // namespace ree_util
}  // namespace arrow
