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

#include <cstdint>
#include <memory>

#include "arrow/compute/kernels/ree_util_internal.h"

#include "arrow/buffer.h"
#include "arrow/memory_pool.h"
#include "arrow/result.h"
#include "arrow/type.h"
#include "arrow/type_traits.h"
#include "arrow/util/logging.h"

namespace arrow {
namespace compute {
namespace internal {
namespace ree_util {

Result<std::shared_ptr<Buffer>> AllocateValuesBuffer(int64_t length, const DataType& type,
                                                     MemoryPool* pool,
                                                     int64_t data_buffer_size) {
  if (type.bit_width() == 1) {
    return AllocateBitmap(length, pool);
  } else if (is_fixed_width(type.id())) {
    return AllocateBuffer(length * type.byte_width(), pool);
  } else {
    DCHECK(is_base_binary_like(type.id()));
    return AllocateBuffer(data_buffer_size, pool);
  }
}

Result<std::shared_ptr<ArrayData>> PreallocateRunEndsArray(
    const std::shared_ptr<DataType>& run_end_type, int64_t physical_length,
    MemoryPool* pool) {
  DCHECK(is_run_end_type(run_end_type->id()));
  ARROW_ASSIGN_OR_RAISE(
      auto run_ends_buffer,
      AllocateBuffer(physical_length * run_end_type->byte_width(), pool));
  return ArrayData::Make(run_end_type, physical_length,
                         {NULLPTR, std::move(run_ends_buffer)}, /*null_count=*/0);
}

Result<std::shared_ptr<ArrayData>> PreallocateValuesArray(
    const std::shared_ptr<DataType>& value_type, bool has_validity_buffer, int64_t length,
    int64_t null_count, MemoryPool* pool, int64_t data_buffer_size) {
  std::vector<std::shared_ptr<Buffer>> values_data_buffers;
  std::shared_ptr<Buffer> validity_buffer = NULLPTR;
  if (has_validity_buffer) {
    ARROW_ASSIGN_OR_RAISE(validity_buffer, AllocateBitmap(length, pool));
  }
  ARROW_ASSIGN_OR_RAISE(auto values_buffer, AllocateValuesBuffer(length, *value_type,
                                                                 pool, data_buffer_size));
  if (is_base_binary_like(value_type->id())) {
    const int offset_byte_width = offset_bit_width(value_type->id()) / 8;
    ARROW_ASSIGN_OR_RAISE(auto offsets_buffer,
                          AllocateBuffer((length + 1) * offset_byte_width, pool));
    // Ensure the first offset is zero
    memset(offsets_buffer->mutable_data(), 0, offset_byte_width);
    offsets_buffer->ZeroPadding();
    values_data_buffers = {std::move(validity_buffer), std::move(offsets_buffer),
                           std::move(values_buffer)};
  } else {
    values_data_buffers = {std::move(validity_buffer), std::move(values_buffer)};
  }
  return ArrayData::Make(value_type, length, std::move(values_data_buffers), null_count);
}

Result<std::shared_ptr<ArrayData>> PreallocateREEArray(
    std::shared_ptr<RunEndEncodedType> ree_type, bool has_validity_buffer,
    int64_t logical_length, int64_t physical_length, int64_t physical_null_count,
    MemoryPool* pool, int64_t data_buffer_size) {
  ARROW_ASSIGN_OR_RAISE(
      auto run_ends_data,
      PreallocateRunEndsArray(ree_type->run_end_type(), physical_length, pool));
  ARROW_ASSIGN_OR_RAISE(
      auto values_data,
      PreallocateValuesArray(ree_type->value_type(), has_validity_buffer, physical_length,
                             physical_null_count, pool, data_buffer_size));

  return ArrayData::Make(std::move(ree_type), logical_length, {NULLPTR},
                         {std::move(run_ends_data), std::move(values_data)},
                         /*null_count=*/0);
}

void WriteSingleRunEnd(ArrayData* run_ends_data, int64_t run_end) {
  DCHECK_GT(run_end, 0);
  DCHECK(is_run_end_type(run_ends_data->type->id()));
  auto* output_run_ends = run_ends_data->template GetMutableValues<uint8_t>(1);
  switch (run_ends_data->type->id()) {
    case Type::INT16:
      *reinterpret_cast<int16_t*>(output_run_ends) = static_cast<int16_t>(run_end);
      break;
    case Type::INT32:
      *reinterpret_cast<int32_t*>(output_run_ends) = static_cast<int32_t>(run_end);
      break;
    default:
      DCHECK_EQ(run_ends_data->type->id(), Type::INT64);
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

}  // namespace ree_util
}  // namespace internal
}  // namespace compute
}  // namespace arrow
