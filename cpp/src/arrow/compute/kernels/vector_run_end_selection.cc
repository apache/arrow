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

#include "arrow/compute/kernels/vector_run_end_selection.h"
#include "arrow/util/logging.h"
#include "arrow/util/ree_util.h"

namespace arrow::compute::internal {

namespace {

template <typename RunEndType, typename FilterRunEndType>
int64_t REEGetREEFilterOutputSizeImpl(
    const ArraySpan& values, const ArraySpan& filter,
    FilterOptions::NullSelectionBehavior null_selection) {
  using RunEndCType = typename RunEndType::c_type;
  using FilterRunEndCType = typename FilterRunEndType::c_type;
  int64_t num_output_runs = 0;

  const ArraySpan& filter_values = ree_util::ValuesArray(filter);
  const int64_t filter_values_offset = filter_values.offset;
  const uint8_t* filter_is_valid = filter_values.buffers[0].data;
  const uint8_t* filter_selection = filter_values.buffers[1].data;

  const ree_util::RunEndEncodedArraySpan<RunEndCType> values_span(values);
  const ree_util::RunEndEncodedArraySpan<FilterRunEndCType> filter_span(filter);
  ree_util::MergedRunsIterator it(values_span, filter_span);
  if (filter_values.MayHaveNulls()) {
    if (null_selection == FilterOptions::EMIT_NULL) {
      while (!it.isEnd()) {
        const int64_t i = filter_values_offset + it.index_into_right_array();
        if (!bit_util::GetBit(filter_is_valid, i) ||
            bit_util::GetBit(filter_selection, i)) {
          num_output_runs += 1;
        }
        ++it;
      }
    } else {  // DROP nulls
      while (!it.isEnd()) {
        const int64_t i = filter_values_offset + it.index_into_right_array();
        if (bit_util::GetBit(filter_is_valid, i) &&
            bit_util::GetBit(filter_selection, i)) {
          num_output_runs += 1;
        }
        ++it;
      }
    }
  } else {
    while (!it.isEnd()) {
      const int64_t i = filter_values_offset + it.index_into_right_array();
      if (bit_util::GetBit(filter_selection, i)) {
        num_output_runs += 1;
      }
      ++it;
    }
  }
  return num_output_runs;
}

}  // namespace

int64_t REEGetREEFilterOutputSize(const ArraySpan& values, const ArraySpan& filter,
                                  FilterOptions::NullSelectionBehavior null_selection) {
  DCHECK_EQ(values.type->id(), Type::RUN_END_ENCODED);
  DCHECK_EQ(filter.type->id(), Type::RUN_END_ENCODED);
  auto values_run_end_type = ree_util::RunEndsArray(values).type->id();
  auto filter_run_end_type = ree_util::RunEndsArray(filter).type->id();

  switch (values_run_end_type) {
    case Type::INT16:
      switch (filter_run_end_type) {
        case Type::INT16:
          return REEGetREEFilterOutputSizeImpl<Int16Type, Int16Type>(values, filter,
                                                                     null_selection);
        case Type::INT32:
          return REEGetREEFilterOutputSizeImpl<Int16Type, Int32Type>(values, filter,
                                                                     null_selection);
        default:
          DCHECK_EQ(filter_run_end_type, Type::INT64);
          return REEGetREEFilterOutputSizeImpl<Int16Type, Int64Type>(values, filter,
                                                                     null_selection);
      }
    case Type::INT32:
      switch (filter_run_end_type) {
        case Type::INT16:
          return REEGetREEFilterOutputSizeImpl<Int32Type, Int16Type>(values, filter,
                                                                     null_selection);
        case Type::INT32:
          return REEGetREEFilterOutputSizeImpl<Int32Type, Int32Type>(values, filter,
                                                                     null_selection);
        default:
          DCHECK_EQ(filter_run_end_type, Type::INT64);
          return REEGetREEFilterOutputSizeImpl<Int32Type, Int64Type>(values, filter,
                                                                     null_selection);
      }
    default:
      DCHECK_EQ(values_run_end_type, Type::INT64);
      switch (filter_run_end_type) {
        case Type::INT16:
          return REEGetREEFilterOutputSizeImpl<Int64Type, Int16Type>(values, filter,
                                                                     null_selection);
        case Type::INT32:
          return REEGetREEFilterOutputSizeImpl<Int64Type, Int32Type>(values, filter,
                                                                     null_selection);
        default:
          DCHECK_EQ(filter_run_end_type, Type::INT64);
          return REEGetREEFilterOutputSizeImpl<Int64Type, Int64Type>(values, filter,
                                                                     null_selection);
      }
  }
}

}  // namespace arrow::compute::internal
