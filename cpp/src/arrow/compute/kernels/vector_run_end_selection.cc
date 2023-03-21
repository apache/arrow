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
#include <tuple>

#include "arrow/compute/kernels/ree_util_internal.h"
#include "arrow/compute/kernels/vector_run_end_selection.h"
#include "arrow/result.h"
#include "arrow/util/logging.h"
#include "arrow/util/ree_util.h"

namespace arrow::compute::internal {

namespace {

/// \brief Iterate over REE values and a REE filter, emitting runs that pass the filter.
///
/// The filtering process can emit runs of the same value close to each other,
/// so to ensure the output of the filter takes advantage of run-end encoding,
/// these fragments should be combined by the caller.
template <typename ValuesRunEndType, typename FilterRunEndType, typename EmitRun>
void VisitREExREEFilterOutputFragments(
    const ArraySpan& values, const ArraySpan& filter,
    FilterOptions::NullSelectionBehavior null_selection, const EmitRun& emit_run) {
  using ValueRunEndCType = typename ValuesRunEndType::c_type;
  using FilterRunEndCType = typename FilterRunEndType::c_type;

  DCHECK_EQ(values.length, filter.length);

  const int64_t values_offset = arrow::ree_util::ValuesArray(values).offset;
  const ArraySpan& filter_values = arrow::ree_util::ValuesArray(filter);
  const int64_t filter_values_offset = filter_values.offset;
  const uint8_t* filter_is_valid = filter_values.buffers[0].data;
  const uint8_t* filter_selection = filter_values.buffers[1].data;
  const bool filter_may_have_nulls =
      filter_is_valid != NULLPTR && filter_values.GetNullCount() != 0;

  const arrow::ree_util::RunEndEncodedArraySpan<ValueRunEndCType> values_span(values);
  const arrow::ree_util::RunEndEncodedArraySpan<FilterRunEndCType> filter_span(filter);
  arrow::ree_util::MergedRunsIterator it(values_span, filter_span);
  if (filter_may_have_nulls) {
    if (null_selection == FilterOptions::EMIT_NULL) {
      int64_t last_i = -1;
      bool last_emit = false;
      bool last_emit_null = false;
      while (!it.is_end()) {
        const int64_t i = filter_values_offset + it.index_into_right_array();
        bool emit = last_emit;
        bool emit_null = last_emit_null;
        if (last_i != i) {
          emit_null = !bit_util::GetBit(filter_is_valid, i);
          emit = emit_null || bit_util::GetBit(filter_selection, i);
        }
        if (emit) {
          emit_run(values_offset + it.index_into_left_array(), it.run_length(),
                   emit_null);
        }
        last_i = i;
        last_emit = emit;
        last_emit_null = emit_null;
        ++it;
      }
    } else {  // DROP nulls
      int64_t last_i = -1;
      bool last_emit = false;
      while (!it.is_end()) {
        const int64_t i = filter_values_offset + it.index_into_right_array();
        const bool emit =
            (last_i == i)
                ? last_emit  // can skip GetBit() if filter index is same as last
                : bit_util::GetBit(filter_is_valid, i) &&
                      bit_util::GetBit(filter_selection, i);
        if (emit) {
          emit_run(values_offset + it.index_into_left_array(), it.run_length(), false);
        }
        last_i = i;
        last_emit = emit;
        ++it;
      }
    }
  } else {
    int64_t last_i = -1;
    bool last_emit = false;
    while (!it.is_end()) {
      const int64_t i = filter_values_offset + it.index_into_right_array();
      const bool emit =
          (last_i == i) ? last_emit  // can skip GetBit() if filter index is same as last
                        : bit_util::GetBit(filter_selection, i);
      if (emit) {
        emit_run(values_offset + it.index_into_left_array(), it.run_length(), false);
      }
      last_i = i;
      last_emit = emit;
      ++it;
    }
  }
}

template <typename FilterRunEndType>
bool REEFilterHasAtLeastOneEmit(const ArraySpan& filter,
                                FilterOptions::NullSelectionBehavior null_selection) {
  DCHECK_EQ(filter.type->id(), Type::RUN_END_ENCODED);
  if (filter.length == 0) {
    return false;
  }
  using FilterRunEndCType = typename FilterRunEndType::c_type;
  const ArraySpan& filter_values = arrow::ree_util::ValuesArray(filter);
  const int64_t filter_values_offset = filter_values.offset;
  const uint8_t* filter_is_valid = filter_values.buffers[0].data;
  const uint8_t* filter_selection = filter_values.buffers[1].data;
  const bool filter_may_have_nulls = filter_values.MayHaveNulls();

  const arrow::ree_util::RunEndEncodedArraySpan<FilterRunEndCType> filter_span(filter);
  auto it = filter_span.begin();
  if (filter_may_have_nulls) {
    if (null_selection == FilterOptions::EMIT_NULL) {
      while (!it.is_end(filter_span)) {
        const int64_t i = filter_values_offset + it.index_into_array();
        if ((!bit_util::GetBit(filter_is_valid, i) ||
             bit_util::GetBit(filter_selection, i)) &&
            ARROW_PREDICT_TRUE(it.run_length() > 0)) {
          return true;
        }
        ++it;
      }
    } else {  // DROP nulls
      while (!it.is_end(filter_span)) {
        const int64_t i = filter_values_offset + it.index_into_array();
        if (bit_util::GetBit(filter_is_valid, i) &&
            bit_util::GetBit(filter_selection, i) &&
            ARROW_PREDICT_TRUE(it.run_length() > 0)) {
          return true;
        }
        ++it;
      }
    }
  } else {
    while (!it.is_end(filter_span)) {
      const int64_t i = filter_values_offset + it.index_into_array();
      if (bit_util::GetBit(filter_selection, i) &&
          ARROW_PREDICT_TRUE(it.run_length() > 0)) {
        return true;
      }
      ++it;
    }
  }
  return false;
}

template <typename ValuesRunEndType, typename ValuesValueType, typename FilterRunEndType>
int64_t GetREExREEFilterOutputSizeImpl(
    const ArraySpan& values, const ArraySpan& filter,
    FilterOptions::NullSelectionBehavior null_selection) {
  const auto values_values = arrow::ree_util::ValuesArray(values);
  const int64_t null_count = values_values.GetNullCount();
  const bool all_values_are_null = null_count == values_values.length;
  const uint8_t* values_is_valid =
      (null_count == 0) ? NULLPTR : values_values.buffers[0].data;
  // values_is_valid implies at least one run value is null but not all
  DCHECK(!values_is_valid || null_count > 0);

  // We don't use anything that depends on has_validity_bitmap,
  // so we can pass false.
  ree_util::ReadWriteValue<ValuesValueType, false> read_write(values_values, NULLPTR);

  int64_t last_emitted_run_i = -1;
  // NOTE: If last_emitted_run_was_null is true, then the values of last_emitted_run_i
  // is irrelevant.
  bool last_emitted_run_was_null = false;
  // NOTE: The last emitted null run does not necessarily come from
  // values[last_emitted_run_i] because NULL values from filters (combined with
  // FilterOptions::EMIT_NULL) can cause nulls to be emitted into the output as well.
  int64_t num_output_runs = 0;
  VisitREExREEFilterOutputFragments<ValuesRunEndType, FilterRunEndType>(
      values, filter, null_selection,
      [all_values_are_null, values_is_valid, &read_write, &last_emitted_run_i,
       &last_emitted_run_was_null, &num_output_runs](
          int64_t i, int64_t run_length, int64_t emit_null_from_filter) noexcept {
        const bool emit_null = all_values_are_null || emit_null_from_filter ||
                               (values_is_valid && !bit_util::GetBit(values_is_valid, i));
        if (emit_null) {
          if (!last_emitted_run_was_null) {
            // Emitting a run of nulls.
            num_output_runs += 1;
            last_emitted_run_was_null = true;
          }
          return;
        }
        // Emitting a valid value run.
        if (last_emitted_run_was_null) {
          // Emitting a valid value run after a run of nulls.
          num_output_runs += 1;
          last_emitted_run_i = i;
          last_emitted_run_was_null = false;
        } else {
          const bool open_new_run =
              last_emitted_run_i != i &&
              (last_emitted_run_i < 0 ||
               !ARROW_PREDICT_FALSE(read_write.CompareValuesAt(last_emitted_run_i, i)));
          if (open_new_run) {
            num_output_runs += 1;
          }
          last_emitted_run_i = i;
        }
      });
  return num_output_runs;
}

/// \brief Common virtual base class for filter functions that involve run-end
/// encoded arrays on one side or another
class REEFilterExec {
 public:
  virtual ~REEFilterExec() = default;
  virtual int64_t CalculateOutputSize() const = 0;
};

template <typename ValuesRunEndType, typename ValuesValueType, typename FilterRunEndType>
class REExREEFilterExec : public REEFilterExec {
 private:
  const ArraySpan& values_;
  const ArraySpan& filter_;
  const FilterOptions::NullSelectionBehavior null_selection_;

 public:
  REExREEFilterExec(const ArraySpan& values, const ArraySpan& filter,
                    FilterOptions::NullSelectionBehavior null_selection)
      : values_(values), filter_(filter), null_selection_(null_selection) {}

  ~REExREEFilterExec() override = default;

  int64_t CalculateOutputSize() const override {
    if constexpr (std::is_same<ValuesValueType, NullType>::value) {
      return REEFilterHasAtLeastOneEmit<FilterRunEndType>(filter_, null_selection_) ? 1
                                                                                    : 0;
    } else {
      return GetREExREEFilterOutputSizeImpl<ValuesRunEndType, ValuesValueType,
                                            FilterRunEndType>(values_, filter_,
                                                              null_selection_);
    }
  }
};

template <typename ValuesValueType>
Result<std::unique_ptr<REEFilterExec>> MakeREExREEFilterExecImpl(
    const ArraySpan& values, const ArraySpan& filter, Type::type values_run_end_type,
    Type::type filter_run_end_type, FilterOptions::NullSelectionBehavior null_selection) {
  switch (values_run_end_type) {
    case Type::INT16:
      switch (filter_run_end_type) {
        case Type::INT16:
          return std::make_unique<
              REExREEFilterExec<Int16Type, ValuesValueType, Int16Type>>(values, filter,
                                                                        null_selection);
        case Type::INT32:
          return std::make_unique<
              REExREEFilterExec<Int16Type, ValuesValueType, Int32Type>>(values, filter,
                                                                        null_selection);
        default:
          DCHECK_EQ(filter_run_end_type, Type::INT64);
          return std::make_unique<
              REExREEFilterExec<Int16Type, ValuesValueType, Int64Type>>(values, filter,
                                                                        null_selection);
      }
    case Type::INT32:
      switch (filter_run_end_type) {
        case Type::INT16:
          return std::make_unique<
              REExREEFilterExec<Int32Type, ValuesValueType, Int16Type>>(values, filter,
                                                                        null_selection);
        case Type::INT32:
          return std::make_unique<
              REExREEFilterExec<Int32Type, ValuesValueType, Int32Type>>(values, filter,
                                                                        null_selection);
        default:
          DCHECK_EQ(filter_run_end_type, Type::INT64);
          return std::make_unique<
              REExREEFilterExec<Int32Type, ValuesValueType, Int64Type>>(values, filter,
                                                                        null_selection);
      }
    default:
      DCHECK_EQ(values_run_end_type, Type::INT64);
      switch (filter_run_end_type) {
        case Type::INT16:
          return std::make_unique<
              REExREEFilterExec<Int64Type, ValuesValueType, Int16Type>>(values, filter,
                                                                        null_selection);
        case Type::INT32:
          return std::make_unique<
              REExREEFilterExec<Int64Type, ValuesValueType, Int32Type>>(values, filter,
                                                                        null_selection);
        default:
          DCHECK_EQ(filter_run_end_type, Type::INT64);
          return std::make_unique<
              REExREEFilterExec<Int64Type, ValuesValueType, Int64Type>>(values, filter,
                                                                        null_selection);
      }
  }
}

}  // namespace

Result<std::unique_ptr<REEFilterExec>> MakeREExREEFilterExec(
    const ArraySpan& values, const ArraySpan& filter,
    FilterOptions::NullSelectionBehavior null_selection) {
  DCHECK_EQ(values.type->id(), Type::RUN_END_ENCODED);
  DCHECK_EQ(filter.type->id(), Type::RUN_END_ENCODED);
  auto values_run_end_type = arrow::ree_util::RunEndsArray(values).type->id();
  auto filter_run_end_type = arrow::ree_util::RunEndsArray(filter).type->id();
  if (ARROW_PREDICT_FALSE(!is_run_end_type(values_run_end_type) ||
                          !is_run_end_type(filter_run_end_type))) {
    const auto& invalid_run_end_type = !is_run_end_type(values_run_end_type)
                                           ? arrow::ree_util::RunEndsArray(values).type
                                           : arrow::ree_util::RunEndsArray(filter).type;
    return Status::Invalid("Invalid run end type: ", invalid_run_end_type);
  }
  switch (arrow::ree_util::ValuesArray(values).type->id()) {
    case Type::NA:
      return MakeREExREEFilterExecImpl<NullType>(values, filter, values_run_end_type,
                                                 filter_run_end_type, null_selection);
    case Type::BOOL:
      return MakeREExREEFilterExecImpl<BooleanType>(values, filter, values_run_end_type,
                                                    filter_run_end_type, null_selection);
    case Type::UINT8:
    case Type::INT8:
      return MakeREExREEFilterExecImpl<UInt8Type>(values, filter, values_run_end_type,
                                                  filter_run_end_type, null_selection);
    case Type::UINT16:
    case Type::INT16:
      return MakeREExREEFilterExecImpl<UInt16Type>(values, filter, values_run_end_type,
                                                   filter_run_end_type, null_selection);
    case Type::UINT32:
    case Type::INT32:
    case Type::FLOAT:
    case Type::DATE32:
    case Type::TIME32:
    case Type::INTERVAL_MONTHS:
      return MakeREExREEFilterExecImpl<UInt32Type>(values, filter, values_run_end_type,
                                                   filter_run_end_type, null_selection);
    case Type::UINT64:
    case Type::INT64:
    case Type::DOUBLE:
    case Type::DATE64:
    case Type::TIMESTAMP:
    case Type::TIME64:
    case Type::DURATION:
    case Type::INTERVAL_DAY_TIME:
      return MakeREExREEFilterExecImpl<UInt64Type>(values, filter, values_run_end_type,
                                                   filter_run_end_type, null_selection);
    case Type::INTERVAL_MONTH_DAY_NANO:
      return MakeREExREEFilterExecImpl<MonthDayNanoIntervalType>(
          values, filter, values_run_end_type, filter_run_end_type, null_selection);
    case Type::DECIMAL128:
      return MakeREExREEFilterExecImpl<Decimal128Type>(
          values, filter, values_run_end_type, filter_run_end_type, null_selection);
    case Type::DECIMAL256:
      return MakeREExREEFilterExecImpl<Decimal256Type>(
          values, filter, values_run_end_type, filter_run_end_type, null_selection);
    case Type::FIXED_SIZE_BINARY:
      return MakeREExREEFilterExecImpl<FixedSizeBinaryType>(
          values, filter, values_run_end_type, filter_run_end_type, null_selection);
    case Type::STRING:
      return MakeREExREEFilterExecImpl<StringType>(values, filter, values_run_end_type,
                                                   filter_run_end_type, null_selection);
    case Type::BINARY:
      return MakeREExREEFilterExecImpl<BinaryType>(values, filter, values_run_end_type,
                                                   filter_run_end_type, null_selection);
    case Type::LARGE_STRING:
      return MakeREExREEFilterExecImpl<LargeStringType>(
          values, filter, values_run_end_type, filter_run_end_type, null_selection);
    case Type::LARGE_BINARY:
      return MakeREExREEFilterExecImpl<LargeBinaryType>(
          values, filter, values_run_end_type, filter_run_end_type, null_selection);
    default:
      DCHECK(false);
      return Status::NotImplemented(
          "Unsupported type for run-end encoded array "
          "filtered by a run-end encoded filter.");
  }
}

Result<int64_t> CalculateREExREEFilterOutputSize(
    const ArraySpan& values, const ArraySpan& filter,
    FilterOptions::NullSelectionBehavior null_selection) {
  ARROW_ASSIGN_OR_RAISE(auto exec, MakeREExREEFilterExec(values, filter, null_selection));
  return exec->CalculateOutputSize();
}

}  // namespace arrow::compute::internal
