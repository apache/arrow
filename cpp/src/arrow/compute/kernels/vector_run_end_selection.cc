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

#include "arrow/compute/api_vector.h"
#include "arrow/compute/kernels/codegen_internal.h"
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

/// \brief Counts how many logical values are emitted by a REE filter
///
/// This is used when we know that the values array is only NULLs (Type::NA
/// arrays).
template <typename FilterRunEndType>
int64_t CountREEFilterEmits(const ArraySpan& filter,
                            FilterOptions::NullSelectionBehavior null_selection) {
  DCHECK_EQ(filter.type->id(), Type::RUN_END_ENCODED);
  if (filter.length == 0) {
    return 0;
  }
  using FilterRunEndCType = typename FilterRunEndType::c_type;
  const ArraySpan& filter_values = arrow::ree_util::ValuesArray(filter);
  const int64_t filter_values_offset = filter_values.offset;
  const uint8_t* filter_is_valid = filter_values.buffers[0].data;
  const uint8_t* filter_selection = filter_values.buffers[1].data;
  const bool filter_may_have_nulls = filter_values.MayHaveNulls();

  int64_t logical_count = 0;
  const arrow::ree_util::RunEndEncodedArraySpan<FilterRunEndCType> filter_span(filter);
  auto it = filter_span.begin();
  if (filter_may_have_nulls) {
    if (null_selection == FilterOptions::EMIT_NULL) {
      while (!it.is_end(filter_span)) {
        const int64_t i = filter_values_offset + it.index_into_array();
        if (!bit_util::GetBit(filter_is_valid, i) ||
            bit_util::GetBit(filter_selection, i)) {
          logical_count += it.run_length();
        }
        ++it;
      }
    } else {  // DROP nulls
      while (!it.is_end(filter_span)) {
        const int64_t i = filter_values_offset + it.index_into_array();
        if (bit_util::GetBit(filter_is_valid, i) &&
            bit_util::GetBit(filter_selection, i)) {
          logical_count += it.run_length();
        }
        ++it;
      }
    }
  } else {
    while (!it.is_end(filter_span)) {
      const int64_t i = filter_values_offset + it.index_into_array();
      if (bit_util::GetBit(filter_selection, i)) {
        logical_count += it.run_length();
      }
      ++it;
    }
  }
  return logical_count;
}

// XXX: inline this in the next commit
template <typename ValuesRunEndType, typename ValuesValueType, typename FilterRunEndType>
int64_t GetREExREEFilterOutputSizeImpl(
    const ArraySpan& values, const ArraySpan& filter,
    FilterOptions::NullSelectionBehavior null_selection) {
  const auto values_values = arrow::ree_util::ValuesArray(values);
  const int64_t null_count = values_values.GetNullCount();
  const bool all_values_are_null = null_count == values_values.length;
  const uint8_t* values_validity =
      (null_count == 0) ? NULLPTR : values_values.buffers[0].data;
  // values_validity implies at least one run value is null but not all
  DCHECK(!values_validity || null_count > 0);

  // We don't use anything that depends on has_validity_bitmap, so we can pass false.
  ree_util::ReadWriteValue<ValuesValueType, false> read_write(values_values, NULLPTR);

  int64_t open_run_length = 0;
  // If open_run_length == 0, the values of open_run_is_null and open_run_value_i are
  // not well-defined.
  bool open_run_is_null = true;
  int64_t open_run_value_i = -1;
  // NOTE: The null value that opens a null run does not necessarily come from
  // values[open_run_value_i] because null values from filters (combined with
  // FilterOptions::EMIT_NULL) can cause nulls to be emitted as well.
  int64_t num_output_runs = 0;
  VisitREExREEFilterOutputFragments<ValuesRunEndType, FilterRunEndType>(
      values, filter, null_selection,
      [all_values_are_null, values_validity, &read_write, &open_run_length,
       &open_run_is_null, &open_run_value_i, &num_output_runs](
          int64_t i, int64_t run_length, int64_t emit_null_from_filter) noexcept {
        const bool emit_null = all_values_are_null || emit_null_from_filter ||
                               (values_validity && !bit_util::GetBit(values_validity, i));
        if (emit_null) {
          if (open_run_is_null) {
            open_run_length += run_length;
          } else {
            // Close currently open non-null run.
            num_output_runs += open_run_length > 0;
            // Open a new null run.
            open_run_length = run_length;
            open_run_is_null = true;
          }
          // We don't need to guard the access to open_run_is_null with a check for
          // open_run_length > 0 because if open_run_length == 0, both branches on
          // open_run_is_null will lead to the same outcome:
          //
          //   /\ UNCHANGED <<num_output_runs>>
          //   /\ open_run_length = open_run_length + run_length
          //   /\ open_run_is_null
        } else {
          if (open_run_is_null) {
            // Close currently open null run.
            num_output_runs += open_run_length > 0;
            // Open a new non-null run.
            open_run_length = run_length;
            open_run_is_null = false;
          } else {
            // If open_run_length > 0, we can trust the !open_run_is_null that led
            // execution to this else branch, and we can trust that open_run_value_i is
            // comparable to i. In case open_run_value_i==i, we can assume equality of
            // the values at these positions, otherwise CompareValuesAt is called.
            // We know these values are valid because !open_run_is_null and
            // !emit_null respectively.
            const bool close_open_run =
                open_run_length <= 0 ||
                (open_run_value_i != i &&
                 !ARROW_PREDICT_FALSE(read_write.CompareValuesAt(open_run_value_i, i)));
            if (close_open_run) {
              // Close currently open non-null run.
              num_output_runs += open_run_length > 0;
              // Open a new non-null run.
              open_run_length = run_length;
              // open_run_is_null remains false.
              // open_run_value_i is updated below.
            } else {
              open_run_length += run_length;
              // This branch can be reached when open_run_length == 0, and in
              // that case, we can't trust the value of open_run_is_null, so we
              // need to prove that the outcome of this branch is the same as
              // the outcome of the if-open_run_is_null branch above:
              //
              //   /\ UNCHANGED <<num_output_runs>>
              //   /\ open_run_length = open_run_length + run_length
              //   /\ not open_run_is_null
              //
              // Proof: given that open_run_length==0:
              // 1) num_output_runs+=open_run_length>0 doesn't change num_output_runs.
              // 2) open_run_length+=run_length and open_run_length=run_length
              //    are equivalent.
              // 3) open_run_is_null is set to false or enters the branch as false.
            }
          }
        }
        // It's safe to unconditionally update open_run_value_i because:
        // 1) access to open_run_value_i is guarded by !open_run_is_null checks,
        //    so it's ok if open_run_value_i points to a null value
        // 2) if values at the previous open_run_value_i and i are equal, updating
        //    open_run_value_i to i doesn't change the outcome of future comparisons
        // 3) otherwise, updating open_run_value_i to i is necessary as it should be an
        //    index to the value of the currently open non-null run
        open_run_value_i = i;
      });
  // Close the trailing open run if one exists.
  num_output_runs += open_run_length > 0;
  return num_output_runs;
}

// This is called from a template with many instantiations, so we don't want to inline it.
ARROW_NOINLINE Status MakeNullREEData(int64_t logical_length, MemoryPool* pool,
                                      ArrayData* out) {
  const auto* ree_type = checked_cast<RunEndEncodedType*>(out->type.get());
  const int64_t physical_length = logical_length > 0 ? 1 : 0;
  ARROW_ASSIGN_OR_RAISE(
      auto run_ends_data,
      ree_util::PreallocateRunEndsArray(ree_type->run_end_type(), physical_length, pool));
  if (logical_length > 0) {
    ree_util::WriteSingleRunEnd(run_ends_data.get(), logical_length);
  }
  auto values_data =
      ArrayData::Make(null(), physical_length, {NULLPTR}, /*null_count=*/1);

  out->length = logical_length;
  out->null_count = 0;
  out->buffers = {NULLPTR};
  out->child_data = {std::move(run_ends_data), std::move(values_data)};
  return Status::OK();
}

// This is called from a template with many instantiations, so we don't want to inline it.
ARROW_NOINLINE Status PreallocateREEData(int64_t physical_length, bool allocate_validity,
                                         int64_t data_buffer_size, MemoryPool* pool,
                                         ArrayData* out) {
  const auto* ree_type = checked_cast<RunEndEncodedType*>(out->type.get());

  ARROW_ASSIGN_OR_RAISE(
      auto run_ends_data,
      ree_util::PreallocateRunEndsArray(ree_type->run_end_type(), physical_length, pool));
  ARROW_ASSIGN_OR_RAISE(auto values_data,
                        ree_util::PreallocateValuesArray(
                            ree_type->value_type(), allocate_validity, physical_length,
                            kUnknownNullCount, pool, data_buffer_size));

  // out->length is set after the filter is computed
  out->null_count = 0;
  out->buffers = {NULLPTR};
  out->child_data = {std::move(run_ends_data), std::move(values_data)};
  return Status::OK();
}

/// \brief Common virtual base class for filter functions that involve run-end
/// encoded arrays on one side or another
class REEFilterExec {
 public:
  virtual ~REEFilterExec() = default;
  virtual int64_t CalculateOutputSize() = 0;
  virtual Status Exec(MemoryPool* pool, ArrayData* out) = 0;
};

template <typename ValuesRunEndType, typename ValuesValueType, typename FilterRunEndType>
class REExREEFilterExecImpl final : public REEFilterExec {
 private:
  using ValuesRunEndCType = typename ValuesRunEndType::c_type;
  using FilterRunEndCType = typename FilterRunEndType::c_type;

  const ArraySpan& values_;
  const ArraySpan& filter_;
  const FilterOptions::NullSelectionBehavior null_selection_;

 public:
  REExREEFilterExecImpl(const ArraySpan& values, const ArraySpan& filter,
                        FilterOptions::NullSelectionBehavior null_selection)
      : values_(values), filter_(filter), null_selection_(null_selection) {}

  ~REExREEFilterExecImpl() override = default;

  int64_t CalculateOutputSize() final {
    if constexpr (std::is_same<ValuesValueType, NullType>::value) {
      return CountREEFilterEmits<FilterRunEndType>(filter_, null_selection_) > 0 ? 1 : 0;
    } else {
      return GetREExREEFilterOutputSizeImpl<ValuesRunEndType, ValuesValueType,
                                            FilterRunEndType>(values_, filter_,
                                                              null_selection_);
    }
  }

 private:
  /// \tparam in_has_validity_buffer whether the input has a validity buffer that
  /// needs to be considered by the filtering process
  /// \tparam out_has_validity_buffer whether the output has a validity buffer that
  /// needs to be populated by the filtering process
  /// \param[out] out the pre-allocated output array data
  /// \return the logical length of the output
  template <bool in_has_validity_buffer, bool out_has_validity_buffer>
  int64_t Exec(ArrayData* out) {
    using ReadWriteValue =
        ree_util::ReadWriteValue<ValuesValueType, in_has_validity_buffer,
                                 out_has_validity_buffer>;
    using ValueRepr = typename ReadWriteValue::ValueRepr;

    const auto& values_array = arrow::ree_util::ValuesArray(values_);
    const auto& filter_values_array = arrow::ree_util::ValuesArray(filter_);
    const int64_t values_offset = values_array.offset;
    const int64_t filter_offset = filter_values_array.offset;
    const uint8_t* filter_validity = filter_values_array.buffers[0].data;
    const uint8_t* filter_data = filter_values_array.buffers[1].data;
    ReadWriteValue read_write{values_array, out->child_data[1].get()};
    auto* out_run_ends = out->child_data[0]->GetMutableValues<ValuesRunEndCType>(1);

    const arrow::ree_util::RunEndEncodedArraySpan<ValuesRunEndCType> values_run_ends(
        values_);
    const arrow::ree_util::RunEndEncodedArraySpan<FilterRunEndCType> filter_run_ends(
        filter_);
    arrow::ree_util::MergedRunsIterator it(values_run_ends, filter_run_ends);
    int64_t logical_length = 0;
    int64_t write_offset = 0;
    ValueRepr value{};
    if (!values_array.MayHaveNulls()) {
      if (!filter_values_array.MayHaveNulls()) {
        // both values and filter are non-nullable, so output is non-nullable
        DCHECK(!in_has_validity_buffer && !out_has_validity_buffer);
        while (!it.isEnd()) {
          const int64_t v = values_offset + it.index_into_left_array();
          const int64_t f = filter_offset + it.index_into_right_array();
          if (bit_util::GetBit(filter_data, f)) {
            (void)read_write.ReadValue(&value, v);
            logical_length += it.run_length();
            read_write.WriteValue(write_offset, true, value);
            out_run_ends[write_offset] = static_cast<ValuesRunEndCType>(logical_length);
            ++write_offset;
          }
          ++it;
        }
      } else if (null_selection_ == FilterOptions::DROP) {
        // values are not null, and nulls from filters are dropped, so output is
        // non-nullable
        DCHECK(!in_has_validity_buffer && !out_has_validity_buffer);
        while (!it.isEnd()) {
          const int64_t v = values_offset + it.index_into_left_array();
          const int64_t f = filter_offset + it.index_into_right_array();
          if (bit_util::GetBit(filter_validity, f) && bit_util::GetBit(filter_data, f)) {
            (void)read_write.ReadValue(&value, v);
            logical_length += it.run_length();
            read_write.WriteValue(write_offset, true, value);
            out_run_ends[write_offset] = static_cast<ValuesRunEndCType>(logical_length);
            ++write_offset;
          }
          ++it;
        }
      } else {  // null_selection_ == FilterOptions::EMIT_NULL
        // filter has nulls and they are being emitted, so output is nullable
        DCHECK(!in_has_validity_buffer && out_has_validity_buffer);
        while (!it.isEnd()) {
          const int64_t v = values_offset + it.index_into_left_array();
          const int64_t f = filter_offset + it.index_into_right_array();
          const bool filter_is_valid = bit_util::GetBit(filter_validity, f);
          if (filter_is_valid) {
            if (bit_util::GetBit(filter_data, f)) {
              logical_length += it.run_length();
              const bool valid = read_write.ReadValue(&value, v);
              read_write.WriteValue(write_offset, valid, value);
              out_run_ends[write_offset] = static_cast<ValuesRunEndCType>(logical_length);
              ++write_offset;
            }
          } else {
            logical_length += it.run_length();
            read_write.WriteValue(write_offset, false, value);
            out_run_ends[write_offset] = static_cast<ValuesRunEndCType>(logical_length);
            ++write_offset;
          }
          ++it;
        }
      }
    } else {  // values input may have nulls
      DCHECK(in_has_validity_buffer && out_has_validity_buffer);
      if (!filter_values_array.MayHaveNulls()) {
        const int64_t v = values_offset + it.index_into_left_array();
        const int64_t f = filter_offset + it.index_into_right_array();
        while (!it.isEnd()) {
          if (bit_util::GetBit(filter_data, f)) {
            logical_length += it.run_length();
            const bool valid = read_write.ReadValue(&value, v);
            read_write.WriteValue(write_offset, valid, value);
            out_run_ends[write_offset] = static_cast<ValuesRunEndCType>(logical_length);
            ++write_offset;
          }
          ++it;
        }
      } else if (null_selection_ == FilterOptions::DROP) {
        while (!it.isEnd()) {
          const int64_t v = values_offset + it.index_into_left_array();
          const int64_t f = filter_offset + it.index_into_right_array();
          if (bit_util::GetBit(filter_validity, f) && bit_util::GetBit(filter_data, f)) {
            logical_length += it.run_length();
            const bool valid = read_write.ReadValue(&value, v);
            read_write.WriteValue(write_offset, valid, value);
            out_run_ends[write_offset] = static_cast<ValuesRunEndCType>(logical_length);
            ++write_offset;
          }
          ++it;
        }
      } else {  // null_selection_ == FilterOptions::EMIT_NULL
        while (!it.isEnd()) {
          const int64_t v = values_offset + it.index_into_left_array();
          const int64_t f = filter_offset + it.index_into_right_array();
          const bool is_valid = bit_util::GetBit(filter_validity, f);
          if (is_valid) {
            if (bit_util::GetBit(filter_data, f)) {
              logical_length += it.run_length();
              const bool valid = read_write.ReadValue(&value, v);
              read_write.WriteValue(write_offset, valid, value);
              out_run_ends[write_offset] = static_cast<ValuesRunEndCType>(logical_length);
              ++write_offset;
            }
          } else {
            logical_length += it.run_length();
            read_write.WriteValue(write_offset, false, value);
            out_run_ends[write_offset] = static_cast<ValuesRunEndCType>(logical_length);
            ++write_offset;
          }
          ++it;
        }
      }
    }
    return logical_length;
  }

  /// \param in_has_validity_buffer whether the input has a validity buffer that
  /// needs to be considered by the filtering process
  /// \param out_has_validity_buffer whether the output has a validity buffer that
  /// needs to be populated by the filtering process
  /// \param[out] out the pre-allocated output array data
  /// \return the logical length of the output
  int64_t Exec(bool in_has_validity_buffer, bool out_has_validity_buffer,
               ArrayData* out) {
    if (in_has_validity_buffer) {
      // If the input has a validity buffer, so does the output
      DCHECK(out_has_validity_buffer);
      return Exec<true, true>(out);
    } else {
      if (out_has_validity_buffer) {
        return Exec<false, true>(out);
      } else {
        return Exec<false, false>(out);
      }
    }
  }

 public:
  Status Exec(MemoryPool* pool, ArrayData* out) final {
    if constexpr (std::is_same<ValuesValueType, NullType>::value) {
      const int64_t logical_length =
          CountREEFilterEmits<FilterRunEndType>(filter_, null_selection_);
      RETURN_NOT_OK(MakeNullREEData(logical_length, pool, out));
    } else {
      const int64_t physical_length = CalculateOutputSize();
      const auto& values_values_array = arrow::ree_util::ValuesArray(values_);
      const auto& filter_values_array = arrow::ree_util::ValuesArray(filter_);

      const bool in_has_validity_buffer = values_values_array.MayHaveNulls();
      const bool out_has_validity_buffer =
          in_has_validity_buffer || (null_selection_ == FilterOptions::EMIT_NULL &&
                                     filter_values_array.MayHaveNulls());

      RETURN_NOT_OK(
          PreallocateREEData(physical_length, out_has_validity_buffer, 0, pool, out));
      const int64_t logical_length =
          Exec(in_has_validity_buffer, out_has_validity_buffer, out);
      // Set length now that we know it (PreallocateREEData filled all the other fields)
      out->length = logical_length;
    }
    return Status::OK();
  }
};

/// \tparam Functor The template of a class that implements the REEFilterExec
/// interface. It takes 3 template paramters and the same 3 parameters as
/// this function
/// \tparam ValuesValueType The type of the values array of the run-end encoded
/// values array
template <template <typename ValuesRunEndType, typename ValuesValueType,
                    typename FilterRunEndType>
          class Functor,
          typename ValuesValueType>
Result<std::unique_ptr<REEFilterExec>> MakeREEFilterExecImpl(
    const ArraySpan& values, const ArraySpan& filter, Type::type values_run_end_type,
    Type::type filter_run_end_type, FilterOptions::NullSelectionBehavior null_selection) {
  switch (values_run_end_type) {
    case Type::INT16:
      switch (filter_run_end_type) {
        case Type::INT16:
          return std::make_unique<Functor<Int16Type, ValuesValueType, Int16Type>>(
              values, filter, null_selection);
        case Type::INT32:
          return std::make_unique<Functor<Int16Type, ValuesValueType, Int32Type>>(
              values, filter, null_selection);
        default:
          DCHECK_EQ(filter_run_end_type, Type::INT64);
          return std::make_unique<Functor<Int16Type, ValuesValueType, Int64Type>>(
              values, filter, null_selection);
      }
    case Type::INT32:
      switch (filter_run_end_type) {
        case Type::INT16:
          return std::make_unique<Functor<Int32Type, ValuesValueType, Int16Type>>(
              values, filter, null_selection);
        case Type::INT32:
          return std::make_unique<Functor<Int32Type, ValuesValueType, Int32Type>>(
              values, filter, null_selection);
        default:
          DCHECK_EQ(filter_run_end_type, Type::INT64);
          return std::make_unique<Functor<Int32Type, ValuesValueType, Int64Type>>(
              values, filter, null_selection);
      }
    default:
      DCHECK_EQ(values_run_end_type, Type::INT64);
      switch (filter_run_end_type) {
        case Type::INT16:
          return std::make_unique<Functor<Int64Type, ValuesValueType, Int16Type>>(
              values, filter, null_selection);
        case Type::INT32:
          return std::make_unique<Functor<Int64Type, ValuesValueType, Int32Type>>(
              values, filter, null_selection);
        default:
          DCHECK_EQ(filter_run_end_type, Type::INT64);
          return std::make_unique<Functor<Int64Type, ValuesValueType, Int64Type>>(
              values, filter, null_selection);
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
      return MakeREEFilterExecImpl<REExREEFilterExecImpl, NullType>(
          values, filter, values_run_end_type, filter_run_end_type, null_selection);
    case Type::BOOL:
      return MakeREEFilterExecImpl<REExREEFilterExecImpl, BooleanType>(
          values, filter, values_run_end_type, filter_run_end_type, null_selection);
    case Type::UINT8:
    case Type::INT8:
      return MakeREEFilterExecImpl<REExREEFilterExecImpl, UInt8Type>(
          values, filter, values_run_end_type, filter_run_end_type, null_selection);
    case Type::UINT16:
    case Type::INT16:
      return MakeREEFilterExecImpl<REExREEFilterExecImpl, UInt16Type>(
          values, filter, values_run_end_type, filter_run_end_type, null_selection);
    case Type::UINT32:
    case Type::INT32:
    case Type::FLOAT:
    case Type::DATE32:
    case Type::TIME32:
    case Type::INTERVAL_MONTHS:
      return MakeREEFilterExecImpl<REExREEFilterExecImpl, UInt32Type>(
          values, filter, values_run_end_type, filter_run_end_type, null_selection);
    case Type::UINT64:
    case Type::INT64:
    case Type::DOUBLE:
    case Type::DATE64:
    case Type::TIMESTAMP:
    case Type::TIME64:
    case Type::DURATION:
    case Type::INTERVAL_DAY_TIME:
      return MakeREEFilterExecImpl<REExREEFilterExecImpl, UInt64Type>(
          values, filter, values_run_end_type, filter_run_end_type, null_selection);
    // case Type::INTERVAL_MONTH_DAY_NANO:
    //   return MakeREEFilterExecImpl<REExREEFilterExecImpl, MonthDayNanoIntervalType>(
    //       values, filter, values_run_end_type, filter_run_end_type, null_selection);
    // case Type::DECIMAL128:
    //   return MakeREEFilterExecImpl<REExREEFilterExecImpl, Decimal128Type>(
    //       values, filter, values_run_end_type, filter_run_end_type, null_selection);
    // case Type::DECIMAL256:
    //   return MakeREEFilterExecImpl<REExREEFilterExecImpl, Decimal256Type>(
    //       values, filter, values_run_end_type, filter_run_end_type, null_selection);
    // case Type::FIXED_SIZE_BINARY:
    //   return MakeREEFilterExecImpl<REExREEFilterExecImpl, FixedSizeBinaryType>(
    //       values, filter, values_run_end_type, filter_run_end_type, null_selection);
    // case Type::STRING:
    //   return MakeREEFilterExecImpl<REExREEFilterExecImpl, StringType>(values, filter,
    //   values_run_end_type,
    //                                                filter_run_end_type,
    //                                                null_selection);
    // case Type::BINARY:
    //   return MakeREEFilterExecImpl<REExREEFilterExecImpl, BinaryType>(values, filter,
    //   values_run_end_type,
    //                                                filter_run_end_type,
    //                                                null_selection);
    // case Type::LARGE_STRING:
    //   return MakeREEFilterExecImpl<REExREEFilterExecImpl, LargeStringType>(
    //       values, filter, values_run_end_type, filter_run_end_type, null_selection);
    // case Type::LARGE_BINARY:
    //   return MakeREEFilterExecImpl<REExREEFilterExecImpl, LargeBinaryType>(
    //       values, filter, values_run_end_type, filter_run_end_type, null_selection);
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

Status REExREEFilterExec(KernelContext* ctx, const ExecSpan& span, ExecResult* result) {
  using FilterState = OptionsWrapper<FilterOptions>;
  const auto& values = span.values[0].array;
  const auto& filter = span.values[1].array;
  ArrayData* out = result->array_data().get();
  DCHECK(out->type->Equals(*values.type));
  const auto null_selection = FilterState::Get(ctx).null_selection_behavior;
  ARROW_ASSIGN_OR_RAISE(auto exec, MakeREExREEFilterExec(values, filter, null_selection));
  return exec->Exec(ctx->memory_pool(), out);
}

}  // namespace arrow::compute::internal
