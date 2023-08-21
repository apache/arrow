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
#include <functional>
#include <memory>
#include <tuple>

#include "arrow/array/builder_decimal.h"
#include "arrow/array/builder_primitive.h"
#include "arrow/array/builder_time.h"
#include "arrow/array/builder_union.h"
#include "arrow/array/util.h"
#include "arrow/compute/api_vector.h"
#include "arrow/compute/kernels/codegen_internal.h"
#include "arrow/compute/kernels/ree_util_internal.h"
#include "arrow/compute/kernels/vector_run_end_selection.h"
#include "arrow/result.h"
#include "arrow/util/bit_block_counter.h"
#include "arrow/util/bitmap_ops.h"
#include "arrow/util/logging.h"
#include "arrow/util/ree_util.h"

namespace arrow::compute::internal {

namespace {

using EmitFragment =
    std::function<void(int64_t absolute_i, int64_t frag_length, bool valid)>;

using EmitRun = std::function<void(int64_t absolute_i, int64_t run_length, bool valid)>;

using VisitFilterOutputFragments = Status (*)(MemoryPool*, const ArraySpan&,
                                              const ArraySpan&,
                                              FilterOptions::NullSelectionBehavior,
                                              const EmitFragment&);

/// \brief Iterate over REE values and a REE filter, emitting fragments of runs that pass
/// the filter.
///
/// The filtering process can emit fragments of the same value in sequence,
/// so to ensure the output of the filter takes advantage of run-end encoding,
/// these fragments should be combined by the caller.
///
/// \see VisitREExAnyFilterCombinedOutputRuns
template <typename ValuesRunEndType, typename FilterRunEndType>
Status VisitREExREEFilterOutputFragments(
    MemoryPool* pool, const ArraySpan& values, const ArraySpan& filter,
    FilterOptions::NullSelectionBehavior null_selection,
    const EmitFragment& emit_fragment) {
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
      bool last_valid = false;
      while (!it.is_end()) {
        const int64_t i = filter_values_offset + it.index_into_right_array();
        bool emit = last_emit;
        bool valid = last_valid;
        if (last_i != i) {
          valid = bit_util::GetBit(filter_is_valid, i);
          emit = !valid || bit_util::GetBit(filter_selection, i);
        }
        if (emit) {
          emit_fragment(values_offset + it.index_into_left_array(), it.run_length(),
                        valid);
        }
        last_i = i;
        last_emit = emit;
        last_valid = valid;
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
          emit_fragment(values_offset + it.index_into_left_array(), it.run_length(),
                        true);
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
        emit_fragment(values_offset + it.index_into_left_array(), it.run_length(), true);
      }
      last_i = i;
      last_emit = emit;
      ++it;
    }
  }
  return Status::OK();
}

/// \param emit_run A callable that takes (run_value_i, run_length, valid).
/// emit_run can be called with run_length=0, but is assumed to be a no-op in that
/// case. run_value_i and valid are undefined if run_length=0. If valid=false,
/// run_value_i should not be used as nulls can be emitted from the filter when
/// null_selection=EMIT_NULL. In that case, run_value_i wouldn't necessarily
/// point to a NULL value in the values array.
template <typename ValuesRunEndType, typename ValuesValueType>
Status VisitREExAnyFilterCombinedOutputRuns(
    MemoryPool* pool, const ArraySpan& values, const ArraySpan& filter,
    FilterOptions::NullSelectionBehavior null_selection,
    const VisitFilterOutputFragments& visit_filter_output_fragments,
    const EmitRun& emit_run) {
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
  // NOTE: The null value that opens a null run does not necessarily come from
  // values[open_run_value_i] because null values from filters (combined with
  // FilterOptions::EMIT_NULL) can cause nulls to be emitted as well.
  int64_t open_run_value_i = -1;
  auto status = visit_filter_output_fragments(
      pool, values, filter, null_selection,
      [all_values_are_null, values_validity, &read_write, &open_run_length,
       &open_run_is_null, &open_run_value_i,
       &emit_run](int64_t i, int64_t run_length, int64_t valid) noexcept {
        const bool emit_null = all_values_are_null || !valid ||
                               (values_validity && !bit_util::GetBit(values_validity, i));
        if (emit_null) {
          if (open_run_is_null) {
            open_run_length += run_length;
          } else {
            // Close currently open non-null run.
            emit_run(open_run_value_i, open_run_length, true);
            // Open a new null run.
            open_run_length = run_length;
            open_run_is_null = true;
          }
          // We don't need to guard the access to open_run_is_null with a check for
          // open_run_length > 0 because if open_run_length == 0, both branches on
          // open_run_is_null will lead to the same outcome:
          //
          //      emit_null() was not called or was a no-op
          //   && open_run_length == open_run_length + run_length
          //   && open_run_is_null
          //
          // NOTE: emit_run is a no-op because run_length==0.
        } else {
          if (open_run_is_null) {
            // Close currently open null run.
            emit_run(open_run_value_i, open_run_length, false);
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
              emit_run(open_run_value_i, open_run_length, true);
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
              //      emit_null() was not called or was a no-op
              //   && open_run_length == open_run_length + run_length
              //   && not open_run_is_null
              //
              // Proof: given that open_run_length==0:
              // 1) emit_null(..,0,..) is a no-op
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
  RETURN_NOT_OK(status);
  // Close the trailing open run. If open_run_length == 0, this is a no-op.
  emit_run(open_run_value_i, open_run_length, !open_run_is_null);
  return Status::OK();
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

/// \brief Counts how many logical values are emitted by a plain boolean filter
///
/// This is used when we know that the values array is only NULLs (Type::NA
/// arrays). The buffers allocated here are not reused and that is OK because,
/// for Type::NA arrays, counting is all that the kernel has to do.
[[maybe_unused]] Result<int64_t> CountPlainFilterEmits(
    MemoryPool* pool, const ArraySpan& filter,
    FilterOptions::NullSelectionBehavior null_selection) {
  DCHECK_EQ(filter.type->id(), Type::BOOL);
  if (filter.length == 0) {
    return 0;
  }

  int64_t logical_count = 0;
  const uint8_t* filter_is_valid = filter.buffers[0].data;
  const uint8_t* filter_selection = filter.buffers[1].data;
  const bool filter_may_have_nulls =
      filter_is_valid != NULLPTR && filter.GetNullCount() != 0;

  if (filter_may_have_nulls) {
    if (null_selection == FilterOptions::EMIT_NULL) {
      // candidates_bitmap = filter_selection | ~filter_is_valid
      ARROW_ASSIGN_OR_RAISE(auto candidates_bitmap,
                            arrow::internal::BitmapOrNot(
                                pool, filter_selection, filter.offset, filter_is_valid,
                                filter.offset, filter.length, filter.offset));
      return arrow::internal::CountSetBits(candidates_bitmap->data(), filter.offset,
                                           filter.length);
    } else {  // DROP_NULLS
      // selection_bitmap = filter_selection & filter_is_valid
      ARROW_ASSIGN_OR_RAISE(auto selection_bitmap,
                            arrow::internal::BitmapAnd(
                                pool, filter_selection, filter.offset, filter_is_valid,
                                filter.offset, filter.length, filter.offset));
      return arrow::internal::CountSetBits(selection_bitmap->data(), filter.offset,
                                           filter.length);
    }
  } else {
    return arrow::internal::CountSetBits(filter_selection, filter.offset, filter.length);
  }
  return logical_count;
}

/// \brief Iterate over REE values and a plain filter, emitting fragments of runs that
/// pass the filter.
///
/// The filtering process can emit runs of the same value close to each other,
/// so to ensure the output of the filter takes advantage of run-end encoding,
/// these fragments should be combined by the caller.
///
/// \see VisitREExAnyFilterCombinedOutputRuns
template <typename ValuesRunEndType>
Status VisitREExPlainFilterOutputFragments(
    MemoryPool* pool, const ArraySpan& values, const ArraySpan& filter,
    FilterOptions::NullSelectionBehavior null_selection,
    const EmitFragment& emit_fragment) {
  using arrow::internal::BitBlockCount;
  using arrow::internal::BitBlockCounter;
  using ValueRunEndCType = typename ValuesRunEndType::c_type;

  DCHECK_EQ(values.length, filter.length);

  const int64_t values_offset = arrow::ree_util::ValuesArray(values).offset;
  const uint8_t* filter_is_valid = filter.buffers[0].data;
  const uint8_t* filter_selection = filter.buffers[1].data;
  const bool filter_may_have_nulls =
      filter_is_valid != NULLPTR && filter.GetNullCount() != 0;

  const arrow::ree_util::RunEndEncodedArraySpan<ValueRunEndCType> values_span(values);
  auto it = values_span.begin();
  if (filter_may_have_nulls) {
    if (null_selection == FilterOptions::EMIT_NULL) {
      // candidates_bitmap = filter_selection | ~filter_is_valid
      ARROW_ASSIGN_OR_RAISE(auto candidates_bitmap,
                            arrow::internal::BitmapOrNot(
                                pool, filter_selection, filter.offset, filter_is_valid,
                                filter.offset, filter.length, filter.offset));
      const uint8_t* candidates_bitmap_data = candidates_bitmap->data();
      // For each run in values, process the bitmaps and emit fragments accordingly.
      while (!it.is_end(values_span)) {
        int64_t offset = it.logical_position();
        const int64_t run_end = it.run_end();
        BitBlockCounter candidates_bit_counter{candidates_bitmap_data,
                                               filter.offset + offset, run_end - offset};
        BitBlockCounter filter_validity_bit_counter{
            filter_is_valid, filter.offset + offset, run_end - offset};
        const int64_t values_physical_position = values_offset + it.index_into_array();
        while (offset < run_end) {
          BitBlockCount candidates_block = candidates_bit_counter.NextWord();
          if (candidates_block.AllSet()) {
            // filter_selection | ~filter_is_valid can mean two things:
            //
            //   1. ~filter_is_valid: The filter value is NULL and a NULL
            //      should be emitted.
            //   2. filter_is_valid: The filter value is non-NULL and that
            //      implies filter_selection is true, so the values should be
            //      emitted.
            //
            // We never have to check filter_selection because its value can be
            // inferred from candidates_block and filter_validity_block.
            BitBlockCount filter_validity_block = filter_validity_bit_counter.NextWord();
            if (filter_validity_block.NoneSet()) {
              emit_fragment(values_physical_position, candidates_block.length, false);
            } else if (filter_validity_block.AllSet()) {
              emit_fragment(values_physical_position, candidates_block.length, true);
            } else {
              for (int64_t i = 0; i < candidates_block.length; ++i) {
                const int64_t j = filter.offset + offset + i;
                const bool valid = bit_util::GetBit(filter_is_valid, j);
                emit_fragment(values_physical_position, 1, valid);
                // We don't complicate this loop by trying to emit longer runs of values
                // here because the caller already has to combine runs of values anyway
                // and it can even skip value comparisons when the
                // values_physical_position is the same as in the previous
                // emit_fragment() call.
              }
            }
          } else if (candidates_block.NoneSet()) {
            // ~(filter_selection | ~filter_is_valid) implies
            // ~filter_selection & filter_is_valid, so we have a non-NULL false
            // in the filter. Nothing needs to be emitted in this case.
            filter_validity_bit_counter.NextWord();
          } else {
            filter_validity_bit_counter.NextWord();
            for (int64_t i = 0; i < candidates_block.length; ++i) {
              const int64_t j = filter.offset + offset + i;
              const bool candidate = bit_util::GetBit(candidates_bitmap_data, j);
              const bool valid = bit_util::GetBit(filter_is_valid, j);
              if (candidate) {
                emit_fragment(values_physical_position, 1, valid);
              }
            }
          }
          offset += candidates_block.length;
        }
        ++it;
      }
    } else {  // DROP nulls
      // For each run in values, process the bitmap and emit fragments accordingly.
      while (!it.is_end(values_span)) {
        int64_t offset = it.logical_position();
        const int64_t run_end = it.run_end();
        BitBlockCounter filter_bit_counter{filter_selection, filter.offset + offset,
                                           run_end - offset};
        BitBlockCounter filter_validity_bit_counter{
            filter_is_valid, filter.offset + offset, run_end - offset};
        const int64_t values_physical_position = values_offset + it.index_into_array();
        while (offset < run_end) {
          BitBlockCount filter_block = filter_bit_counter.NextWord();
          if (filter_block.AllSet()) {
            BitBlockCount filter_validity_block = filter_validity_bit_counter.NextWord();
            if (filter_validity_block.AllSet()) {
              emit_fragment(values_physical_position, filter_block.length, true);
            } else if (filter_validity_block.NoneSet()) {
              // Drop all the nulls.
            } else {
              for (int64_t i = 0; i < filter_block.length; ++i) {
                const int64_t j = filter.offset + offset + i;
                const bool emit = bit_util::GetBit(filter_selection, j) &&
                                  bit_util::GetBit(filter_is_valid, j);
                if (emit) {
                  emit_fragment(values_physical_position, 1, false);
                }
              }
            }
          } else if (filter_block.NoneSet()) {
            // Skip.
            filter_validity_bit_counter.NextWord();
          } else {
            filter_validity_bit_counter.NextWord();
            for (int64_t i = 0; i < filter_block.length; ++i) {
              const int64_t j = filter.offset + offset + i;
              const bool emit = bit_util::GetBit(filter_selection, j) &&
                                bit_util::GetBit(filter_is_valid, j);
              if (emit) {
                emit_fragment(values_physical_position, 1, true);
              }
            }
          }
          offset += filter_block.length;
        }
        ++it;
      }
    }
  } else {
    // For each run in values, process the bitmap and emit fragments accordingly.
    while (!it.is_end(values_span)) {
      int64_t offset = it.logical_position();
      const int64_t run_end = it.run_end();
      BitBlockCounter selection_bit_counter{filter_selection, filter.offset + offset,
                                            run_end - offset};
      const int64_t values_physical_position = values_offset + it.index_into_array();
      while (offset < run_end) {
        BitBlockCount selection_block = selection_bit_counter.NextWord();
        if (selection_block.AllSet()) {
          emit_fragment(values_physical_position, selection_block.length, true);
        } else if (selection_block.NoneSet()) {
          // Skip.
        } else {
          for (int64_t i = 0; i < selection_block.length; ++i) {
            const int64_t j = filter.offset + offset + i;
            if (bit_util::GetBit(filter_selection, j)) {
              emit_fragment(values_physical_position, 1, true);
            }
          }
        }
        offset += selection_block.length;
      }
      ++it;
    }
  }
  return Status::OK();
}

// This is called from templates with many instantiations, so we don't want to inline it.
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

// This is called from templates with many instantiations, so we don't want to inline it.
ARROW_NOINLINE Status PreallocateREEData(int64_t physical_length, bool allocate_validity,
                                         int64_t data_buffer_size, MemoryPool* pool,
                                         ArrayData* out) {
  const auto* ree_type = checked_cast<RunEndEncodedType*>(out->type.get());

  ARROW_ASSIGN_OR_RAISE(
      auto run_ends_data,
      ree_util::PreallocateRunEndsArray(ree_type->run_end_type(), physical_length, pool));
  ARROW_ASSIGN_OR_RAISE(auto values_data, ree_util::PreallocateValuesArray(
                                              ree_type->value_type(), allocate_validity,
                                              physical_length, pool, data_buffer_size));

  // out->length is set after the filter is computed
  out->null_count = 0;
  out->buffers = {NULLPTR};
  out->child_data = {std::move(run_ends_data), std::move(values_data)};
  return Status::OK();
}

/// \brief Common virtual base class for filter functions on REE arrays.
class ARROW_EXPORT REEFilterExec {
 public:
  virtual ~REEFilterExec() = default;

  virtual Status Exec(ArrayData* out) = 0;
};

template <typename ValuesRunEndType, typename ValuesValueType, typename FilterRunEndType>
class REExREEFilterExecImpl final : public REEFilterExec {
 private:
  using ValuesRunEndCType = typename ValuesRunEndType::c_type;
  using FilterRunEndCType = typename FilterRunEndType::c_type;

  MemoryPool* pool_;
  const ArraySpan& values_;
  const ArraySpan& filter_;
  const FilterOptions::NullSelectionBehavior null_selection_;

 public:
  REExREEFilterExecImpl(MemoryPool* pool, const ArraySpan& values,
                        const ArraySpan& filter, const FilterOptions& options) noexcept
      : pool_(pool),
        values_(values),
        filter_(filter),
        null_selection_(options.null_selection_behavior) {}

  ~REExREEFilterExecImpl() override = default;

 private:
  Status VisitCombinedOutputRuns(const EmitRun& emit_run) {
    auto* visit_filter_output_fragments =
        &VisitREExREEFilterOutputFragments<ValuesRunEndType, FilterRunEndType>;
    return VisitREExAnyFilterCombinedOutputRuns<ValuesRunEndType, ValuesValueType>(
        pool_, values_, filter_, null_selection_, visit_filter_output_fragments,
        emit_run);
  }

  Result<int64_t> CalculatePhysicalOutputSize() {
    if constexpr (std::is_same<ValuesValueType, NullType>::value) {
      return CountREEFilterEmits<FilterRunEndType>(filter_, null_selection_) > 0 ? 1 : 0;
    } else {
      int64_t num_output_runs = 0;
      auto status = VisitCombinedOutputRuns(
          [&num_output_runs](int64_t, int64_t run_length, bool) noexcept {
            num_output_runs += run_length > 0;
          });
      RETURN_NOT_OK(status);
      return num_output_runs;
    }
  }

  /// \tparam out_has_validity_buffer whether the output has a validity buffer that
  /// needs to be populated by the filtering process
  /// \param[out] out the pre-allocated output array data
  /// \return the logical length of the output
  template <bool out_has_validity_buffer>
  Result<int64_t> ExecInternal(ArrayData* out) {
    // in_has_validity_buffer is false because all calls to ReadWriteValue::ReadValue()
    // below are already guarded by a validity check based on the values provided by
    // VisitREExAnyFilterCombinedOutputRuns() when it calls the EmitRun.
    using ReadWriteValue =
        ree_util::ReadWriteValue<ValuesValueType, /*in_has_validity_buffer=*/false,
                                 out_has_validity_buffer>;
    using ValueRepr = typename ReadWriteValue::ValueRepr;

    const auto& values_array = arrow::ree_util::ValuesArray(values_);
    ReadWriteValue read_write{values_array, out->child_data[1].get()};
    auto* out_run_ends = out->child_data[0]->GetMutableValues<ValuesRunEndCType>(1);

    int64_t logical_length = 0;
    int64_t write_offset = 0;
    auto status =
        VisitCombinedOutputRuns([&](int64_t i, int64_t run_length, bool valid) noexcept {
          ValueRepr value = {};
          logical_length += run_length;
          if (run_length > 0) {
            out_run_ends[write_offset] = static_cast<ValuesRunEndCType>(logical_length);
            if (valid) {
              (void)read_write.ReadValue(&value, i);
            }
            read_write.WriteValue(write_offset, valid, value);
            write_offset += 1;
          }
        });
    RETURN_NOT_OK(status);
    return logical_length;
  }

 public:
  Status Exec(ArrayData* out) final {
    if constexpr (std::is_same<ValuesValueType, NullType>::value) {
      const int64_t logical_length =
          CountREEFilterEmits<FilterRunEndType>(filter_, null_selection_);
      RETURN_NOT_OK(MakeNullREEData(logical_length, pool_, out));
    } else {
      ARROW_ASSIGN_OR_RAISE(const int64_t physical_length, CalculatePhysicalOutputSize());
      const auto& values_values_array = arrow::ree_util::ValuesArray(values_);
      const auto& filter_values_array = arrow::ree_util::ValuesArray(filter_);

      const bool in_has_validity_buffer = values_values_array.MayHaveNulls();
      const bool out_has_validity_buffer =
          in_has_validity_buffer || (null_selection_ == FilterOptions::EMIT_NULL &&
                                     filter_values_array.MayHaveNulls());

      RETURN_NOT_OK(
          PreallocateREEData(physical_length, out_has_validity_buffer, 0, pool_, out));
      // The length is set after the filtering process makes it known to us
      // (PreallocateREEData filled all the other fields)
      ARROW_ASSIGN_OR_RAISE(out->length, out_has_validity_buffer
                                             ? ExecInternal<true>(out)
                                             : ExecInternal<false>(out));
    }
    return Status::OK();
  }
};

template <typename ValuesRunEndType, typename ValuesValueType>
class REExPlainFilterExecImpl final : public REEFilterExec {
 private:
  using ValuesRunEndCType = typename ValuesRunEndType::c_type;

  MemoryPool* pool_;
  const ArraySpan& values_;
  const ArraySpan& filter_;
  const FilterOptions::NullSelectionBehavior null_selection_;

 public:
  REExPlainFilterExecImpl(MemoryPool* pool, const ArraySpan& values,
                          const ArraySpan& filter, const FilterOptions& options) noexcept
      : pool_(pool),
        values_(values),
        filter_(filter),
        null_selection_(options.null_selection_behavior) {}

  ~REExPlainFilterExecImpl() override = default;

 private:
  Status VisitCombinedOutputRuns(const EmitRun& emit_run) {
    auto* visit_filter_output_fragments =
        &VisitREExPlainFilterOutputFragments<ValuesRunEndType>;
    return VisitREExAnyFilterCombinedOutputRuns<ValuesRunEndType, ValuesValueType>(
        pool_, values_, filter_, null_selection_, visit_filter_output_fragments,
        emit_run);
  }

  Result<int64_t> CalculatePhysicalOutputSize() {
    if constexpr (std::is_same<ValuesValueType, NullType>::value) {
      ARROW_ASSIGN_OR_RAISE(int64_t logical_count,
                            CountPlainFilterEmits(pool_, filter_, null_selection_));
      return logical_count > 0 ? 1 : 0;
    } else {
      int64_t num_output_runs = 0;
      auto status = VisitCombinedOutputRuns(
          [&num_output_runs](int64_t, int64_t run_length, bool) noexcept {
            num_output_runs += run_length > 0;
          });
      RETURN_NOT_OK(status);
      return num_output_runs;
    }
  }

  /// \tparam out_has_validity_buffer whether the output has a validity buffer that
  /// needs to be populated by the filtering process
  /// \param[out] out the pre-allocated output array data
  /// \return the logical length of the output
  template <bool out_has_validity_buffer>
  Result<int64_t> ExecInternal(ArrayData* out) {
    // in_has_validity_buffer is false because all calls to ReadWriteValue::ReadValue()
    // below are already guarded by a validity check based on the values provided by
    // VisitREExAnyFilterCombinedOutputRuns() when it calls the EmitRun.
    using ReadWriteValue =
        ree_util::ReadWriteValue<ValuesValueType, /*in_has_validity_buffer=*/false,
                                 out_has_validity_buffer>;
    using ValueRepr = typename ReadWriteValue::ValueRepr;

    const auto& values_array = arrow::ree_util::ValuesArray(values_);
    ReadWriteValue read_write{values_array, out->child_data[1].get()};
    auto* out_run_ends = out->child_data[0]->GetMutableValues<ValuesRunEndCType>(1);

    int64_t logical_length = 0;
    int64_t write_offset = 0;
    auto status =
        VisitCombinedOutputRuns([&](int64_t i, int64_t run_length, bool valid) noexcept {
          ValueRepr value = {};
          logical_length += run_length;
          if (run_length > 0) {
            out_run_ends[write_offset] = static_cast<ValuesRunEndCType>(logical_length);
            if (valid) {
              (void)read_write.ReadValue(&value, i);
            }
            read_write.WriteValue(write_offset, valid, value);
            write_offset += 1;
          }
        });
    RETURN_NOT_OK(status);
    return logical_length;
  }

 public:
  Status Exec(ArrayData* out) final {
    if constexpr (std::is_same<ValuesValueType, NullType>::value) {
      ARROW_ASSIGN_OR_RAISE(const int64_t logical_length,
                            CountPlainFilterEmits(pool_, filter_, null_selection_));
      RETURN_NOT_OK(MakeNullREEData(logical_length, pool_, out));
    } else {
      ARROW_ASSIGN_OR_RAISE(const int64_t physical_length, CalculatePhysicalOutputSize());
      const auto& values_values_array = arrow::ree_util::ValuesArray(values_);

      const bool in_has_validity_buffer = values_values_array.MayHaveNulls();
      const bool out_has_validity_buffer =
          in_has_validity_buffer ||
          (null_selection_ == FilterOptions::EMIT_NULL && filter_.MayHaveNulls());

      RETURN_NOT_OK(
          PreallocateREEData(physical_length, out_has_validity_buffer, 0, pool_, out));
      // The length is set after the filtering process makes it known to us
      // (PreallocateREEData filled all the other fields)
      ARROW_ASSIGN_OR_RAISE(out->length, out_has_validity_buffer
                                             ? ExecInternal<true>(out)
                                             : ExecInternal<false>(out));
    }
    return Status::OK();
  }
};

template <template <typename ArrowType> class FactoryFunctor>
REEFilterExec* MakeREEFilterExec(MemoryPool* pool, const DataType& value_type,
                                 const ArraySpan& values, const ArraySpan& filter,
                                 const FilterOptions& options) noexcept {
  switch (value_type.id()) {
    case Type::NA:
      return FactoryFunctor<NullType>{}(pool, values, filter, options);
    case Type::BOOL:
      return FactoryFunctor<BooleanType>{}(pool, values, filter, options);
    case Type::UINT8:
    case Type::INT8:
      return FactoryFunctor<UInt8Type>{}(pool, values, filter, options);
    case Type::UINT16:
    case Type::INT16:
    case Type::HALF_FLOAT:
      return FactoryFunctor<UInt16Type>{}(pool, values, filter, options);
    case Type::UINT32:
    case Type::INT32:
    case Type::FLOAT:
    case Type::DATE32:
    case Type::TIME32:
    case Type::INTERVAL_MONTHS:
      return FactoryFunctor<UInt32Type>{}(pool, values, filter, options);
    case Type::UINT64:
    case Type::INT64:
    case Type::DOUBLE:
    case Type::DATE64:
    case Type::TIMESTAMP:
    case Type::TIME64:
    case Type::DURATION:
    case Type::INTERVAL_DAY_TIME:
      return FactoryFunctor<UInt64Type>{}(pool, values, filter, options);
    case Type::INTERVAL_MONTH_DAY_NANO:
      return FactoryFunctor<MonthDayNanoIntervalType>{}(pool, values, filter, options);
    case Type::DECIMAL128:
      return FactoryFunctor<Decimal128Type>{}(pool, values, filter, options);
    case Type::DECIMAL256:
      return FactoryFunctor<Decimal256Type>{}(pool, values, filter, options);
    case Type::FIXED_SIZE_BINARY:
      return FactoryFunctor<FixedSizeBinaryType>{}(pool, values, filter, options);
    case Type::STRING:
      return FactoryFunctor<StringType>{}(pool, values, filter, options);
    case Type::BINARY:
      return FactoryFunctor<BinaryType>{}(pool, values, filter, options);
    case Type::LARGE_STRING:
      return FactoryFunctor<LargeStringType>{}(pool, values, filter, options);
    case Type::LARGE_BINARY:
      return FactoryFunctor<LargeBinaryType>{}(pool, values, filter, options);
    default:
      DCHECK(false);
      return NULLPTR;
  }
}

Status ValidateRunEndType(const ArraySpan& array) {
  DCHECK_EQ(array.type->id(), Type::RUN_END_ENCODED);
  auto run_end_type = arrow::ree_util::RunEndsArray(array).type->id();
  if (ARROW_PREDICT_FALSE(!is_run_end_type(run_end_type))) {
    return Status::Invalid("Invalid run-end type: ", run_end_type);
  }
  return Status::OK();
}

/// \tparam ArrowType The DataType of the physical values array nested in values
template <typename ArrowType>
struct REExREEFilterExecFactory {
  REEFilterExec* operator()(MemoryPool* pool, const ArraySpan& values,
                            const ArraySpan& filter,
                            const FilterOptions& options) noexcept {
    using ValuesValueType = ArrowType;
    auto values_run_end_type = arrow::ree_util::RunEndsArray(values).type->id();
    auto filter_run_end_type = arrow::ree_util::RunEndsArray(filter).type->id();
    switch (values_run_end_type) {
      case Type::INT16:
        switch (filter_run_end_type) {
          case Type::INT16:
            return new REExREEFilterExecImpl<Int16Type, ValuesValueType, Int16Type>(
                pool, values, filter, options);
          case Type::INT32:
            return new REExREEFilterExecImpl<Int16Type, ValuesValueType, Int32Type>(
                pool, values, filter, options);
          default:
            DCHECK_EQ(filter_run_end_type, Type::INT64);
            return new REExREEFilterExecImpl<Int16Type, ValuesValueType, Int64Type>(
                pool, values, filter, options);
        }
      case Type::INT32:
        switch (filter_run_end_type) {
          case Type::INT16:
            return new REExREEFilterExecImpl<Int32Type, ValuesValueType, Int16Type>(
                pool, values, filter, options);
          case Type::INT32:
            return new REExREEFilterExecImpl<Int32Type, ValuesValueType, Int32Type>(
                pool, values, filter, options);
          default:
            DCHECK_EQ(filter_run_end_type, Type::INT64);
            return new REExREEFilterExecImpl<Int32Type, ValuesValueType, Int64Type>(
                pool, values, filter, options);
        }
      default:
        DCHECK_EQ(values_run_end_type, Type::INT64);
        switch (filter_run_end_type) {
          case Type::INT16:
            return new REExREEFilterExecImpl<Int64Type, ValuesValueType, Int16Type>(
                pool, values, filter, options);
          case Type::INT32:
            return new REExREEFilterExecImpl<Int64Type, ValuesValueType, Int32Type>(
                pool, values, filter, options);
          default:
            DCHECK_EQ(filter_run_end_type, Type::INT64);
            return new REExREEFilterExecImpl<Int64Type, ValuesValueType, Int64Type>(
                pool, values, filter, options);
        }
    }
  }
};

/// \tparam ArrowType The DataType of the physical values array nested in values
template <typename ArrowType>
struct REExPlainFilterExecFactory {
  REEFilterExec* operator()(MemoryPool* pool, const ArraySpan& values,
                            const ArraySpan& filter, const FilterOptions& options) {
    using ValuesValueType = ArrowType;
    switch (arrow::ree_util::RunEndsArray(values).type->id()) {
      case Type::INT16:
        return new REExPlainFilterExecImpl<Int16Type, ValuesValueType>(pool, values,
                                                                       filter, options);
      case Type::INT32:
        return new REExPlainFilterExecImpl<Int32Type, ValuesValueType>(pool, values,
                                                                       filter, options);
      default:
        return new REExPlainFilterExecImpl<Int64Type, ValuesValueType>(pool, values,
                                                                       filter, options);
    }
  }
};

ARROW_NOINLINE Result<std::unique_ptr<REEFilterExec>> UniquePtrFromHeapPtr(
    const char* func_name, const DataType& type, REEFilterExec* ptr) {
  if (!ptr) {
    return Status::NotImplemented(func_name, ": ArrowType=", type.ToString(), ".");
  }
  return std::unique_ptr<REEFilterExec>{ptr};
}

}  // namespace

using FilterState = OptionsWrapper<FilterOptions>;

Status REExREEFilterExec(KernelContext* ctx, const ExecSpan& span, ExecResult* result) {
  const auto& options = FilterState::Get(ctx);
  const auto& values = span.values[0].array;
  const auto& filter = span.values[1].array;
  ArrayData* out = result->array_data().get();
  DCHECK(out->type->Equals(*values.type));
  RETURN_NOT_OK(ValidateRunEndType(values));
  RETURN_NOT_OK(ValidateRunEndType(filter));
  const auto* values_value_type = arrow::ree_util::ValuesArray(values).type;
  ARROW_ASSIGN_OR_RAISE(
      auto exec, UniquePtrFromHeapPtr("REExREEFilterExec", *values.type,
                                      MakeREEFilterExec<REExREEFilterExecFactory>(
                                          ctx->memory_pool(), *values_value_type, values,
                                          filter, options)));
  return exec->Exec(out);
}

Status REExPlainFilterExec(KernelContext* ctx, const ExecSpan& span, ExecResult* result) {
  const auto& options = FilterState::Get(ctx);
  const auto& values = span.values[0].array;
  const auto& filter = span.values[1].array;
  ArrayData* out = result->array_data().get();
  DCHECK(out->type->Equals(*values.type));
  RETURN_NOT_OK(ValidateRunEndType(values));
  const auto* values_value_type = arrow::ree_util::ValuesArray(values).type;
  ARROW_ASSIGN_OR_RAISE(
      auto exec, UniquePtrFromHeapPtr("REExPlainFilterExec", *values.type,
                                      MakeREEFilterExec<REExPlainFilterExecFactory>(
                                          ctx->memory_pool(), *values_value_type, values,
                                          filter, options)));
  return exec->Exec(out);
}

}  // namespace arrow::compute::internal
