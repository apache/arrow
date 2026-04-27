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

#include "arrow/compute/api_vector.h"

#include <algorithm>
#include <memory>
#include <numeric>
#include <optional>
#include <ranges>
#include <span>
#include <type_traits>
#include <utility>

#include "arrow/array/array_primitive.h"
#include "arrow/array/array_run_end.h"
#include "arrow/array/builder_primitive.h"
#include "arrow/array/util.h"
#include "arrow/chunk_resolver.h"
#include "arrow/compute/function.h"
#include "arrow/compute/kernels/codegen_internal.h"
#include "arrow/compute/kernels/vector_sort_internal.h"
#include "arrow/compute/registry.h"
#include "arrow/compute/registry_internal.h"
#include "arrow/type_traits.h"
#include "arrow/util/checked_cast.h"
#include "arrow/util/logging_internal.h"
#include "arrow/util/ree_util.h"

namespace arrow {

using internal::checked_cast;

namespace compute::internal {
namespace {

const SearchSortedOptions* GetDefaultSearchSortedOptions() {
  static const auto kDefaultSearchSortedOptions = SearchSortedOptions::Defaults();
  return &kDefaultSearchSortedOptions;
}

const FunctionDoc search_sorted_doc(
    "Find insertion indices for sorted input",
    ("Return the index where each needle should be inserted in a sorted input array\n"
     "to maintain ascending order.\n"
     "\n"
     "With side='left', returns the first suitable index (lower bound).\n"
     "With side='right', returns the last suitable index (upper bound).\n"
     "\n"
     "The searched values may be provided as an array or chunked array and must\n"
     "already be sorted in ascending order. Null values in the searched array are\n"
     "supported when clustered entirely at the start or\n"
     "entirely at the end. Non-null needles are matched only against the non-null\n"
     "portion of the searched array. Needles may be a scalar, array, or chunked\n"
     "array. Null needles emit nulls in the output."),
    {"values", "needles"}, "SearchSortedOptions");

// This file implements search_sorted as a small pipeline that first normalizes
// Arrow input shapes and then runs one typed binary-search core on logical
// values.
//
// Plain arrays, run-end encoded arrays, chunked arrays, and scalar needles are
// all adapted into a common accessor and run-visitor model so the search logic
// does not care about physical layout.
//
// After validation, the kernel isolates the contiguous non-null window of the
// searched values, because nulls are only supported when clustered at one end.
// That window uses logical null counting for run-end encoded inputs, whose
// nulls live in the values child rather than in a top-level validity bitmap.
//
// Needles are then visited as logical runs. Scalars become a single run, plain
// arrays become one-element runs, and run-end encoded inputs preserve their
// logical run lengths. Each non-null needle run is resolved with a lower-bound
// or upper-bound binary search over the sorted non-null range.
//
// Output materialization is unified behind small output sinks. Non-null-only
// needles write directly into a preallocated uint64 buffer, while nullable
// needles append null and non-null runs through a UInt64Builder. The builder
// path is optimized for repeated runs by bulk-filling reserved memory instead
// of appending one insertion index at a time.
//
// High-level flow:
//
//   values datum
//       |
//       +--> ValidateSortedValuesInput
//       |
//       +--> LogicalType / FindNonNullValuesRange
//       |
//       +--> VisitValuesAccessor
//             |
//             +--> PlainArrayAccessor
//             |
//             +--> RunEndEncodedValuesAccessor
//             |
//             +--> ChunkedArrayAccessor
//             |
//             `--> ChunkedRunEndEncodedValuesAccessor
//
//   needles datum
//       |
//       +--> ValidateNeedleInput
//       |
//       +--> DatumHasNulls
//       |
//       `--> VisitNeedleRuns
//             |
//             +--> scalar needle  -> one logical run
//             |
//             +--> plain array    -> one-element runs
//             |
//             +--> REE array      -> one logical run per encoded run
//             |
//             `--> chunked input  -> recurse chunk by chunk
//
//   normalized values accessor + normalized needle runs
//       |
//       `--> FindInsertionPoint<T>
//             |
//             +--> side = left  -> lower_bound semantics
//             |
//             `--> side = right -> upper_bound semantics
//
//   result materialization
//       |
//       +--> no needle nulls
//       |     +--> MakeMutableUInt64Array
//       |     `--> PreallocatedInsertionIndexOutput
//       |           `--> fill output buffer directly
//       |
//       `--> nullable needles
//             +--> UInt64Builder
//             `--> BuilderInsertionIndexOutput
//                   +--> AppendNulls for null runs
//                   `--> bulk fill + UnsafeAdvance for repeated indices
//
// A rough map of the file:
//
//   [validation + type helpers]
//           |
//   [value accessors]
//           |
//   [needle visitors]
//           |
//   [typed search + output helpers]
//           |
//   [meta-function dispatch]
//

#define VISIT_SEARCH_SORTED_TYPES(VISIT) \
  VISIT(BooleanType)                     \
  VISIT(Int8Type)                        \
  VISIT(Int16Type)                       \
  VISIT(Int32Type)                       \
  VISIT(Int64Type)                       \
  VISIT(UInt8Type)                       \
  VISIT(UInt16Type)                      \
  VISIT(UInt32Type)                      \
  VISIT(UInt64Type)                      \
  VISIT(FloatType)                       \
  VISIT(DoubleType)                      \
  VISIT(Date32Type)                      \
  VISIT(Date64Type)                      \
  VISIT(Time32Type)                      \
  VISIT(Time64Type)                      \
  VISIT(TimestampType)                   \
  VISIT(DurationType)                    \
  VISIT(BinaryType)                      \
  VISIT(StringType)                      \
  VISIT(LargeBinaryType)                 \
  VISIT(LargeStringType)                 \
  VISIT(BinaryViewType)                  \
  VISIT(StringViewType)

template <typename ArrowType>
using SearchValue = typename GetViewType<ArrowType>::T;

struct NonNullValuesRange {
  int64_t offset = 0;
  int64_t length = 0;

  /// Return whether the range spans the full searched values input.
  bool is_identity(int64_t full_length) const {
    return (offset == 0) && (length == full_length);
  }
};

template <typename ReturnType, typename Visitor>
ReturnType DispatchRunEndEncodedByRunEndType(const RunEndEncodedArray& array,
                                             const char* argument_name,
                                             Visitor&& visitor);

inline int64_t GetRunEndValue(const ArraySpan& run_ends, int64_t physical_index) {
  switch (run_ends.type->id()) {
    case Type::INT16:
      return run_ends.GetValues<int16_t>(1)[physical_index];
    case Type::INT32:
      return run_ends.GetValues<int32_t>(1)[physical_index];
    case Type::INT64:
      return run_ends.GetValues<int64_t>(1)[physical_index];
    default:
      DCHECK(false) << "Unexpected run-end type for search_sorted values: "
                    << run_ends.type->ToString();
      return 0;
  }
}

/// Comparator implementing Arrow's ascending-order semantics for supported types.
template <typename ArrowType>
struct SearchSortedCompare {
  using ValueType = SearchValue<ArrowType>;

  int operator()(const ValueType& left, const ValueType& right) const {
    return CompareTypeValues<ArrowType>(left, right, SortOrder::Ascending,
                                        NullPlacement::AtEnd);
  }
};

/// Access logical values from a plain Arrow array.
template <typename ArrowType>
class PlainArrayAccessor {
 public:
  using ArrayType = typename TypeTraits<ArrowType>::ArrayType;
  using ValueType = SearchValue<ArrowType>;

  /// Build a typed accessor over a plain array payload.
  explicit PlainArrayAccessor(const std::shared_ptr<ArrayData>& array_data)
      : array_(array_data) {}

  /// Return the logical length of the searched values.
  int64_t length() const { return array_.length(); }

  /// Return the logical value at the given logical position.
  ValueType Value(int64_t index) const {
    return GetViewType<ArrowType>::LogicalValue(array_.GetView(index));
  }

  uint64_t LogicalInsertionIndex(int64_t index) const {
    return static_cast<uint64_t>(index);
  }

 private:
  ArrayType array_;
};

/// Access logical values from a run-end encoded Arrow array.
template <typename ArrowType>
class RunEndEncodedValuesAccessor {
 public:
  using ArrayType = typename TypeTraits<ArrowType>::ArrayType;
  using ValueType = SearchValue<ArrowType>;

  /// Build a typed accessor over a run-end encoded payload.
  explicit RunEndEncodedValuesAccessor(const RunEndEncodedArray& array)
      : RunEndEncodedValuesAccessor(array, /*logical_offset=*/0,
                                    /*logical_length=*/array.length()) {}

  RunEndEncodedValuesAccessor(const RunEndEncodedArray& array, int64_t logical_offset,
                              int64_t logical_length)
      : array_(array),
        values_(array.values()->data()),
        array_span_(*array.data()),
        absolute_offset_(array.offset() + logical_offset),
        logical_length_(logical_length),
        physical_range_(ree_util::FindPhysicalRange(
            array_span_, absolute_offset_, logical_length_)) {}

  /// Return the number of physical runs used as the search domain.
  int64_t length() const { return physical_range_.second; }

  /// Return the logical value at the given physical run position.
  ValueType Value(int64_t index) const {
    const auto physical_index = physical_range_.first + index;
    return GetViewType<ArrowType>::LogicalValue(values_.GetView(physical_index));
  }

  ValueType LogicalValue(int64_t index) const {
    const auto physical_index = ree_util::FindPhysicalIndex(array_span_, index,
                                                            absolute_offset_);
    return GetViewType<ArrowType>::LogicalValue(values_.GetView(physical_index));
  }

  uint64_t LogicalInsertionIndex(int64_t index) const {
    DCHECK_GE(index, 0);
    DCHECK_LE(index, physical_range_.second);

    if (index == 0) {
      return 0;
    }
    if (index == physical_range_.second) {
      return static_cast<uint64_t>(logical_length_);
    }
    return static_cast<uint64_t>(LogicalRunEnd(physical_range_.first + index - 1));
  }

  const RunEndEncodedArray& array() const { return array_; }

 private:
  int64_t LogicalRunEnd(int64_t physical_index) const {
    const int64_t logical_run_end =
        std::max<int64_t>(GetRunEndValue(ree_util::RunEndsArray(array_span_), physical_index) -
                              absolute_offset_,
                          0);
    return std::min(logical_run_end, logical_length_);
  }

  const RunEndEncodedArray& array_;
  ArrayType values_;
  ArraySpan array_span_;
  int64_t absolute_offset_;
  int64_t logical_length_;
  std::pair<int64_t, int64_t> physical_range_;
};

/// Access logical values from a chunked Arrow array without combining chunks.
template <typename ArrowType>
class ChunkedArrayAccessor {
 public:
  using ArrayType = typename TypeTraits<ArrowType>::ArrayType;
  using ValueType = SearchValue<ArrowType>;

  explicit ChunkedArrayAccessor(const ChunkedArray& chunked_array)
      : chunked_array_(chunked_array), resolver_(chunked_array.chunks()) {
    chunks_.reserve(static_cast<size_t>(chunked_array_.num_chunks()));
    for (const auto& chunk : chunked_array_.chunks()) {
      DCHECK_NE(chunk->type_id(), Type::RUN_END_ENCODED);
      chunks_.emplace_back(chunk->data());
    }
  }

  int64_t length() const { return chunked_array_.length(); }

  ValueType Value(int64_t index) const {
    const auto location = resolver_.Resolve(index);
    DCHECK_LT(location.chunk_index, chunked_array_.num_chunks());
    return GetViewType<ArrowType>::LogicalValue(
        chunks_[location.chunk_index].GetView(location.index_in_chunk));
  }

  uint64_t LogicalInsertionIndex(int64_t index) const {
    return static_cast<uint64_t>(index);
  }

 private:
  const ChunkedArray& chunked_array_;
  ChunkResolver resolver_;
  std::vector<ArrayType> chunks_;
};

template <typename ArrowType>
class ChunkedRunEndEncodedValuesAccessor {
 public:
  using ValueType = SearchValue<ArrowType>;

  explicit ChunkedRunEndEncodedValuesAccessor(const ChunkedArray& chunked_array)
      : ChunkedRunEndEncodedValuesAccessor(chunked_array, /*logical_offset=*/0,
                                           /*logical_length=*/chunked_array.length()) {}

  ChunkedRunEndEncodedValuesAccessor(const ChunkedArray& chunked_array,
                                     int64_t logical_offset, int64_t logical_length)
      : chunked_array_(chunked_array), logical_length_(logical_length) {
    const auto chunk_count = chunked_array_.num_chunks();
    run_offsets_.reserve(static_cast<size_t>(chunk_count));
    logical_offsets_.reserve(static_cast<size_t>(chunk_count));
    accessors_.reserve(static_cast<size_t>(chunk_count));

    int64_t chunk_logical_start = 0;
    int64_t selected_run_start = 0;
    int64_t selected_logical_start = 0;
    const int64_t logical_end = logical_offset + logical_length;

    for (const auto& chunk : chunked_array_.chunks()) {
      const int64_t chunk_logical_end = chunk_logical_start + chunk->length();
      const int64_t local_offset = std::max<int64_t>(logical_offset, chunk_logical_start) -
                                   chunk_logical_start;
      const int64_t local_end =
          std::min<int64_t>(logical_end, chunk_logical_end) - chunk_logical_start;
      const int64_t local_length = local_end - local_offset;

      if (local_length > 0) {
        DCHECK_EQ(chunk->type_id(), Type::RUN_END_ENCODED);

        const auto& ree_chunk = checked_cast<const RunEndEncodedArray&>(*chunk);
        run_offsets_.push_back(selected_run_start);
        logical_offsets_.push_back(selected_logical_start);
        accessors_.emplace_back(ree_chunk, local_offset, local_length);

        selected_run_start += accessors_.back().length();
        selected_logical_start += local_length;
      }

      chunk_logical_start = chunk_logical_end;
    }

    DCHECK_EQ(selected_logical_start, logical_length_);
    total_run_count_ = selected_run_start;
  }

  int64_t length() const { return total_run_count_; }

  ValueType Value(int64_t index) const {
    const auto [chunk_index, local_index] = ResolveRun(index);
    return accessors_[chunk_index].Value(local_index);
  }

  uint64_t LogicalInsertionIndex(int64_t index) const {
    DCHECK_GE(index, 0);
    DCHECK_LE(index, total_run_count_);

    if (index == 0) {
      return 0;
    }
    if (index == total_run_count_) {
      return static_cast<uint64_t>(logical_length_);
    }

    const auto [chunk_index, local_index] = ResolveRun(index);
    return static_cast<uint64_t>(logical_offsets_[chunk_index]) +
           accessors_[chunk_index].LogicalInsertionIndex(local_index);
  }

  const ChunkedArray& chunked_array() const { return chunked_array_; }

 private:
  std::pair<size_t, int64_t> ResolveRun(int64_t index) const {
    DCHECK_LT(index, total_run_count_);
    const auto it = std::upper_bound(run_offsets_.begin(), run_offsets_.end(), index);
    DCHECK_NE(it, run_offsets_.begin());
    const auto chunk_index = static_cast<size_t>(std::distance(run_offsets_.begin(), it) - 1);
    return {chunk_index, index - run_offsets_[chunk_index]};
  }

  const ChunkedArray& chunked_array_;
  int64_t logical_length_;
  int64_t total_run_count_ = 0;
  std::vector<int64_t> run_offsets_;
  std::vector<int64_t> logical_offsets_;
  std::vector<RunEndEncodedValuesAccessor<ArrowType>> accessors_;
};

constexpr std::string_view kClusteredNullValuesError =
    "search_sorted values with nulls must be clustered at the start or end.";

inline Result<NonNullValuesRange> MakeNonNullValuesRange(int64_t full_length,
                                                         int64_t null_count,
                                                         int64_t leading_null_count,
                                                         int64_t trailing_null_count) {
  NonNullValuesRange non_null_values_range{.offset = 0, .length = full_length};

  if (leading_null_count == full_length) {
    non_null_values_range.offset = full_length;
    non_null_values_range.length = 0;
    return non_null_values_range;
  }

  if (leading_null_count > 0) {
    if (leading_null_count != null_count) {
      return Status::Invalid(kClusteredNullValuesError);
    }
    non_null_values_range.offset = leading_null_count;
    non_null_values_range.length = full_length - leading_null_count;
    return non_null_values_range;
  }

  if (trailing_null_count == 0 || trailing_null_count != null_count) {
    return Status::Invalid(kClusteredNullValuesError);
  }

  non_null_values_range.length = full_length - trailing_null_count;
  return non_null_values_range;
}

inline Result<NonNullValuesRange> MakeNonNullValuesRangeFromNullPlacement(
    int64_t full_length, int64_t null_count, bool has_leading_nulls) {
  return MakeNonNullValuesRange(full_length, null_count,
                                has_leading_nulls ? null_count : 0,
                                has_leading_nulls ? 0 : null_count);
}

inline const std::shared_ptr<Array>* FindFirstNonEmptyChunk(const ChunkedArray& values) {
  const auto it =
      std::ranges::find_if(values.chunks(), [](const std::shared_ptr<Array>& chunk) {
        return chunk->length() != 0;
      });
  return it == values.chunks().end() ? nullptr : &*it;
}

inline int64_t GetLogicalNullCount(const ArrayData& values) {
  if (!values.MayHaveLogicalNulls()) {
    return 0;
  }
  if (values.type->id() == Type::RUN_END_ENCODED) {
    return values.ComputeLogicalNullCount();
  }
  return values.GetNullCount();
}

inline int64_t GetLogicalNullCount(const ChunkedArray& values) {
  if (values.type()->id() != Type::RUN_END_ENCODED) {
    return values.null_count();
  }

  auto chunk_null_counts = values.chunks() | std::views::transform([](const auto& chunk) {
                            return GetLogicalNullCount(*chunk->data());
                          });
  return std::reduce(chunk_null_counts.begin(), chunk_null_counts.end(), int64_t{0});
}

/// Present a contiguous non-null slice of the searched values through the same
/// accessor interface as the original values container.
template <typename ValuesAccessor>
class NonNullValuesAccessor {
 public:
  /// Wrap the original accessor with the discovered non-null subrange.
  explicit NonNullValuesAccessor(const ValuesAccessor& values,
                                 const NonNullValuesRange& non_null_values_range)
      : values_(values),
        offset_(non_null_values_range.offset),
  length_(non_null_values_range.length),
  base_insertion_index_(values_.LogicalInsertionIndex(offset_)) {}

  /// Return the number of accessible non-null values.
  int64_t length() const noexcept { return length_; }

  /// Return the value at the given index within the non-null subrange.
  auto Value(int64_t index) const { return values_.Value(offset_ + index); }

  uint64_t LogicalInsertionIndex(int64_t index) const {
    return values_.LogicalInsertionIndex(offset_ + index) - base_insertion_index_;
  }

 private:
  const ValuesAccessor& values_;
  int64_t offset_;
  int64_t length_;
  uint64_t base_insertion_index_;
};

/// Return the logical type of a datum, unwrapping run-end encoding when present.
inline const DataType& LogicalType(const Datum& datum) {
  const auto& type = *datum.type();
  if (type.id() == Type::RUN_END_ENCODED) {
    return *checked_cast<const RunEndEncodedType&>(type).value_type();
  }
  return type;
}

/// Return whether a scalar or array needle input contains any logical nulls.
inline bool DatumHasNulls(const Datum& datum) {
  if (datum.is_scalar()) {
    return !datum.scalar()->is_valid;
  }

  if (datum.is_chunked_array()) {
    const auto& chunked_array = *datum.chunked_array();
    if (chunked_array.null_count() > 0) {
      return true;
    }
    if (chunked_array.type()->id() != Type::RUN_END_ENCODED) {
      return false;
    }
    return std::ranges::any_of(
        chunked_array.chunks(), [](const std::shared_ptr<Array>& chunk) {
          const auto& ree_chunk = checked_cast<const RunEndEncodedArray&>(*chunk);
          return ree_chunk.values()->null_count() != 0;
        });
  }

  const auto& array_data = datum.array();
  const bool has_nulls = array_data->GetNullCount() > 0;
  if (array_data->type->id() == Type::RUN_END_ENCODED) {
    RunEndEncodedArray run_end_encoded(array_data);
    return has_nulls || (run_end_encoded.values()->null_count() != 0);
  }
  return has_nulls;
}

/// Reject nested run-end encoded values. TODO: Support this case in the future if there
/// is demand for it.
inline Status ValidateRunEndEncodedLogicalValueType(const DataType& type,
                                                    const char* name) {
  const auto& ree_type = checked_cast<const RunEndEncodedType&>(type);
  if (ree_type.value_type()->id() == Type::RUN_END_ENCODED) {
    return Status::TypeError("Nested run-end encoded ", name, " are not supported");
  }
  return Status::OK();
}

/// Compute the contiguous non-null window of the searched values.
///
inline Result<NonNullValuesRange> FindNonNullValuesRange(const ArrayData& values) {
  NonNullValuesRange non_null_values_range{.offset = 0, .length = values.length};

  const auto null_count = GetLogicalNullCount(values);
  if (null_count == 0) {
    return non_null_values_range;
  }

  const bool has_leading_nulls = values.IsNull(0);
  return MakeNonNullValuesRangeFromNullPlacement(values.length, null_count,
                                                 has_leading_nulls);
}

/// Validate the searched values input shape and supported encoding.
inline Status ValidateSortedValuesInput(const Datum& datum) {
  if (!(datum.is_array() || datum.is_chunked_array())) {
    return Status::TypeError("search_sorted values must be an array or chunked array");
  }

  const auto& type = *datum.type();
  if (type.id() == Type::RUN_END_ENCODED) {
    return ValidateRunEndEncodedLogicalValueType(type, "values");
  }

  return Status::OK();
}

/// Validate the needles input shape and supported encoding.
/// Needles can be either a scalar or an array, but if an array is provided it must not
/// have nested run-end encoding since that is not currently supported.
inline Status ValidateNeedleInput(const Datum& datum) {
  if (!(datum.is_array() || datum.is_chunked_array() || datum.is_scalar())) {
    return Status::TypeError(
        "search_sorted needles must be a scalar, array, or chunked array");
  }

  if ((datum.is_array() || datum.is_chunked_array()) &&
      datum.type()->id() == Type::RUN_END_ENCODED) {
    return ValidateRunEndEncodedLogicalValueType(*datum.type(), "needles");
  }
  return Status::OK();
}

inline Result<NonNullValuesRange> FindNonNullValuesRange(const ChunkedArray& values) {
  NonNullValuesRange non_null_values_range{.offset = 0, .length = values.length()};

  const auto null_count = GetLogicalNullCount(values);
  if (null_count == 0) {
    return non_null_values_range;
  }

  const auto* first_non_empty_chunk = FindFirstNonEmptyChunk(values);
  DCHECK_NE(first_non_empty_chunk, nullptr);

  const bool has_leading_nulls = (*first_non_empty_chunk)->IsNull(0);
  return MakeNonNullValuesRangeFromNullPlacement(values.length(), null_count,
                                                 has_leading_nulls);
}

/// Perform a lower- or upper-bound binary search over already sorted values.
template <SearchSortedOptions::Side side, typename ArrowType, typename Accessor>
uint64_t FindInsertionPointImpl(const Accessor& sorted_values,
                                const SearchValue<ArrowType>& needle) {
  SearchSortedCompare<ArrowType> compare;
  int64_t first = 0;
  int64_t count = sorted_values.length();

  // TODO(search_sorted): For fixed-width primitive haystacks, investigate a SIMD-friendly
  // batched search path .
  while (count > 0) {
    const int64_t step = count / 2;
    const int64_t it = first + step;
    const bool advance = [&] {
      if constexpr (side == SearchSortedOptions::Left) {
        return compare(sorted_values.Value(it), needle) < 0;
      } else {
        return compare(needle, sorted_values.Value(it)) >= 0;
      }
    }();
    if (advance) {
      first = it + 1;
      count -= step + 1;
    } else {
      count = step;
    }
  }
  return static_cast<uint64_t>(first);
}

template <typename ArrowType, typename Accessor>
uint64_t FindInsertionPoint(const Accessor& sorted_values,
                            const SearchValue<ArrowType>& needle,
                            SearchSortedOptions::Side side) {
  switch (side) {
    case SearchSortedOptions::Left:
      return FindInsertionPointImpl<SearchSortedOptions::Left, ArrowType>(sorted_values,
                                                                          needle);
    case SearchSortedOptions::Right:
      return FindInsertionPointImpl<SearchSortedOptions::Right, ArrowType>(sorted_values,
                                                                           needle);
  }
  DCHECK(false) << "Unexpected SearchSortedOptions::Side value";
  return FindInsertionPointImpl<SearchSortedOptions::Left, ArrowType>(sorted_values,
                                                                      needle);
}

template <typename ArrowType, typename Accessor>
uint64_t FindLogicalInsertionIndex(const Accessor& sorted_values,
                                   const SearchValue<ArrowType>& needle,
                                   SearchSortedOptions::Side side,
                                   uint64_t insertion_offset) {
  const auto search_index =
      static_cast<int64_t>(FindInsertionPoint<ArrowType>(sorted_values, needle, side));
  return sorted_values.LogicalInsertionIndex(search_index) + insertion_offset;
}

/// Read a scalar needle without materializing a one-element array.
template <typename ArrowType>
SearchValue<ArrowType> ExtractScalarValue(const Scalar& scalar) {
  using ScalarType = typename TypeTraits<ArrowType>::ScalarType;
  const auto& typed_scalar = checked_cast<const ScalarType&>(scalar);

  if constexpr (std::is_base_of_v<BaseBinaryScalar, ScalarType>) {
    return GetViewType<ArrowType>::LogicalValue(typed_scalar.view());
  } else {
    return GetViewType<ArrowType>::LogicalValue(typed_scalar.value);
  }
}

/// Append the same insertion index repeatedly for a logical run of needles.
inline void AppendInsertionIndex(UInt64Builder& builder, uint64_t insertion_index,
                                 int64_t count) {
  if (count == 0) {
    return;
  }
  DCHECK_LE(builder.length() + count, builder.capacity());
  std::fill_n(builder.GetMutableValue(builder.length()), count, insertion_index);
  builder.UnsafeAdvance(count);
}

/// Dispatch a run-end encoded array to the matching run-end physical type.
template <typename ReturnType, typename Visitor>
ReturnType DispatchRunEndEncodedByRunEndType(const RunEndEncodedArray& array,
                                             const char* argument_name,
                                             Visitor&& visitor) {
  const auto& ree_type = checked_cast<const RunEndEncodedType&>(*array.type());
  switch (ree_type.run_end_type()->id()) {
    case Type::INT16:
      return std::forward<Visitor>(visitor).template operator()<int16_t>(array);
    case Type::INT32:
      return std::forward<Visitor>(visitor).template operator()<int32_t>(array);
    case Type::INT64:
      return std::forward<Visitor>(visitor).template operator()<int64_t>(array);
    default:
      return ReturnType(Status::TypeError("Unsupported run-end type for search_sorted ",
                                          argument_name, ": ", array.type()->ToString()));
  }
}

template <typename ArrowType, bool EmitNulls>
using VisitedNeedle = std::conditional_t<EmitNulls, std::optional<SearchValue<ArrowType>>,
                                         SearchValue<ArrowType>>;

/// Normalize a non-null logical needle into the visitor payload type.
template <typename ArrowType, bool EmitNulls>
VisitedNeedle<ArrowType, EmitNulls> MakeVisitedNeedle(
    const SearchValue<ArrowType>& needle) {
  if constexpr (EmitNulls) {
    return std::optional<SearchValue<ArrowType>>(needle);
  } else {
    return needle;
  }
}

/// Read one logical needle value from a physical array position.
template <typename ArrowType, bool EmitNulls, typename ArrayType>
VisitedNeedle<ArrowType, EmitNulls> ReadVisitedNeedle(const ArrayType& array,
                                                      int64_t physical_index) {
  if constexpr (EmitNulls) {
    if (array.IsNull(physical_index)) {
      return std::nullopt;
    }
  }
  const auto needle = GetViewType<ArrowType>::LogicalValue(array.GetView(physical_index));
  return MakeVisitedNeedle<ArrowType, EmitNulls>(needle);
}

/// Visit each plain-array needle as single-element logical runs.
template <typename ArrowType, bool EmitNulls, typename Visitor>
Status VisitArrayNeedleRuns(const std::shared_ptr<ArrayData>& needles_data,
                            Visitor&& visitor) {
  using ArrayType = typename TypeTraits<ArrowType>::ArrayType;

  ArrayType array(needles_data);
  for (int64_t index = 0; index < array.length(); ++index) {
    RETURN_NOT_OK(visitor(ReadVisitedNeedle<ArrowType, EmitNulls>(array, index), 1));
  }
  return Status::OK();
}

/// Visit each run of a run-end encoded needle array as one logical span.
template <typename ArrowType, typename RunEndCType, bool EmitNulls, typename Visitor>
Status VisitRunEndEncodedNeedleRuns(const RunEndEncodedArray& needles,
                                    Visitor&& visitor) {
  using ArrayType = typename TypeTraits<ArrowType>::ArrayType;

  ArrayType values(needles.values()->data());
  ArraySpan array_span(*needles.data());
  ::arrow::ree_util::RunEndEncodedArraySpan<RunEndCType> span(array_span);

  for (auto it = span.begin(); !it.is_end(span); ++it) {
    RETURN_NOT_OK(
        visitor(ReadVisitedNeedle<ArrowType, EmitNulls>(values, it.index_into_array()),
                it.logical_position(), it.run_end()));
  }
  return Status::OK();
}

template <typename ArrowType, typename RunEndCType, bool EmitNulls, typename Visitor>
Status VisitRunEndEncodedNeedleValues(const RunEndEncodedArray& needles,
                                      Visitor&& visitor) {
  using ArrayType = typename TypeTraits<ArrowType>::ArrayType;

  ArrayType values(needles.values()->data());
  ArraySpan array_span(*needles.data());
  ::arrow::ree_util::RunEndEncodedArraySpan<RunEndCType> span(array_span);

  for (auto it = span.begin(); !it.is_end(span); ++it) {
    RETURN_NOT_OK(
        visitor(ReadVisitedNeedle<ArrowType, EmitNulls>(values, it.index_into_array()),
                it.run_length()));
  }
  return Status::OK();
}

/// Visit scalar, plain-array, run-end encoded, or chunked needles through a
/// uniform callback interface of logical run lengths.
template <typename ArrowType, bool EmitNulls, typename Visitor>
Status VisitNeedleRuns(const Datum& needles, Visitor&& visitor) {
  if (needles.is_scalar()) {
    if constexpr (EmitNulls) {
      if (!needles.scalar()->is_valid) {
        return visitor(std::optional<SearchValue<ArrowType>>{}, 1);
      }
    }
    return visitor(MakeVisitedNeedle<ArrowType, EmitNulls>(
                       ExtractScalarValue<ArrowType>(*needles.scalar())),
                   1);
  }

  if (needles.is_chunked_array()) {
    for (const auto& chunk : needles.chunked_array()->chunks()) {
      ARROW_RETURN_NOT_OK(
          (VisitNeedleRuns<ArrowType, EmitNulls>(Datum(chunk), visitor)));
    }
    return Status::OK();
  }

  const auto& needle_data = needles.array();
  if (needle_data->type->id() == Type::RUN_END_ENCODED) {
    RunEndEncodedArray ree(needle_data);
    return DispatchRunEndEncodedByRunEndType<Status>(
        ree, "needles",
        [&]<typename RunEndCType>(const RunEndEncodedArray& run_end_encoded_needles) {
          return VisitRunEndEncodedNeedleValues<ArrowType, RunEndCType, EmitNulls>(
              run_end_encoded_needles, visitor);
        });
  }

  return VisitArrayNeedleRuns<ArrowType, EmitNulls>(needle_data, visitor);
}

/// Output sink for the no-null needle path.
///
/// This preserves the cheapest materialization strategy: write repeated
/// insertion indices directly into the preallocated result buffer, while still
/// sharing the same EmitInsertionIndices traversal used by the nullable path.
class PreallocatedInsertionIndexOutput {
 public:
  explicit PreallocatedInsertionIndexOutput(uint64_t* out_values) : out_values_(out_values) {}

  Status AppendValue(uint64_t insertion_index, int64_t run_length) {
    std::ranges::fill(
        std::span<uint64_t>(out_values_ + length_, static_cast<size_t>(run_length)),
        insertion_index);
    length_ += run_length;
    return Status::OK();
  }

 private:
  uint64_t* out_values_;
  int64_t length_ = 0;
};

/// Output sink for the nullable needle path.
///
/// This keeps null emission and repeated-value appends behind the same output
/// interface as the preallocated fast path, so the search loop itself does not
/// need separate nullable and non-null implementations.
class BuilderInsertionIndexOutput {
 public:
  explicit BuilderInsertionIndexOutput(UInt64Builder* builder) : builder_(builder) {}

  Status AppendNulls(int64_t run_length) { return builder_->AppendNulls(run_length); }

  Status AppendValue(uint64_t insertion_index, int64_t run_length) {
    AppendInsertionIndex(*builder_, insertion_index, run_length);
    return Status::OK();
  }

 private:
  UInt64Builder* builder_;
};

/// Visit normalized needle runs and emit insertion indices through an output
/// policy object.
///
/// The output sink abstraction keeps the binary-search traversal shared between
/// the preallocated no-null fast path and the builder-backed nullable path.
template <typename ArrowType, bool EmitNulls, typename ValuesAccessor, typename Output>
Status EmitInsertionIndices(const ValuesAccessor& sorted_values, const Datum& needles,
                            SearchSortedOptions::Side side,
                            uint64_t insertion_offset, Output* output) {
  auto emit_search_result =
      [&](const VisitedNeedle<ArrowType, EmitNulls>& needle, int64_t run_length) -> Status {
    if constexpr (EmitNulls) {
      if (!needle.has_value()) {
        return output->AppendNulls(run_length);
      }
    }

    const auto insertion_index = [&] {
      if constexpr (EmitNulls) {
        return FindLogicalInsertionIndex<ArrowType>(sorted_values, *needle, side,
                                                    insertion_offset);
      } else {
        return FindLogicalInsertionIndex<ArrowType>(sorted_values, needle, side,
                                                    insertion_offset);
      }
    }();
    return output->AppendValue(insertion_index, run_length);
  };

  return VisitNeedleRuns<ArrowType, EmitNulls>(needles, emit_search_result);
}

/// Materialize output for scalar or array needles.
template <typename ArrowType, typename ValuesAccessor>
Result<Datum> ComputeInsertionIndices(const ValuesAccessor& sorted_values,
                                      const Datum& needles,
                                      SearchSortedOptions::Side side,
                                      uint64_t insertion_offset, ExecContext* ctx) {
  if (needles.is_scalar()) {
    auto scalar = needles.scalar();
    if (!scalar->is_valid) {
      return Datum(std::make_shared<UInt64Scalar>());
    }

    const auto insertion_index = FindLogicalInsertionIndex<ArrowType>(
      sorted_values, ExtractScalarValue<ArrowType>(*scalar), side, insertion_offset);
    return Datum(std::make_shared<UInt64Scalar>(insertion_index));
  }

  if (DatumHasNulls(needles)) {
    UInt64Builder builder(ctx->memory_pool());
    ARROW_RETURN_NOT_OK(builder.Reserve(needles.length()));
    BuilderInsertionIndexOutput output(&builder);
    ARROW_RETURN_NOT_OK((EmitInsertionIndices<ArrowType, true>(
        sorted_values, needles, side, insertion_offset, &output)));
    ARROW_ASSIGN_OR_RAISE(auto out, builder.Finish());
    return Datum(std::move(out));
  }

  ARROW_ASSIGN_OR_RAISE(auto out,
                        MakeMutableUInt64Array(needles.length(), ctx->memory_pool()));
  auto* out_values = out->GetMutableValues<uint64_t>(1);
  PreallocatedInsertionIndexOutput output(out_values);

  ARROW_RETURN_NOT_OK((EmitInsertionIndices<ArrowType, false>(
      sorted_values, needles, side, insertion_offset, &output)));
  return Datum(MakeArray(std::move(out)));
}

// Main entry point for search_sorted over a single array of sorted values and scalar or
// array needles. Handles null presence in the needles and dispatches to the appropriate
// search implementation.
template <typename ArrowType, typename ValuesAccessor>
Result<Datum> SearchWithAccessor(const ValuesAccessor& values_accessor,
                                 const NonNullValuesRange& non_null_values_range,
                                 const Datum& needles, SearchSortedOptions::Side side,
                                 ExecContext* ctx) {
  if (non_null_values_range.is_identity(values_accessor.length())) {
    return ComputeInsertionIndices<ArrowType>(values_accessor, needles, side,
                                              /*insertion_offset=*/0, ctx);
  }

  NonNullValuesAccessor non_null_values(values_accessor, non_null_values_range);
  return ComputeInsertionIndices<ArrowType>(
      non_null_values, needles, side, static_cast<uint64_t>(non_null_values_range.offset),
      ctx);
}

template <typename ArrowType>
Result<Datum> SearchWithAccessor(
    const RunEndEncodedValuesAccessor<ArrowType>& values_accessor,
    const NonNullValuesRange& non_null_values_range, const Datum& needles,
    SearchSortedOptions::Side side, ExecContext* ctx) {
  if (non_null_values_range.is_identity(values_accessor.array().length())) {
    return ComputeInsertionIndices<ArrowType>(values_accessor, needles, side,
                                              /*insertion_offset=*/0, ctx);
  }

  RunEndEncodedValuesAccessor<ArrowType> non_null_values(values_accessor.array(),
                                                         non_null_values_range.offset,
                                                         non_null_values_range.length);
  return ComputeInsertionIndices<ArrowType>(
      non_null_values, needles, side, static_cast<uint64_t>(non_null_values_range.offset),
      ctx);
}

template <typename ArrowType>
Result<Datum> SearchWithAccessor(
    const ChunkedRunEndEncodedValuesAccessor<ArrowType>& values_accessor,
    const NonNullValuesRange& non_null_values_range, const Datum& needles,
    SearchSortedOptions::Side side, ExecContext* ctx) {
  if (non_null_values_range.is_identity(values_accessor.chunked_array().length())) {
    return ComputeInsertionIndices<ArrowType>(values_accessor, needles, side,
                                              /*insertion_offset=*/0, ctx);
  }
  ChunkedRunEndEncodedValuesAccessor<ArrowType> non_null_values(
      values_accessor.chunked_array(), non_null_values_range.offset,
      non_null_values_range.length);
  return ComputeInsertionIndices<ArrowType>(
      non_null_values, needles, side, static_cast<uint64_t>(non_null_values_range.offset),
      ctx);
}

// Meta-function implementation for the search_sorted public compute entrypoint.
template <typename ArrowType, typename Visitor>
Result<Datum> VisitValuesAccessor(const std::shared_ptr<ArrayData>& values_data,
                                  Visitor&& visitor) {
  if (values_data->type->id() == Type::RUN_END_ENCODED) {
    RunEndEncodedArray ree(values_data);
    RunEndEncodedValuesAccessor<ArrowType> values_accessor(ree);
    return visitor(values_accessor);
  }

  PlainArrayAccessor<ArrowType> values_accessor(values_data);
  return visitor(values_accessor);
}

template <typename ArrowType, typename Visitor>
Result<Datum> VisitValuesAccessor(const ChunkedArray& values, Visitor&& visitor) {
  if (values.type()->id() == Type::RUN_END_ENCODED) {
    ChunkedRunEndEncodedValuesAccessor<ArrowType> values_accessor(values);
    return visitor(values_accessor);
  }

  ChunkedArrayAccessor<ArrowType> values_accessor(values);
  return visitor(values_accessor);
}

/// Meta-function implementation for the search_sorted public compute entrypoint.
/// Validates input shapes and types, normalizes to logical value accessors, and
/// dispatches to the typed search implementation.
class SearchSortedMetaFunction : public MetaFunction {
 public:
  /// Construct the registry entry with default options and documentation.
  SearchSortedMetaFunction()
      : MetaFunction("search_sorted", Arity::Binary(), search_sorted_doc,
                     GetDefaultSearchSortedOptions()) {}

  /// Validate inputs, normalize options, and dispatch to the typed search implementation.
  Result<Datum> ExecuteImpl(const std::vector<Datum>& args,
                            const FunctionOptions* options,
                            ExecContext* ctx) const override {
    RETURN_NOT_OK(ValidateSortedValuesInput(args[0]));
    RETURN_NOT_OK(ValidateNeedleInput(args[1]));

    const auto& values_type = LogicalType(args[0]);
    const auto& needles_type = LogicalType(args[1]);
    if (!values_type.Equals(needles_type)) {
      return Status::TypeError(
          "search_sorted arguments must have matching logical types, got ",
          values_type.ToString(), " and ", needles_type.ToString());
    }

    ARROW_ASSIGN_OR_RAISE(auto non_null_values_range, FindNonNullValuesRange(args[0]));
    auto result = DispatchByType(args[0], non_null_values_range, args[1],
                                 static_cast<const SearchSortedOptions&>(*options), ctx);
    return result;
  }

 private:
  [[nodiscard]] Result<NonNullValuesRange> FindNonNullValuesRange(const Datum& values) const {
    if (values.is_chunked_array()) {
      return ::arrow::compute::internal::FindNonNullValuesRange(*values.chunked_array());
    }
    return ::arrow::compute::internal::FindNonNullValuesRange(*values.array());
  }

  /// Dispatch the logical value type to the matching template specialization.
  Result<Datum> DispatchByType(const Datum& values,
                               const NonNullValuesRange& non_null_values_range,
                               const Datum& needles, const SearchSortedOptions& options,
                               ExecContext* ctx) const {
    switch (LogicalType(values).id()) {
#define VISIT(TYPE)                                                                     \
  case TYPE::type_id:                                                                   \
    return DispatchHaystack<TYPE>(values, non_null_values_range, needles, options.side, \
                                  ctx);
      VISIT_SEARCH_SORTED_TYPES(VISIT)
#undef VISIT
      default:
        break;
    }
    return Status::NotImplemented("search_sorted is not implemented for type ",
                                  LogicalType(values).ToString());
  }

  /// Dispatch the physical representation of the searched values.
  template <typename ArrowType>
  Result<Datum> DispatchHaystack(const Datum& values,
                                 const NonNullValuesRange& non_null_values_range,
                                 const Datum& needles, SearchSortedOptions::Side side,
                                 ExecContext* ctx) const {
    if (values.is_chunked_array()) {
      return VisitValuesAccessor<ArrowType>(
          *values.chunked_array(), [&](const auto& values_accessor) {
            return SearchWithAccessor<ArrowType>(values_accessor, non_null_values_range,
                                                 needles, side, ctx);
          });
    }

    return VisitValuesAccessor<ArrowType>(
        values.array(), [&](const auto& values_accessor) {
          return SearchWithAccessor<ArrowType>(values_accessor, non_null_values_range,
                                               needles, side, ctx);
        });
  }
};

}  // namespace

/// Register the search_sorted vector kernel in the global compute registry.
void RegisterVectorSearchSorted(FunctionRegistry* registry) {
  DCHECK_OK(registry->AddFunction(std::make_shared<SearchSortedMetaFunction>()));
}

}  // namespace compute::internal
}  // namespace arrow
