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
#include <optional>
#include <ranges>
#include <type_traits>
#include <utility>

#include "arrow/array/array_primitive.h"
#include "arrow/array/array_run_end.h"
#include "arrow/array/concatenate.h"
#include "arrow/array/util.h"
#include "arrow/buffer_builder.h"
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
#include "arrow/util/unreachable.h"

namespace arrow {

using internal::checked_cast;

namespace compute::internal {
namespace {

/// Return the static default options instance used by the meta-function.
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

// This file implements search_sorted as a normalization pipeline around one
// typed binary-search core.
//
// The searched values are first validated, unwrapped to their logical type,
// and adapted to a uniform accessor interface. Plain arrays and chunked arrays
// expose logical element access directly. Run-end encoded (REE) arrays expose a
// search domain over physical runs while still translating insertion positions
// back to logical indices. The typed accessors are intentionally thin: shared
// offset, length, run-resolution, and logical-index bookkeeping lives in
// non-templated helpers so that the binary search is specialized by physical
// value type without cloning the surrounding normalization logic.
//
// Values null handling is normalized before any search happens. Nulls are only
// accepted when clustered entirely at the start or entirely at the end of the
// sorted values. The implementation computes the contiguous non-null logical
// window once and then searches only within that window. For REE values this
// requires logical null counting, because nullness lives in the values child
// rather than in a top-level validity bitmap.
//
// Needles are normalized before accessor dispatch where possible. Null scalar
// needles return a null scalar result immediately. Non-null scalar needles are
// materialized as length-1 arrays so the main implementation only needs one
// array-oriented emit path. REE needles are handled separately: the kernel
// searches each physical REE value once, rebuilds a temporary REE UInt64
// result with the same logical run ends, and then run-end decodes it back to
// the dense public output shape. Plain array needles are visited element by
// element through one callback interface that propagates logical nulls.
//
// The actual comparison/search step is shared across all normalized inputs.
// After dispatching to the logical/physical Arrow representation, the kernel
// runs a lower-bound or upper-bound binary search depending on
// `SearchSortedOptions::side`, then maps the found position back to the caller-
// visible logical insertion index.
//
// Output materialization is centralized in a UInt64 builder with an optional
// validity bitmap. Non-null-only needles only build the values buffer, while
// nullable needles also emit the null bitmap. Logical null detection uses
// `ComputeLogicalNullCount()` so plain arrays, chunked arrays, and REE inputs
// all participate in the same output-nullability decision.
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
//       +--> scalar null
//       |     `--> return null scalar
//       |
//       +--> scalar value
//       |     `--> MakeArrayFromScalar(length=1)
//       |
//       +--> REE needles
//       |     +--> search physical runs once
//       |     +--> rebuild temporary REE uint64 result
//       |     `--> RunEndDecode back to dense output
//       |
//       `--> DatumHasNulls / VisitNeedleRuns
//             |
//             +--> plain array    -> one logical element per slot
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
//       |     `--> InsertionIndexBuilder<false>
//       |           `--> fill uint64 buffer directly
//       |
//       `--> nullable needles
//             `--> InsertionIndexBuilder<true>
//                   +--> AppendNulls for null runs
//                   `--> bulk fill repeated indices and validity bits
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

#define VISIT_SEARCH_SORTED_PHYSICAL_TYPES(VISIT) \
  VISIT(BooleanType)                              \
  VISIT(Int8Type)                                 \
  VISIT(Int16Type)                                \
  VISIT(Int32Type)                                \
  VISIT(Int64Type)                                \
  VISIT(UInt8Type)                                \
  VISIT(UInt16Type)                               \
  VISIT(UInt32Type)                               \
  VISIT(UInt64Type)                               \
  VISIT(FloatType)                                \
  VISIT(DoubleType)                               \
  VISIT(BinaryType)                               \
  VISIT(LargeBinaryType)                          \
  VISIT(BinaryViewType)

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

// Convert ArrayData to its physical representation so that typed accessors
// can be constructed with a physical ArrowType (e.g. Date32 → Int32).
// For REE arrays, only the values child type is converted; the REE wrapper
// type stays unchanged.
inline std::shared_ptr<ArrayData> ToPhysicalData(
    const std::shared_ptr<ArrayData>& data,
    const std::shared_ptr<DataType>& physical_type) {
  if (data->type->id() == Type::RUN_END_ENCODED) {
    auto result = data->Copy();
    auto values_copy = result->child_data[1]->Copy();
    values_copy->type = physical_type;
    result->child_data[1] = std::move(values_copy);
    return result;
  }
  auto result = data->Copy();
  result->type = physical_type;
  return result;
}

/// Read a run-end value from any supported run-end integer representation.
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

class SearchWindow {
 public:
  explicit SearchWindow(NonNullValuesRange non_null_range)
      : offset_(non_null_range.offset), length_(non_null_range.length) {}

  int64_t length() const { return length_; }

  uint64_t LogicalInsertionIndex(int64_t index) const {
    return static_cast<uint64_t>(index);
  }

 protected:
  int64_t physical_offset() const { return offset_; }

 private:
  int64_t offset_ = 0;
  int64_t length_;
};

/// Access logical values from a plain Arrow array.
template <typename ArrowType>
class PlainArrayAccessor : public SearchWindow {
 public:
  using ArrayType = typename TypeTraits<ArrowType>::ArrayType;
  using ValueType = SearchValue<ArrowType>;

  /// Build a typed accessor over a plain array payload, optionally restricted
  /// to a non-null subrange.
  explicit PlainArrayAccessor(const std::shared_ptr<ArrayData>& array_data,
                              NonNullValuesRange non_null_range = {})
      : SearchWindow(non_null_range), array_(array_data) {}

  /// Return the logical value at the given position within the search window.
  ValueType Value(int64_t index) const {
    return GetViewType<ArrowType>::LogicalValue(
        array_.GetView(physical_offset() + index));
  }

 private:
  ArrayType array_;
};

class RunEndEncodedValuesAccessorBase {
 public:
  explicit RunEndEncodedValuesAccessorBase(const RunEndEncodedArray& array,
                                           NonNullValuesRange logical_non_null_range = {})
      : array_(array),
        values_(array.values()),
        array_span_(*array.data()),
        physical_range_(::arrow::ree_util::FindPhysicalRange(array_span_, array.offset(),
                                                             array.length())) {
    InitSearchWindow(logical_non_null_range);
  }

  int64_t length() const { return search_length_; }

  int64_t NullCount() const {
    return values_->Slice(physical_range_.first, physical_range_.second)->null_count();
  }

  uint64_t LogicalInsertionIndex(int64_t index) const {
    DCHECK_GE(index, 0);
    DCHECK_LE(index, search_length_);
    return PhysicalLogicalInsertionIndex(search_offset_ + index) -
           PhysicalLogicalInsertionIndex(search_offset_);
  }

  int64_t logical_length() const { return array_.length(); }

 protected:
  int64_t PhysicalIndex(int64_t index) const {
    return physical_range_.first + search_offset_ + index;
  }

 private:
  /// Initialize the search window from a logical non-null range, converting to
  /// physical-run coordinates.
  void InitSearchWindow(const NonNullValuesRange& logical_non_null_range) {
    search_length_ = physical_range_.second;
    if (!logical_non_null_range.is_identity(array_.length())) {
      const auto null_runs = NullCount();
      search_offset_ = logical_non_null_range.offset > 0 ? null_runs : 0;
      search_length_ = physical_range_.second - null_runs;
    }
  }

  /// Compute a logical insertion index from an absolute physical run index
  /// (before search-window offsetting).
  uint64_t PhysicalLogicalInsertionIndex(int64_t physical_index) const {
    if (physical_index == 0) {
      return 0;
    }
    if (physical_index == physical_range_.second) {
      return static_cast<uint64_t>(array_.length());
    }
    return static_cast<uint64_t>(
        LogicalRunEnd(physical_range_.first + physical_index - 1));
  }

  /// Return the logical run end corresponding to a physical run index.
  int64_t LogicalRunEnd(int64_t physical_index) const {
    // The run-end value is an absolute (cumulative) logical position in the
    // full array. Subtract array_.offset() to get a position relative to the
    // current slice. Clamp to 0, when the slice offset falls in the middle of
    // a physical run the first runend after the slice start is always positive,
    // but defensive clamping guards against edge cases where a run-end lands
    // exactly at (or before) the slice offset.
    const int64_t logical_run_end = std::max<int64_t>(
        GetRunEndValue(::arrow::ree_util::RunEndsArray(array_span_), physical_index) -
            array_.offset(),
        0);
    // The physical range returned by FindPhysicalRange may include a trailing
    // run that extends beyond the logical slice. Clamp to array_.length() so
    // the result stays within the slice boundary.
    return std::min(logical_run_end, array_.length());
  }

  const RunEndEncodedArray& array_;
  std::shared_ptr<Array> values_;
  ArraySpan array_span_;
  std::pair<int64_t, int64_t> physical_range_;
  int64_t search_offset_ = 0;
  int64_t search_length_ = 0;
};

/// Access logical values from a run-end encoded Arrow array.
template <typename ArrowType>
class RunEndEncodedValuesAccessor : public RunEndEncodedValuesAccessorBase {
 public:
  using ArrayType = typename TypeTraits<ArrowType>::ArrayType;
  using ValueType = SearchValue<ArrowType>;

  /// Build a typed accessor over a run-end encoded payload, optionally restricted
  /// to a logical non-null subrange.
  explicit RunEndEncodedValuesAccessor(const RunEndEncodedArray& array,
                                       NonNullValuesRange logical_non_null_range = {})
      : RunEndEncodedValuesAccessorBase(array, logical_non_null_range),
        values_(array.values()->data()) {}

  /// Return the logical value at the given physical run position within the
  /// search window.
  ValueType Value(int64_t index) const {
    return GetViewType<ArrowType>::LogicalValue(values_.GetView(PhysicalIndex(index)));
  }

  ArrayType values_;
};

/// Access logical values from a chunked Arrow array without combining chunks.
template <typename ArrowType>
class ChunkedArrayAccessor : public SearchWindow {
 public:
  using ArrayType = typename TypeTraits<ArrowType>::ArrayType;
  using ValueType = SearchValue<ArrowType>;

  /// Build an accessor that resolves logical indices across chunk boundaries
  /// without concatenating the input, optionally restricted to a subrange.
  explicit ChunkedArrayAccessor(const ChunkedArray& chunked_array,
                                NonNullValuesRange non_null_range = {})
      : SearchWindow(non_null_range),
        chunked_array_(chunked_array),
        resolver_(chunked_array.chunks()) {
    chunks_.reserve(static_cast<size_t>(chunked_array.num_chunks()));
    for (const auto& chunk : chunked_array.chunks()) {
      DCHECK_NE(chunk->type_id(), Type::RUN_END_ENCODED);
      chunks_.emplace_back(chunk->data());
    }
  }

  /// Resolve a logical index within the search window to its chunk-local
  /// storage and return that value.
  ValueType Value(int64_t index) const {
    const auto location = resolver_.Resolve(physical_offset() + index);
    DCHECK_LT(location.chunk_index, chunked_array_.num_chunks());
    return GetViewType<ArrowType>::LogicalValue(
        chunks_[location.chunk_index].GetView(location.index_in_chunk));
  }

 private:
  const ChunkedArray& chunked_array_;
  ChunkResolver resolver_;
  std::vector<ArrayType> chunks_;
};

struct ChunkedRunEndEncodedMetadata {
  std::vector<int64_t> logical_offsets;
  std::vector<int64_t> run_offsets;
  int64_t total_run_count = 0;
};

inline int64_t GetPhysicalRunCount(const RunEndEncodedArray& array) {
  ArraySpan span(*array.data());
  return ::arrow::ree_util::FindPhysicalRange(span, array.offset(), array.length())
      .second;
}

inline ChunkedRunEndEncodedMetadata MakeChunkedRunEndEncodedMetadata(
    const ChunkedArray& chunked_array) {
  ChunkedRunEndEncodedMetadata metadata;
  metadata.logical_offsets.reserve(static_cast<size_t>(chunked_array.num_chunks()));
  metadata.run_offsets.reserve(static_cast<size_t>(chunked_array.num_chunks()) + 1);
  metadata.run_offsets.push_back(0);

  int64_t logical_offset = 0;
  for (const auto& chunk : chunked_array.chunks()) {
    if (chunk->length() == 0) {
      continue;
    }
    DCHECK_EQ(chunk->type_id(), Type::RUN_END_ENCODED);
    const auto& ree_chunk = checked_cast<const RunEndEncodedArray&>(*chunk);
    metadata.logical_offsets.push_back(logical_offset);
    logical_offset += chunk->length();
    metadata.total_run_count += GetPhysicalRunCount(ree_chunk);
    metadata.run_offsets.push_back(metadata.total_run_count);
  }

  DCHECK_EQ(logical_offset, chunked_array.length());
  return metadata;
}

class ChunkedRunEndEncodedValuesAccessorBase {
 public:
  ChunkedRunEndEncodedValuesAccessorBase(int64_t logical_length,
                                         ChunkedRunEndEncodedMetadata metadata)
      : logical_length_(logical_length),
        total_run_count_(metadata.total_run_count),
        run_resolver_(std::move(metadata.run_offsets)),
        logical_offsets_(std::move(metadata.logical_offsets)) {}

  int64_t length() const { return search_length_; }

  int64_t logical_length() const { return logical_length_; }

 protected:
  void FinalizeSearchWindow(const NonNullValuesRange& logical_non_null_range,
                            int64_t null_runs) {
    search_length_ = total_run_count_;
    if (!logical_non_null_range.is_identity(logical_length_)) {
      offset_ = logical_non_null_range.offset > 0 ? null_runs : 0;
      search_length_ = total_run_count_ - null_runs;
    }
  }

  int64_t SearchOffset() const { return offset_; }

  int64_t PhysicalRunIndex(int64_t index) const { return offset_ + index; }

  std::pair<int64_t, int64_t> ResolveRunIndex(int64_t index) const {
    DCHECK_LT(index, total_run_count_);
    const auto location = run_resolver_.Resolve(index);
    return {location.chunk_index, location.index_in_chunk};
  }

  int64_t total_run_count() const { return total_run_count_; }

  int64_t logical_offset(size_t chunk_index) const {
    return logical_offsets_[chunk_index];
  }

 private:
  int64_t logical_length_;
  int64_t total_run_count_ = 0;
  ChunkResolver run_resolver_;
  std::vector<int64_t> logical_offsets_;
  int64_t offset_ = 0;
  int64_t search_length_ = 0;
};

template <typename ArrowType>
class ChunkedRunEndEncodedValuesAccessor : public ChunkedRunEndEncodedValuesAccessorBase {
 public:
  using ValueType = SearchValue<ArrowType>;

  /// Flatten a chunked REE input into a logical sequence of physical runs while
  /// preserving enough offset information to map search results back to logical
  /// array positions, optionally restricted to a non-null subrange.
  explicit ChunkedRunEndEncodedValuesAccessor(
      const ChunkedArray& chunked_array, NonNullValuesRange logical_non_null_range = {})
      : ChunkedRunEndEncodedValuesAccessorBase(
            chunked_array.length(), MakeChunkedRunEndEncodedMetadata(chunked_array)) {
    const auto chunk_count = chunked_array.num_chunks();
    accessors_.reserve(static_cast<size_t>(chunk_count));
    for (const auto& chunk : chunked_array.chunks()) {
      if (chunk->length() != 0) {
        DCHECK_EQ(chunk->type_id(), Type::RUN_END_ENCODED);
        const auto& ree_chunk = checked_cast<const RunEndEncodedArray&>(*chunk);
        accessors_.emplace_back(
            ree_chunk, NonNullValuesRange{.offset = 0, .length = ree_chunk.length()});
      }
    }
    FinalizeSearchWindow(logical_non_null_range, NullCount());
  }

  /// Translate a binary-search position within the search window back to a
  /// logical insertion index relative to the window start.
  uint64_t LogicalInsertionIndex(int64_t index) const {
    DCHECK_GE(index, 0);
    DCHECK_LE(index, length());

    const auto phys_index = PhysicalRunIndex(index);
    return RawLogicalInsertionIndex(phys_index) -
           RawLogicalInsertionIndex(SearchOffset());
  }

  /// Resolve a global physical-run index within the search window to the
  /// owning chunk accessor.
  ValueType Value(int64_t index) const {
    const auto [chunk_index, local_index] = ResolveRunIndex(PhysicalRunIndex(index));
    return accessors_[chunk_index].Value(local_index);
  }

  /// Count null physical runs across chunks. Validation guarantees that any
  /// null runs are clustered entirely at one end of the logical values.
  int64_t NullCount() const {
    int64_t null_run_count = 0;
    for (const auto& accessor : accessors_) {
      null_run_count += accessor.NullCount();
    }
    return null_run_count;
  }

 private:
  uint64_t RawLogicalInsertionIndex(int64_t phys_index) const {
    DCHECK_GE(phys_index, 0);
    DCHECK_LE(phys_index, total_run_count());
    if (phys_index == 0) return 0;
    if (phys_index == total_run_count()) return logical_length();
    const auto [chunk_index, local_index] = ResolveRunIndex(phys_index);
    return static_cast<uint64_t>(logical_offset(chunk_index)) +
           accessors_[chunk_index].LogicalInsertionIndex(local_index);
  }

  std::vector<RunEndEncodedValuesAccessor<ArrowType>> accessors_;
};

/// Validate the supplied null counts and produce the logical non-null window
/// that will actually participate in binary search.
inline NonNullValuesRange MakeNonNullValuesRange(int64_t full_length, int64_t null_count,
                                                 int64_t leading_null_count,
                                                 int64_t trailing_null_count) {
  if (leading_null_count > 0) {
    return {.offset = leading_null_count, .length = full_length - leading_null_count};
  } else {
    return {.offset = 0, .length = full_length - trailing_null_count};
  }
}

/// Build the searchable non-null window once the side containing clustered
/// nulls is already known.
inline Result<NonNullValuesRange> MakeNonNullValuesRangeFromNullPlacement(
    int64_t full_length, int64_t null_count, bool has_leading_nulls) {
  return MakeNonNullValuesRange(full_length, null_count,
                                has_leading_nulls ? null_count : 0,
                                has_leading_nulls ? 0 : null_count);
}

/// Return whether a logical position in a chunked array is null.
inline bool IsNull(const ChunkedArray& values, int64_t index) {
  DCHECK_GE(index, 0);
  DCHECK_LT(index, values.length());

  ChunkResolver resolver(values.chunks());
  const auto location = resolver.Resolve(index);
  return values.chunk(static_cast<int>(location.chunk_index))
      ->IsNull(location.index_in_chunk);
}

/// Infer the non-null search window from total null count plus a predicate that
/// can test whether any logical position is null.
template <typename IsNullAt>
inline Result<NonNullValuesRange> FindNonNullValuesRangeFromNullCount(
    int64_t length, int64_t null_count, IsNullAt&& is_null_at) {
  DCHECK_GT(null_count, 0);
  DCHECK_LE(null_count, length);

  const bool has_leading_nulls = is_null_at(0);
  return MakeNonNullValuesRangeFromNullPlacement(length, null_count, has_leading_nulls);
}

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
    return datum.chunked_array()->ComputeLogicalNullCount() > 0;
  }

  return datum.array()->ComputeLogicalNullCount() > 0;
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
inline Result<NonNullValuesRange> FindNonNullValuesRange(const ArrayData& values) {
  NonNullValuesRange non_null_values_range{.offset = 0, .length = values.length};

  const auto null_count = values.ComputeLogicalNullCount();
  if (null_count == 0) {
    return non_null_values_range;
  }

  if (values.type->id() == Type::RUN_END_ENCODED) {
    const ArraySpan values_span(values);
    return FindNonNullValuesRangeFromNullCount(
        values.length, null_count, [&](int64_t index) {
          const auto physical_index =
              ree_util::FindPhysicalIndex(values_span, index, values_span.offset);
          return ree_util::ValuesArray(values_span).IsNull(physical_index);
        });
  }

  return FindNonNullValuesRangeFromNullCount(
      values.length, null_count, [&](int64_t index) { return values.IsNull(index); });
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
/// Needles can be a scalar, array, or chunked array. Array-like needles must not have
/// nested run-end encoding since that is not currently supported.
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

/// Compute the non-null search window for chunked values.
inline Result<NonNullValuesRange> FindNonNullValuesRange(const ChunkedArray& values) {
  NonNullValuesRange non_null_values_range{.offset = 0, .length = values.length()};

  const auto null_count = values.ComputeLogicalNullCount();
  if (null_count == 0) {
    return non_null_values_range;
  }

  return FindNonNullValuesRangeFromNullCount(
      values.length(), null_count, [&](int64_t index) { return IsNull(values, index); });
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

/// Dispatch lower-bound or upper-bound semantics at runtime.
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
  ::arrow::Unreachable("Invalid SearchSortedOptions::Side value");
  return 0;
}

/// Convert the physical search result into the final logical insertion index,
/// including any offset introduced by stripping clustered nulls.
template <typename ArrowType, typename Accessor>
uint64_t FindLogicalInsertionIndex(const Accessor& sorted_values,
                                   const SearchValue<ArrowType>& needle,
                                   SearchSortedOptions::Side side,
                                   uint64_t insertion_offset) {
  const auto search_index =
      static_cast<int64_t>(FindInsertionPoint<ArrowType>(sorted_values, needle, side));
  return sorted_values.LogicalInsertionIndex(search_index) + insertion_offset;
}

template <typename ArrowType>
using VisitedNeedle = std::optional<SearchValue<ArrowType>>;

/// Read one logical needle value from a physical array position.
template <typename ArrowType, typename ArrayType>
VisitedNeedle<ArrowType> ReadVisitedNeedle(const ArrayType& array,
                                           int64_t physical_index) {
  if (array.IsNull(physical_index)) {
    return std::nullopt;
  }
  const auto needle = GetViewType<ArrowType>::LogicalValue(array.GetView(physical_index));
  return std::optional<SearchValue<ArrowType>>(needle);
}

/// Visit each plain-array needle as single-element logical runs.
template <typename ArrowType, typename Visitor>
Status VisitArrayNeedleRuns(const std::shared_ptr<ArrayData>& needles_data,
                            Visitor&& visitor) {
  using ArrayType = typename TypeTraits<ArrowType>::ArrayType;

  auto physical_type = TypeTraits<ArrowType>::type_singleton();
  auto physical_data = ToPhysicalData(needles_data, physical_type);
  ArrayType array(physical_data);
  for (int64_t index = 0; index < array.length(); ++index) {
    RETURN_NOT_OK(visitor(ReadVisitedNeedle<ArrowType>(array, index)));
  }
  return Status::OK();
}

/// Visit scalar or plain-array needles through a uniform callback interface
/// of logical elements.
template <typename ArrowType, typename Visitor>
Status VisitNeedleRuns(const Datum& needles, Visitor&& visitor) {
  if (needles.is_scalar()) {
    if (!needles.scalar()->is_valid) {
      return visitor(std::optional<SearchValue<ArrowType>>{});
    }
    ARROW_ASSIGN_OR_RAISE(auto scalar_array, MakeArrayFromScalar(*needles.scalar(), 1));
    return VisitArrayNeedleRuns<ArrowType>(scalar_array->data(),
                                           std::forward<Visitor>(visitor));
  }

  const auto& needle_data = needles.array();
  return VisitArrayNeedleRuns<ArrowType>(needle_data, visitor);
}

/// Build uint64 insertion-index arrays with an optional null bitmap.
class InsertionIndexBuilder {
 public:
  explicit InsertionIndexBuilder(MemoryPool* pool, bool nullable)
      : indices_builder_(pool), null_bitmap_builder_(pool), nullable_(nullable) {}

  /// Reserve the final output size up front so append operations can use the
  /// builders' unchecked fast path.
  Status Init(int64_t length) {
    expected_length_ = length;
    RETURN_NOT_OK(indices_builder_.Reserve(length));
    if (nullable_) {
      RETURN_NOT_OK(null_bitmap_builder_.Reserve(length));
    }
    return Status::OK();
  }

  /// Append a null output slot for a null needle.
  Status AppendNull() {
    DCHECK_LE(length_ + 1, expected_length_);
    DCHECK(nullable_);
    indices_builder_.UnsafeAppend(uint64_t{0});
    null_bitmap_builder_.UnsafeAppend(false);
    ++null_count_;
    ++length_;
    return Status::OK();
  }

  /// Append one computed insertion index for a non-null needle.
  Status AppendValue(uint64_t insertion_index) {
    DCHECK_LE(length_ + 1, expected_length_);
    indices_builder_.UnsafeAppend(insertion_index);
    if (nullable_) {
      null_bitmap_builder_.UnsafeAppend(true);
    }
    ++length_;
    return Status::OK();
  }

  /// Finish building the output UInt64 array, attaching the null bitmap only
  /// when nullable output was requested.
  Result<std::shared_ptr<Array>> Finish() && {
    DCHECK_EQ(length_, expected_length_);
    ARROW_ASSIGN_OR_RAISE(auto indices, indices_builder_.Finish());

    std::shared_ptr<Buffer> null_bitmap;
    if (nullable_) {
      ARROW_ASSIGN_OR_RAISE(null_bitmap, null_bitmap_builder_.Finish());
    }

    return MakeArray(ArrayData::Make(
        uint64(), length_, {std::move(null_bitmap), std::move(indices)}, null_count_));
  }

 private:
  TypedBufferBuilder<uint64_t> indices_builder_;
  TypedBufferBuilder<bool> null_bitmap_builder_;
  bool nullable_;
  int64_t expected_length_ = 0;
  int64_t length_ = 0;
  int64_t null_count_ = 0;
};

/// Visit normalized needle runs and emit insertion indices through an output
/// policy object.
template <typename ArrowType, typename ValuesAccessor, typename Output>
Status EmitInsertionIndices(const ValuesAccessor& sorted_values, const Datum& needles,
                            SearchSortedOptions::Side side, uint64_t insertion_offset,
                            Output* output) {
  auto emit_search_result = [&](const VisitedNeedle<ArrowType>& needle) -> Status {
    if (!needle.has_value()) {
      return output->AppendNull();
    }
    const auto insertion_index = FindLogicalInsertionIndex<ArrowType>(
        sorted_values, *needle, side, insertion_offset);
    return output->AppendValue(insertion_index);
  };

  return VisitNeedleRuns<ArrowType>(needles, emit_search_result);
}

inline Result<Datum> ComputeRunEndEncodedNeedleInsertionIndices(
    const Datum& values, const RunEndEncodedArray& needles,
    SearchSortedOptions::Side side, ExecContext* ctx) {
  ExecContext* exec_ctx = ctx != NULLPTR ? ctx : default_exec_context();

  // Search each physical REE value once, then rebuild the run-end encoded shape
  // and decode back to the dense logical result expected by the public API.
  ARROW_ASSIGN_OR_RAISE(auto physical_results,
                        SearchSorted(values, Datum(needles.LogicalValues()),
                                     SearchSortedOptions(side), exec_ctx));

  ARROW_ASSIGN_OR_RAISE(auto logical_run_ends,
                        needles.LogicalRunEnds(exec_ctx->memory_pool()));
  ARROW_ASSIGN_OR_RAISE(auto ree_result,
                        RunEndEncodedArray::Make(needles.length(), logical_run_ends,
                                                 physical_results.make_array()));
  return RunEndDecode(Datum(ree_result), exec_ctx);
}

inline ChunkedArray MakePhysicalRunEndEncodedChunkedArray(
    const ChunkedArray& values, const std::shared_ptr<DataType>& physical_type) {
  ArrayVector physical_chunks;
  physical_chunks.reserve(static_cast<size_t>(values.num_chunks()));
  for (const auto& chunk : values.chunks()) {
    physical_chunks.push_back(MakeArray(ToPhysicalData(chunk->data(), physical_type)));
  }
  return ChunkedArray(std::move(physical_chunks));
}

inline ChunkedArray MakePhysicalChunkedArray(
    const ChunkedArray& values, const std::shared_ptr<DataType>& physical_type) {
  auto physical_chunks = GetPhysicalChunks(values, physical_type);
  return ChunkedArray(std::move(physical_chunks), physical_type);
}

/// Materialize output for plain array needles.
template <typename ArrowType, typename ValuesAccessor>
Result<Datum> ComputeInsertionIndices(const ValuesAccessor& sorted_values,
                                      const Datum& needles,
                                      SearchSortedOptions::Side side,
                                      uint64_t insertion_offset, ExecContext* ctx) {
  auto has_nulls = DatumHasNulls(needles);
  InsertionIndexBuilder output(ctx->memory_pool(), has_nulls);
  ARROW_RETURN_NOT_OK(output.Init(needles.length()));
  ARROW_RETURN_NOT_OK((EmitInsertionIndices<ArrowType>(sorted_values, needles, side,
                                                       insertion_offset, &output)));
  ARROW_ASSIGN_OR_RAISE(auto out, std::move(output).Finish());
  return Datum(std::move(out));
}

/// Normalize a single ArrayData values input to the correct accessor type and
/// invoke the supplied visitor with that accessor.
template <typename ArrowType, typename Visitor>
Result<Datum> VisitValuesAccessor(const std::shared_ptr<ArrayData>& values_data,
                                  const NonNullValuesRange& non_null_values_range,
                                  Visitor&& visitor) {
  auto physical_type = TypeTraits<ArrowType>::type_singleton();
  auto physical_data = ToPhysicalData(values_data, physical_type);

  if (physical_data->type->id() == Type::RUN_END_ENCODED) {
    RunEndEncodedArray ree(physical_data);
    RunEndEncodedValuesAccessor<ArrowType> values_accessor(ree, non_null_values_range);
    return visitor(values_accessor);
  }

  PlainArrayAccessor<ArrowType> values_accessor(physical_data, non_null_values_range);
  return visitor(values_accessor);
}

/// Normalize a chunked values input to the correct accessor type and invoke
/// the supplied visitor with that accessor.
template <typename ArrowType, typename Visitor>
Result<Datum> VisitValuesAccessor(const ChunkedArray& values,
                                  const NonNullValuesRange& non_null_values_range,
                                  Visitor&& visitor) {
  auto physical_type = TypeTraits<ArrowType>::type_singleton();

  if (values.type()->id() == Type::RUN_END_ENCODED) {
    auto physical_chunked = MakePhysicalRunEndEncodedChunkedArray(values, physical_type);
    ChunkedRunEndEncodedValuesAccessor<ArrowType> values_accessor(physical_chunked,
                                                                  non_null_values_range);
    return visitor(values_accessor);
  }

  auto physical_chunked = MakePhysicalChunkedArray(values, physical_type);
  ChunkedArrayAccessor<ArrowType> values_accessor(physical_chunked,
                                                  non_null_values_range);
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

    // Chunked needles are handled at the top level so the typed dispatch
    // below only ever sees non-chunked (scalar /array) needles.
    if (args[1].is_chunked_array()) {
      return ExecuteChunkedNeedles(args[0], *args[1].chunked_array(),
                                   static_cast<const SearchSortedOptions&>(*options),
                                   ctx);
    }

    ARROW_ASSIGN_OR_RAISE(auto non_null_values_range, FindNonNullValuesRange(args[0]));
    auto result = DispatchByType(args[0], non_null_values_range, args[1],
                                 static_cast<const SearchSortedOptions&>(*options), ctx);
    return result;
  }

 private:
  /// Process each needle chunk independently and concatenate the results.
  Result<Datum> ExecuteChunkedNeedles(const Datum& values, const ChunkedArray& needles,
                                      const SearchSortedOptions& options,
                                      ExecContext* ctx) const {
    ArrayVector result_chunks;
    result_chunks.reserve(static_cast<size_t>(needles.num_chunks()));
    for (const auto& chunk : needles.chunks()) {
      ARROW_ASSIGN_OR_RAISE(auto chunk_result,
                            ExecuteImpl({values, Datum(chunk)}, &options, ctx));
      result_chunks.push_back(chunk_result.make_array());
    }
    ARROW_ASSIGN_OR_RAISE(auto out, Concatenate(result_chunks, ctx->memory_pool()));
    return Datum(std::move(out));
  }

  /// Compute the non-null search window on the logical view of the values
  /// input, regardless of its physical storage.
  [[nodiscard]] Result<NonNullValuesRange> FindNonNullValuesRange(
      const Datum& values) const {
    if (values.is_chunked_array()) {
      return ::arrow::compute::internal::FindNonNullValuesRange(*values.chunked_array());
    }
    return ::arrow::compute::internal::FindNonNullValuesRange(*values.array());
  }

  /// Dispatch the logical value type to the matching template specialization.
  /// Resolves logical types to physical types via GetPhysicalType() so that
  /// types sharing the same physical layout (e.g. Date32/Int32, String/Binary)
  /// share a single code path, reducing template instantiations.
  Result<Datum> DispatchByType(const Datum& values,
                               const NonNullValuesRange& non_null_values_range,
                               const Datum& needles, const SearchSortedOptions& options,
                               ExecContext* ctx) const {
    // Resolve to logical type first (stripping REE wrapper if present).
    auto logical_type_ptr = values.type();
    if (logical_type_ptr->id() == Type::RUN_END_ENCODED) {
      logical_type_ptr =
          checked_cast<const RunEndEncodedType&>(*logical_type_ptr).value_type();
    }

    auto physical_type = GetPhysicalType(logical_type_ptr);
    switch (physical_type->id()) {
#define VISIT(TYPE)                                                                     \
  case TYPE::type_id:                                                                   \
    return DispatchHaystack<TYPE>(values, non_null_values_range, needles, options.side, \
                                  ctx);
      VISIT_SEARCH_SORTED_PHYSICAL_TYPES(VISIT)
#undef VISIT
      default:
        break;
    }
    return Status::NotImplemented("search_sorted is not implemented for type ",
                                  logical_type_ptr->ToString());
  }

  /// Dispatch the physical representation of the searched values.
  template <typename ArrowType>
  Result<Datum> DispatchHaystack(const Datum& values,
                                 const NonNullValuesRange& non_null_values_range,
                                 const Datum& needles, SearchSortedOptions::Side side,
                                 ExecContext* ctx) const {
    if (needles.is_scalar()) {
      auto scalar = needles.scalar();
      if (!scalar->is_valid) {
        return Datum(std::make_shared<UInt64Scalar>());
      }

      ARROW_ASSIGN_OR_RAISE(auto scalar_arr, MakeArrayFromScalar(*scalar, 1));
      ARROW_ASSIGN_OR_RAISE(auto result,
                            DispatchHaystack<ArrowType>(values, non_null_values_range,
                                                        Datum(scalar_arr), side, ctx));
      ARROW_ASSIGN_OR_RAISE(auto result_scalar, result.make_array()->GetScalar(0));
      return Datum(std::move(result_scalar));
    }

    if (needles.type()->id() == Type::RUN_END_ENCODED) {
      return ComputeRunEndEncodedNeedleInsertionIndices(
          values, RunEndEncodedArray(needles.array()), side, ctx);
    }

    if (values.is_chunked_array()) {
      return VisitValuesAccessor<ArrowType>(
          *values.chunked_array(), non_null_values_range,
          [&](const auto& values_accessor) {
            return ComputeInsertionIndices<ArrowType>(
                values_accessor, needles, side,
                static_cast<uint64_t>(non_null_values_range.offset), ctx);
          });
    }

    return VisitValuesAccessor<ArrowType>(
        values.array(), non_null_values_range, [&](const auto& values_accessor) {
          return ComputeInsertionIndices<ArrowType>(
              values_accessor, needles, side,
              static_cast<uint64_t>(non_null_values_range.offset), ctx);
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
