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
#include "arrow/util/float16.h"
#include "arrow/util/logging_internal.h"
#include "arrow/util/ree_util.h"
#include "arrow/util/unreachable.h"

namespace arrow {

using internal::checked_cast;
using util::Float16;

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
  VISIT(HalfFloatType)                            \
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

// The three first members are ordered by "nullness"
enum class NullGeometry : int { NoNulls, AllNans, AllNulls, Empty, AtStart, AtEnd };

inline bool IsNanPrimitive(const Array& array, int64_t index) {
  switch (array.type_id()) {
    case Type::FLOAT:
      return std::isnan(checked_cast<const FloatArray&>(array).Value(index));
    case Type::DOUBLE:
      return std::isnan(checked_cast<const DoubleArray&>(array).Value(index));
    case Type::HALF_FLOAT:
      return Float16::FromBits(checked_cast<const HalfFloatArray&>(array).Value(index))
          .is_nan();
    default:
      return false;
  }
}

/// Detect the NullGeometry of a primitive or run-end-encoded array
NullGeometry DetectNullGeometry(const Array& array) {
  if (array.length() == 0) {
    return NullGeometry::Empty;
  }
  if (array.type_id() == Type::RUN_END_ENCODED) {
    const auto& ree_array = checked_cast<const RunEndEncodedArray&>(array);
    auto range = ::arrow::ree_util::FindPhysicalRange(*array.data(), array.offset(),
                                                      array.length());
    return DetectNullGeometry(*ree_array.values()->Slice(range.first, range.second));
  }
  bool null_at_start = array.IsNull(0);
  bool null_at_end = array.IsNull(array.length() - 1);
  if (null_at_start && !null_at_end) {
    return NullGeometry::AtStart;
  }
  if (!null_at_start && null_at_end) {
    return NullGeometry::AtEnd;
  }
  if (null_at_start && null_at_end) {
    return NullGeometry::AllNulls;
  }

  // No nulls, look at NaNs
  bool nan_at_start = IsNanPrimitive(array, 0);
  bool nan_at_end = IsNanPrimitive(array, array.length() - 1);
  if (nan_at_start && !nan_at_end) {
    return NullGeometry::AtStart;
  }
  if (!nan_at_start && nan_at_end) {
    return NullGeometry::AtEnd;
  }
  if (nan_at_start && nan_at_end) {
    return NullGeometry::AllNans;
  }
  return NullGeometry::NoNulls;
}

/// Detect the NullGeometry of a chunked array
NullGeometry DetectNullGeometry(const ChunkedArray& chunked_array) {
  auto previous_geometry = NullGeometry::Empty;

  for (const auto& chunk : chunked_array.chunks()) {
    auto null_geometry = DetectNullGeometry(*chunk);
    if (null_geometry == NullGeometry::Empty) {
      continue;
    }
    if (null_geometry == NullGeometry::AtStart || null_geometry == NullGeometry::AtEnd) {
      // We can conclude from this chunk alone
      return null_geometry;
    }
    // If the previous chunk had different results, we can compare and decide
    if (previous_geometry != NullGeometry::Empty && previous_geometry != null_geometry) {
      return previous_geometry < null_geometry ? NullGeometry::AtEnd
                                               : NullGeometry::AtStart;
    }
    previous_geometry = null_geometry;
  }
  return previous_geometry;
}

/// Validate the supplied null counts and produce the logical non-null window
/// that will actually participate in binary search.
NonNullValuesRange MakeNonNullValuesRange(int64_t full_length, int64_t null_count,
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
NonNullValuesRange MakeNonNullValuesRangeFromNullPlacement(int64_t full_length,
                                                           int64_t null_count,
                                                           NullPlacement null_placement) {
  return MakeNonNullValuesRange(
      full_length, null_count, null_placement == NullPlacement::AtStart ? null_count : 0,
      null_placement == NullPlacement::AtStart ? 0 : null_count);
}

// Convert ArrayData to its physical representation so that typed accessors
// can be constructed with a physical ArrowType (e.g. Date32 → Int32).
// For REE arrays, only the values child type is converted; the REE wrapper
// type stays unchanged.
std::shared_ptr<ArrayData> ToPhysicalData(
    const std::shared_ptr<ArrayData>& data,
    const std::shared_ptr<DataType>& physical_type) {
  if (data->type->id() == Type::RUN_END_ENCODED) {
    const auto& ree_type = checked_cast<const RunEndEncodedType&>(*data->type);
    auto result = data->Copy();
    auto values_copy = result->child_data[1]->Copy();
    values_copy->type = physical_type;
    result->type = run_end_encoded(ree_type.run_end_type(), physical_type);
    result->child_data[1] = std::move(values_copy);
    return result;
  }
  auto result = data->Copy();
  result->type = physical_type;
  return result;
}

/// Read a run-end value from any supported run-end integer representation.
int64_t GetRunEndValue(const ArraySpan& run_ends, int64_t physical_index) {
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

class SearchWindow {
 public:
  explicit SearchWindow(NonNullValuesRange non_null_range)
      : offset_(non_null_range.offset), length_(non_null_range.length) {}

  int64_t length() const { return length_; }

  int64_t LogicalInsertionIndex(int64_t index) const { return index + physical_offset(); }

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

  PlainArrayAccessor(const std::shared_ptr<ArrayData>& array_data,
                     NullPlacement null_placement)
      : PlainArrayAccessor(array_data, MakeNonNullValuesRangeFromNullPlacement(
                                           array_data->length, array_data->GetNullCount(),
                                           null_placement)) {}

  PlainArrayAccessor(const std::shared_ptr<ArrayData>& array_data,
                     NonNullValuesRange non_null_range)
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
  explicit RunEndEncodedValuesAccessorBase(RunEndEncodedArray array)
      : array_(std::move(array)),
        array_span_(*array_.data()),
        run_ends_span_(::arrow::ree_util::RunEndsArray(array_span_)),
        physical_range_(::arrow::ree_util::FindPhysicalRange(array_span_, array_.offset(),
                                                             array_.length())) {
    values_ = array_.values()->Slice(physical_range_.first, physical_range_.second);
  }

 protected:
  int64_t PhysicalIndex(int64_t index) const {
    return physical_range_.first + /*search_offset_ + */ index;
  }

  RunEndEncodedArray array_;
  std::shared_ptr<Array> values_;
  ArraySpan array_span_;
  ArraySpan run_ends_span_;
  std::pair<int64_t, int64_t> physical_range_;
};

/// Access logical values from a run-end encoded Arrow array.
template <typename ArrowType>
class RunEndEncodedValuesAccessor : public RunEndEncodedValuesAccessorBase {
 public:
  using ArrayType = typename TypeTraits<ArrowType>::ArrayType;
  using ValueType = SearchValue<ArrowType>;

  RunEndEncodedValuesAccessor(RunEndEncodedArray array, NullPlacement null_placement)
      : RunEndEncodedValuesAccessorBase(std::move(array)),
        physical_accessor_(values_->data(), null_placement) {}

  RunEndEncodedValuesAccessor(const std::shared_ptr<ArrayData>& array_data,
                              NullPlacement null_placement)
      : RunEndEncodedValuesAccessor(RunEndEncodedArray(array_data), null_placement) {}

  /// Return the logical value at the given physical run position within the
  /// search window.
  ValueType Value(int64_t index) const { return physical_accessor_.Value(index); }

  int64_t length() const { return physical_accessor_.length(); }

  int64_t LogicalInsertionIndex(int64_t index) const {
    auto physical_index = physical_accessor_.LogicalInsertionIndex(index);

    DCHECK_GE(physical_index, 0);
    DCHECK_LE(physical_index, physical_range_.second);
    if (physical_index == 0) {
      return 0;
    } else if (physical_index == physical_range_.second) {
      return array_.length();
    } else {
      auto run_end =
          GetRunEndValue(run_ends_span_, physical_index + physical_range_.first - 1);
      DCHECK_GE(run_end, array_.offset());
      DCHECK_LE(run_end, array_.offset() + array_.length());
      return run_end - array_.offset();
    }
  }

 protected:
  PlainArrayAccessor<ArrowType> physical_accessor_;
};

/// Return the logical type of a datum, unwrapping run-end encoding when present.
const DataType& LogicalType(const Datum& datum) {
  const auto& type = *datum.type();
  if (type.id() == Type::RUN_END_ENCODED) {
    return *checked_cast<const RunEndEncodedType&>(type).value_type();
  }
  return type;
}

/// Reject nested run-end encoded values. TODO: Support this case in the future if there
/// is demand for it.
Status ValidateRunEndEncodedLogicalValueType(const DataType& type, const char* name) {
  const auto& ree_type = checked_cast<const RunEndEncodedType&>(type);
  if (ree_type.value_type()->id() == Type::RUN_END_ENCODED) {
    return Status::TypeError("Nested run-end encoded ", name, " are not supported");
  }
  return Status::OK();
}

/// Validate the searched values input shape and supported encoding.
Status ValidateSortedValuesInput(const Datum& datum) {
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
Status ValidateNeedleInput(const Datum& datum) {
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

/// Find the insertion point into a dense array
template <typename ArrowType, typename Accessor>
int64_t FindInsertionDense(const Accessor& array, const SearchValue<ArrowType>& needle,
                           SearchSortedOptions::Side side, NullPlacement null_placement) {
  // When looking for the Left side, we want equal values to be considered greater
  // than the needle (1), otherwise smaller (-1).
  const int on_equality = (side == SearchSortedOptions::Left) ? 1 : -1;
  int64_t first = 0;
  int64_t count = array.length();

  auto compare = [&](auto left, auto right) {
    // The same comparison function as used for sorting, taking account null_placement
    // when NaNs are involved.
    // XXX Instead of detecting NaN-ness during each comparison, we could
    // take advantage of null_placement to single out the range of NaNs that's at
    // the beginning or end of the array.
    return CompareTypeValues<ArrowType>(left, right, SortOrder::Ascending, null_placement,
                                        /*on_equality=*/on_equality);
  };

  while (count > 0) {
    const int64_t step = count / 2;
    const int64_t it = first + step;
    const bool advance = compare(array.Value(it), needle) < 0;
    if (advance) {
      first = it + 1;
      count -= step + 1;
    } else {
      count = step;
    }
  }
  return first;
}

/// Find the insertion chunk in an array of vector chunks. The chunk index is returned.
template <typename ArrowType, typename Accessor>
int64_t FindInsertionChunk(const std::vector<Accessor>& chunks,
                           const SearchValue<ArrowType>& needle,
                           SearchSortedOptions::Side side, NullPlacement null_placement) {
  // When looking for the Left side, we want equal values to be considered greater
  // than the needle (1), otherwise smaller (-1).
  const int on_equality = (side == SearchSortedOptions::Left) ? 1 : -1;
  int64_t first = 0;
  int64_t count = static_cast<int64_t>(chunks.size());

  auto compare = [&](auto left, auto right) {
    // The same comparison function as used for sorting, taking account null_placement
    // when NaNs are involved.
    // XXX Instead of detecting NaN-ness during each comparison, we could
    // take advantage of null_placement to single out the range of NaNs that's at
    // the beginning or end of the sorted_values.
    return CompareTypeValues<ArrowType>(left, right, SortOrder::Ascending, null_placement,
                                        on_equality);
  };

  while (count > 0) {
    const int64_t step = count / 2;
    const int64_t it = first + step;
    bool advance;
    const auto& chunk = chunks[it];
    if (chunk.length() == 0) {
      // If nulls are clustered at the start, advance towards the end.
      advance = (null_placement == NullPlacement::AtStart);
    } else {
      auto chunk_first = chunk.Value(0);
      auto chunk_last = chunk.Value(chunk.length() - 1);
      if (compare(chunk_first, needle) > 0) {
        // First chunk value too large => go left
        advance = false;
      } else if (compare(chunk_last, needle) < 0) {
        // Last chunk value too small => go right
        advance = true;
      } else {
        // Insertion point is in this chunk
        first = it;
        break;
      }
    }
    if (advance) {
      first = it + 1;
      count -= step + 1;
    } else {
      count = step;
    }
  }
  return first;
}

/// Find the insertion point into a chunked array.
template <typename ArrowType, typename Accessor>
ChunkLocation FindInsertionChunked(const std::vector<Accessor>& chunks,
                                   const SearchValue<ArrowType>& needle,
                                   SearchSortedOptions::Side side,
                                   NullPlacement null_placement) {
  // A naive implementation would search directly in the chunked array,
  // with each indexed access taking O(log n) time.
  // It is much faster to first narrow down the search to a single chunk
  // (by using a binary search among chunk boundaries, see FindInsertionChunk)
  // and then do a dense binary search (FindInsertionDense).
  DCHECK_GT(chunks.size(), 0);
  int64_t chunk_index =
      FindInsertionChunk<ArrowType>(chunks, needle, side, null_placement);
  int64_t index_in_chunk;
  if (chunk_index == static_cast<int64_t>(chunks.size())) {
    // Inserting at the right of the last chunk
    --chunk_index;
    index_in_chunk = chunks.back().length();
  } else {
    index_in_chunk =
        FindInsertionDense<ArrowType>(chunks[chunk_index], needle, side, null_placement);
  }
  return {chunk_index, chunks[chunk_index].LogicalInsertionIndex(index_in_chunk)};
}

template <typename ArrowType, typename Accessor>
class ChunkedSearchSorted {
 public:
  ChunkedSearchSorted(const ArrayVector& chunks, SearchSortedOptions::Side side,
                      NullPlacement null_placement)
      : side_(side), null_placement_(null_placement) {
    // Initialize accessors from non-empty chunks
    chunk_accessors_.reserve(chunks.size());
    chunk_offsets_.reserve(chunks.size());
    int64_t offset = 0;
    for (const auto& chunk : chunks) {
      if (chunk->length() > 0) {
        auto accessor = Accessor(chunk->data(), null_placement);
        chunk_accessors_.push_back(std::move(accessor));
        chunk_offsets_.push_back(offset);
        offset += chunk->length();
      }
    }
  }

  int64_t FindLogicalInsertionIndex(const SearchValue<ArrowType>& needle) const {
    if (chunk_accessors_.empty()) {
      return 0;
    }
    ChunkLocation location =
        FindInsertionChunked<ArrowType>(chunk_accessors_, needle, side_, null_placement_);
    DCHECK_LT(location.chunk_index, static_cast<int64_t>(chunk_offsets_.size()));
    return chunk_offsets_[location.chunk_index] + location.index_in_chunk;
  }

 protected:
  SearchSortedOptions::Side side_;
  NullPlacement null_placement_;
  std::vector<Accessor> chunk_accessors_;
  std::vector<int64_t> chunk_offsets_;
};

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
    DCHECK(nullable_);
    indices_builder_.UnsafeAppend(uint64_t{0});
    null_bitmap_builder_.UnsafeAppend(false);
    ++null_count_;
    return Status::OK();
  }

  /// Append one computed insertion index for a non-null needle.
  Status AppendValue(uint64_t insertion_index) {
    indices_builder_.UnsafeAppend(insertion_index);
    if (nullable_) {
      null_bitmap_builder_.UnsafeAppend(true);
    }
    return Status::OK();
  }

  /// Finish building the output UInt64 array, attaching the null bitmap only
  /// when nullable output was requested.
  Result<std::shared_ptr<Array>> Finish() && {
    DCHECK_EQ(indices_builder_.length(), expected_length_);
    ARROW_ASSIGN_OR_RAISE(auto indices, indices_builder_.Finish());

    std::shared_ptr<Buffer> null_bitmap;
    if (nullable_) {
      DCHECK_EQ(null_bitmap_builder_.length(), expected_length_);
      ARROW_ASSIGN_OR_RAISE(null_bitmap, null_bitmap_builder_.Finish());
    }

    return MakeArray(ArrayData::Make(uint64(), expected_length_,
                                     {std::move(null_bitmap), std::move(indices)},
                                     null_count_));
  }

 private:
  TypedBufferBuilder<uint64_t> indices_builder_;
  TypedBufferBuilder<bool> null_bitmap_builder_;
  bool nullable_;
  int64_t expected_length_ = 0;
  int64_t null_count_ = 0;
};

Result<Datum> ComputeRunEndEncodedNeedleInsertionIndices(
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

template <typename ArrowType, typename ValuesAccessor>
Result<Datum> ComputeInsertionIndicesWithAccessor(
    const Datum& sorted_values, const Datum& needles, SearchSortedOptions::Side side,
    NullPlacement null_placement, uint64_t insertion_offset, ExecContext* ctx) {
  // Only emit a null bitmap if necessary
  const bool has_nulls = needles.ComputeLogicalNullCount() > 0;
  InsertionIndexBuilder output(ctx->memory_pool(), has_nulls);
  ARROW_RETURN_NOT_OK(output.Init(needles.length()));

  // Array and ChunkedArray follow the same path, an Array having just a single chunk.
  // The trivial case with one chunk does not add overhead, so it's not worth
  // the maintenance hassle to have a separate path for Array.
  ChunkedSearchSorted<ArrowType, ValuesAccessor> search_sorted(sorted_values.chunks(),
                                                               side, null_placement);

  auto emit_search_result = [&](const VisitedNeedle<ArrowType>& needle) -> Status {
    if (!needle.has_value()) {
      return output.AppendNull();
    }
    const auto insertion_index = search_sorted.FindLogicalInsertionIndex(*needle);
    return output.AppendValue(static_cast<uint64_t>(insertion_index));
  };

  RETURN_NOT_OK(VisitNeedleRuns<ArrowType>(needles, emit_search_result));

  return std::move(output).Finish().As<Datum>();
}

template <typename ArrowType>
Result<Datum> ComputeInsertionIndices(const Datum& sorted_values, const Datum& needles,
                                      SearchSortedOptions::Side side,
                                      NullPlacement null_placement,
                                      uint64_t insertion_offset, ExecContext* ctx) {
  auto physical_type = TypeTraits<ArrowType>::type_singleton();
  DCHECK_NE(physical_type->id(), Type::RUN_END_ENCODED);

  if (sorted_values.type()->id() == Type::RUN_END_ENCODED) {
    return ComputeInsertionIndicesWithAccessor<ArrowType,
                                               RunEndEncodedValuesAccessor<ArrowType>>(
        sorted_values, needles, side, null_placement, insertion_offset, ctx);
  } else {
    return ComputeInsertionIndicesWithAccessor<ArrowType, PlainArrayAccessor<ArrowType>>(
        sorted_values, needles, side, null_placement, insertion_offset, ctx);
  }
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

  /// Validate inputs, normalize options, and dispatch to the typed search
  /// implementation.
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

    auto null_placement = DetectNullPlacement(args[0]);
    ARROW_ASSIGN_OR_RAISE(auto non_null_values_range,
                          FindNonNullValuesRange(args[0], null_placement));
    auto result = DispatchByType(args[0], non_null_values_range, args[1],
                                 static_cast<const SearchSortedOptions&>(*options),
                                 null_placement, ctx);
    return result;
  }

 private:
  /// Process each needle chunk independently and concatenate the results.
  Result<Datum> ExecuteChunkedNeedles(const Datum& values, const ChunkedArray& needles,
                                      const SearchSortedOptions& options,
                                      ExecContext* ctx) const {
    if (needles.num_chunks() == 0) {
      return MakeEmptyArray(uint64(), ctx->memory_pool()).As<Datum>();
    }
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
  Result<NonNullValuesRange> FindNonNullValuesRange(const Datum& values,
                                                    NullPlacement null_placement) const {
    const int64_t null_count = values.ComputeLogicalNullCount();
    return MakeNonNullValuesRangeFromNullPlacement(values.length(), null_count,
                                                   null_placement);
  }

  NullPlacement DetectNullPlacement(const Datum& values) const {
    const auto null_geometry = values.is_chunked_array()
                                   ? DetectNullGeometry(*values.chunked_array())
                                   : DetectNullGeometry(*values.make_array());
    switch (null_geometry) {
      case NullGeometry::AtStart:
        return NullPlacement::AtStart;
      case NullGeometry::AtEnd:
        return NullPlacement::AtEnd;
      default:
        // Shouldn't matter as there are either no nulls or only nulls
        DCHECK(values.null_count() == 0 || values.null_count() == values.length());
        return NullPlacement::AtEnd;
    }
  }

  /// Dispatch the logical value type to the matching template specialization.
  /// Resolves logical types to physical types via GetPhysicalType() so that
  /// types sharing the same physical layout (e.g. Date32/Int32, String/Binary)
  /// share a single code path, reducing template instantiations.
  Result<Datum> DispatchByType(const Datum& values,
                               const NonNullValuesRange& non_null_values_range,
                               const Datum& needles, const SearchSortedOptions& options,
                               NullPlacement null_placement, ExecContext* ctx) const {
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
                                  null_placement, ctx);
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
                                 NullPlacement null_placement, ExecContext* ctx) const {
    if (needles.is_scalar()) {
      auto scalar = needles.scalar();
      if (!scalar->is_valid) {
        return Datum(std::make_shared<UInt64Scalar>());
      }

      ARROW_ASSIGN_OR_RAISE(auto scalar_arr, MakeArrayFromScalar(*scalar, 1));
      ARROW_ASSIGN_OR_RAISE(
          auto result,
          DispatchHaystack<ArrowType>(values, non_null_values_range, Datum(scalar_arr),
                                      side, null_placement, ctx));
      ARROW_ASSIGN_OR_RAISE(auto result_scalar, result.make_array()->GetScalar(0));
      return Datum(std::move(result_scalar));
    }

    // XXX This doesn't need to be in the type-specialized DispatchHaystack
    if (needles.type()->id() == Type::RUN_END_ENCODED) {
      return ComputeRunEndEncodedNeedleInsertionIndices(
          values, RunEndEncodedArray(needles.array()), side, ctx);
    }

    return ComputeInsertionIndices<ArrowType>(
        values, needles, side, null_placement,
        static_cast<uint64_t>(non_null_values_range.offset), ctx);
  }
};

}  // namespace

/// Register the search_sorted vector kernel in the global compute registry.
void RegisterVectorSearchSorted(FunctionRegistry* registry) {
  DCHECK_OK(registry->AddFunction(std::make_shared<SearchSortedMetaFunction>()));
}

}  // namespace compute::internal
}  // namespace arrow
