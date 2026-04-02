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
#include <span>
#include <type_traits>
#include <utility>

#include "arrow/array/array_primitive.h"
#include "arrow/array/array_run_end.h"
#include "arrow/array/builder_primitive.h"
#include "arrow/array/util.h"
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
     "The input array must already be sorted in ascending order. Null values in\n"
     "the searched array are supported when clustered entirely at the start or\n"
     "entirely at the end. Non-null needles are matched only against the non-null\n"
     "portion of the searched array. Null needles emit nulls in the output."),
    {"values", "needles"}, "SearchSortedOptions");

// This file implements search_sorted as a small pipeline that first normalizes
// Arrow input shapes and then runs one typed binary-search core on logical
// values.
//
// Plain arrays, run-end encoded arrays, and scalar needles are all
// adapted into the same accessor and visitor model so the search logic does
// not care about physical layout.
//
// After validation, the kernel isolates the contiguous non-null window of the searched
// values, because nulls are only supported when clustered at one end.
// Needles are then visited either as single values or as logical runs, and each non-null
// needle is resolved with a lower-bound or upper-bound binary search over the sorted
// non-null range.
//
// Output materialization is split by null handling: non-null-only needles write directly
// into a preallocated uint64 buffer, while nullable needles append null and non-null
// spans through a UInt64Builder. That builder path is optimized for repeated runs by
// bulk-filling reserved memory instead of appending one insertion index at a time.
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
//             `--> RunEndEncodedValuesAccessor
//
//   needles datum
//       |
//       +--> ValidateNeedleInput
//       |
//       +--> DatumHasNulls
//       |
//       `--> VisitNeedles
//             |
//             +--> scalar needle -> one logical span
//             |
//             +--> plain array   -> one span per element
//             |
//             `--> REE array     -> one span per logical run
//
//   normalized values accessor + normalized needle spans
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
//       |     `--> MakeMutableUInt64Array
//       |           `--> fill output buffer directly
//       |
//       `--> nullable needles
//             `--> UInt64Builder
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
// The file follows that layout: validation and type helpers first, then value
// accessors, then needle visitors, then typed search and output helpers, and
// finally the meta-function dispatch that selects the Arrow type and accessor.

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

 private:
  ArrayType array_;
};

/// Access logical values from a run-end encoded Arrow array.
template <typename ArrowType, typename RunEndCType>
class RunEndEncodedValuesAccessor {
 public:
  using ArrayType = typename TypeTraits<ArrowType>::ArrayType;
  using ValueType = SearchValue<ArrowType>;

  /// Build a typed accessor over a run-end encoded payload.
  explicit RunEndEncodedValuesAccessor(const RunEndEncodedArray& array)
      : values_(array.values()->data()), array_span_(*array.data()), span_(array_span_) {}

  /// Return the logical length of the searched values.
  int64_t length() const { return span_.length(); }

  /// Return the logical value at the given logical position.
  ValueType Value(int64_t index) const {
    const auto physical_index = span_.PhysicalIndex(index);
    return GetViewType<ArrowType>::LogicalValue(values_.GetView(physical_index));
  }

 private:
  ArrayType values_;
  ArraySpan array_span_;
  ::arrow::ree_util::RunEndEncodedArraySpan<RunEndCType> span_;
};

struct NonNullValuesRange {
  int64_t offset = 0;
  int64_t length = 0;

  /// Return whether the range spans the full searched values input.
  bool is_identity(int64_t full_length) const {
    return offset == 0 && length == full_length;
  }
};

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
        length_(non_null_values_range.length) {}

  /// Return the number of accessible non-null values.
  int64_t length() const { return length_; }

  /// Return the value at the given index within the non-null subrange.
  auto Value(int64_t index) const { return values_.Value(offset_ + index); }

 private:
  const ValuesAccessor& values_;
  int64_t offset_;
  int64_t length_;
};

/// Return the logical type of an array, unwrapping run-end encoding when present.
inline const DataType& LogicalType(const ArrayData& array) {
  const auto& type = *array.type;
  if (type.id() == Type::RUN_END_ENCODED) {
    return *checked_cast<const RunEndEncodedType&>(type).value_type();
  }
  return type;
}

/// Return the logical type of a datum, unwrapping run-end encoding when present.
inline const DataType& LogicalType(const Datum& datum) {
  if (datum.is_scalar()) {
    return *datum.scalar()->type;
  }
  return LogicalType(*datum.array());
}

/// Return whether a scalar or array needle input contains any logical nulls.
inline bool DatumHasNulls(const Datum& datum) {
  if (datum.is_scalar()) {
    return !datum.scalar()->is_valid;
  }

  const auto& array_data = datum.array();
  const bool has_nulls = array_data->GetNullCount() > 0;
  if (array_data->type->id() == Type::RUN_END_ENCODED) {
    RunEndEncodedArray run_end_encoded(array_data);
    return run_end_encoded.values()->null_count() != 0 || has_nulls;
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

  const auto null_count = values.GetNullCount();
  if (null_count == 0) {
    return non_null_values_range;
  }

  int64_t leading_null_count = 0;
  while (leading_null_count < values.length && values.IsNull(leading_null_count)) {
    ++leading_null_count;
  }

  if (leading_null_count == values.length) {
    non_null_values_range.offset = values.length;
    non_null_values_range.length = 0;
    return non_null_values_range;
  }

  if (leading_null_count > 0) {
    if (leading_null_count != null_count) {
      return Status::Invalid(
          "search_sorted values with nulls must be clustered at the start or end");
    }
    non_null_values_range.offset = leading_null_count;
    non_null_values_range.length = values.length - leading_null_count;
    return non_null_values_range;
  }

  int64_t trailing_null_count = 0;
  while (trailing_null_count < values.length &&
         values.IsNull(values.length - 1 - trailing_null_count)) {
    ++trailing_null_count;
  }

  if (trailing_null_count == 0 || trailing_null_count != null_count) {
    return Status::Invalid(
        "search_sorted values with nulls must be clustered at the start or end");
  }

  non_null_values_range.length = values.length - trailing_null_count;
  return non_null_values_range;
}

/// Validate the searched values input shape and supported encoding.
inline Status ValidateSortedValuesInput(const Datum& datum) {
  if (!datum.is_array()) {
    return Status::TypeError("search_sorted values must be an array");
  }

  const auto& type = *datum.type();
  if (type.id() == Type::RUN_END_ENCODED) {
    return ValidateRunEndEncodedLogicalValueType(type, "values");
  }

  return Status::OK();
}

/// Validate the needles input shape and supported encoding.
inline Status ValidateNeedleInput(const Datum& datum) {
  if (!(datum.is_array() || datum.is_scalar())) {
    return Status::TypeError("search_sorted needles must be a scalar or array");
  }

  if (datum.is_array() && datum.type()->id() == Type::RUN_END_ENCODED) {
    return ValidateRunEndEncodedLogicalValueType(*datum.type(), "needles");
  }
  return Status::OK();
}

/// Perform a lower- or upper-bound binary search over already sorted values.
template <typename ArrowType, typename Accessor>
uint64_t FindInsertionPoint(const Accessor& sorted_values,
                            const SearchValue<ArrowType>& needle,
                            SearchSortedOptions::Side side) {
  SearchSortedCompare<ArrowType> compare;
  int64_t first = 0;
  int64_t count = sorted_values.length();

  // TODO(search_sorted): For fixed-width primitive haystacks, investigate a SIMD-friendly
  // batched search path .
  while (count > 0) {
    const int64_t step = count / 2;
    const int64_t it = first + step;
    const bool advance = side == SearchSortedOptions::Left
                             ? compare(sorted_values.Value(it), needle) < 0
                             : compare(needle, sorted_values.Value(it)) >= 0;
    if (advance) {
      first = it + 1;
      count -= step + 1;
    } else {
      count = step;
    }
  }
  return static_cast<uint64_t>(first);
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

/// Visit each plain-array needle as a single-value logical span.
template <typename ArrowType, bool EmitNulls, typename Visitor>
Status VisitArrayNeedles(const std::shared_ptr<ArrayData>& needles_data,
                         Visitor&& visitor) {
  using ArrayType = typename TypeTraits<ArrowType>::ArrayType;

  ArrayType array(needles_data);
  for (int64_t index = 0; index < array.length(); ++index) {
    RETURN_NOT_OK(
        visitor(ReadVisitedNeedle<ArrowType, EmitNulls>(array, index), index, index + 1));
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
    const auto physical_index = it.index_into_array();
    RETURN_NOT_OK(visitor(ReadVisitedNeedle<ArrowType, EmitNulls>(values, physical_index),
                          it.logical_position(), it.run_end()));
  }
  return Status::OK();
}

/// Visit scalar, plain-array, or run-end encoded needles through a uniform
/// callback interface of [begin, end) logical spans.
template <typename ArrowType, bool EmitNulls, typename Visitor>
Status VisitNeedles(const Datum& needles, Visitor&& visitor) {
  if (needles.is_scalar()) {
    if constexpr (EmitNulls) {
      if (!needles.scalar()->is_valid) {
        return visitor(std::optional<SearchValue<ArrowType>>{}, 0, 1);
      }
    }
    return visitor(MakeVisitedNeedle<ArrowType, EmitNulls>(
                       ExtractScalarValue<ArrowType>(*needles.scalar())),
                   0, 1);
  }

  const auto& needle_data = needles.array();
  if (needle_data->type->id() == Type::RUN_END_ENCODED) {
    RunEndEncodedArray ree(needle_data);
    return DispatchRunEndEncodedByRunEndType<Status>(
        ree, "needles",
        [&]<typename RunEndCType>(const RunEndEncodedArray& run_end_encoded_needles) {
          return VisitRunEndEncodedNeedleRuns<ArrowType, RunEndCType, EmitNulls>(
              run_end_encoded_needles, visitor);
        });
  }

  return VisitArrayNeedles<ArrowType, EmitNulls>(needle_data, visitor);
}

/// Search all needle values and write insertion indices into the preallocated output.
template <typename ArrowType, typename ValuesAccessor>
Status SearchNeedleValues(const ValuesAccessor& sorted_values, const Datum& needles,
                          SearchSortedOptions::Side side, uint64_t insertion_offset,
                          uint64_t* out) {
  auto emit_search_result = [&](const SearchValue<ArrowType>& needle, int64_t begin,
                                int64_t end) -> Status {
    const auto insertion_index =
        FindInsertionPoint<ArrowType>(sorted_values, needle, side) + insertion_offset;
    std::ranges::fill(std::span<uint64_t>(out + begin, static_cast<size_t>(end - begin)),
                      insertion_index);
    return Status::OK();
  };

  return VisitNeedles<ArrowType, false>(needles, emit_search_result);
}

/// Search needle values while emitting nulls for null needles.
template <typename ArrowType, typename ValuesAccessor>
Status AppendInsertionIndicesWithNulls(const ValuesAccessor& sorted_values,
                                       const Datum& needles,
                                       SearchSortedOptions::Side side,
                                       uint64_t insertion_offset,
                                       UInt64Builder& builder) {
  auto emit_search_result = [&](const std::optional<SearchValue<ArrowType>>& needle,
                                int64_t begin, int64_t end) -> Status {
    const auto span_length = end - begin;
    if (!needle.has_value()) {
      return builder.AppendNulls(span_length);
    }
    const auto insertion_index =
        FindInsertionPoint<ArrowType>(sorted_values, *needle, side) + insertion_offset;
    AppendInsertionIndex(builder, insertion_index, span_length);
    return Status::OK();
  };

  return VisitNeedles<ArrowType, true>(needles, emit_search_result);
}

/// Materialize output for scalar or array needles.
template <typename ArrowType, typename ValuesAccessor>
Result<Datum> ComputeInsertionIndices(const ValuesAccessor& sorted_values,
                                      const Datum& needles,
                                      SearchSortedOptions::Side side,
                                      uint64_t insertion_offset, ExecContext* ctx) {
  if (needles.is_scalar() && !needles.scalar()->is_valid) {
    return Datum(std::make_shared<UInt64Scalar>());
  }

  if (needles.is_scalar()) {
    const auto insertion_index =
        FindInsertionPoint<ArrowType>(
            sorted_values, ExtractScalarValue<ArrowType>(*needles.scalar()), side) +
        insertion_offset;
    return Datum(std::make_shared<UInt64Scalar>(insertion_index));
  }

  if (DatumHasNulls(needles)) {
    UInt64Builder builder(ctx->memory_pool());
    ARROW_RETURN_NOT_OK(builder.Reserve(needles.length()));
    RETURN_NOT_OK(AppendInsertionIndicesWithNulls<ArrowType>(sorted_values, needles, side,
                                                             insertion_offset, builder));
    ARROW_ASSIGN_OR_RAISE(auto out, builder.Finish());
    return Datum(std::move(out));
  }

  ARROW_ASSIGN_OR_RAISE(auto out,
                        MakeMutableUInt64Array(needles.length(), ctx->memory_pool()));
  auto* out_values = out->GetMutableValues<uint64_t>(1);

  RETURN_NOT_OK(SearchNeedleValues<ArrowType>(sorted_values, needles, side,
                                              insertion_offset, out_values));
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

// Meta-function implementation for the search_sorted public compute entrypoint.
template <typename ArrowType, typename Visitor>
Result<Datum> VisitValuesAccessor(const std::shared_ptr<ArrayData>& values_data,
                                  Visitor&& visitor) {
  if (values_data->type->id() == Type::RUN_END_ENCODED) {
    RunEndEncodedArray ree(values_data);
    return DispatchRunEndEncodedByRunEndType<Result<Datum>>(
        ree, "values",
        [&]<typename RunEndCType>(const RunEndEncodedArray& run_end_encoded_values) {
          RunEndEncodedValuesAccessor<ArrowType, RunEndCType> values_accessor(
              run_end_encoded_values);
          return visitor(values_accessor);
        });
  }

  PlainArrayAccessor<ArrowType> values_accessor(values_data);
  return visitor(values_accessor);
}

/// Meta-function implementation for the search_sorted public compute entrypoint.
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

    const auto& values_array = args[0].array();
    ARROW_ASSIGN_OR_RAISE(auto non_null_values_range,
                          FindNonNullValuesRange(*values_array));
    auto result = DispatchByType(values_array, non_null_values_range, args[1],
                                 static_cast<const SearchSortedOptions&>(*options), ctx);
    return result;
  }

 private:
  /// Dispatch the logical value type to the matching template specialization.
  Result<Datum> DispatchByType(const std::shared_ptr<ArrayData>& values,
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
  Result<Datum> DispatchHaystack(const std::shared_ptr<ArrayData>& values,
                                 const NonNullValuesRange& non_null_values_range,
                                 const Datum& needles, SearchSortedOptions::Side side,
                                 ExecContext* ctx) const {
    return VisitValuesAccessor<ArrowType>(values, [&](const auto& values_accessor) {
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