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
#include <sstream>
#include <utility>
#include <vector>

#include "arrow/array/array_nested.h"
#include "arrow/array/builder_primitive.h"
#include "arrow/compute/exec.h"
#include "arrow/compute/function_internal.h"
#include "arrow/compute/kernels/vector_sort_internal.h"
#include "arrow/compute/registry.h"
#include "arrow/datum.h"
#include "arrow/record_batch.h"
#include "arrow/result.h"
#include "arrow/util/checked_cast.h"
#include "arrow/util/logging.h"

namespace arrow {

using internal::checked_cast;
using internal::checked_pointer_cast;

namespace internal {

using compute::DictionaryEncodeOptions;
using compute::FilterOptions;
using compute::NullPlacement;
using compute::RankOptions;

template <>
struct EnumTraits<FilterOptions::NullSelectionBehavior>
    : BasicEnumTraits<FilterOptions::NullSelectionBehavior, FilterOptions::DROP,
                      FilterOptions::EMIT_NULL> {
  static std::string name() { return "FilterOptions::NullSelectionBehavior"; }
  static std::string value_name(FilterOptions::NullSelectionBehavior value) {
    switch (value) {
      case FilterOptions::DROP:
        return "DROP";
      case FilterOptions::EMIT_NULL:
        return "EMIT_NULL";
    }
    return "<INVALID>";
  }
};
template <>
struct EnumTraits<DictionaryEncodeOptions::NullEncodingBehavior>
    : BasicEnumTraits<DictionaryEncodeOptions::NullEncodingBehavior,
                      DictionaryEncodeOptions::ENCODE, DictionaryEncodeOptions::MASK> {
  static std::string name() { return "DictionaryEncodeOptions::NullEncodingBehavior"; }
  static std::string value_name(DictionaryEncodeOptions::NullEncodingBehavior value) {
    switch (value) {
      case DictionaryEncodeOptions::ENCODE:
        return "ENCODE";
      case DictionaryEncodeOptions::MASK:
        return "MASK";
    }
    return "<INVALID>";
  }
};
template <>
struct EnumTraits<NullPlacement>
    : BasicEnumTraits<NullPlacement, NullPlacement::AtStart, NullPlacement::AtEnd> {
  static std::string name() { return "NullPlacement"; }
  static std::string value_name(NullPlacement value) {
    switch (value) {
      case NullPlacement::AtStart:
        return "AtStart";
      case NullPlacement::AtEnd:
        return "AtEnd";
    }
    return "<INVALID>";
  }
};
template <>
struct EnumTraits<RankOptions::Tiebreaker>
    : BasicEnumTraits<RankOptions::Tiebreaker, RankOptions::Min, RankOptions::Max,
                      RankOptions::First, RankOptions::Dense> {
  static std::string name() { return "Tiebreaker"; }
  static std::string value_name(RankOptions::Tiebreaker value) {
    switch (value) {
      case RankOptions::Min:
        return "Min";
      case RankOptions::Max:
        return "Max";
      case RankOptions::First:
        return "First";
      case RankOptions::Dense:
        return "Dense";
    }
    return "<INVALID>";
  }
};

}  // namespace internal

namespace compute {

// ----------------------------------------------------------------------
// Function options

namespace internal {
namespace {
using ::arrow::internal::DataMember;
static auto kFilterOptionsType = GetFunctionOptionsType<FilterOptions>(
    DataMember("null_selection_behavior", &FilterOptions::null_selection_behavior));
static auto kTakeOptionsType = GetFunctionOptionsType<TakeOptions>(
    DataMember("boundscheck", &TakeOptions::boundscheck));
static auto kDictionaryEncodeOptionsType =
    GetFunctionOptionsType<DictionaryEncodeOptions>(DataMember(
        "null_encoding_behavior", &DictionaryEncodeOptions::null_encoding_behavior));
static auto kRunEndEncodeOptionsType = GetFunctionOptionsType<RunEndEncodeOptions>(
    DataMember("run_end_type", &RunEndEncodeOptions::run_end_type));
static auto kArraySortOptionsType = GetFunctionOptionsType<ArraySortOptions>(
    DataMember("order", &ArraySortOptions::order),
    DataMember("null_placement", &ArraySortOptions::null_placement));
static auto kSortOptionsType = GetFunctionOptionsType<SortOptions>(
    DataMember("sort_keys", &SortOptions::sort_keys),
    DataMember("null_placement", &SortOptions::null_placement));
static auto kPartitionNthOptionsType = GetFunctionOptionsType<PartitionNthOptions>(
    DataMember("pivot", &PartitionNthOptions::pivot),
    DataMember("null_placement", &PartitionNthOptions::null_placement));
static auto kSelectKOptionsType = GetFunctionOptionsType<SelectKOptions>(
    DataMember("k", &SelectKOptions::k),
    DataMember("sort_keys", &SelectKOptions::sort_keys));
static auto kCumulativeSumOptionsType = GetFunctionOptionsType<CumulativeSumOptions>(
    DataMember("start", &CumulativeSumOptions::start),
    DataMember("skip_nulls", &CumulativeSumOptions::skip_nulls));
static auto kRankOptionsType = GetFunctionOptionsType<RankOptions>(
    DataMember("sort_keys", &RankOptions::sort_keys),
    DataMember("null_placement", &RankOptions::null_placement),
    DataMember("tiebreaker", &RankOptions::tiebreaker));
}  // namespace
}  // namespace internal

FilterOptions::FilterOptions(NullSelectionBehavior null_selection)
    : FunctionOptions(internal::kFilterOptionsType),
      null_selection_behavior(null_selection) {}
constexpr char FilterOptions::kTypeName[];

TakeOptions::TakeOptions(bool boundscheck)
    : FunctionOptions(internal::kTakeOptionsType), boundscheck(boundscheck) {}
constexpr char TakeOptions::kTypeName[];

DictionaryEncodeOptions::DictionaryEncodeOptions(NullEncodingBehavior null_encoding)
    : FunctionOptions(internal::kDictionaryEncodeOptionsType),
      null_encoding_behavior(null_encoding) {}
constexpr char DictionaryEncodeOptions::kTypeName[];

RunEndEncodeOptions::RunEndEncodeOptions(std::shared_ptr<DataType> run_end_type)
    : FunctionOptions(internal::kRunEndEncodeOptionsType),
      run_end_type{std::move(run_end_type)} {}

ArraySortOptions::ArraySortOptions(SortOrder order, NullPlacement null_placement)
    : FunctionOptions(internal::kArraySortOptionsType),
      order(order),
      null_placement(null_placement) {}
constexpr char ArraySortOptions::kTypeName[];

SortOptions::SortOptions(std::vector<SortKey> sort_keys, NullPlacement null_placement)
    : FunctionOptions(internal::kSortOptionsType),
      sort_keys(std::move(sort_keys)),
      null_placement(null_placement) {}
SortOptions::SortOptions(const Ordering& ordering)
    : FunctionOptions(internal::kSortOptionsType),
      sort_keys(ordering.sort_keys()),
      null_placement(ordering.null_placement()) {}
constexpr char SortOptions::kTypeName[];

PartitionNthOptions::PartitionNthOptions(int64_t pivot, NullPlacement null_placement)
    : FunctionOptions(internal::kPartitionNthOptionsType),
      pivot(pivot),
      null_placement(null_placement) {}
constexpr char PartitionNthOptions::kTypeName[];

SelectKOptions::SelectKOptions(int64_t k, std::vector<SortKey> sort_keys)
    : FunctionOptions(internal::kSelectKOptionsType),
      k(k),
      sort_keys(std::move(sort_keys)) {}
constexpr char SelectKOptions::kTypeName[];

CumulativeSumOptions::CumulativeSumOptions(double start, bool skip_nulls)
    : CumulativeSumOptions(std::make_shared<DoubleScalar>(start), skip_nulls) {}
CumulativeSumOptions::CumulativeSumOptions(std::shared_ptr<Scalar> start, bool skip_nulls)
    : FunctionOptions(internal::kCumulativeSumOptionsType),
      start(std::move(start)),
      skip_nulls(skip_nulls) {}
constexpr char CumulativeSumOptions::kTypeName[];

RankOptions::RankOptions(std::vector<SortKey> sort_keys, NullPlacement null_placement,
                         RankOptions::Tiebreaker tiebreaker)
    : FunctionOptions(internal::kRankOptionsType),
      sort_keys(std::move(sort_keys)),
      null_placement(null_placement),
      tiebreaker(tiebreaker) {}
constexpr char RankOptions::kTypeName[];

namespace internal {
void RegisterVectorOptions(FunctionRegistry* registry) {
  DCHECK_OK(registry->AddFunctionOptionsType(kFilterOptionsType));
  DCHECK_OK(registry->AddFunctionOptionsType(kTakeOptionsType));
  DCHECK_OK(registry->AddFunctionOptionsType(kDictionaryEncodeOptionsType));
  DCHECK_OK(registry->AddFunctionOptionsType(kRunEndEncodeOptionsType));
  DCHECK_OK(registry->AddFunctionOptionsType(kArraySortOptionsType));
  DCHECK_OK(registry->AddFunctionOptionsType(kSortOptionsType));
  DCHECK_OK(registry->AddFunctionOptionsType(kPartitionNthOptionsType));
  DCHECK_OK(registry->AddFunctionOptionsType(kSelectKOptionsType));
  DCHECK_OK(registry->AddFunctionOptionsType(kCumulativeSumOptionsType));
  DCHECK_OK(registry->AddFunctionOptionsType(kRankOptionsType));
}
}  // namespace internal

// ----------------------------------------------------------------------
// Direct exec interface to kernels

Result<std::shared_ptr<Array>> NthToIndices(const Array& values,
                                            const PartitionNthOptions& options,
                                            ExecContext* ctx) {
  ARROW_ASSIGN_OR_RAISE(Datum result, CallFunction("partition_nth_indices",
                                                   {Datum(values)}, &options, ctx));
  return result.make_array();
}

Result<std::shared_ptr<Array>> NthToIndices(const Array& values, int64_t n,
                                            ExecContext* ctx) {
  PartitionNthOptions options(/*pivot=*/n);
  ARROW_ASSIGN_OR_RAISE(Datum result, CallFunction("partition_nth_indices",
                                                   {Datum(values)}, &options, ctx));
  return result.make_array();
}

Result<std::shared_ptr<Array>> SelectKUnstable(const Datum& datum,
                                               const SelectKOptions& options,
                                               ExecContext* ctx) {
  ARROW_ASSIGN_OR_RAISE(Datum result,
                        CallFunction("select_k_unstable", {datum}, &options, ctx));
  return result.make_array();
}

Result<Datum> ReplaceWithMask(const Datum& values, const Datum& mask,
                              const Datum& replacements, ExecContext* ctx) {
  return CallFunction("replace_with_mask", {values, mask, replacements}, ctx);
}

Result<Datum> FillNullForward(const Datum& values, ExecContext* ctx) {
  return CallFunction("fill_null_forward", {values}, ctx);
}

Result<Datum> FillNullBackward(const Datum& values, ExecContext* ctx) {
  return CallFunction("fill_null_backward", {values}, ctx);
}

Result<std::shared_ptr<Array>> SortIndices(const Array& values,
                                           const ArraySortOptions& options,
                                           ExecContext* ctx) {
  ARROW_ASSIGN_OR_RAISE(
      Datum result, CallFunction("array_sort_indices", {Datum(values)}, &options, ctx));
  return result.make_array();
}

Result<std::shared_ptr<Array>> SortIndices(const Array& values, SortOrder order,
                                           ExecContext* ctx) {
  ArraySortOptions options(order);
  ARROW_ASSIGN_OR_RAISE(
      Datum result, CallFunction("array_sort_indices", {Datum(values)}, &options, ctx));
  return result.make_array();
}

Result<std::shared_ptr<Array>> SortIndices(const ChunkedArray& chunked_array,
                                           const ArraySortOptions& array_options,
                                           ExecContext* ctx) {
  SortOptions options({SortKey("", array_options.order)}, array_options.null_placement);
  ARROW_ASSIGN_OR_RAISE(
      Datum result, CallFunction("sort_indices", {Datum(chunked_array)}, &options, ctx));
  return result.make_array();
}

Result<std::shared_ptr<Array>> SortIndices(const ChunkedArray& chunked_array,
                                           SortOrder order, ExecContext* ctx) {
  return SortIndices(chunked_array, ArraySortOptions(order), ctx);
}

Result<std::shared_ptr<Array>> SortIndices(const Datum& datum, const SortOptions& options,
                                           ExecContext* ctx) {
  ARROW_ASSIGN_OR_RAISE(Datum result,
                        CallFunction("sort_indices", {datum}, &options, ctx));
  return result.make_array();
}

Result<std::shared_ptr<Array>> Unique(const Datum& value, ExecContext* ctx) {
  ARROW_ASSIGN_OR_RAISE(Datum result, CallFunction("unique", {value}, ctx));
  return result.make_array();
}

Result<Datum> DictionaryEncode(const Datum& value, const DictionaryEncodeOptions& options,
                               ExecContext* ctx) {
  return CallFunction("dictionary_encode", {value}, &options, ctx);
}

Result<Datum> RunEndEncode(const Datum& value, const RunEndEncodeOptions& options,
                           ExecContext* ctx) {
  return CallFunction("run_end_encode", {value}, &options, ctx);
}

Result<Datum> RunEndDecode(const Datum& value, ExecContext* ctx) {
  return CallFunction("run_end_decode", {value}, ctx);
}

const char kValuesFieldName[] = "values";
const char kCountsFieldName[] = "counts";
const int32_t kValuesFieldIndex = 0;
const int32_t kCountsFieldIndex = 1;

Result<std::shared_ptr<StructArray>> ValueCounts(const Datum& value, ExecContext* ctx) {
  ARROW_ASSIGN_OR_RAISE(Datum result, CallFunction("value_counts", {value}, ctx));
  return checked_pointer_cast<StructArray>(result.make_array());
}

// ----------------------------------------------------------------------
// Filter- and take-related selection functions

Result<Datum> Filter(const Datum& values, const Datum& filter,
                     const FilterOptions& options, ExecContext* ctx) {
  // Invoke metafunction which deals with Datum kinds other than just Array,
  // ChunkedArray.
  return CallFunction("filter", {values, filter}, &options, ctx);
}

Result<Datum> Take(const Datum& values, const Datum& indices, const TakeOptions& options,
                   ExecContext* ctx) {
  // Invoke metafunction which deals with Datum kinds other than just Array,
  // ChunkedArray.
  return CallFunction("take", {values, indices}, &options, ctx);
}

Result<std::shared_ptr<Array>> Take(const Array& values, const Array& indices,
                                    const TakeOptions& options, ExecContext* ctx) {
  ARROW_ASSIGN_OR_RAISE(Datum out, Take(Datum(values), Datum(indices), options, ctx));
  return out.make_array();
}

// ----------------------------------------------------------------------
// Dropnull functions

Result<Datum> DropNull(const Datum& values, ExecContext* ctx) {
  // Invoke metafunction which deals with Datum kinds other than just Array,
  // ChunkedArray.
  return CallFunction("drop_null", {values}, ctx);
}

Result<std::shared_ptr<Array>> DropNull(const Array& values, ExecContext* ctx) {
  ARROW_ASSIGN_OR_RAISE(Datum out, DropNull(Datum(values), ctx));
  return out.make_array();
}

// ----------------------------------------------------------------------
// Cumulative functions

Result<Datum> CumulativeSum(const Datum& values, const CumulativeSumOptions& options,
                            bool check_overflow, ExecContext* ctx) {
  auto func_name = check_overflow ? "cumulative_sum_checked" : "cumulative_sum";
  return CallFunction(func_name, {Datum(values)}, &options, ctx);
}

// ----------------------------------------------------------------------
// Deprecated functions

Result<std::shared_ptr<Array>> SortToIndices(const Array& values, ExecContext* ctx) {
  return SortIndices(values, SortOrder::Ascending, ctx);
}

}  // namespace compute
}  // namespace arrow
