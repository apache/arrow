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

#include <memory>
#include <sstream>
#include <utility>
#include <vector>

#include "arrow/array/array_nested.h"
#include "arrow/array/builder_primitive.h"
#include "arrow/compute/exec.h"
#include "arrow/compute/function_internal.h"
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
}  // namespace internal

namespace compute {

// ----------------------------------------------------------------------
// Function options

bool SortKey::Equals(const SortKey& other) const {
  return name == other.name && order == other.order;
}
std::string SortKey::ToString() const {
  std::stringstream ss;
  ss << name << ' ';
  switch (order) {
    case SortOrder::Ascending:
      ss << "ASC";
      break;
    case SortOrder::Descending:
      ss << "DESC";
      break;
  }
  return ss.str();
}

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
static auto kArraySortOptionsType = GetFunctionOptionsType<ArraySortOptions>(
    DataMember("order", &ArraySortOptions::order));
static auto kSortOptionsType =
    GetFunctionOptionsType<SortOptions>(DataMember("sort_keys", &SortOptions::sort_keys));
static auto kPartitionNthOptionsType = GetFunctionOptionsType<PartitionNthOptions>(
    DataMember("pivot", &PartitionNthOptions::pivot));
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

ArraySortOptions::ArraySortOptions(SortOrder order)
    : FunctionOptions(internal::kArraySortOptionsType), order(order) {}
constexpr char ArraySortOptions::kTypeName[];

SortOptions::SortOptions(std::vector<SortKey> sort_keys)
    : FunctionOptions(internal::kSortOptionsType), sort_keys(std::move(sort_keys)) {}
constexpr char SortOptions::kTypeName[];

PartitionNthOptions::PartitionNthOptions(int64_t pivot)
    : FunctionOptions(internal::kPartitionNthOptionsType), pivot(pivot) {}
constexpr char PartitionNthOptions::kTypeName[];

namespace internal {
void RegisterVectorOptions(FunctionRegistry* registry) {
  DCHECK_OK(registry->AddFunctionOptionsType(kFilterOptionsType));
  DCHECK_OK(registry->AddFunctionOptionsType(kTakeOptionsType));
  DCHECK_OK(registry->AddFunctionOptionsType(kDictionaryEncodeOptionsType));
  DCHECK_OK(registry->AddFunctionOptionsType(kArraySortOptionsType));
  DCHECK_OK(registry->AddFunctionOptionsType(kSortOptionsType));
  DCHECK_OK(registry->AddFunctionOptionsType(kPartitionNthOptionsType));
}
}  // namespace internal

// ----------------------------------------------------------------------
// Direct exec interface to kernels

Result<std::shared_ptr<Array>> NthToIndices(const Array& values, int64_t n,
                                            ExecContext* ctx) {
  PartitionNthOptions options(/*pivot=*/n);
  ARROW_ASSIGN_OR_RAISE(Datum result, CallFunction("partition_nth_indices",
                                                   {Datum(values)}, &options, ctx));
  return result.make_array();
}

Result<Datum> ReplaceWithMask(const Datum& values, const Datum& mask,
                              const Datum& replacements, ExecContext* ctx) {
  return CallFunction("replace_with_mask", {values, mask, replacements}, ctx);
}

Result<std::shared_ptr<Array>> SortIndices(const Array& values, SortOrder order,
                                           ExecContext* ctx) {
  ArraySortOptions options(order);
  ARROW_ASSIGN_OR_RAISE(
      Datum result, CallFunction("array_sort_indices", {Datum(values)}, &options, ctx));
  return result.make_array();
}

Result<std::shared_ptr<Array>> SortIndices(const ChunkedArray& chunked_array,
                                           SortOrder order, ExecContext* ctx) {
  SortOptions options({SortKey("not-used", order)});
  ARROW_ASSIGN_OR_RAISE(
      Datum result, CallFunction("sort_indices", {Datum(chunked_array)}, &options, ctx));
  return result.make_array();
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

Result<Datum> Take(const Datum& values, const Datum& filter, const TakeOptions& options,
                   ExecContext* ctx) {
  // Invoke metafunction which deals with Datum kinds other than just Array,
  // ChunkedArray.
  return CallFunction("take", {values, filter}, &options, ctx);
}

Result<std::shared_ptr<Array>> Take(const Array& values, const Array& indices,
                                    const TakeOptions& options, ExecContext* ctx) {
  ARROW_ASSIGN_OR_RAISE(Datum out, Take(Datum(values), Datum(indices), options, ctx));
  return out.make_array();
}

// ----------------------------------------------------------------------
// Deprecated functions

Result<std::shared_ptr<ChunkedArray>> Take(const ChunkedArray& values,
                                           const Array& indices,
                                           const TakeOptions& options, ExecContext* ctx) {
  ARROW_ASSIGN_OR_RAISE(Datum result, Take(Datum(values), Datum(indices), options, ctx));
  return result.chunked_array();
}

Result<std::shared_ptr<ChunkedArray>> Take(const ChunkedArray& values,
                                           const ChunkedArray& indices,
                                           const TakeOptions& options, ExecContext* ctx) {
  ARROW_ASSIGN_OR_RAISE(Datum result, Take(Datum(values), Datum(indices), options, ctx));
  return result.chunked_array();
}

Result<std::shared_ptr<ChunkedArray>> Take(const Array& values,
                                           const ChunkedArray& indices,
                                           const TakeOptions& options, ExecContext* ctx) {
  ARROW_ASSIGN_OR_RAISE(Datum result, Take(Datum(values), Datum(indices), options, ctx));
  return result.chunked_array();
}

Result<std::shared_ptr<RecordBatch>> Take(const RecordBatch& batch, const Array& indices,
                                          const TakeOptions& options, ExecContext* ctx) {
  ARROW_ASSIGN_OR_RAISE(Datum result, Take(Datum(batch), Datum(indices), options, ctx));
  return result.record_batch();
}

Result<std::shared_ptr<Table>> Take(const Table& table, const Array& indices,
                                    const TakeOptions& options, ExecContext* ctx) {
  ARROW_ASSIGN_OR_RAISE(Datum result, Take(Datum(table), Datum(indices), options, ctx));
  return result.table();
}

Result<std::shared_ptr<Table>> Take(const Table& table, const ChunkedArray& indices,
                                    const TakeOptions& options, ExecContext* ctx) {
  ARROW_ASSIGN_OR_RAISE(Datum result, Take(Datum(table), Datum(indices), options, ctx));
  return result.table();
}

Result<std::shared_ptr<Array>> SortToIndices(const Array& values, ExecContext* ctx) {
  return SortIndices(values, SortOrder::Ascending, ctx);
}

}  // namespace compute
}  // namespace arrow
