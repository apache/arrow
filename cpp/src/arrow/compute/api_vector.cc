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
#include <utility>
#include <vector>

#include "arrow/array/builder_primitive.h"
#include "arrow/compute/exec.h"
#include "arrow/datum.h"
#include "arrow/record_batch.h"
#include "arrow/result.h"

namespace arrow {
namespace compute {

// ----------------------------------------------------------------------
// Direct exec interface to kernels

Result<std::shared_ptr<Array>> NthToIndices(const Array& values, int64_t n,
                                            ExecContext* ctx) {
  PartitionNthOptions options(/*pivot=*/n);
  ARROW_ASSIGN_OR_RAISE(Datum result, CallFunction("partition_nth_indices",
                                                   {Datum(values)}, &options, ctx));
  return result.make_array();
}

Result<std::shared_ptr<Array>> SortToIndices(const Array& values, ExecContext* ctx) {
  ARROW_ASSIGN_OR_RAISE(Datum result, CallFunction("sort_indices", {Datum(values)}, ctx));
  return result.make_array();
}

Result<std::shared_ptr<Array>> Unique(const Datum& value, ExecContext* ctx) {
  ARROW_ASSIGN_OR_RAISE(Datum result, CallFunction("unique", {value}, ctx));
  return result.make_array();
}

Result<Datum> DictionaryEncode(const Datum& value, ExecContext* ctx) {
  return CallFunction("dictionary_encode", {value}, ctx);
}

const char kValuesFieldName[] = "values";
const char kCountsFieldName[] = "counts";
const int32_t kValuesFieldIndex = 0;
const int32_t kCountsFieldIndex = 1;

Result<std::shared_ptr<Array>> ValueCounts(const Datum& value, ExecContext* ctx) {
  ARROW_ASSIGN_OR_RAISE(Datum result, CallFunction("value_counts", {value}, ctx));
  return result.make_array();
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

}  // namespace compute
}  // namespace arrow
