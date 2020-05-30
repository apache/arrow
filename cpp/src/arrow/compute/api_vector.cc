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

#include "arrow/array/concatenate.h"
#include "arrow/compute/exec.h"
#include "arrow/compute/kernels/vector_selection_internal.h"
#include "arrow/datum.h"
#include "arrow/result.h"

namespace arrow {
namespace compute {

// ----------------------------------------------------------------------
// Direct exec interface to kernels

Result<std::shared_ptr<Array>> NthToIndices(const Array& values, int64_t n,
                                            ExecContext* ctx) {
  PartitionOptions options(/*pivot=*/n);
  ARROW_ASSIGN_OR_RAISE(
      Datum result, CallFunction("partition_indices", {Datum(values)}, &options, ctx));
  return result.make_array();
}

Result<std::shared_ptr<Array>> SortToIndices(const Array& values, ExecContext* ctx) {
  ARROW_ASSIGN_OR_RAISE(Datum result, CallFunction("sort_indices", {Datum(values)}, ctx));
  return result.make_array();
}

Result<Datum> Take(const Datum& values, const Datum& indices, const TakeOptions& options,
                   ExecContext* ctx) {
  return CallFunction("take", {values, indices}, &options, ctx);
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
// Filter with conveniences to filter RecordBatch, Table

Result<std::shared_ptr<RecordBatch>> FilterRecordBatch(const RecordBatch& batch,
                                                       const Datum& filter,
                                                       FilterOptions options,
                                                       ExecContext* ctx) {
  if (!filter.is_array()) {
    return Status::Invalid("Cannot filter a RecordBatch with a filter of kind ",
                           filter.kind());
  }

  // TODO: Rewrite this to convert to selection vector and use Take
  std::vector<std::shared_ptr<Array>> columns(batch.num_columns());
  for (int i = 0; i < batch.num_columns(); ++i) {
    ARROW_ASSIGN_OR_RAISE(Datum out,
                          Filter(batch.column(i)->data(), filter, options, ctx));
    columns[i] = out.make_array();
  }

  int64_t out_length;
  if (columns.size() == 0) {
    out_length =
        internal::FilterOutputSize(options.null_selection_behavior, *filter.make_array());
  } else {
    out_length = columns[0]->length();
  }
  return RecordBatch::Make(batch.schema(), out_length, columns);
}

Result<std::shared_ptr<Table>> FilterTable(const Table& table, const Datum& filter,
                                           FilterOptions options, ExecContext* ctx) {
  auto new_columns = table.columns();
  for (auto& column : new_columns) {
    ARROW_ASSIGN_OR_RAISE(Datum out_column, Filter(column, filter, options, ctx));
    column = out_column.chunked_array();
  }
  return Table::Make(table.schema(), std::move(new_columns));
}

Result<Datum> Filter(const Datum& values, const Datum& filter, FilterOptions options,
                     ExecContext* ctx) {
  if (values.kind() == Datum::RECORD_BATCH) {
    auto values_batch = values.record_batch();
    ARROW_ASSIGN_OR_RAISE(std::shared_ptr<RecordBatch> out_batch,
                          FilterRecordBatch(*values_batch, filter, options, ctx));
    return Datum(out_batch);
  } else if (values.kind() == Datum::TABLE) {
    ARROW_ASSIGN_OR_RAISE(std::shared_ptr<Table> out_table,
                          FilterTable(*values.table(), filter, options, ctx));
    return Datum(out_table);
  } else {
    return CallFunction("filter", {values, filter}, &options, ctx);
  }
}

// ----------------------------------------------------------------------
// Take invocation conveniences

Result<std::shared_ptr<Array>> Take(const Array& values, const Array& indices,
                                    const TakeOptions& options, ExecContext* ctx) {
  ARROW_ASSIGN_OR_RAISE(Datum out_datum,
                        Take(Datum(values.data()), Datum(indices.data()), options, ctx));
  return out_datum.make_array();
}

Result<std::shared_ptr<ChunkedArray>> Take(const ChunkedArray& values,
                                           const Array& indices,
                                           const TakeOptions& options, ExecContext* ctx) {
  auto num_chunks = values.num_chunks();
  std::vector<std::shared_ptr<Array>> new_chunks(1);  // Hard-coded 1 for now
  std::shared_ptr<Array> current_chunk;

  // Case 1: `values` has a single chunk, so just use it
  if (num_chunks == 1) {
    current_chunk = values.chunk(0);
  } else {
    // TODO Case 2: See if all `indices` fall in the same chunk and call Array Take on it
    // See
    // https://github.com/apache/arrow/blob/6f2c9041137001f7a9212f244b51bc004efc29af/r/src/compute.cpp#L123-L151
    // TODO Case 3: If indices are sorted, can slice them and call Array Take

    // Case 4: Else, concatenate chunks and call Array Take
    RETURN_NOT_OK(Concatenate(values.chunks(), default_memory_pool(), &current_chunk));
  }
  // Call Array Take on our single chunk
  ARROW_ASSIGN_OR_RAISE(new_chunks[0], Take(*current_chunk, indices, options, ctx));
  return std::make_shared<ChunkedArray>(std::move(new_chunks));
}

Result<std::shared_ptr<ChunkedArray>> Take(const ChunkedArray& values,
                                           const ChunkedArray& indices,
                                           const TakeOptions& options, ExecContext* ctx) {
  auto num_chunks = indices.num_chunks();
  std::vector<std::shared_ptr<Array>> new_chunks(num_chunks);
  for (int i = 0; i < num_chunks; i++) {
    // Take with that indices chunk
    // Note that as currently implemented, this is inefficient because `values`
    // will get concatenated on every iteration of this loop
    ARROW_ASSIGN_OR_RAISE(std::shared_ptr<ChunkedArray> current_chunk,
                          Take(values, *indices.chunk(i), options, ctx));
    // Concatenate the result to make a single array for this chunk
    RETURN_NOT_OK(
        Concatenate(current_chunk->chunks(), default_memory_pool(), &new_chunks[i]));
  }
  return std::make_shared<ChunkedArray>(std::move(new_chunks));
}

Result<std::shared_ptr<ChunkedArray>> Take(const Array& values,
                                           const ChunkedArray& indices,
                                           const TakeOptions& options, ExecContext* ctx) {
  auto num_chunks = indices.num_chunks();
  std::vector<std::shared_ptr<Array>> new_chunks(num_chunks);
  for (int i = 0; i < num_chunks; i++) {
    // Take with that indices chunk
    ARROW_ASSIGN_OR_RAISE(new_chunks[i], Take(values, *indices.chunk(i), options, ctx));
  }
  return std::make_shared<ChunkedArray>(std::move(new_chunks));
}

Result<std::shared_ptr<RecordBatch>> Take(const RecordBatch& batch, const Array& indices,
                                          const TakeOptions& options, ExecContext* ctx) {
  auto ncols = batch.num_columns();
  auto nrows = indices.length();
  std::vector<std::shared_ptr<Array>> columns(ncols);
  for (int j = 0; j < ncols; j++) {
    ARROW_ASSIGN_OR_RAISE(columns[j], Take(*batch.column(j), indices, options, ctx));
  }
  return RecordBatch::Make(batch.schema(), nrows, columns);
}

Result<std::shared_ptr<Table>> Take(const Table& table, const Array& indices,
                                    const TakeOptions& options, ExecContext* ctx) {
  auto ncols = table.num_columns();
  std::vector<std::shared_ptr<ChunkedArray>> columns(ncols);

  for (int j = 0; j < ncols; j++) {
    ARROW_ASSIGN_OR_RAISE(columns[j], Take(*table.column(j), indices, options, ctx));
  }
  return Table::Make(table.schema(), columns);
}

Result<std::shared_ptr<Table>> Take(const Table& table, const ChunkedArray& indices,
                                    const TakeOptions& options, ExecContext* ctx) {
  auto ncols = table.num_columns();
  std::vector<std::shared_ptr<ChunkedArray>> columns(ncols);
  for (int j = 0; j < ncols; j++) {
    ARROW_ASSIGN_OR_RAISE(columns[j], Take(*table.column(j), indices, options, ctx));
  }
  return Table::Make(table.schema(), columns);
}

}  // namespace compute
}  // namespace arrow
