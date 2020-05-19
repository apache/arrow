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

#include "arrow/array/concatenate.h"
#include "arrow/compute/exec.h"
#include "arrow/compute/options.h"
#include "arrow/datum.h"
#include "arrow/result.h"

namespace arrow {
namespace compute {

// ----------------------------------------------------------------------
// Direct exec interface to kernels

Result<std::shared_ptr<Array>> NthToIndices(const Array& values, int64_t n,
                                            ExecContext* ctx) {
  PartitionOptions options(/*pivot=*/n);
  ARROW_ASSIGN_OR_RAISE(Datum result, ExecVectorFunction(ctx, "partition_indices",
                                                         {Datum(values)}, &options));
  return result.make_array();
}

Result<std::shared_ptr<Array>> SortToIndices(const Array& values, ExecContext* ctx) {
  ARROW_ASSIGN_OR_RAISE(Datum result,
                        ExecVectorFunction(ctx, "sort_indices", {Datum(values)}));
  return result.make_array();
}

Result<Datum> Filter(const Datum& values, const Datum& filter, FilterOptions options,
                     ExecContext* ctx) {
  return ExecVectorFunction(ctx, "take", {values, filter}, &options);
}

Result<Datum> Take(const Datum& values, const Datum& indices, const TakeOptions& options,
                   ExecContext* ctx) {
  return ExecVectorFunction(ctx, "take", {values, indices}, &options);
}

namespace {

// Status InvokeHash(FunctionContext* ctx, HashKernel* func, const Datum& value,
//                   std::vector<Datum>* kernel_outputs,
//                   std::shared_ptr<Array>* dictionary) {
//   RETURN_NOT_OK(detail::InvokeUnaryArrayKernel(ctx, func, value, kernel_outputs));
//   std::shared_ptr<ArrayData> dict_data;
//   RETURN_NOT_OK(func->GetDictionary(&dict_data));
//   *dictionary = MakeArray(dict_data);
//   return Status::OK();
// }

}  // namespace

Result<std::shared_ptr<Array>> Unique(const Datum& value, ExecContext* ctx) {
  // std::unique_ptr<HashKernel> func;
  // RETURN_NOT_OK(GetUniqueKernel(ctx, value.type(), &func));
  // std::vector<Datum> dummy_outputs;
  // return InvokeHash(ctx, func.get(), value, &dummy_outputs, out);
  return Status::NotImplemented("NYI");
}

Result<Datum> DictionaryEncode(const Datum& value, ExecContext* ctx) {
  // std::unique_ptr<HashKernel> func;
  // RETURN_NOT_OK(GetDictionaryEncodeKernel(ctx, value.type(), &func));
  // std::shared_ptr<Array> dict;
  // std::vector<Datum> indices_outputs;
  // RETURN_NOT_OK(InvokeHash(ctx, func.get(), value, &indices_outputs, &dict));
  // auto dict_type = dictionary(func->out_type(), dict->type());
  // // Wrap indices in dictionary arrays for result
  // std::vector<std::shared_ptr<Array>> dict_chunks;
  // for (const Datum& datum : indices_outputs) {
  //   dict_chunks.emplace_back(
  //       std::make_shared<DictionaryArray>(dict_type, datum.make_array(), dict));
  // }
  // *out = detail::WrapArraysLike(value, dict_type, dict_chunks);
  // return Status::OK();
  return Status::NotImplemented("NYI");
}

const char kValuesFieldName[] = "values";
const char kCountsFieldName[] = "counts";
const int32_t kValuesFieldIndex = 0;
const int32_t kCountsFieldIndex = 1;

Result<std::shared_ptr<Array>> ValueCounts(const Datum& value, ExecContext* ctx) {
  // std::unique_ptr<HashKernel> func;
  // RETURN_NOT_OK(GetValueCountsKernel(ctx, value.type(), &func));
  // // Calls return nothing for counts.
  // std::vector<Datum> unused_output;
  // std::shared_ptr<Array> uniques;
  // RETURN_NOT_OK(InvokeHash(ctx, func.get(), value, &unused_output, &uniques));
  // Datum value_counts;
  // RETURN_NOT_OK(func->FlushFinal(&value_counts));
  // auto data_type = std::make_shared<StructType>(std::vector<std::shared_ptr<Field>>{
  //     std::make_shared<Field>(kValuesFieldName, uniques->type()),
  //     std::make_shared<Field>(kCountsFieldName, int64())});
  // *counts = std::make_shared<StructArray>(
  //     data_type, uniques->length(),
  //     std::vector<std::shared_ptr<Array>>{uniques, MakeArray(value_counts.array())});
  // return Status::OK();
  return Status::NotImplemented("NYI");
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

// ----------------------------------------------------------------------
// Filter invocation conveniences

// Status FilterRecordBatch(KernelContext* ctx, const RecordBatch& batch,
//                          const Array& filter, FilterOptions options,
//                          std::shared_ptr<RecordBatch>* out) {
//   RETURN_NOT_OK(CheckFilterType(filter.type()));
//   const auto& filter_array = checked_cast<const BooleanArray&>(filter);
//   std::vector<std::unique_ptr<FilterKernel>> kernels(batch.num_columns());
//   for (int i = 0; i < batch.num_columns(); ++i) {
//     RETURN_NOT_OK(
//         FilterKernel::Make(batch.schema()->field(i)->type(), options, &kernels[i]));
//   }
//   std::vector<std::shared_ptr<Array>> columns(batch.num_columns());
//   auto out_length = OutputSize(options, filter_array);
//   for (int i = 0; i < batch.num_columns(); ++i) {
//     RETURN_NOT_OK(
//         kernels[i]->Filter(ctx, *batch.column(i), filter_array, out_length,
//         &columns[i]));
//   }
//   *out = RecordBatch::Make(batch.schema(), out_length, columns);
//   return Status::OK();
// }

// Status Filter(KernelContext* ctx, const Datum& values, const Datum& filter,
//               FilterOptions options, Datum* out) {
//   if (values.kind() == Datum::RECORD_BATCH) {
//     if (!filter.is_array()) {
//       return Status::Invalid("Cannot filter a RecordBatch with a filter of kind ",
//                              filter.kind());
//     }
//     auto values_batch = values.record_batch();
//     auto filter_array = filter.make_array();
//     std::shared_ptr<RecordBatch> out_batch;
//     RETURN_NOT_OK(
//         FilterRecordBatch(ctx, *values_batch, *filter_array, options, &out_batch));
//     *out = std::move(out_batch);
//     return Status::OK();
//   }
//   if (values.kind() == Datum::TABLE) {
//     auto values_table = values.table();
//     std::shared_ptr<Table> out_table;
//     RETURN_NOT_OK(FilterTable(ctx, *values_table, filter, options, &out_table));
//     *out = std::move(out_table);
//     return Status::OK();
//   }
//   std::unique_ptr<FilterKernel> kernel;
//   RETURN_NOT_OK(FilterKernel::Make(values.type(), options, &kernel));
//   return kernel->Call(ctx, values, filter, out);
// }

}  // namespace compute
}  // namespace arrow
