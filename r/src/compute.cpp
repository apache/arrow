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

#include "./arrow_types.h"

#if defined(ARROW_R_WITH_ARROW)

// [[arrow::export]]
std::shared_ptr<arrow::compute::CastOptions> compute___CastOptions__initialize(
    bool allow_int_overflow, bool allow_time_truncate, bool allow_float_truncate) {
  auto options = std::make_shared<arrow::compute::CastOptions>();
  options->allow_int_overflow = allow_int_overflow;
  options->allow_time_truncate = allow_time_truncate;
  options->allow_float_truncate = allow_float_truncate;
  return options;
}

// [[arrow::export]]
std::shared_ptr<arrow::Array> Array__cast(
    const std::shared_ptr<arrow::Array>& array,
    const std::shared_ptr<arrow::DataType>& target_type,
    const std::shared_ptr<arrow::compute::CastOptions>& options) {
  std::shared_ptr<arrow::Array> out;
  arrow::compute::FunctionContext context;
  StopIfNotOk(arrow::compute::Cast(&context, *array, target_type, *options, &out));
  return out;
}

// [[arrow::export]]
std::shared_ptr<arrow::ChunkedArray> ChunkedArray__cast(
    const std::shared_ptr<arrow::ChunkedArray>& chunked_array,
    const std::shared_ptr<arrow::DataType>& target_type,
    const std::shared_ptr<arrow::compute::CastOptions>& options) {
  arrow::compute::Datum value(chunked_array);
  arrow::compute::Datum out;
  arrow::compute::FunctionContext context;
  StopIfNotOk(arrow::compute::Cast(&context, value, target_type, *options, &out));
  return out.chunked_array();
}

// [[arrow::export]]
std::shared_ptr<arrow::RecordBatch> RecordBatch__cast(
    const std::shared_ptr<arrow::RecordBatch>& batch,
    const std::shared_ptr<arrow::Schema>& schema,
    const std::shared_ptr<arrow::compute::CastOptions>& options) {
  auto nc = batch->num_columns();

  arrow::ArrayVector columns(nc);
  for (int i = 0; i < nc; i++) {
    columns[i] = Array__cast(batch->column(i), schema->field(i)->type(), options);
  }

  return arrow::RecordBatch::Make(schema, batch->num_rows(), std::move(columns));
}

// [[arrow::export]]
std::shared_ptr<arrow::Table> Table__cast(
    const std::shared_ptr<arrow::Table>& table,
    const std::shared_ptr<arrow::Schema>& schema,
    const std::shared_ptr<arrow::compute::CastOptions>& options) {
  auto nc = table->num_columns();

  using ColumnVector = std::vector<std::shared_ptr<arrow::ChunkedArray>>;
  ColumnVector columns(nc);
  for (int i = 0; i < nc; i++) {
    columns[i] = ChunkedArray__cast(table->column(i), schema->field(i)->type(), options);
  }
  return arrow::Table::Make(schema, std::move(columns), table->num_rows());
}

// [[arrow::export]]
std::shared_ptr<arrow::Array> Array__Take(const std::shared_ptr<arrow::Array>& values,
                                          const std::shared_ptr<arrow::Array>& indices) {
  std::shared_ptr<arrow::Array> out;
  arrow::compute::FunctionContext context;
  arrow::compute::TakeOptions options;
  StopIfNotOk(arrow::compute::Take(&context, *values, *indices, options, &out));
  return out;
}

// [[arrow::export]]
std::shared_ptr<arrow::ChunkedArray> Array__TakeChunked(
    const std::shared_ptr<arrow::Array>& values,
    const std::shared_ptr<arrow::ChunkedArray>& indices) {
  std::shared_ptr<arrow::ChunkedArray> out;
  arrow::compute::FunctionContext context;
  arrow::compute::TakeOptions options;

  StopIfNotOk(arrow::compute::Take(&context, *values, *indices, options, &out));
  return out;
}

// [[arrow::export]]
std::shared_ptr<arrow::RecordBatch> RecordBatch__Take(
    const std::shared_ptr<arrow::RecordBatch>& batch,
    const std::shared_ptr<arrow::Array>& indices) {
  std::shared_ptr<arrow::RecordBatch> out;
  arrow::compute::FunctionContext context;
  arrow::compute::TakeOptions options;
  StopIfNotOk(arrow::compute::Take(&context, *batch, *indices, options, &out));
  return out;
}

// [[arrow::export]]
std::shared_ptr<arrow::ChunkedArray> ChunkedArray__Take(
    const std::shared_ptr<arrow::ChunkedArray>& values,
    const std::shared_ptr<arrow::Array>& indices) {
  std::shared_ptr<arrow::ChunkedArray> out;
  arrow::compute::FunctionContext context;
  arrow::compute::TakeOptions options;

  StopIfNotOk(arrow::compute::Take(&context, *values, *indices, options, &out));
  return out;
}

// [[arrow::export]]
std::shared_ptr<arrow::ChunkedArray> ChunkedArray__TakeChunked(
    const std::shared_ptr<arrow::ChunkedArray>& values,
    const std::shared_ptr<arrow::ChunkedArray>& indices) {
  std::shared_ptr<arrow::ChunkedArray> out;
  arrow::compute::FunctionContext context;
  arrow::compute::TakeOptions options;

  StopIfNotOk(arrow::compute::Take(&context, *values, *indices, options, &out));
  return out;
}

// [[arrow::export]]
std::shared_ptr<arrow::Table> Table__Take(const std::shared_ptr<arrow::Table>& table,
                                          const std::shared_ptr<arrow::Array>& indices) {
  std::shared_ptr<arrow::Table> out;
  arrow::compute::FunctionContext context;
  arrow::compute::TakeOptions options;

  StopIfNotOk(arrow::compute::Take(&context, *table, *indices, options, &out));
  return out;
}

// [[arrow::export]]
std::shared_ptr<arrow::Table> Table__TakeChunked(
    const std::shared_ptr<arrow::Table>& table,
    const std::shared_ptr<arrow::ChunkedArray>& indices) {
  std::shared_ptr<arrow::Table> out;
  arrow::compute::FunctionContext context;
  arrow::compute::TakeOptions options;

  StopIfNotOk(arrow::compute::Take(&context, *table, *indices, options, &out));
  return out;
}

// [[arrow::export]]
std::shared_ptr<arrow::Array> Array__Filter(const std::shared_ptr<arrow::Array>& values,
                                            const std::shared_ptr<arrow::Array>& filter,
                                            bool keep_na) {
  arrow::compute::FunctionContext context;
  arrow::compute::Datum out;
  // Use the EMIT_NULL filter option to match R's behavior in [
  arrow::compute::FilterOptions options;
  if (keep_na) {
    options.null_selection_behavior = arrow::compute::FilterOptions::EMIT_NULL;
  }
  StopIfNotOk(arrow::compute::Filter(&context, values, filter, {}, &out));
  return out.make_array();
}

// [[arrow::export]]
std::shared_ptr<arrow::RecordBatch> RecordBatch__Filter(
    const std::shared_ptr<arrow::RecordBatch>& batch,
    const std::shared_ptr<arrow::Array>& filter, bool keep_na) {
  arrow::compute::FunctionContext context;
  arrow::compute::Datum out;
  // Use the EMIT_NULL filter option to match R's behavior in [
  arrow::compute::FilterOptions options;
  if (keep_na) {
    options.null_selection_behavior = arrow::compute::FilterOptions::EMIT_NULL;
  }
  StopIfNotOk(arrow::compute::Filter(&context, batch, filter, options, &out));
  return out.record_batch();
}

// [[arrow::export]]
std::shared_ptr<arrow::ChunkedArray> ChunkedArray__Filter(
    const std::shared_ptr<arrow::ChunkedArray>& values,
    const std::shared_ptr<arrow::Array>& filter, bool keep_na) {
  arrow::compute::FunctionContext context;
  arrow::compute::Datum out;
  // Use the EMIT_NULL filter option to match R's behavior in [
  arrow::compute::FilterOptions options;
  if (keep_na) {
    options.null_selection_behavior = arrow::compute::FilterOptions::EMIT_NULL;
  }
  StopIfNotOk(arrow::compute::Filter(&context, values, filter, options, &out));
  return out.chunked_array();
}

// [[arrow::export]]
std::shared_ptr<arrow::ChunkedArray> ChunkedArray__FilterChunked(
    const std::shared_ptr<arrow::ChunkedArray>& values,
    const std::shared_ptr<arrow::ChunkedArray>& filter, bool keep_na) {
  arrow::compute::FunctionContext context;
  arrow::compute::Datum out;
  // Use the EMIT_NULL filter option to match R's behavior in [
  arrow::compute::FilterOptions options;
  if (keep_na) {
    options.null_selection_behavior = arrow::compute::FilterOptions::EMIT_NULL;
  }
  StopIfNotOk(arrow::compute::Filter(&context, values, filter, options, &out));
  return out.chunked_array();
}

// [[arrow::export]]
std::shared_ptr<arrow::Table> Table__Filter(const std::shared_ptr<arrow::Table>& table,
                                            const std::shared_ptr<arrow::Array>& filter,
                                            bool keep_na) {
  arrow::compute::FunctionContext context;
  arrow::compute::Datum out;
  // Use the EMIT_NULL filter option to match R's behavior in [
  arrow::compute::FilterOptions options;
  if (keep_na) {
    options.null_selection_behavior = arrow::compute::FilterOptions::EMIT_NULL;
  }
  StopIfNotOk(arrow::compute::Filter(&context, table, filter, options, &out));
  std::shared_ptr<arrow::Table> tab = out.table();
  if (tab->num_rows() == 0) {
    // Slight hack: if there are no rows in the result, instead do a 0-length
    // slice so that we get chunked arrays with 1 chunk (itself length 0).
    // We need that because the Arrow-to-R converter fails when there are 0 chunks.
    return table->Slice(0, 0);
  }
  return tab;
}

// [[arrow::export]]
std::shared_ptr<arrow::Table> Table__FilterChunked(
    const std::shared_ptr<arrow::Table>& table,
    const std::shared_ptr<arrow::ChunkedArray>& filter, bool keep_na) {
  arrow::compute::FunctionContext context;
  arrow::compute::Datum out;
  // Use the EMIT_NULL filter option to match R's behavior in [
  arrow::compute::FilterOptions options;
  if (keep_na) {
    options.null_selection_behavior = arrow::compute::FilterOptions::EMIT_NULL;
  }
  StopIfNotOk(arrow::compute::Filter(&context, table, filter, options, &out));
  std::shared_ptr<arrow::Table> tab = out.table();
  if (tab->num_rows() == 0) {
    // Slight hack: if there are no rows in the result, instead do a 0-length
    // slice so that we get chunked arrays with 1 chunk (itself length 0).
    // We need that because the Arrow-to-R converter fails when there are 0 chunks.
    return table->Slice(0, 0);
  }
  return tab;
}
#endif
