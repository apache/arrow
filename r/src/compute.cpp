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
  STOP_IF_NOT_OK(arrow::compute::Cast(&context, *array, target_type, *options, &out));
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
  STOP_IF_NOT_OK(arrow::compute::Cast(&context, value, target_type, *options, &out));
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
  STOP_IF_NOT_OK(arrow::compute::Take(&context, *values, *indices, options, &out));
  return out;
}

// [[arrow::export]]
std::shared_ptr<arrow::RecordBatch> RecordBatch__Take(
    const std::shared_ptr<arrow::RecordBatch>& batch,
    const std::shared_ptr<arrow::Array>& indices) {
  int ncols = batch->num_columns();
  auto nrows = indices->length();

  std::vector<std::shared_ptr<arrow::Array>> columns(ncols);

  for (R_xlen_t j = 0; j < ncols; j++) {
    columns[j] = Array__Take(batch->column(j), indices);
  }

  return arrow::RecordBatch::Make(batch->schema(), nrows, columns);
}

// [[arrow::export]]
std::shared_ptr<arrow::ChunkedArray> ChunkedArray__Take(
    const std::shared_ptr<arrow::ChunkedArray>& values, Rcpp::IntegerVector& indices) {
  int num_chunks = values->num_chunks();
  std::vector<std::shared_ptr<arrow::Array>> new_chunks(1);  // Hard-coded 1 for now
  // 1) If there's only one chunk, just take from it
  if (num_chunks == 1) {
    new_chunks[0] = Array__Take(
        values->chunk(0), arrow::r::Array__from_vector(indices, arrow::int32(), true));
    return std::make_shared<arrow::ChunkedArray>(std::move(new_chunks));
  }

  std::shared_ptr<arrow::Array> current_chunk;
  std::shared_ptr<arrow::Array> current_indices;
  int offset = 0;
  int len;
  int min_i = indices[0];
  int max_i = indices[0];

  // 2) See if all i are in the same chunk, call Array__Take on that
  for (R_xlen_t i = 1; i < indices.size(); i++) {
    if (indices[i] < min_i) {
      min_i = indices[i];
    } else if (indices[i] > max_i) {
      max_i = indices[i];
    }
  }
  for (R_xlen_t chk = 0; chk < num_chunks; chk++) {
    current_chunk = values->chunk(chk);
    len = current_chunk->length();
    if (min_i >= offset & max_i < offset + len) {
      for (R_xlen_t i = 0; i < indices.size(); i++) {
        // Subtract offset from all indices
        indices[i] -= offset;
      }
      current_indices = arrow::r::Array__from_vector(indices, arrow::int32(), true);
      new_chunks[0] = Array__Take(current_chunk, current_indices);
      return std::make_shared<arrow::ChunkedArray>(std::move(new_chunks));
    }
    offset += len;
  }

  // TODO 3) If they're not all in the same chunk but are sorted, we can slice
  // the indices (offset appropriately) and take from each chunk

  // 4) Last resort: concatenate the chunks
  STOP_IF_NOT_OK(
      arrow::Concatenate(values->chunks(), arrow::default_memory_pool(), &current_chunk));
  current_indices = arrow::r::Array__from_vector(indices, arrow::int32(), true);
  new_chunks[0] = Array__Take(current_chunk, current_indices);
  return std::make_shared<arrow::ChunkedArray>(std::move(new_chunks));
}

// [[arrow::export]]
std::shared_ptr<arrow::Table> Table__Take(const std::shared_ptr<arrow::Table>& table,
                                          Rcpp::IntegerVector& indices) {
  auto ncols = table->num_columns();
  std::vector<std::shared_ptr<arrow::ChunkedArray>> columns(ncols);

  for (R_xlen_t j = 0; j < ncols; j++) {
    columns[j] = ChunkedArray__Take(table->column(j), indices);
  }

  return arrow::Table::Make(table->schema(), columns);
}

// [[arrow::export]]
std::shared_ptr<arrow::Array> Array__Filter(const std::shared_ptr<arrow::Array>& values,
                                            const std::shared_ptr<arrow::Array>& filter) {
  std::shared_ptr<arrow::Array> out;
  arrow::compute::FunctionContext context;
  STOP_IF_NOT_OK(arrow::compute::Filter(&context, *values, *filter, &out));
  return out;
}

// [[arrow::export]]
std::shared_ptr<arrow::RecordBatch> RecordBatch__Filter(
    const std::shared_ptr<arrow::RecordBatch>& batch,
    const std::shared_ptr<arrow::Array>& filter) {
  int ncols = batch->num_columns();

  std::vector<std::shared_ptr<arrow::Array>> columns(ncols);

  for (R_xlen_t j = 0; j < ncols; j++) {
    columns[j] = Array__Filter(batch->column(j), filter);
  }

  return arrow::RecordBatch::Make(batch->schema(), columns[0]->length(), columns);
}

// [[arrow::export]]
std::shared_ptr<arrow::ChunkedArray> ChunkedArray__Filter(
    const std::shared_ptr<arrow::ChunkedArray>& values,
    const std::shared_ptr<arrow::Array>& filter) {
  int num_chunks = values->num_chunks();
  std::vector<std::shared_ptr<arrow::Array>> new_chunks(num_chunks);
  std::shared_ptr<arrow::Array> current_chunk;
  int offset = 0;
  int len;

  for (R_xlen_t i = 0; i < num_chunks; i++) {
    current_chunk = values->chunk(i);
    len = current_chunk->length();
    new_chunks[i] = Array__Filter(current_chunk, filter->Slice(offset, len));
    offset += len;
  }

  return std::make_shared<arrow::ChunkedArray>(std::move(new_chunks));
}

// [[arrow::export]]
std::shared_ptr<arrow::ChunkedArray> ChunkedArray__FilterChunked(
    const std::shared_ptr<arrow::ChunkedArray>& values,
    const std::shared_ptr<arrow::ChunkedArray>& filter) {
  int num_chunks = values->num_chunks();
  std::vector<std::shared_ptr<arrow::Array>> new_chunks(num_chunks);
  std::shared_ptr<arrow::Array> current_chunk;
  std::shared_ptr<arrow::ChunkedArray> current_chunked_filter;
  std::shared_ptr<arrow::Array> current_filter;

  int offset = 0;
  int len;

  for (R_xlen_t i = 0; i < num_chunks; i++) {
    current_chunk = values->chunk(i);
    len = current_chunk->length();
    current_chunked_filter = filter->Slice(offset, len);
    if (current_chunked_filter->num_chunks() == 1) {
      current_filter = current_chunked_filter->chunk(0);
    } else {
      // Concatenate the chunks of the filter so we have an Array
      STOP_IF_NOT_OK(arrow::Concatenate(current_chunked_filter->chunks(),
                                        arrow::default_memory_pool(), &current_filter));
    }
    new_chunks[i] = Array__Filter(current_chunk, current_filter);
    offset += len;
  }

  return std::make_shared<arrow::ChunkedArray>(std::move(new_chunks));
}

// [[arrow::export]]
std::shared_ptr<arrow::Table> Table__Filter(const std::shared_ptr<arrow::Table>& table,
                                            const std::shared_ptr<arrow::Array>& filter) {
  auto ncols = table->num_columns();
  std::vector<std::shared_ptr<arrow::ChunkedArray>> columns(ncols);

  for (R_xlen_t j = 0; j < ncols; j++) {
    columns[j] = ChunkedArray__Filter(table->column(j), filter);
  }

  return arrow::Table::Make(table->schema(), columns);
}

// [[arrow::export]]
std::shared_ptr<arrow::Table> Table__FilterChunked(
    const std::shared_ptr<arrow::Table>& table,
    const std::shared_ptr<arrow::ChunkedArray>& filter) {
  auto ncols = table->num_columns();
  std::vector<std::shared_ptr<arrow::ChunkedArray>> columns(ncols);

  for (R_xlen_t j = 0; j < ncols; j++) {
    columns[j] = ChunkedArray__FilterChunked(table->column(j), filter);
  }

  return arrow::Table::Make(table->schema(), columns);
}
#endif
