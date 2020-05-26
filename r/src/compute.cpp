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
#include <arrow/compute/api.h>

using Rcpp::List_;

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
  return ValueOrStop(arrow::compute::Cast(*array, target_type, *options));
}

// [[arrow::export]]
std::shared_ptr<arrow::ChunkedArray> ChunkedArray__cast(
    const std::shared_ptr<arrow::ChunkedArray>& chunked_array,
    const std::shared_ptr<arrow::DataType>& target_type,
    const std::shared_ptr<arrow::compute::CastOptions>& options) {
  arrow::Datum value(chunked_array);
  arrow::Datum out = ValueOrStop(arrow::compute::Cast(value, target_type, *options));
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
std::shared_ptr<arrow::ChunkedArray> Array__TakeChunked(
    const std::shared_ptr<arrow::Array>& values,
    const std::shared_ptr<arrow::ChunkedArray>& indices) {
  arrow::compute::TakeOptions options;
  return ValueOrStop(arrow::compute::Take(*values, *indices, options));
}

// [[arrow::export]]
std::shared_ptr<arrow::RecordBatch> RecordBatch__Take(
    const std::shared_ptr<arrow::RecordBatch>& batch,
    const std::shared_ptr<arrow::Array>& indices) {
  arrow::compute::TakeOptions options;
  return ValueOrStop(arrow::compute::Take(*batch, *indices, options));
}

// [[arrow::export]]
std::shared_ptr<arrow::ChunkedArray> ChunkedArray__Take(
    const std::shared_ptr<arrow::ChunkedArray>& values,
    const std::shared_ptr<arrow::Array>& indices) {
  arrow::compute::TakeOptions options;
  return ValueOrStop(arrow::compute::Take(*values, *indices, options));
}

// [[arrow::export]]
std::shared_ptr<arrow::ChunkedArray> ChunkedArray__TakeChunked(
    const std::shared_ptr<arrow::ChunkedArray>& values,
    const std::shared_ptr<arrow::ChunkedArray>& indices) {
  arrow::compute::TakeOptions options;
  return ValueOrStop(arrow::compute::Take(*values, *indices, options));
}

// [[arrow::export]]
std::shared_ptr<arrow::Table> Table__Take(const std::shared_ptr<arrow::Table>& table,
                                          const std::shared_ptr<arrow::Array>& indices) {
  arrow::compute::TakeOptions options;
  return ValueOrStop(arrow::compute::Take(*table, *indices, options));
}

// [[arrow::export]]
std::shared_ptr<arrow::Table> Table__TakeChunked(
    const std::shared_ptr<arrow::Table>& table,
    const std::shared_ptr<arrow::ChunkedArray>& indices) {
  arrow::compute::TakeOptions options;
  return ValueOrStop(arrow::compute::Take(*table, *indices, options));
}

// [[arrow::export]]
std::shared_ptr<arrow::RecordBatch> RecordBatch__Filter(
    const std::shared_ptr<arrow::RecordBatch>& batch,
    const std::shared_ptr<arrow::Array>& filter, bool keep_na) {
  // Use the EMIT_NULL filter option to match R's behavior in [
  arrow::compute::FilterOptions options;
  if (keep_na) {
    options.null_selection_behavior = arrow::compute::FilterOptions::EMIT_NULL;
  }
  arrow::Datum out = ValueOrStop(arrow::compute::Filter(batch, filter, options));
  return out.record_batch();
}

// [[arrow::export]]
std::shared_ptr<arrow::ChunkedArray> ChunkedArray__Filter(
    const std::shared_ptr<arrow::ChunkedArray>& values,
    const std::shared_ptr<arrow::Array>& filter, bool keep_na) {
  // Use the EMIT_NULL filter option to match R's behavior in [
  arrow::compute::FilterOptions options;
  if (keep_na) {
    options.null_selection_behavior = arrow::compute::FilterOptions::EMIT_NULL;
  }
  arrow::Datum out = ValueOrStop(arrow::compute::Filter(values, filter, options));
  return out.chunked_array();
}

// [[arrow::export]]
std::shared_ptr<arrow::ChunkedArray> ChunkedArray__FilterChunked(
    const std::shared_ptr<arrow::ChunkedArray>& values,
    const std::shared_ptr<arrow::ChunkedArray>& filter, bool keep_na) {
  // Use the EMIT_NULL filter option to match R's behavior in [
  arrow::compute::FilterOptions options;
  if (keep_na) {
    options.null_selection_behavior = arrow::compute::FilterOptions::EMIT_NULL;
  }
  arrow::Datum out = ValueOrStop(arrow::compute::Filter(values, filter, options));
  return out.chunked_array();
}

// [[arrow::export]]
std::shared_ptr<arrow::Table> Table__Filter(const std::shared_ptr<arrow::Table>& table,
                                            const std::shared_ptr<arrow::Array>& filter,
                                            bool keep_na) {
  // Use the EMIT_NULL filter option to match R's behavior in [
  arrow::compute::FilterOptions options;
  if (keep_na) {
    options.null_selection_behavior = arrow::compute::FilterOptions::EMIT_NULL;
  }
  arrow::Datum out = ValueOrStop(arrow::compute::Filter(table, filter, options));
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
  // Use the EMIT_NULL filter option to match R's behavior in [
  arrow::compute::FilterOptions options;
  if (keep_na) {
    options.null_selection_behavior = arrow::compute::FilterOptions::EMIT_NULL;
  }
  arrow::Datum out = ValueOrStop(arrow::compute::Filter(table, filter, options));
  std::shared_ptr<arrow::Table> tab = out.table();
  if (tab->num_rows() == 0) {
    // Slight hack: if there are no rows in the result, instead do a 0-length
    // slice so that we get chunked arrays with 1 chunk (itself length 0).
    // We need that because the Arrow-to-R converter fails when there are 0 chunks.
    return table->Slice(0, 0);
  }
  return tab;
}

arrow::Datum to_datum(SEXP x) {
  // TODO: this is repetitive, can we DRY it out?
  if (Rf_inherits(x, "ArrowObject") && Rf_inherits(x, "Array")) {
    Rcpp::ConstReferenceSmartPtrInputParameter<std::shared_ptr<arrow::Array>> obj(x);
    return static_cast<std::shared_ptr<arrow::Array>>(obj);
  } else if (Rf_inherits(x, "ArrowObject") && Rf_inherits(x, "ChunkedArray")) {
    Rcpp::ConstReferenceSmartPtrInputParameter<std::shared_ptr<arrow::ChunkedArray>> obj(x);
    return static_cast<std::shared_ptr<arrow::ChunkedArray>>(obj);
  } else if (Rf_inherits(x, "ArrowObject") && Rf_inherits(x, "RecordBatch")) {
    Rcpp::ConstReferenceSmartPtrInputParameter<std::shared_ptr<arrow::RecordBatch>> obj(x);
    return static_cast<std::shared_ptr<arrow::RecordBatch>>(obj);
  } else if (Rf_inherits(x, "ArrowObject") && Rf_inherits(x, "Table")) {
    Rcpp::ConstReferenceSmartPtrInputParameter<std::shared_ptr<arrow::Table>> obj(x);
    return static_cast<std::shared_ptr<arrow::Table>>(obj);
  } else {
    // TODO: scalar?
    // This assumes that R objects have already been converted to Arrow objects;
    // that seems right but should we do the wrapping here too/instead?
    Rcpp::stop("to_datum: Not implemented");
  }
}

SEXP from_datum(arrow::Datum datum) {
  if (datum.is_array()) {
    return Rcpp::wrap(datum.make_array());
  } else if (datum.is_arraylike()) {
    return Rcpp::wrap(datum.chunked_array());
  } else {
    // TODO: the other datum types
    Rcpp::stop("from_datum: Not implemented");
  }
}

std::shared_ptr<arrow::compute::FunctionOptions> make_compute_options(std::string func_name,
    List_ options) {
  if (func_name == "filter") {
    auto out = std::make_shared<arrow::compute::FilterOptions>(arrow::compute::FilterOptions::Defaults());
    if (!Rf_isNull(options["keep_na"]) && options["keep_na"]) {
      out->null_selection_behavior = arrow::compute::FilterOptions::EMIT_NULL;
    }
    return out;
  } else if (func_name == "take") {
    auto out = std::make_shared<arrow::compute::TakeOptions>(arrow::compute::TakeOptions::Defaults());
    return out;
  } else {
    return nullptr;
  }
  // TODO: make sure the correct destructor is called?
}

// [[arrow::export]]
SEXP compute__CallFunction(std::string func_name, List_ args, List_ options) {
  auto opts = make_compute_options(func_name, options);
  std::vector<arrow::Datum> datum_args;
  for (auto arg:args) {
    datum_args.push_back(to_datum(arg));
  }
  auto out = ValueOrStop(arrow::compute::CallFunction(func_name, datum_args, opts.get()));
  return from_datum(out);
}

#endif
