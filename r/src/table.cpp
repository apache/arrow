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

#include <arrow/io/file.h>
#include <arrow/ipc/reader.h>
#include <arrow/ipc/writer.h>
#include "./arrow_types.h"

using Rcpp::DataFrame;

// [[Rcpp::export]]
int Table__num_columns(const std::shared_ptr<arrow::Table>& x) {
  return x->num_columns();
}

// [[Rcpp::export]]
int Table__num_rows(const std::shared_ptr<arrow::Table>& x) { return x->num_rows(); }

// [[Rcpp::export]]
std::shared_ptr<arrow::Schema> Table__schema(const std::shared_ptr<arrow::Table>& x) {
  return x->schema();
}

// [[Rcpp::export]]
std::shared_ptr<arrow::Column> Table__column(const std::shared_ptr<arrow::Table>& table,
                                             int i) {
  return table->column(i);
}

// [[Rcpp::export]]
std::vector<std::shared_ptr<arrow::Column>> Table__columns(
    const std::shared_ptr<arrow::Table>& table) {
  auto nc = table->num_columns();
  std::vector<std::shared_ptr<arrow::Column>> res(nc);
  for (int i = 0; i < nc; i++) {
    res[i] = table->column(i);
  }
  return res;
}

bool all_record_batches(SEXP lst){
  R_xlen_t n = XLENGTH(lst);
  for(R_xlen_t i = 0; i<n; i++) {
    if (!Rf_inherits(VECTOR_ELT(lst, i), "arrow::RecordBatch")) return false;
  }
  return true;
}

// [[Rcpp::export]]
std::shared_ptr<arrow::Table> Table__from_arrays(SEXP lst) {
  // lst can be aither:
  // - a list of record batches, in which case we call Table::FromRecordBatches

  if (all_record_batches(lst)) {
    auto batches = arrow::r::list_to_shared_ptr_vector<arrow::RecordBatch>(lst);
    std::shared_ptr<arrow::Table> tab;
    STOP_IF_NOT_OK(arrow::Table::FromRecordBatches(batches, &tab));
    return tab;
  }

  // - a list of arrays, chunked arrays or r vectors

  CharacterVector names(Rf_getAttrib(lst, R_NamesSymbol));

  R_xlen_t n = XLENGTH(lst);
  std::vector<std::shared_ptr<arrow::Column>> columns(n);
  std::vector<std::shared_ptr<arrow::Field>> fields(n);

  for (R_xlen_t i = 0; i<n; i++) {
    SEXP x = VECTOR_ELT(lst, i);
    if (Rf_inherits(x, "arrow::Column")) {
      columns[i] = arrow::r::extract<arrow::Column>(x);
      fields[i] = columns[i]->field();
    } else if (Rf_inherits(x, "arrow::ChunkedArray")) {
      auto chunked_array = arrow::r::extract<arrow::ChunkedArray>(x);
      fields[i] = std::make_shared<arrow::Field>(std::string(names[i]), chunked_array->type());
      columns[i] = std::make_shared<arrow::Column>(fields[i], chunked_array);
    } else if (Rf_inherits(x, "arrow::Array")) {
      auto array = arrow::r::extract<arrow::Array>(x);
      fields[i] = std::make_shared<arrow::Field>(std::string(names[i]), array->type());
      columns[i] = std::make_shared<arrow::Column>(fields[i], array);
    } else {
      auto array = Array__from_vector(x);
      fields[i] = std::make_shared<arrow::Field>(std::string(names[i]), array->type());
      columns[i] = std::make_shared<arrow::Column>(fields[i], array);
    }
  }

  auto schema = std::make_shared<arrow::Schema>(std::move(fields));
  return arrow::Table::Make(schema, columns);

}
