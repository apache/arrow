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

#include <arrow/io/file.h>
#include <arrow/ipc/reader.h>
#include <arrow/ipc/writer.h>

using Rcpp::DataFrame;

// [[arrow::export]]
std::shared_ptr<arrow::Table> Table__from_dataframe(DataFrame tbl) {
  auto rb = RecordBatch__from_dataframe(tbl);

  std::shared_ptr<arrow::Table> out;
  STOP_IF_NOT_OK(arrow::Table::FromRecordBatches({std::move(rb)}, &out));
  return out;
}

// [[arrow::export]]
int Table__num_columns(const std::shared_ptr<arrow::Table>& x) {
  return x->num_columns();
}

// [[arrow::export]]
int Table__num_rows(const std::shared_ptr<arrow::Table>& x) { return x->num_rows(); }

// [[arrow::export]]
std::shared_ptr<arrow::Schema> Table__schema(const std::shared_ptr<arrow::Table>& x) {
  return x->schema();
}

// [[arrow::export]]
std::shared_ptr<arrow::Column> Table__column(const std::shared_ptr<arrow::Table>& table,
                                             int i) {
  return table->column(i);
}

// [[arrow::export]]
std::vector<std::shared_ptr<arrow::Column>> Table__columns(
    const std::shared_ptr<arrow::Table>& table) {
  auto nc = table->num_columns();
  std::vector<std::shared_ptr<arrow::Column>> res(nc);
  for (int i = 0; i < nc; i++) {
    res[i] = table->column(i);
  }
  return res;
}

// [[arrow::export]]
Rcpp::CharacterVector Table__column_names(const std::shared_ptr<arrow::Table>& table) {
  int nc = table->num_columns();
  Rcpp::CharacterVector res(nc);
  for (int i = 0; i < nc; i++) {
    res[i] = table->column(i)->name();
  }
  return res;
}

// [[arrow::export]]
std::shared_ptr<arrow::Table> Table__select(const std::shared_ptr<arrow::Table>& table,
                                            const Rcpp::IntegerVector& indices) {
  R_xlen_t n = indices.size();

  std::vector<std::shared_ptr<arrow::Field>> fields(n);
  std::vector<std::shared_ptr<arrow::Column>> columns(n);

  for (R_xlen_t i = 0; i < n; i++) {
    int pos = indices[i] - 1;
    fields[i] = table->schema()->field(pos);
    columns[i] = table->column(pos);
  }

  auto schema = std::make_shared<arrow::Schema>(std::move(fields));
  return arrow::Table::Make(schema, columns);
}

bool all_record_batches(SEXP lst) {
  R_xlen_t n = XLENGTH(lst);
  for (R_xlen_t i = 0; i < n; i++) {
    if (!Rf_inherits(VECTOR_ELT(lst, i), "arrow::RecordBatch")) return false;
  }
  return true;
}

// [[arrow::export]]
std::shared_ptr<arrow::Table> Table__from_dots(SEXP lst, SEXP schema_sxp) {
  // lst can be either:
  // - a list of record batches, in which case we call Table::FromRecordBatches

  if (all_record_batches(lst)) {
    auto batches = arrow::r::List_to_shared_ptr_vector<arrow::RecordBatch>(lst);
    std::shared_ptr<arrow::Table> tab;

    if (Rf_inherits(schema_sxp, "arrow::Schema")) {
      auto schema = arrow::r::extract<arrow::Schema>(schema_sxp);
      STOP_IF_NOT_OK(arrow::Table::FromRecordBatches(schema, batches, &tab));
    } else {
      STOP_IF_NOT_OK(arrow::Table::FromRecordBatches(batches, &tab));
    }
    return tab;
  }

  R_xlen_t n = XLENGTH(lst);
  std::vector<std::shared_ptr<arrow::Column>> columns(n);
  std::shared_ptr<arrow::Schema> schema;

  if (Rf_isNull(schema_sxp)) {
    // infer the schema from the ...
    std::vector<std::shared_ptr<arrow::Field>> fields(n);
    Rcpp::CharacterVector names(Rf_getAttrib(lst, R_NamesSymbol));

    for (R_xlen_t i = 0; i < n; i++) {
      SEXP x = VECTOR_ELT(lst, i);
      if (Rf_inherits(x, "arrow::Column")) {
        columns[i] = arrow::r::extract<arrow::Column>(x);
        fields[i] = columns[i]->field();
      } else if (Rf_inherits(x, "arrow::ChunkedArray")) {
        auto chunked_array = arrow::r::extract<arrow::ChunkedArray>(x);
        fields[i] =
            std::make_shared<arrow::Field>(std::string(names[i]), chunked_array->type());
        columns[i] = std::make_shared<arrow::Column>(fields[i], chunked_array);
      } else if (Rf_inherits(x, "arrow::Array")) {
        auto array = arrow::r::extract<arrow::Array>(x);
        fields[i] = std::make_shared<arrow::Field>(std::string(names[i]), array->type());
        columns[i] = std::make_shared<arrow::Column>(fields[i], array);
      } else {
        auto array = Array__from_vector(x, R_NilValue);
        fields[i] = std::make_shared<arrow::Field>(std::string(names[i]), array->type());
        columns[i] = std::make_shared<arrow::Column>(fields[i], array);
      }
    }
    schema = std::make_shared<arrow::Schema>(std::move(fields));
  } else {
    // use the schema that is given
    schema = arrow::r::extract<arrow::Schema>(schema_sxp);

    for (R_xlen_t i = 0; i < n; i++) {
      SEXP x = VECTOR_ELT(lst, i);
      if (Rf_inherits(x, "arrow::Column")) {
        columns[i] = arrow::r::extract<arrow::Column>(x);
      } else if (Rf_inherits(x, "arrow::ChunkedArray")) {
        auto chunked_array = arrow::r::extract<arrow::ChunkedArray>(x);
        columns[i] = std::make_shared<arrow::Column>(schema->field(i), chunked_array);
      } else if (Rf_inherits(x, "arrow::Array")) {
        auto array = arrow::r::extract<arrow::Array>(x);
        columns[i] = std::make_shared<arrow::Column>(schema->field(i), array);
      } else {
        auto type = schema->field(i)->type();
        auto array = arrow::r::Array__from_vector(x, type, false);
        columns[i] = std::make_shared<arrow::Column>(schema->field(i), array);
      }
    }
  }

  return arrow::Table::Make(schema, columns);
}

#endif
