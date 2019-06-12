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
#include <arrow/io/memory.h>
#include <arrow/ipc/reader.h>
#include <arrow/ipc/writer.h>

// [[arrow::export]]
int RecordBatch__num_columns(const std::shared_ptr<arrow::RecordBatch>& x) {
  return x->num_columns();
}

// [[arrow::export]]
int RecordBatch__num_rows(const std::shared_ptr<arrow::RecordBatch>& x) {
  return x->num_rows();
}

// [[arrow::export]]
std::shared_ptr<arrow::Schema> RecordBatch__schema(
    const std::shared_ptr<arrow::RecordBatch>& x) {
  return x->schema();
}

// [[arrow::export]]
arrow::ArrayVector RecordBatch__columns(
    const std::shared_ptr<arrow::RecordBatch>& batch) {
  auto nc = batch->num_columns();
  arrow::ArrayVector res(nc);
  for (int i = 0; i < nc; i++) {
    res[i] = batch->column(i);
  }
  return res;
}

// [[arrow::export]]
std::shared_ptr<arrow::Array> RecordBatch__column(
    const std::shared_ptr<arrow::RecordBatch>& batch, int i) {
  return batch->column(i);
}

// [[arrow::export]]
std::shared_ptr<arrow::RecordBatch> RecordBatch__from_dataframe(Rcpp::DataFrame tbl) {
  Rcpp::CharacterVector names = tbl.names();

  std::vector<std::shared_ptr<arrow::Field>> fields;
  std::vector<std::shared_ptr<arrow::Array>> arrays;

  for (int i = 0; i < tbl.size(); i++) {
    SEXP x = tbl[i];
    arrays.push_back(Array__from_vector(x, R_NilValue));
    fields.push_back(
        std::make_shared<arrow::Field>(std::string(names[i]), arrays[i]->type()));
  }
  auto schema = std::make_shared<arrow::Schema>(std::move(fields));

  return arrow::RecordBatch::Make(schema, tbl.nrow(), std::move(arrays));
}

// [[arrow::export]]
bool RecordBatch__Equals(const std::shared_ptr<arrow::RecordBatch>& self,
                         const std::shared_ptr<arrow::RecordBatch>& other) {
  return self->Equals(*other);
}

// [[arrow::export]]
std::shared_ptr<arrow::RecordBatch> RecordBatch__RemoveColumn(
    const std::shared_ptr<arrow::RecordBatch>& batch, int i) {
  std::shared_ptr<arrow::RecordBatch> res;
  STOP_IF_NOT_OK(batch->RemoveColumn(i, &res));
  return res;
}

// [[arrow::export]]
std::string RecordBatch__column_name(const std::shared_ptr<arrow::RecordBatch>& batch,
                                     int i) {
  return batch->column_name(i);
}

// [[arrow::export]]
Rcpp::CharacterVector RecordBatch__names(
    const std::shared_ptr<arrow::RecordBatch>& batch) {
  int n = batch->num_columns();
  Rcpp::CharacterVector names(n);
  for (int i = 0; i < n; i++) {
    names[i] = batch->column_name(i);
  }
  return names;
}

// [[arrow::export]]
std::shared_ptr<arrow::RecordBatch> RecordBatch__Slice1(
    const std::shared_ptr<arrow::RecordBatch>& self, int offset) {
  return self->Slice(offset);
}

// [[arrow::export]]
std::shared_ptr<arrow::RecordBatch> RecordBatch__Slice2(
    const std::shared_ptr<arrow::RecordBatch>& self, int offset, int length) {
  return self->Slice(offset, length);
}

// [[arrow::export]]
Rcpp::RawVector ipc___SerializeRecordBatch__Raw(
    const std::shared_ptr<arrow::RecordBatch>& batch) {
  // how many bytes do we need ?
  int64_t size;
  STOP_IF_NOT_OK(arrow::ipc::GetRecordBatchSize(*batch, &size));

  // allocate the result raw vector
  Rcpp::RawVector out(Rcpp::no_init(size));

  // serialize into the bytes of the raw vector
  auto buffer = std::make_shared<arrow::r::RBuffer<RAWSXP, Rcpp::RawVector>>(out);
  arrow::io::FixedSizeBufferWriter stream(buffer);
  STOP_IF_NOT_OK(
      arrow::ipc::SerializeRecordBatch(*batch, arrow::default_memory_pool(), &stream));
  STOP_IF_NOT_OK(stream.Close());

  return out;
}

// [[arrow::export]]
std::shared_ptr<arrow::RecordBatch> ipc___ReadRecordBatch__InputStream__Schema(
    const std::shared_ptr<arrow::io::InputStream>& stream,
    const std::shared_ptr<arrow::Schema>& schema) {
  std::shared_ptr<arrow::RecordBatch> batch;
  // TODO: promote to function arg
  arrow::ipc::DictionaryMemo memo;
  STOP_IF_NOT_OK(arrow::ipc::ReadRecordBatch(schema, &memo, stream.get(), &batch));
  return batch;
}

arrow::Status check_consistent_array_size(
    const std::vector<std::shared_ptr<arrow::Array>>& arrays, int64_t* num_rows) {
  if (arrays.size()) {
    *num_rows = arrays[0]->length();

    for (const auto& array : arrays) {
      if (array->length() != *num_rows) {
        return arrow::Status::Invalid("All arrays must have the same length");
      }
    }
  }

  return arrow::Status::OK();
}

std::shared_ptr<arrow::RecordBatch> RecordBatch__from_arrays__known_schema(
    const std::shared_ptr<arrow::Schema>& schema, SEXP lst) {
  R_xlen_t n_arrays = XLENGTH(lst);
  if (schema->num_fields() != n_arrays) {
    Rcpp::stop("incompatible. schema has %d fields, and %d arrays are supplied",
               schema->num_fields(), n_arrays);
  }

  // convert lst to a vector of arrow::Array
  std::vector<std::shared_ptr<arrow::Array>> arrays(n_arrays);
  SEXP names = Rf_getAttrib(lst, R_NamesSymbol);
  bool has_names = !Rf_isNull(names);

  for (R_xlen_t i = 0; i < n_arrays; i++) {
    if (has_names && schema->field(i)->name() != CHAR(STRING_ELT(names, i))) {
      Rcpp::stop("field at index %d has name '%s' != '%s'", i + 1,
                 schema->field(i)->name(), CHAR(STRING_ELT(names, i)));
    }
    arrays[i] =
        arrow::r::Array__from_vector(VECTOR_ELT(lst, i), schema->field(i)->type(), false);
  }

  int64_t num_rows = 0;
  STOP_IF_NOT_OK(check_consistent_array_size(arrays, &num_rows));
  return arrow::RecordBatch::Make(schema, num_rows, arrays);
}

// [[arrow::export]]
std::shared_ptr<arrow::RecordBatch> RecordBatch__from_arrays(SEXP schema_sxp, SEXP lst) {
  if (Rf_inherits(schema_sxp, "arrow::Schema")) {
    return RecordBatch__from_arrays__known_schema(
        arrow::r::extract<arrow::Schema>(schema_sxp), lst);
  }

  R_xlen_t n_arrays = XLENGTH(lst);

  // convert lst to a vector of arrow::Array
  std::vector<std::shared_ptr<arrow::Array>> arrays(n_arrays);
  for (R_xlen_t i = 0; i < n_arrays; i++) {
    arrays[i] = Array__from_vector(VECTOR_ELT(lst, i), R_NilValue);
  }

  // generate schema from the types that have been infered
  std::shared_ptr<arrow::Schema> schema;
  if (Rf_inherits(schema_sxp, "arrow::Schema")) {
    schema = arrow::r::extract<arrow::Schema>(schema_sxp);
  } else {
    Rcpp::CharacterVector names(Rf_getAttrib(lst, R_NamesSymbol));
    std::vector<std::shared_ptr<arrow::Field>> fields(n_arrays);
    for (R_xlen_t i = 0; i < n_arrays; i++) {
      fields[i] =
          std::make_shared<arrow::Field>(std::string(names[i]), arrays[i]->type());
    }
    schema = std::make_shared<arrow::Schema>(std::move(fields));
  }

  Rcpp::CharacterVector names(Rf_getAttrib(lst, R_NamesSymbol));
  std::vector<std::shared_ptr<arrow::Field>> fields(n_arrays);
  for (R_xlen_t i = 0; i < n_arrays; i++) {
    fields[i] = std::make_shared<arrow::Field>(std::string(names[i]), arrays[i]->type());
  }
  schema = std::make_shared<arrow::Schema>(std::move(fields));

  // check all sizes are the same
  int64_t num_rows = 0;
  STOP_IF_NOT_OK(check_consistent_array_size(arrays, &num_rows));

  return arrow::RecordBatch::Make(schema, num_rows, arrays);
}

#endif
