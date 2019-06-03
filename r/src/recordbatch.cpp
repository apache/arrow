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
#include <arrow/io/memory.h>
#include <arrow/ipc/reader.h>
#include <arrow/ipc/writer.h>
#include "./arrow_types.h"

// [[Rcpp::export]]
int RecordBatch__num_columns(const std::shared_ptr<arrow::RecordBatch>& x) {
  return x->num_columns();
}

// [[Rcpp::export]]
int RecordBatch__num_rows(const std::shared_ptr<arrow::RecordBatch>& x) {
  return x->num_rows();
}

// [[Rcpp::export]]
std::shared_ptr<arrow::Schema> RecordBatch__schema(
    const std::shared_ptr<arrow::RecordBatch>& x) {
  return x->schema();
}

// [[Rcpp::export]]
arrow::ArrayVector RecordBatch__columns(
    const std::shared_ptr<arrow::RecordBatch>& batch) {
  auto nc = batch->num_columns();
  arrow::ArrayVector res(nc);
  for (int i = 0; i < nc; i++) {
    res[i] = batch->column(i);
  }
  return res;
}

// [[Rcpp::export]]
std::shared_ptr<arrow::Array> RecordBatch__column(
    const std::shared_ptr<arrow::RecordBatch>& batch, int i) {
  return batch->column(i);
}

// [[Rcpp::export]]
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

// [[Rcpp::export]]
bool RecordBatch__Equals(const std::shared_ptr<arrow::RecordBatch>& self,
                         const std::shared_ptr<arrow::RecordBatch>& other) {
  return self->Equals(*other);
}

// [[Rcpp::export]]
std::shared_ptr<arrow::RecordBatch> RecordBatch__RemoveColumn(
    const std::shared_ptr<arrow::RecordBatch>& batch, int i) {
  std::shared_ptr<arrow::RecordBatch> res;
  STOP_IF_NOT_OK(batch->RemoveColumn(i, &res));
  return res;
}

// [[Rcpp::export]]
std::string RecordBatch__column_name(const std::shared_ptr<arrow::RecordBatch>& batch,
                                     int i) {
  return batch->column_name(i);
}

// [[Rcpp::export]]
Rcpp::CharacterVector RecordBatch__names(
    const std::shared_ptr<arrow::RecordBatch>& batch) {
  int n = batch->num_columns();
  Rcpp::CharacterVector names(n);
  for (int i = 0; i < n; i++) {
    names[i] = batch->column_name(i);
  }
  return names;
}

// [[Rcpp::export]]
std::shared_ptr<arrow::RecordBatch> RecordBatch__Slice1(
    const std::shared_ptr<arrow::RecordBatch>& self, int offset) {
  return self->Slice(offset);
}

// [[Rcpp::export]]
std::shared_ptr<arrow::RecordBatch> RecordBatch__Slice2(
    const std::shared_ptr<arrow::RecordBatch>& self, int offset, int length) {
  return self->Slice(offset, length);
}

// [[Rcpp::export]]
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

// [[Rcpp::export]]
std::shared_ptr<arrow::RecordBatch> ipc___ReadRecordBatch__InputStream__Schema(
    const std::shared_ptr<arrow::io::InputStream>& stream,
    const std::shared_ptr<arrow::Schema>& schema) {
  std::shared_ptr<arrow::RecordBatch> batch;
  // TODO: promote to function arg
  arrow::ipc::DictionaryMemo memo;
  STOP_IF_NOT_OK(arrow::ipc::ReadRecordBatch(schema, &memo, stream.get(), &batch));
  return batch;
}
