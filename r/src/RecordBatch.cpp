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

#include "arrow_types.h"
#include <arrow/io/file.h>
#include <arrow/ipc/writer.h>
#include <arrow/ipc/reader.h>

using namespace Rcpp;
using namespace arrow;

// [[Rcpp::export]]
int RecordBatch__num_columns(const std::shared_ptr<arrow::RecordBatch>& x){
  return x->num_columns();
}

// [[Rcpp::export]]
int RecordBatch__num_rows(const std::shared_ptr<arrow::RecordBatch>& x){
  return x->num_rows();
}

// [[Rcpp::export]]
std::shared_ptr<arrow::Schema> RecordBatch__schema(const std::shared_ptr<arrow::RecordBatch>& x){
  return x->schema();
}

// [[Rcpp::export]]
std::shared_ptr<arrow::Array> RecordBatch__column(const std::shared_ptr<arrow::RecordBatch>& batch, int i){
  return batch->column(i);
}

// [[Rcpp::export]]
List RecordBatch_to_dataframe(const std::shared_ptr<arrow::RecordBatch>& batch){
  int nc = batch->num_columns();
  int nr = batch->num_rows();
  List tbl(nc);
  CharacterVector names(nc);
  for(int i=0; i<nc; i++) {
    tbl[i] = Array__as_vector(batch->column(i));
    names[i] = batch->column_name(i);
  }
  tbl.attr("names") = names;
  tbl.attr("class") = CharacterVector::create("tbl_df", "tbl", "data.frame");
  tbl.attr("row.names") = IntegerVector::create(NA_INTEGER, -nr);
  return tbl;
}

// [[Rcpp::export]]
std::shared_ptr<arrow::RecordBatch> read_record_batch_(std::string path) {
  std::shared_ptr<arrow::io::ReadableFile> stream;
  std::shared_ptr<arrow::ipc::RecordBatchFileReader> rbf_reader;

  R_ERROR_NOT_OK(arrow::io::ReadableFile::Open(path, &stream));
  R_ERROR_NOT_OK(arrow::ipc::RecordBatchFileReader::Open(stream, &rbf_reader));

  std::shared_ptr<arrow::RecordBatch> batch;
  R_ERROR_NOT_OK(rbf_reader->ReadRecordBatch(0, &batch));

  R_ERROR_NOT_OK(stream->Close());
  return batch;
}

// [[Rcpp::export]]
int RecordBatch_to_file(const std::shared_ptr<arrow::RecordBatch>& batch, std::string path) {
  std::shared_ptr<arrow::io::OutputStream> stream;
  std::shared_ptr<arrow::ipc::RecordBatchWriter> file_writer;

  R_ERROR_NOT_OK(arrow::io::FileOutputStream::Open(path, &stream));
  R_ERROR_NOT_OK(arrow::ipc::RecordBatchFileWriter::Open(stream.get(), batch->schema(), &file_writer));
  R_ERROR_NOT_OK(file_writer->WriteRecordBatch(*batch, true));
  R_ERROR_NOT_OK(file_writer->Close());

  int64_t offset;
  R_ERROR_NOT_OK(stream->Tell(&offset));
  R_ERROR_NOT_OK(stream->Close());
  return offset;
}
