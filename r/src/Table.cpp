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
#include "arrow_types.h"

using namespace Rcpp;
using namespace arrow;

// [[Rcpp::export]]
std::shared_ptr<arrow::Table> Table__from_dataframe(DataFrame tbl) {
  auto rb = RecordBatch__from_dataframe(tbl);

  std::shared_ptr<arrow::Table> out;
  R_ERROR_NOT_OK(arrow::Table::FromRecordBatches({std::move(rb)}, &out));
  return out;
}

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
int Table__to_file(const std::shared_ptr<arrow::Table>& table, std::string path) {
  std::shared_ptr<arrow::io::OutputStream> stream;
  std::shared_ptr<arrow::ipc::RecordBatchWriter> file_writer;

  R_ERROR_NOT_OK(arrow::io::FileOutputStream::Open(path, &stream));
  R_ERROR_NOT_OK(arrow::ipc::RecordBatchFileWriter::Open(stream.get(), table->schema(),
                                                         &file_writer));
  R_ERROR_NOT_OK(file_writer->WriteTable(*table));
  R_ERROR_NOT_OK(file_writer->Close());

  int64_t offset;
  R_ERROR_NOT_OK(stream->Tell(&offset));
  R_ERROR_NOT_OK(stream->Close());
  return offset;
}

// [[Rcpp::export]]
std::shared_ptr<arrow::Table> read_table_(std::string path) {
  std::shared_ptr<arrow::io::ReadableFile> stream;
  std::shared_ptr<arrow::ipc::RecordBatchFileReader> rbf_reader;

  R_ERROR_NOT_OK(arrow::io::ReadableFile::Open(path, &stream));
  R_ERROR_NOT_OK(arrow::ipc::RecordBatchFileReader::Open(stream, &rbf_reader));

  int num_batches = rbf_reader->num_record_batches();
  std::vector<std::shared_ptr<arrow::RecordBatch>> batches(num_batches);
  for (int i = 0; i < num_batches; i++) {
    R_ERROR_NOT_OK(rbf_reader->ReadRecordBatch(i, &batches[i]));
  }

  std::shared_ptr<arrow::Table> table;
  R_ERROR_NOT_OK(arrow::Table::FromRecordBatches(std::move(batches), &table));
  R_ERROR_NOT_OK(stream->Close());
  return table;
}

// [[Rcpp::export]]
List Table__to_dataframe(const std::shared_ptr<arrow::Table>& table) {
  int nc = table->num_columns();
  int nr = table->num_rows();
  List tbl(nc);
  CharacterVector names(nc);
  for (int i = 0; i < nc; i++) {
    auto column = table->column(i);
    tbl[i] = ChunkedArray__as_vector(column->data());
    names[i] = column->name();
  }
  tbl.attr("names") = names;
  tbl.attr("class") = CharacterVector::create("tbl_df", "tbl", "data.frame");
  tbl.attr("row.names") = IntegerVector::create(NA_INTEGER, -nr);
  return tbl;
}

// [[Rcpp::export]]
std::shared_ptr<arrow::Column> Table__column(const std::shared_ptr<arrow::Table>& table,
                                             int i) {
  return table->column(i);
}
