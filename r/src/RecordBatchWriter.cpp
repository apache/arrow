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

// [[Rcpp::export]]
std::shared_ptr<arrow::ipc::RecordBatchWriter> ipc___RecordBatchFileWriter__Open(
    const std::shared_ptr<arrow::io::OutputStream>& stream,
    const std::shared_ptr<arrow::Schema>& schema) {
  std::shared_ptr<arrow::ipc::RecordBatchWriter> file_writer;
  R_ERROR_NOT_OK(
      arrow::ipc::RecordBatchFileWriter::Open(stream.get(), schema, &file_writer));
  return file_writer;
}

// [[Rcpp::export]]
std::shared_ptr<arrow::ipc::RecordBatchWriter> ipc___RecordBatchStreamWriter__Open(
    const std::shared_ptr<arrow::io::OutputStream>& stream,
    const std::shared_ptr<arrow::Schema>& schema) {
  std::shared_ptr<arrow::ipc::RecordBatchWriter> stream_writer;
  R_ERROR_NOT_OK(
      arrow::ipc::RecordBatchStreamWriter::Open(stream.get(), schema, &stream_writer));
  return stream_writer;
}

// [[Rcpp::export]]
void ipc___RecordBatchWriter__WriteRecordBatch(
    const std::shared_ptr<arrow::ipc::RecordBatchWriter>& batch_writer,
    const std::shared_ptr<arrow::RecordBatch>& batch, bool allow_64bit) {
  R_ERROR_NOT_OK(batch_writer->WriteRecordBatch(*batch, allow_64bit));
}

// [[Rcpp::export]]
void ipc___RecordBatchWriter__WriteTable(
    const std::shared_ptr<arrow::ipc::RecordBatchWriter>& batch_writer,
    const std::shared_ptr<arrow::Table>& table) {
  R_ERROR_NOT_OK(batch_writer->WriteTable(*table));
}

// [[Rcpp::export]]
void ipc___RecordBatchWriter__Close(
    const std::shared_ptr<arrow::ipc::RecordBatchWriter>& batch_writer) {
  R_ERROR_NOT_OK(batch_writer->Close());
}
