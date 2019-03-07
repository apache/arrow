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

// [[Rcpp::export]]
std::shared_ptr<arrow::Schema> RecordBatchReader__schema(
    const std::shared_ptr<arrow::RecordBatchReader>& reader) {
  return reader->schema();
}

// [[Rcpp::export]]
std::shared_ptr<arrow::RecordBatch> RecordBatchReader__ReadNext(
    const std::shared_ptr<arrow::RecordBatchReader>& reader) {
  std::shared_ptr<arrow::RecordBatch> batch;
  STOP_IF_NOT_OK(reader->ReadNext(&batch));
  return batch;
}

// -------- RecordBatchStreamReader

// [[Rcpp::export]]
std::shared_ptr<arrow::RecordBatchReader> ipc___RecordBatchStreamReader__Open(
    const std::shared_ptr<arrow::io::InputStream>& stream) {
  std::shared_ptr<arrow::RecordBatchReader> reader;
  STOP_IF_NOT_OK(arrow::ipc::RecordBatchStreamReader::Open(stream, &reader));
  return reader;
}

// [[Rcpp::export]]
std::vector<std::shared_ptr<arrow::RecordBatch>> ipc___RecordBatchStreamReader__batches(
    const std::shared_ptr<arrow::ipc::RecordBatchStreamReader>& reader) {
  std::vector<std::shared_ptr<arrow::RecordBatch>> res;

  while (true) {
    std::shared_ptr<arrow::RecordBatch> batch;
    STOP_IF_NOT_OK(reader->ReadNext(&batch));
    if (!batch) break;

    res.push_back(batch);
  }

  return res;
}

// -------- RecordBatchFileReader

// [[Rcpp::export]]
std::shared_ptr<arrow::Schema> ipc___RecordBatchFileReader__schema(
    const std::shared_ptr<arrow::ipc::RecordBatchFileReader>& reader) {
  return reader->schema();
}

// [[Rcpp::export]]
int ipc___RecordBatchFileReader__num_record_batches(
    const std::shared_ptr<arrow::ipc::RecordBatchFileReader>& reader) {
  return reader->num_record_batches();
}

// [[Rcpp::export]]
std::shared_ptr<arrow::RecordBatch> ipc___RecordBatchFileReader__ReadRecordBatch(
    const std::shared_ptr<arrow::ipc::RecordBatchFileReader>& reader, int i) {
  std::shared_ptr<arrow::RecordBatch> batch;
  if (i >= 0 && i < reader->num_record_batches()) {
    STOP_IF_NOT_OK(reader->ReadRecordBatch(i, &batch));
  }
  return batch;
}

// [[Rcpp::export]]
std::shared_ptr<arrow::ipc::RecordBatchFileReader> ipc___RecordBatchFileReader__Open(
    const std::shared_ptr<arrow::io::RandomAccessFile>& file) {
  std::shared_ptr<arrow::ipc::RecordBatchFileReader> reader;
  STOP_IF_NOT_OK(arrow::ipc::RecordBatchFileReader::Open(file, &reader));
  return reader;
}

// [[Rcpp::export]]
std::shared_ptr<arrow::Table> Table__from_RecordBatchFileReader(
    const std::shared_ptr<arrow::ipc::RecordBatchFileReader>& reader) {
  int num_batches = reader->num_record_batches();
  std::vector<std::shared_ptr<arrow::RecordBatch>> batches(num_batches);
  for (int i = 0; i < num_batches; i++) {
    STOP_IF_NOT_OK(reader->ReadRecordBatch(i, &batches[i]));
  }

  std::shared_ptr<arrow::Table> table;
  STOP_IF_NOT_OK(arrow::Table::FromRecordBatches(std::move(batches), &table));

  return table;
}

// [[Rcpp::export]]
std::shared_ptr<arrow::Table> Table__from_RecordBatchStreamReader(
    const std::shared_ptr<arrow::ipc::RecordBatchStreamReader>& reader) {
  std::shared_ptr<arrow::RecordBatch> batch;
  std::vector<std::shared_ptr<arrow::RecordBatch>> batches;
  while (true) {
    STOP_IF_NOT_OK(reader->ReadNext(&batch));
    if (!batch) break;
    batches.push_back(batch);
  }

  std::shared_ptr<arrow::Table> table;
  STOP_IF_NOT_OK(arrow::Table::FromRecordBatches(std::move(batches), &table));

  return table;
}

// [[Rcpp::export]]
std::vector<std::shared_ptr<arrow::RecordBatch>> ipc___RecordBatchFileReader__batches(
    const std::shared_ptr<arrow::ipc::RecordBatchFileReader>& reader) {
  auto n = reader->num_record_batches();
  std::vector<std::shared_ptr<arrow::RecordBatch>> res(n);

  for (int i = 0; i < n; i++) {
    STOP_IF_NOT_OK(reader->ReadRecordBatch(i, &res[i]));
  }

  return res;
}
