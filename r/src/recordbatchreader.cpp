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
#include <arrow/ipc/reader.h>
#include <arrow/table.h>

// [[arrow::export]]
std::shared_ptr<arrow::Schema> RecordBatchReader__schema(
    const std::shared_ptr<arrow::RecordBatchReader>& reader) {
  return reader->schema();
}

// [[arrow::export]]
std::shared_ptr<arrow::RecordBatch> RecordBatchReader__ReadNext(
    const std::shared_ptr<arrow::RecordBatchReader>& reader) {
  std::shared_ptr<arrow::RecordBatch> batch;
  StopIfNotOk(reader->ReadNext(&batch));
  return batch;
}

// [[arrow::export]]
cpp11::list RecordBatchReader__batches(
    const std::shared_ptr<arrow::RecordBatchReader>& reader) {
  return arrow::r::to_r_list(ValueOrStop(reader->ToRecordBatches()));
}

// [[arrow::export]]
std::shared_ptr<arrow::Table> Table__from_RecordBatchReader(
    const std::shared_ptr<arrow::RecordBatchReader>& reader) {
  return ValueOrStop(reader->ToTable());
}

// [[arrow::export]]
std::shared_ptr<arrow::Table> RecordBatchReader__Head(
    const std::shared_ptr<arrow::RecordBatchReader>& reader, int64_t num_rows) {
  std::vector<std::shared_ptr<arrow::RecordBatch>> batches;
  std::shared_ptr<arrow::RecordBatch> this_batch;
  while (num_rows > 0) {
    this_batch = ValueOrStop(reader->Next());
    if (this_batch == nullptr) break;
    batches.push_back(this_batch->Slice(0, num_rows));
    num_rows -= this_batch->num_rows();
  }
  return ValueOrStop(arrow::Table::FromRecordBatches(reader->schema(), batches));
}

// -------- RecordBatchStreamReader

// [[arrow::export]]
std::shared_ptr<arrow::ipc::RecordBatchStreamReader> ipc___RecordBatchStreamReader__Open(
    const std::shared_ptr<arrow::io::InputStream>& stream) {
  auto options = arrow::ipc::IpcReadOptions::Defaults();
  options.memory_pool = gc_memory_pool();
  return ValueOrStop(arrow::ipc::RecordBatchStreamReader::Open(stream, options));
}

// -------- RecordBatchFileReader

// [[arrow::export]]
std::shared_ptr<arrow::Schema> ipc___RecordBatchFileReader__schema(
    const std::shared_ptr<arrow::ipc::RecordBatchFileReader>& reader) {
  return reader->schema();
}

// [[arrow::export]]
int ipc___RecordBatchFileReader__num_record_batches(
    const std::shared_ptr<arrow::ipc::RecordBatchFileReader>& reader) {
  return reader->num_record_batches();
}

// [[arrow::export]]
std::shared_ptr<arrow::RecordBatch> ipc___RecordBatchFileReader__ReadRecordBatch(
    const std::shared_ptr<arrow::ipc::RecordBatchFileReader>& reader, int i) {
  if (i < 0 && i >= reader->num_record_batches()) {
    cpp11::stop("Record batch index out of bounds");
  }
  return ValueOrStop(reader->ReadRecordBatch(i));
}

// [[arrow::export]]
std::shared_ptr<arrow::ipc::RecordBatchFileReader> ipc___RecordBatchFileReader__Open(
    const std::shared_ptr<arrow::io::RandomAccessFile>& file) {
  auto options = arrow::ipc::IpcReadOptions::Defaults();
  options.memory_pool = gc_memory_pool();
  return ValueOrStop(arrow::ipc::RecordBatchFileReader::Open(file, options));
}

// [[arrow::export]]
std::shared_ptr<arrow::Table> Table__from_RecordBatchFileReader(
    const std::shared_ptr<arrow::ipc::RecordBatchFileReader>& reader) {
  // RecordBatchStreamReader inherits from RecordBatchReader
  // but RecordBatchFileReader apparently does not
  int num_batches = reader->num_record_batches();
  std::vector<std::shared_ptr<arrow::RecordBatch>> batches(num_batches);
  for (int i = 0; i < num_batches; i++) {
    batches[i] = ValueOrStop(reader->ReadRecordBatch(i));
  }

  return ValueOrStop(arrow::Table::FromRecordBatches(std::move(batches)));
}

// [[arrow::export]]
cpp11::list ipc___RecordBatchFileReader__batches(
    const std::shared_ptr<arrow::ipc::RecordBatchFileReader>& reader) {
  auto n = reader->num_record_batches();
  std::vector<std::shared_ptr<arrow::RecordBatch>> res(n);

  for (int i = 0; i < n; i++) {
    res[i] = ValueOrStop(reader->ReadRecordBatch(i));
  }

  return arrow::r::to_r_list(res);
}

#endif
