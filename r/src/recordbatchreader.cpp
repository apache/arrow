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

namespace cpp11 {
template <> std::string r6_class_name<arrow::ipc::RecordBatchStreamReader>(const std::shared_ptr<arrow::ipc::RecordBatchStreamReader>& x) {
  return "RecordBatchStreamReader";
}
template <> std::string r6_class_name<arrow::ipc::RecordBatchFileReader>(const std::shared_ptr<arrow::ipc::RecordBatchFileReader>& x) {
  return "RecordBatchFileReader";
}
}

// [[arrow::export]]
R6 RecordBatchReader__schema(const std::shared_ptr<arrow::RecordBatchReader>& reader) {
  return reader->schema();
}

// [[arrow::export]]
R6 RecordBatchReader__ReadNext(const std::shared_ptr<arrow::RecordBatchReader>& reader) {
  std::shared_ptr<arrow::RecordBatch> batch;
  StopIfNotOk(reader->ReadNext(&batch));
  return batch;
}

// -------- RecordBatchStreamReader

// [[arrow::export]]
R6 ipc___RecordBatchStreamReader__Open(
    const std::shared_ptr<arrow::io::InputStream>& stream) {
  return ValueOrStop(arrow::ipc::RecordBatchStreamReader::Open(stream));
}

// [[arrow::export]]
cpp11::list ipc___RecordBatchStreamReader__batches(
    const std::shared_ptr<arrow::ipc::RecordBatchStreamReader>& reader) {
  std::vector<std::shared_ptr<arrow::RecordBatch>> res;

  while (true) {
    std::shared_ptr<arrow::RecordBatch> batch;
    StopIfNotOk(reader->ReadNext(&batch));
    if (!batch) break;

    res.push_back(batch);
  }

  return arrow::r::to_r_list(res, cpp11::to_r6<arrow::RecordBatch>);
}

// -------- RecordBatchFileReader

// [[arrow::export]]
R6 ipc___RecordBatchFileReader__schema(
    const std::shared_ptr<arrow::ipc::RecordBatchFileReader>& reader) {
  return reader->schema();
}

// [[arrow::export]]
int ipc___RecordBatchFileReader__num_record_batches(
    const std::shared_ptr<arrow::ipc::RecordBatchFileReader>& reader) {
  return reader->num_record_batches();
}

// [[arrow::export]]
R6 ipc___RecordBatchFileReader__ReadRecordBatch(
    const std::shared_ptr<arrow::ipc::RecordBatchFileReader>& reader, int i) {
  if (i < 0 && i >= reader->num_record_batches()) {
    cpp11::stop("Record batch index out of bounds");
  }
  return ValueOrStop(reader->ReadRecordBatch(i));
}

// [[arrow::export]]
R6 ipc___RecordBatchFileReader__Open(
    const std::shared_ptr<arrow::io::RandomAccessFile>& file) {
  return ValueOrStop(arrow::ipc::RecordBatchFileReader::Open(file));
}

// [[arrow::export]]
R6 Table__from_RecordBatchFileReader(
    const std::shared_ptr<arrow::ipc::RecordBatchFileReader>& reader) {
  int num_batches = reader->num_record_batches();
  std::vector<std::shared_ptr<arrow::RecordBatch>> batches(num_batches);
  for (int i = 0; i < num_batches; i++) {
    batches[i] = ValueOrStop(reader->ReadRecordBatch(i));
  }

  return ValueOrStop(arrow::Table::FromRecordBatches(std::move(batches)));
}

// [[arrow::export]]
R6 Table__from_RecordBatchStreamReader(
    const std::shared_ptr<arrow::ipc::RecordBatchStreamReader>& reader) {
  std::shared_ptr<arrow::RecordBatch> batch;
  std::vector<std::shared_ptr<arrow::RecordBatch>> batches;
  while (true) {
    StopIfNotOk(reader->ReadNext(&batch));
    if (!batch) break;
    batches.push_back(batch);
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

  return arrow::r::to_r_list(res, cpp11::to_r6<arrow::RecordBatch>);
}

#endif
