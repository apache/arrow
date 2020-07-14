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
#include <arrow/ipc/writer.h>

RCPP_EXPOSED_ENUM_NODECL(arrow::ipc::MetadataVersion)

// [[arrow::export]]
void ipc___RecordBatchWriter__WriteRecordBatch(
    const std::shared_ptr<arrow::ipc::RecordBatchWriter>& batch_writer,
    const std::shared_ptr<arrow::RecordBatch>& batch) {
  StopIfNotOk(batch_writer->WriteRecordBatch(*batch));
}

// [[arrow::export]]
void ipc___RecordBatchWriter__WriteTable(
    const std::shared_ptr<arrow::ipc::RecordBatchWriter>& batch_writer,
    const std::shared_ptr<arrow::Table>& table) {
  StopIfNotOk(batch_writer->WriteTable(*table));
}

// [[arrow::export]]
void ipc___RecordBatchWriter__Close(
    const std::shared_ptr<arrow::ipc::RecordBatchWriter>& batch_writer) {
  StopIfNotOk(batch_writer->Close());
}

// [[arrow::export]]
std::shared_ptr<arrow::ipc::RecordBatchWriter> ipc___RecordBatchFileWriter__Open(
    const std::shared_ptr<arrow::io::OutputStream>& stream,
    const std::shared_ptr<arrow::Schema>& schema, bool use_legacy_format,
    arrow::ipc::MetadataVersion metadata_version) {
  auto options = arrow::ipc::IpcWriteOptions::Defaults();
  options.write_legacy_ipc_format = use_legacy_format;
  options.metadata_version = metadata_version;
  return ValueOrStop(arrow::ipc::NewFileWriter(stream.get(), schema, options));
}

// [[arrow::export]]
std::shared_ptr<arrow::ipc::RecordBatchWriter> ipc___RecordBatchStreamWriter__Open(
    const std::shared_ptr<arrow::io::OutputStream>& stream,
    const std::shared_ptr<arrow::Schema>& schema, bool use_legacy_format,
    arrow::ipc::MetadataVersion metadata_version) {
  auto options = arrow::ipc::IpcWriteOptions::Defaults();
  options.write_legacy_ipc_format = use_legacy_format;
  options.metadata_version = metadata_version;
  return ValueOrStop(NewStreamWriter(stream.get(), schema, options));
}

#endif
