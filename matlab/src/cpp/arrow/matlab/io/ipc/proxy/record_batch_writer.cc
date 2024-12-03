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

#include "arrow/matlab/io/ipc/proxy/record_batch_writer.h"
#include "arrow/io/file.h"
#include "arrow/matlab/error/error.h"
#include "arrow/matlab/tabular/proxy/record_batch.h"
#include "arrow/matlab/tabular/proxy/schema.h"
#include "arrow/matlab/tabular/proxy/table.h"

#include "libmexclass/proxy/ProxyManager.h"

namespace arrow::matlab::io::ipc::proxy {

RecordBatchWriter::RecordBatchWriter(
    const std::shared_ptr<arrow::ipc::RecordBatchWriter> writer)
    : writer{std::move(writer)} {
  REGISTER_METHOD(RecordBatchWriter, close);
  REGISTER_METHOD(RecordBatchWriter, writeRecordBatch);
  REGISTER_METHOD(RecordBatchWriter, writeTable);
}

void RecordBatchWriter::writeRecordBatch(libmexclass::proxy::method::Context& context) {
  namespace mda = ::matlab::data;
  using RecordBatchProxy = ::arrow::matlab::tabular::proxy::RecordBatch;

  mda::StructArray opts = context.inputs[0];
  const mda::TypedArray<uint64_t> record_batch_proxy_id_mda =
      opts[0]["RecordBatchProxyID"];
  const uint64_t record_batch_proxy_id = record_batch_proxy_id_mda[0];

  auto proxy = libmexclass::proxy::ProxyManager::getProxy(record_batch_proxy_id);
  auto record_batch_proxy = std::static_pointer_cast<RecordBatchProxy>(proxy);
  auto record_batch = record_batch_proxy->unwrap();

  MATLAB_ERROR_IF_NOT_OK_WITH_CONTEXT(writer->WriteRecordBatch(*record_batch), context,
                                      error::IPC_RECORD_BATCH_WRITE_FAILED);
}

void RecordBatchWriter::writeTable(libmexclass::proxy::method::Context& context) {
  namespace mda = ::matlab::data;
  using TableProxy = ::arrow::matlab::tabular::proxy::Table;

  mda::StructArray opts = context.inputs[0];
  const mda::TypedArray<uint64_t> table_proxy_id_mda = opts[0]["TableProxyID"];
  const uint64_t table_proxy_id = table_proxy_id_mda[0];

  auto proxy = libmexclass::proxy::ProxyManager::getProxy(table_proxy_id);
  auto table_proxy = std::static_pointer_cast<TableProxy>(proxy);
  auto table = table_proxy->unwrap();

  MATLAB_ERROR_IF_NOT_OK_WITH_CONTEXT(writer->WriteTable(*table), context,
                                      error::IPC_RECORD_BATCH_WRITE_FAILED);
}

void RecordBatchWriter::close(libmexclass::proxy::method::Context& context) {
  MATLAB_ERROR_IF_NOT_OK_WITH_CONTEXT(writer->Close(), context,
                                      error::IPC_RECORD_BATCH_WRITE_CLOSE_FAILED);
}

}  // namespace arrow::matlab::io::ipc::proxy
