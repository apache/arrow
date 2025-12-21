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

#include "arrow/matlab/io/ipc/proxy/record_batch_stream_reader.h"
#include "arrow/io/file.h"
#include "arrow/io/memory.h"
#include "arrow/matlab/buffer/matlab_buffer.h"
#include "arrow/matlab/error/error.h"
#include "arrow/matlab/tabular/proxy/record_batch.h"
#include "arrow/matlab/tabular/proxy/schema.h"
#include "arrow/matlab/tabular/proxy/table.h"
#include "arrow/util/utf8.h"

#include "libmexclass/proxy/ProxyManager.h"

namespace arrow::matlab::io::ipc::proxy {

RecordBatchStreamReader::RecordBatchStreamReader(
    const std::shared_ptr<arrow::ipc::RecordBatchStreamReader> reader)
    : reader{std::move(reader)} {
  REGISTER_METHOD(RecordBatchStreamReader, getSchema);
  REGISTER_METHOD(RecordBatchStreamReader, readRecordBatch);
  REGISTER_METHOD(RecordBatchStreamReader, hasNextRecordBatch);
  REGISTER_METHOD(RecordBatchStreamReader, readTable);
}

libmexclass::proxy::MakeResult RecordBatchStreamReader::fromFile(
    const libmexclass::proxy::FunctionArguments& constructor_arguments) {
  namespace mda = ::matlab::data;
  using RecordBatchStreamReaderProxy =
      arrow::matlab::io::ipc::proxy::RecordBatchStreamReader;

  const mda::StructArray opts = constructor_arguments[0];
  const mda::StringArray filename_mda = opts[0]["Filename"];
  const auto filename_utf16 = std::u16string(filename_mda[0]);
  MATLAB_ASSIGN_OR_ERROR(const auto filename_utf8,
                         arrow::util::UTF16StringToUTF8(filename_utf16),
                         error::UNICODE_CONVERSION_ERROR_ID);

  MATLAB_ASSIGN_OR_ERROR(auto input_stream, arrow::io::ReadableFile::Open(filename_utf8),
                         error::FAILED_TO_OPEN_FILE_FOR_READ);

  MATLAB_ASSIGN_OR_ERROR(auto reader,
                         arrow::ipc::RecordBatchStreamReader::Open(input_stream),
                         error::IPC_RECORD_BATCH_READER_OPEN_FAILED);

  return std::make_shared<RecordBatchStreamReaderProxy>(std::move(reader));
}

libmexclass::proxy::MakeResult RecordBatchStreamReader::fromBytes(
    const libmexclass::proxy::FunctionArguments& constructor_arguments) {
  namespace mda = ::matlab::data;
  using RecordBatchStreamReaderProxy =
      arrow::matlab::io::ipc::proxy::RecordBatchStreamReader;

  const mda::StructArray opts = constructor_arguments[0];
  const ::matlab::data::TypedArray<uint8_t> bytes_mda = opts[0]["Bytes"];
  const auto matlab_buffer =
      std::make_shared<arrow::matlab::buffer::MatlabBuffer>(bytes_mda);
  auto buffer_reader = std::make_shared<arrow::io::BufferReader>(matlab_buffer);
  MATLAB_ASSIGN_OR_ERROR(auto reader,
                         arrow::ipc::RecordBatchStreamReader::Open(buffer_reader),
                         error::IPC_RECORD_BATCH_READER_OPEN_FAILED);
  return std::make_shared<RecordBatchStreamReaderProxy>(std::move(reader));
}

libmexclass::proxy::MakeResult RecordBatchStreamReader::make(
    const libmexclass::proxy::FunctionArguments& constructor_arguments) {
  namespace mda = ::matlab::data;
  const mda::StructArray opts = constructor_arguments[0];

  // Dispatch to the appropriate static "make" method depending
  // on the input type.
  const mda::StringArray type_mda = opts[0]["Type"];
  const auto type_utf16 = std::u16string(type_mda[0]);
  if (type_utf16 == u"Bytes") {
    return RecordBatchStreamReader::fromBytes(constructor_arguments);
  } else if (type_utf16 == u"File") {
    return RecordBatchStreamReader::fromFile(constructor_arguments);
  } else {
    return libmexclass::error::Error{
        "arrow:io:ipc:InvalidConstructionType",
        "Invalid construction type for RecordBatchStreamReader."};
  }
}

void RecordBatchStreamReader::getSchema(libmexclass::proxy::method::Context& context) {
  namespace mda = ::matlab::data;
  using SchemaProxy = arrow::matlab::tabular::proxy::Schema;

  auto schema = reader->schema();

  auto schema_proxy = std::make_shared<SchemaProxy>(std::move(schema));
  const auto schema_proxy_id =
      libmexclass::proxy::ProxyManager::manageProxy(schema_proxy);

  mda::ArrayFactory factory;
  const auto schema_proxy_id_mda = factory.createScalar(schema_proxy_id);
  context.outputs[0] = schema_proxy_id_mda;
}

void RecordBatchStreamReader::readTable(libmexclass::proxy::method::Context& context) {
  namespace mda = ::matlab::data;
  using TableProxy = arrow::matlab::tabular::proxy::Table;

  MATLAB_ASSIGN_OR_ERROR_WITH_CONTEXT(auto table, reader->ToTable(), context,
                                      error::IPC_TABLE_READ_FAILED);
  auto table_proxy = std::make_shared<TableProxy>(table);
  const auto table_proxy_id = libmexclass::proxy::ProxyManager::manageProxy(table_proxy);

  mda::ArrayFactory factory;
  const auto table_proxy_id_mda = factory.createScalar(table_proxy_id);
  context.outputs[0] = table_proxy_id_mda;
}

void RecordBatchStreamReader::readRecordBatch(
    libmexclass::proxy::method::Context& context) {
  namespace mda = ::matlab::data;
  using RecordBatchProxy = arrow::matlab::tabular::proxy::RecordBatch;
  using namespace libmexclass::error;
  // If we don't have a "pre-cached" record batch to return, then try reading another
  // record batch from the IPC Stream. If there are no more record batches in the stream,
  // then error.
  if (!nextRecordBatch) {
    MATLAB_ASSIGN_OR_ERROR_WITH_CONTEXT(nextRecordBatch, reader->Next(), context,
                                        error::IPC_RECORD_BATCH_READ_FAILED);
  }
  // Even if the read was "successful", the resulting record batch may be empty,
  // signaling the end of the stream.
  if (!nextRecordBatch) {
    context.error =
        Error{error::IPC_END_OF_STREAM,
              "Reached end of Arrow IPC Stream. No more record batches to read."};
    return;
  }
  auto record_batch_proxy = std::make_shared<RecordBatchProxy>(nextRecordBatch);
  const auto record_batch_proxy_id =
      libmexclass::proxy::ProxyManager::manageProxy(record_batch_proxy);
  // Once we have "consumed" the next RecordBatch, set nextRecordBatch to nullptr
  // so that the next call to hasNextRecordBatch correctly checks whether there are more
  // record batches remaining in the IPC Stream.
  nextRecordBatch = nullptr;
  mda::ArrayFactory factory;
  const auto record_batch_proxy_id_mda = factory.createScalar(record_batch_proxy_id);
  context.outputs[0] = record_batch_proxy_id_mda;
}

void RecordBatchStreamReader::hasNextRecordBatch(
    libmexclass::proxy::method::Context& context) {
  namespace mda = ::matlab::data;
  bool has_next_record_batch = true;
  if (!nextRecordBatch) {
    // Try to read another RecordBatch from the
    // IPC Stream.
    auto maybe_record_batch = reader->Next();
    if (!maybe_record_batch.ok()) {
      has_next_record_batch = false;
    } else {
      // If we read a RecordBatch successfully,
      // then "cache" the RecordBatch
      // so that we can return it on the next
      // call to readRecordBatch.
      nextRecordBatch = *maybe_record_batch;

      // Even if the read was "successful", the resulting
      // record batch may be empty, signaling that
      // the end of the IPC stream has been reached.
      if (!nextRecordBatch) {
        has_next_record_batch = false;
      }
    }
  }

  mda::ArrayFactory factory;
  context.outputs[0] = factory.createScalar(has_next_record_batch);
}

}  // namespace arrow::matlab::io::ipc::proxy
