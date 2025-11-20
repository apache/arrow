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

#include "arrow/matlab/io/ipc/proxy/record_batch_file_reader.h"
#include "arrow/io/file.h"
#include "arrow/matlab/error/error.h"
#include "arrow/matlab/tabular/proxy/record_batch.h"
#include "arrow/matlab/tabular/proxy/schema.h"
#include "arrow/util/utf8.h"

#include "libmexclass/proxy/ProxyManager.h"

namespace arrow::matlab::io::ipc::proxy {

namespace {
libmexclass::error::Error makeInvalidNumericIndexError(const int32_t matlab_index,
                                                       const int32_t num_batches) {
  std::stringstream error_message_stream;
  error_message_stream << "Invalid record batch index: ";
  error_message_stream << matlab_index;
  error_message_stream
      << ". Record batch index must be between 1 and the number of record batches (";
  error_message_stream << num_batches;
  error_message_stream << ").";
  return libmexclass::error::Error{error::IPC_RECORD_BATCH_READ_INVALID_INDEX,
                                   error_message_stream.str()};
}
}  // namespace

RecordBatchFileReader::RecordBatchFileReader(
    const std::shared_ptr<arrow::ipc::RecordBatchFileReader> reader)
    : reader{std::move(reader)} {
  REGISTER_METHOD(RecordBatchFileReader, getNumRecordBatches);
  REGISTER_METHOD(RecordBatchFileReader, getSchema);
  REGISTER_METHOD(RecordBatchFileReader, readRecordBatchAtIndex);
}

libmexclass::proxy::MakeResult RecordBatchFileReader::make(
    const libmexclass::proxy::FunctionArguments& constructor_arguments) {
  namespace mda = ::matlab::data;
  using RecordBatchFileReaderProxy = arrow::matlab::io::ipc::proxy::RecordBatchFileReader;

  const mda::StructArray opts = constructor_arguments[0];

  const mda::StringArray filename_mda = opts[0]["Filename"];
  const auto filename_utf16 = std::u16string(filename_mda[0]);
  MATLAB_ASSIGN_OR_ERROR(const auto filename_utf8,
                         arrow::util::UTF16StringToUTF8(filename_utf16),
                         error::UNICODE_CONVERSION_ERROR_ID);

  MATLAB_ASSIGN_OR_ERROR(auto input_stream, arrow::io::ReadableFile::Open(filename_utf8),
                         error::FAILED_TO_OPEN_FILE_FOR_WRITE);

  MATLAB_ASSIGN_OR_ERROR(auto reader,
                         arrow::ipc::RecordBatchFileReader::Open(input_stream),
                         error::IPC_RECORD_BATCH_READER_OPEN_FAILED);

  return std::make_shared<RecordBatchFileReaderProxy>(std::move(reader));
}

void RecordBatchFileReader::getNumRecordBatches(
    libmexclass::proxy::method::Context& context) {
  namespace mda = ::matlab::data;

  mda::ArrayFactory factory;
  const auto num_batches = reader->num_record_batches();
  context.outputs[0] = factory.createScalar(num_batches);
}

void RecordBatchFileReader::getSchema(libmexclass::proxy::method::Context& context) {
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

void RecordBatchFileReader::readRecordBatchAtIndex(
    libmexclass::proxy::method::Context& context) {
  namespace mda = ::matlab::data;
  using RecordBatchProxy = arrow::matlab::tabular::proxy::RecordBatch;

  mda::StructArray opts = context.inputs[0];
  const mda::TypedArray<int32_t> matlab_index_mda = opts[0]["Index"];

  const auto matlab_index = matlab_index_mda[0];
  const auto num_record_batches = reader->num_record_batches();
  if (matlab_index < 1 || matlab_index > num_record_batches) {
    context.error = makeInvalidNumericIndexError(matlab_index, num_record_batches);
    return;
  }
  const auto arrow_index = matlab_index - 1;

  MATLAB_ASSIGN_OR_ERROR_WITH_CONTEXT(const auto record_batch,
                                      reader->ReadRecordBatch(arrow_index), context,
                                      error::IPC_RECORD_BATCH_READ_FAILED);

  auto record_batch_proxy = std::make_shared<RecordBatchProxy>(std::move(record_batch));
  const auto record_batch_proxy_id =
      libmexclass::proxy::ProxyManager::manageProxy(record_batch_proxy);

  mda::ArrayFactory factory;
  const auto record_batch_proxyy_id_mda = factory.createScalar(record_batch_proxy_id);
  context.outputs[0] = record_batch_proxyy_id_mda;
}

}  // namespace arrow::matlab::io::ipc::proxy