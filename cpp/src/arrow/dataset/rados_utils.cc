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

#include "arrow/dataset/rados_utils.h"

#include <iostream>
#include <vector>

namespace arrow {
namespace dataset {

Status int64_to_char(uint8_t *num_buffer, int64_t num) {
  BasicDecimal128 decimal(num);
  decimal.ToBytes(num_buffer);
  return Status::OK();
}

Status char_to_int64(uint8_t *num_buffer, int64_t &num) {
  BasicDecimal128 basic_decimal(num_buffer);
  Decimal128 decimal(basic_decimal);
  num = (int64_t)decimal;
  return Status::OK();
}

Status serialize_scan_request_to_bufferlist(std::shared_ptr<Expression> filter, std::shared_ptr<Schema> schema, librados::bufferlist &bl) {
  ARROW_ASSIGN_OR_RAISE(auto filter_buffer, filter->Serialize());
  ARROW_ASSIGN_OR_RAISE(auto schema_buffer, ipc::SerializeSchema(*schema));

  char *filter_size_buffer = new char[8];
  ARROW_RETURN_NOT_OK(int64_to_char((uint8_t*)filter_size_buffer, filter_buffer->size()));

  char *schema_size_buffer = new char[8];
  ARROW_RETURN_NOT_OK(int64_to_char((uint8_t*)schema_size_buffer, schema_buffer->size()));

  bl.append(filter_size_buffer, 8);
  bl.append((char*)filter_buffer->data(), filter_buffer->size());

  bl.append(schema_size_buffer, 8);
  bl.append((char*)schema_buffer->data(), schema_buffer->size());

  return Status::OK();
}

Status deserialize_scan_request_from_bufferlist(std::shared_ptr<Expression> *filter, std::shared_ptr<Schema> *schema, librados::bufferlist bl) {
  int64_t filter_size = 0;
  char filter_size_buffer[8];
  bl.begin(0).copy(8, filter_size_buffer);
  ARROW_RETURN_NOT_OK(char_to_int64((uint8_t*)filter_size_buffer, filter_size));

  char *filter_buffer = new char[filter_size];
  bl.begin(8).copy(filter_size, filter_buffer);

  int64_t schema_size = 0;
  char schema_size_buffer[8];
  bl.begin(8 + filter_size).copy(8, schema_size_buffer);
  ARROW_RETURN_NOT_OK(char_to_int64((uint8_t*)schema_size_buffer, schema_size));

  char *schema_buffer = new char[schema_size];
  bl.begin(16 + filter_size).copy(schema_size, schema_buffer);

  ARROW_ASSIGN_OR_RAISE(auto filter_, Expression::Deserialize(Buffer((uint8_t*)filter_buffer, filter_size)));
  *filter = filter_;

  ipc::DictionaryMemo empty_memo;
  io::BufferReader schema_reader((uint8_t*)schema_buffer, schema_size);

  ARROW_ASSIGN_OR_RAISE(auto schema_, ipc::ReadSchema(&schema_reader, &empty_memo));
  *schema = schema_;

  return Status::OK();
}

Status read_table_from_bufferlist(std::shared_ptr<Table> *table, librados::bufferlist bl) {
  io::BufferReader reader((uint8_t*)bl.c_str(), bl.length());
  ARROW_ASSIGN_OR_RAISE(auto record_batch_reader, ipc::RecordBatchStreamReader::Open(&reader));
  ARROW_ASSIGN_OR_RAISE(auto table_, Table::FromRecordBatchReader(record_batch_reader.get()));
  *table = table_;
  return Status::OK();
}

Status write_table_to_bufferlist(std::shared_ptr<Table> &table, librados::bufferlist &bl) {
  ARROW_ASSIGN_OR_RAISE(auto buffer_output_stream, io::BufferOutputStream::Create());
  const auto options = ipc::IpcWriteOptions::Defaults();
  ARROW_ASSIGN_OR_RAISE(auto writer, ipc::NewStreamWriter(buffer_output_stream.get(), table->schema(), options));

  writer->WriteTable(*table);
  writer->Close();

  ARROW_ASSIGN_OR_RAISE(auto buffer, buffer_output_stream->Finish());
  bl.append((char*)buffer->data(), buffer->size());
  return Status::OK();
}

Status scan_batches(std::shared_ptr<Expression> &filter, std::shared_ptr<Schema> &schema, RecordBatchVector &batches, std::shared_ptr<Table> *table) {
  std::shared_ptr<ScanContext> scan_context = std::make_shared<ScanContext>();
  std::shared_ptr<InMemoryFragment> fragment = std::make_shared<InMemoryFragment>(batches);
  auto batch_schema = batches[0]->schema();
  std::shared_ptr<ScannerBuilder> builder = std::make_shared<ScannerBuilder>(batch_schema, fragment, scan_context);
  ARROW_RETURN_NOT_OK(builder->Filter(filter));
  ARROW_RETURN_NOT_OK(builder->Project(schema->field_names()));
  ARROW_ASSIGN_OR_RAISE(auto scanner, builder->Finish());
  ARROW_ASSIGN_OR_RAISE(auto table_, scanner->ToTable());

  *table = table_;
  return Status::OK();
}

Status extract_batches_from_bufferlist(RecordBatchVector *batches, librados::bufferlist &bl) {
  std::shared_ptr<Buffer> buffer = std::make_shared<Buffer>((uint8_t*)bl.c_str(), bl.length());
  std::shared_ptr<io::BufferReader> buffer_reader = std::make_shared<io::BufferReader>(buffer);
  ARROW_ASSIGN_OR_RAISE(auto record_batch_reader, ipc::RecordBatchStreamReader::Open(buffer_reader));
  ARROW_RETURN_NOT_OK(record_batch_reader->ReadAll(batches));
  return Status::OK();
}

}  // namespace dataset
}  // namespace arrow