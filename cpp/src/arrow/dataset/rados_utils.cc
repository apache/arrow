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

namespace arrow {
namespace dataset {

/// \brief A union for convertions between char buffer
/// and a 64-bit integer. The conversion always
/// happen in Little-Endian format.
union {
  int64_t integer_;
  char bytes_[8];
} converter_;

Status int64_to_char(char* buffer, int64_t num) {
  /// Pass the integer through the union to
  /// get the byte representation.
  num = BitUtil::ToLittleEndian(num);
  converter_.integer_ = num;
  memcpy(buffer, converter_.bytes_, 8);
  return Status::OK();
}

Status char_to_int64(char* buffer, int64_t& num) {
  /// Pass the byte representation through the union to
  /// get the integer.
  memcpy(converter_.bytes_, buffer, 8);
  num = BitUtil::ToLittleEndian(converter_.integer_);
  return Status::OK();
}

Status SerializeScanRequestToBufferlist(std::shared_ptr<Expression> filter,
                                        std::shared_ptr<Expression> partition_expression,
                                        std::shared_ptr<Schema> schema, int64_t format,
                                        librados::bufferlist& bl) {
  /// Serialize the filter Expression and the Schema.
  ARROW_ASSIGN_OR_RAISE(auto filter_buffer, filter->Serialize());
  ARROW_ASSIGN_OR_RAISE(auto partition_expression_buffer,
                        partition_expression->Serialize());
  ARROW_ASSIGN_OR_RAISE(auto schema_buffer, ipc::SerializeSchema(*schema));

  /// Convert filter Expression size to buffer.
  char* filter_size_buffer = new char[8];
  ARROW_RETURN_NOT_OK(int64_to_char(filter_size_buffer, filter_buffer->size()));

  /// Convert partition expression size to buffer.
  char* partition_expression_size_buffer = new char[8];
  ARROW_RETURN_NOT_OK(int64_to_char(partition_expression_size_buffer,
                                    partition_expression_buffer->size()));

  /// Convert Schema size to buffer.
  char* schema_size_buffer = new char[8];
  ARROW_RETURN_NOT_OK(int64_to_char(schema_size_buffer, schema_buffer->size()));

  char* format_buffer = new char[8];
  ARROW_RETURN_NOT_OK(int64_to_char(format_buffer, format));

  /// Append the filter Expression size.
  bl.append(filter_size_buffer, 8);
  /// Append the filter Expression data.
  bl.append((char*)filter_buffer->data(), filter_buffer->size());

  /// Append the partition Expression size.
  bl.append(partition_expression_size_buffer, 8);
  /// Append the partition Expression data.
  bl.append((char*)partition_expression_buffer->data(),
            partition_expression_buffer->size());

  /// Append the Schema size.
  bl.append(schema_size_buffer, 8);
  /// Append the Schema data.
  bl.append((char*)schema_buffer->data(), schema_buffer->size());

  bl.append(format_buffer, 8);

  return Status::OK();
}

Status DeserializeScanRequestFromBufferlist(std::shared_ptr<Expression>* filter,
                                            std::shared_ptr<Expression>* part_expr,
                                            std::shared_ptr<Schema>* schema,
                                            int64_t* format, librados::bufferlist& bl) {
  librados::bufferlist::iterator itr = bl.begin();

  int64_t filter_size = 0;
  char* filter_size_buffer = new char[8];
  itr.copy(8, filter_size_buffer);
  ARROW_RETURN_NOT_OK(char_to_int64(filter_size_buffer, filter_size));

  char* filter_buffer = new char[filter_size];
  itr.copy(filter_size, filter_buffer);

  int64_t part_expr_size = 0;
  char* part_expr_size_buffer = new char[8];
  itr.copy(8, part_expr_size_buffer);
  ARROW_RETURN_NOT_OK(char_to_int64(part_expr_size_buffer, part_expr_size));

  char* part_expr_buffer = new char[part_expr_size];
  itr.copy(part_expr_size, part_expr_buffer);

  int64_t schema_size = 0;
  char* schema_size_buffer = new char[8];
  itr.copy(8, schema_size_buffer);
  ARROW_RETURN_NOT_OK(char_to_int64(schema_size_buffer, schema_size));

  char* schema_buffer = new char[schema_size];
  itr.copy(schema_size, schema_buffer);

  int64_t format_ = 0;
  char* format_buffer = new char[8];
  itr.copy(8, format_buffer);
  ARROW_RETURN_NOT_OK(char_to_int64(format_buffer, format_));

  ARROW_ASSIGN_OR_RAISE(auto filter_, Expression::Deserialize(
                                          Buffer((uint8_t*)filter_buffer, filter_size)));
  *filter = filter_;

  ARROW_ASSIGN_OR_RAISE(
      auto part_expr_,
      Expression::Deserialize(Buffer((uint8_t*)part_expr_buffer, part_expr_size)));
  *part_expr = part_expr_;

  ipc::DictionaryMemo empty_memo;
  io::BufferReader schema_reader((uint8_t*)schema_buffer, schema_size);

  ARROW_ASSIGN_OR_RAISE(auto schema_, ipc::ReadSchema(&schema_reader, &empty_memo));
  *schema = schema_;

  *format = format_;

  return Status::OK();
}

Status SerializeTableToIPCStream(std::shared_ptr<Table>& table,
                                 librados::bufferlist& bl) {
  ARROW_ASSIGN_OR_RAISE(auto buffer_output_stream, io::BufferOutputStream::Create());

  const auto options = ipc::IpcWriteOptions::Defaults();
  ARROW_ASSIGN_OR_RAISE(
      auto writer, ipc::MakeStreamWriter(buffer_output_stream, table->schema(), options));

  ARROW_RETURN_NOT_OK(writer->WriteTable(*table));
  ARROW_RETURN_NOT_OK(writer->Close());

  ARROW_ASSIGN_OR_RAISE(auto buffer, buffer_output_stream->Finish());
  bl.append((char*)buffer->data(), buffer->size());
  return Status::OK();
}

Status SerializeTableToParquetStream(std::shared_ptr<Table>& table,
                                     librados::bufferlist& bl) {
  ARROW_ASSIGN_OR_RAISE(auto buffer_output_stream, io::BufferOutputStream::Create());

  PARQUET_THROW_NOT_OK(parquet::arrow::WriteTable(*table, arrow::default_memory_pool(),
                                                  buffer_output_stream, 1));

  ARROW_ASSIGN_OR_RAISE(auto buffer, buffer_output_stream->Finish());
  bl.append((char*)buffer->data(), buffer->size());
  return Status::OK();
}

Status DeserializeTableFromBufferlist(std::shared_ptr<Table>* table,
                                      librados::bufferlist& bl) {
  io::BufferReader reader((uint8_t*)bl.c_str(), bl.length());
  ARROW_ASSIGN_OR_RAISE(auto record_batch_reader,
                        ipc::RecordBatchStreamReader::Open(&reader));
  ARROW_ASSIGN_OR_RAISE(auto table_,
                        Table::FromRecordBatchReader(record_batch_reader.get()));
  *table = table_;
  return Status::OK();
}

}  // namespace dataset
}  // namespace arrow
