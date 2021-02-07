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

Status Int64ToChar(char* buffer, int64_t num) {
  /// Pass the integer through the union to
  /// get the byte representation.
  num = BitUtil::ToLittleEndian(num);
  converter_.integer_ = num;
  memcpy(buffer, converter_.bytes_, 8);
  return Status::OK();
}

Status CharToInt64(char* buffer, int64_t& num) {
  /// Pass the byte representation through the union to
  /// get the integer.
  memcpy(converter_.bytes_, buffer, 8);
  num = BitUtil::ToLittleEndian(converter_.integer_);
  return Status::OK();
}

Status SerializeScanRequestToBufferlist(Expression filter, Expression part_expr,
                                        std::shared_ptr<Schema> projection_schema,
                                        std::shared_ptr<Schema> dataset_schema,
                                        std::shared_ptr<librados::bufferlist>& bl) {
  // serialize the filter expression's and the schema's.
  ARROW_ASSIGN_OR_RAISE(auto filter_buffer, Serialize(filter));
  ARROW_ASSIGN_OR_RAISE(auto part_expr_buffer, Serialize(part_expr));
  ARROW_ASSIGN_OR_RAISE(auto projection_schema_buffer,
                        ipc::SerializeSchema(*projection_schema));
  ARROW_ASSIGN_OR_RAISE(auto dataset_schema_buffer,
                        ipc::SerializeSchema(*dataset_schema));

  // convert filter Expression size to buffer.
  char* filter_size_buffer = new char[8];
  ARROW_RETURN_NOT_OK(Int64ToChar(filter_size_buffer, filter_buffer->size()));

  // convert partition expression size to buffer.
  char* part_expr_size_buffer = new char[8];
  ARROW_RETURN_NOT_OK(Int64ToChar(part_expr_size_buffer, part_expr_buffer->size()));

  // convert projection schema size to buffer.
  char* projection_schema_size_buffer = new char[8];
  ARROW_RETURN_NOT_OK(
      Int64ToChar(projection_schema_size_buffer, projection_schema_buffer->size()));

  // convert dataset schema to buffer
  char* dataset_schema_size_buffer = new char[8];
  ARROW_RETURN_NOT_OK(
      Int64ToChar(dataset_schema_size_buffer, dataset_schema_buffer->size()));

  // append the filter expression size and data.
  bl->append(filter_size_buffer, 8);
  bl->append((char*)filter_buffer->data(), filter_buffer->size());

  // append the partition expression size and data
  bl->append(part_expr_size_buffer, 8);
  bl->append((char*)part_expr_buffer->data(), part_expr_buffer->size());

  // append the projection schema size and data.
  bl->append(projection_schema_size_buffer, 8);
  bl->append((char*)projection_schema_buffer->data(), projection_schema_buffer->size());

  // append the dataset schema size and data.
  bl->append(dataset_schema_size_buffer, 8);
  bl->append((char*)dataset_schema_buffer->data(), dataset_schema_buffer->size());
  return Status::OK();
}

Status DeserializeScanRequestFromBufferlist(Expression* filter, Expression* part_expr,
                                            std::shared_ptr<Schema>* projection_schema,
                                            std::shared_ptr<Schema>* dataset_schema,
                                            std::shared_ptr<librados::bufferlist>& bl) {
  librados::bufferlist::iterator itr = bl->begin();

  int64_t filter_size = 0;
  char* filter_size_buffer = new char[8];
  itr.copy(8, filter_size_buffer);
  ARROW_RETURN_NOT_OK(CharToInt64(filter_size_buffer, filter_size));
  char* filter_buffer = new char[filter_size];
  itr.copy(filter_size, filter_buffer);

  int64_t part_expr_size = 0;
  char* part_expr_size_buffer = new char[8];
  itr.copy(8, part_expr_size_buffer);
  ARROW_RETURN_NOT_OK(CharToInt64(part_expr_size_buffer, part_expr_size));
  char* part_expr_buffer = new char[part_expr_size];
  itr.copy(part_expr_size, part_expr_buffer);

  int64_t projection_schema_size = 0;
  char* projection_schema_size_buffer = new char[8];
  itr.copy(8, projection_schema_size_buffer);
  ARROW_RETURN_NOT_OK(CharToInt64(projection_schema_size_buffer, projection_schema_size));
  char* projection_schema_buffer = new char[projection_schema_size];
  itr.copy(projection_schema_size, projection_schema_buffer);

  int64_t dataset_schema_size = 0;
  char* dataset_schema_size_buffer = new char[8];
  itr.copy(8, dataset_schema_size_buffer);
  ARROW_RETURN_NOT_OK(CharToInt64(dataset_schema_size_buffer, dataset_schema_size));
  char* dataset_schema_buffer = new char[dataset_schema_size];
  itr.copy(dataset_schema_size, dataset_schema_buffer);

  ARROW_ASSIGN_OR_RAISE(auto filter_, Deserialize(std::make_shared<Buffer>(
                                          (uint8_t*)filter_buffer, filter_size)));
  *filter = filter_;

  ARROW_ASSIGN_OR_RAISE(
      auto part_expr_,
      Deserialize(std::make_shared<Buffer>((uint8_t*)part_expr_buffer, part_expr_size)));
  *part_expr = part_expr_;

  ipc::DictionaryMemo empty_memo;
  io::BufferReader projection_schema_reader((uint8_t*)projection_schema_buffer,
                                            projection_schema_size);
  io::BufferReader dataset_schema_reader((uint8_t*)dataset_schema_buffer,
                                         dataset_schema_size);

  ARROW_ASSIGN_OR_RAISE(auto projection_schema_,
                        ipc::ReadSchema(&projection_schema_reader, &empty_memo));
  *projection_schema = projection_schema_;

  ARROW_ASSIGN_OR_RAISE(auto dataset_schema_,
                        ipc::ReadSchema(&dataset_schema_reader, &empty_memo));
  *dataset_schema = dataset_schema_;

  return Status::OK();
}

Status SerializeTableToBufferlist(std::shared_ptr<Table>& table,
                                  std::shared_ptr<librados::bufferlist>& bl) {
  ARROW_ASSIGN_OR_RAISE(auto buffer_output_stream, io::BufferOutputStream::Create());

  const auto options = ipc::IpcWriteOptions::Defaults();
  ARROW_ASSIGN_OR_RAISE(
      auto writer, ipc::MakeStreamWriter(buffer_output_stream, table->schema(), options));

  ARROW_RETURN_NOT_OK(writer->WriteTable(*table));
  ARROW_RETURN_NOT_OK(writer->Close());

  ARROW_ASSIGN_OR_RAISE(auto buffer, buffer_output_stream->Finish());
  bl->append((char*)buffer->data(), buffer->size());
  return Status::OK();
}

}  // namespace dataset
}  // namespace arrow
