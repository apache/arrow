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
#include <flatbuffers/flatbuffers.h> 

#include "arrow/dataset/rados_utils.h"
#include "arrow/util/compression.h"
#include "generated/Request_generated.h"

namespace arrow {

namespace flatbuf = org::apache::arrow::flatbuf;

namespace dataset {

Status SerializeScanRequestToBufferlist(std::shared_ptr<ScanOptions> options,
                                        int64_t file_size, ceph::bufferlist& bl) {
  ARROW_ASSIGN_OR_RAISE(auto filter, compute::Serialize(options->filter));
  ARROW_ASSIGN_OR_RAISE(auto partition,
                        compute::Serialize(options->partition_expression));
  ARROW_ASSIGN_OR_RAISE(auto projection,
                        ipc::SerializeSchema(*options->projected_schema));
  ARROW_ASSIGN_OR_RAISE(auto schema,
                        ipc::SerializeSchema(*options->dataset_schema));

  flatbuffers::FlatBufferBuilder builder(1024);

  auto filter_vec = builder.CreateVector(filter->data(), filter->size());
  auto partition_vec = builder.CreateVector(partition->data(), partition->size());
  auto projected_schema_vec = builder.CreateVector(projection->data(), projection->size());
  auto dataset_schema_vec = builder.CreateVector(schema->data(), schema->size());

  auto request = flatbuf::CreateRequest(builder, file_size, filter_vec, partition_vec, dataset_schema_vec, projected_schema_vec);
  builder.Finish(request);
  uint8_t *buf = builder.GetBufferPointer();
  int size = builder.GetSize();

  bl.append((char*)buf, size);
  return Status::OK();
}

Status DeserializeScanRequestFromBufferlist(compute::Expression* filter,
                                            compute::Expression* partition,
                                            std::shared_ptr<Schema>* projected_schema,
                                            std::shared_ptr<Schema>* dataset_schema,
                                            int64_t& file_size, ceph::bufferlist& bl) {
  
  auto request = flatbuf::GetRequest((uint8_t*)bl.c_str());

  ARROW_ASSIGN_OR_RAISE(auto filter_, compute::Deserialize(std::make_shared<Buffer>(request->filter()->data(), request->filter()->size())));
  *filter = filter_;

  ARROW_ASSIGN_OR_RAISE(auto partition_, compute::Deserialize(std::make_shared<Buffer>(request->partition()->data(), request->partition()->size())));
  *partition = partition_;

  ipc::DictionaryMemo empty_memo;
  io::BufferReader projection_reader(request->projection_schema()->data(), request->projection_schema()->size());
  io::BufferReader schema_reader(request->dataset_schema()->data(), request->dataset_schema()->size());

  ARROW_ASSIGN_OR_RAISE(auto projected_schema_, ipc::ReadSchema(&projection_reader, &empty_memo));
  *projected_schema = projected_schema_;

  ARROW_ASSIGN_OR_RAISE(auto dataset_schema_, ipc::ReadSchema(&schema_reader, &empty_memo));
  *dataset_schema = dataset_schema_;

  file_size = request->file_size();
  return Status::OK();
}

Status SerializeTableToBufferlist(std::shared_ptr<Table>& table, ceph::bufferlist& bl) {
  ARROW_ASSIGN_OR_RAISE(auto buffer_output_stream, io::BufferOutputStream::Create());

  ipc::IpcWriteOptions options = ipc::IpcWriteOptions::Defaults();
  ARROW_ASSIGN_OR_RAISE(
      options.codec,
      util::Codec::Create(Compression::LZ4_FRAME, std::numeric_limits<int>::min()));
  ARROW_ASSIGN_OR_RAISE(
      auto writer, ipc::MakeStreamWriter(buffer_output_stream, table->schema(), options));

  ARROW_RETURN_NOT_OK(writer->WriteTable(*table));
  ARROW_RETURN_NOT_OK(writer->Close());

  ARROW_ASSIGN_OR_RAISE(auto buffer, buffer_output_stream->Finish());
  bl.append((char*)buffer->data(), buffer->size());
  return Status::OK();
}

}  // namespace dataset
}  // namespace arrow
