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

#include "jni/parquet/ParquetWriter.h"

#include <stdlib.h>
#include <iostream>
#include <utility>
#include "jni/parquet/FileConnector.h"
#include "jni/parquet/HdfsConnector.h"

namespace jni {
namespace parquet {

ParquetWriter::ParquetWriter(std::string path, std::shared_ptr<::arrow::Schema>& schema)
    : pool(::arrow::default_memory_pool()), schema(schema) {
  if (path.find("hdfs") != std::string::npos) {
    connector = new HdfsConnector(path);
  } else {
    connector = new FileConnector(path);
  }
}

ParquetWriter::~ParquetWriter() {
  if (arrow_writer) {
    arrow_writer->Close();
  }
  connector->teardown();
  delete connector;
}

::arrow::Status ParquetWriter::initialize(bool useHdfs3, int replication) {
  ::arrow::Status msg;
  msg = connector->openWritable(useHdfs3, replication);
  if (!msg.ok()) {
    std::cerr << "Create connector failed, error msg: " << msg << std::endl;
    return msg;
  }

  std::shared_ptr<::parquet::WriterProperties> properties =
      ::parquet::default_writer_properties();
  msg = ::parquet::arrow::ToParquetSchema(schema.get(), *properties.get(),
                                          &schema_description);
  if (!msg.ok()) {
    std::cerr << "Convert Arrow ::arrow::Schema to "
              << "Parquet ::arrow::Schema failed, error msg: " << msg << std::endl;
    return msg;
  }

  ::parquet::schema::NodeVector group_node_fields;
  for (int i = 0; i < schema_description->group_node()->field_count(); i++) {
    group_node_fields.push_back(schema_description->group_node()->field(i));
  }
  auto parquet_schema = std::static_pointer_cast<::parquet::schema::GroupNode>(
      ::parquet::schema::GroupNode::Make(schema_description->schema_root()->name(),
                                         schema_description->schema_root()->repetition(),
                                         group_node_fields));

  msg = ::parquet::arrow::FileWriter::Make(
      pool, ::parquet::ParquetFileWriter::Open(connector->getWriter(), parquet_schema),
      schema, ::parquet::default_arrow_writer_properties(), &arrow_writer);
  if (!msg.ok()) {
    std::cerr << "Open parquet file failed, error msg: " << msg << std::endl;
    return msg;
  }
  return msg;
}

::arrow::Status ParquetWriter::writeNext(int num_rows, int64_t* in_buf_addrs,
                                         int64_t* in_buf_sizes, int in_bufs_len) {
  std::shared_ptr<::arrow::RecordBatch> batch;
  ::arrow::Status msg =
      makeRecordBatch(schema, num_rows, in_buf_addrs, in_buf_sizes, in_bufs_len, &batch);
  if (!msg.ok()) {
    return msg;
  }

  std::lock_guard<std::mutex> lck(threadMtx);
  record_batch_buffer_list.push_back(batch);

  return msg;
}

::arrow::Status ParquetWriter::flush() {
  std::shared_ptr<::arrow::Table> table;
  ::arrow::Status msg =
      ::arrow::Table::FromRecordBatches(record_batch_buffer_list, &table);
  if (!msg.ok()) {
    std::cerr << "Table::FromRecordBatches failed" << std::endl;
    return msg;
  }

  msg = arrow_writer->WriteTable(*table.get(), table->num_rows());
  if (!msg.ok()) {
    std::cerr << "arrow_writer->WriteTable failed" << std::endl;
    return msg;
  }

  msg = connector->getWriter()->Flush();
  return msg;
}

::arrow::Status ParquetWriter::writeNext(std::shared_ptr<::arrow::RecordBatch>& rb) {
  std::lock_guard<std::mutex> lck(threadMtx);
  record_batch_buffer_list.push_back(rb);
  return ::arrow::Status::OK();
}

::arrow::Status ParquetWriter::makeRecordBatch(
    std::shared_ptr<::arrow::Schema>& schema, int num_rows, int64_t* in_buf_addrs,
    int64_t* in_buf_sizes, int in_bufs_len, std::shared_ptr<arrow::RecordBatch>* batch) {
  std::vector<std::shared_ptr<::arrow::ArrayData>> arrays;
  auto num_fields = schema->num_fields();
  int buf_idx = 0;
  int sz_idx = 0;

  for (int i = 0; i < num_fields; i++) {
    auto field = schema->field(i);
    std::vector<std::shared_ptr<::arrow::Buffer>> buffers;

    if (buf_idx >= in_bufs_len) {
      return ::arrow::Status::Invalid("insufficient number of in_buf_addrs");
    }
    int64_t validity_addr = in_buf_addrs[buf_idx++];
    int64_t validity_size = in_buf_sizes[sz_idx++];
    auto validity = std::shared_ptr<::arrow::Buffer>(
        new ::arrow::Buffer(reinterpret_cast<uint8_t*>(validity_addr), validity_size));
    buffers.push_back(validity);

    if (buf_idx >= in_bufs_len) {
      return ::arrow::Status::Invalid("insufficient number of in_buf_addrs");
    }
    int64_t value_addr = in_buf_addrs[buf_idx++];
    int64_t value_size = in_buf_sizes[sz_idx++];
    auto data = std::shared_ptr<::arrow::Buffer>(
        new ::arrow::Buffer(reinterpret_cast<uint8_t*>(value_addr), value_size));
    buffers.push_back(data);

    if (arrow::is_binary_like(field->type()->id())) {
      if (buf_idx >= in_bufs_len) {
        return ::arrow::Status::Invalid("insufficient number of in_buf_addrs");
      }

      // add offsets buffer for variable-len fields.
      int64_t offsets_addr = in_buf_addrs[buf_idx++];
      int64_t offsets_size = in_buf_sizes[sz_idx++];
      auto offsets = std::shared_ptr<::arrow::Buffer>(
          new ::arrow::Buffer(reinterpret_cast<uint8_t*>(offsets_addr), offsets_size));
      buffers.push_back(offsets);
    }

    auto array_data =
        ::arrow::ArrayData::Make(field->type(), num_rows, std::move(buffers));
    arrays.push_back(array_data);
  }
  *batch = arrow::RecordBatch::Make(schema, num_rows, arrays);
  return ::arrow::Status::OK();
}

}  // namespace parquet
}  // namespace jni
