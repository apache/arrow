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

#include "jni/parquet/parquet_writer.h"

#include <stdlib.h>
#include <iostream>
#include <utility>
#include "jni/parquet/file_connector.h"
#include "jni/parquet/hdfs_connector.h"

namespace jni {
namespace parquet {

ParquetWriter::ParquetWriter(std::string path,
                             const std::shared_ptr<::arrow::Schema>& schema)
    : pool_(::arrow::default_memory_pool()), schema(schema) {
  if (path.find("hdfs") != std::string::npos) {
    connector_ = new HdfsConnector(path);
  } else {
    connector_ = new FileConnector(path);
  }
}

ParquetWriter::~ParquetWriter() {
  if (arrow_writer_) {
    arrow_writer_->Close();
  }
  connector_->TearDown();
  delete connector_;
}

::arrow::Status ParquetWriter::Initialize(bool use_hdfs3, int replication) {
  ::arrow::Status msg;
  msg = connector_->OpenWritable(use_hdfs3, replication);
  if (!msg.ok()) {
    std::cerr << "Create connector_ failed, error msg: " << msg << std::endl;
    return msg;
  }

  std::shared_ptr<::parquet::WriterProperties> properties =
      ::parquet::default_writer_properties();
  msg = ::parquet::arrow::ToParquetSchema(schema.get(), *properties.get(),
                                          &schema_description_);
  if (!msg.ok()) {
    std::cerr << "Convert Arrow ::arrow::Schema to "
              << "Parquet ::arrow::Schema failed, error msg: " << msg << std::endl;
    return msg;
  }

  ::parquet::schema::NodeVector group_node_fields;
  for (int i = 0; i < schema_description_->group_node()->field_count(); i++) {
    group_node_fields.push_back(schema_description_->group_node()->field(i));
  }
  auto parquet_schema = std::static_pointer_cast<::parquet::schema::GroupNode>(
      ::parquet::schema::GroupNode::Make(schema_description_->schema_root()->name(),
                                         schema_description_->schema_root()->repetition(),
                                         group_node_fields));

  msg = ::parquet::arrow::FileWriter::Make(
      pool_, ::parquet::ParquetFileWriter::Open(connector_->GetWriter(), parquet_schema),
      schema, ::parquet::default_arrow_writer_properties(), &arrow_writer_);
  if (!msg.ok()) {
    std::cerr << "Open parquet file failed, error msg: " << msg << std::endl;
    return msg;
  }
  return msg;
}

::arrow::Status ParquetWriter::WriteNext(int num_rows, int64_t* in_buf_addrs,
                                         int64_t* in_buf_sizes, int in_bufs_len) {
  std::shared_ptr<::arrow::RecordBatch> batch;
  ::arrow::Status msg =
      MakeRecordBatch(schema, num_rows, in_buf_addrs, in_buf_sizes, in_bufs_len, &batch);
  if (!msg.ok()) {
    return msg;
  }

  std::lock_guard<std::mutex> lck(thread_mtx_);
  record_batch_buffer_list_.push_back(batch);

  return msg;
}

::arrow::Status ParquetWriter::Flush() {
  std::shared_ptr<::arrow::Table> table;
  ::arrow::Status msg =
      ::arrow::Table::FromRecordBatches(record_batch_buffer_list_, &table);
  if (!msg.ok()) {
    std::cerr << "Table::FromRecordBatches failed" << std::endl;
    return msg;
  }

  msg = arrow_writer_->WriteTable(*table.get(), table->num_rows());
  if (!msg.ok()) {
    std::cerr << "arrow_writer_->WriteTable failed" << std::endl;
    return msg;
  }

  msg = connector_->GetWriter()->Flush();
  if (!msg.ok()) {
    std::cerr << "ParquetWriter::Flush() failed" << std::endl;
    return msg;
  }
  return msg;
}

::arrow::Status ParquetWriter::WriteNext(
    const std::shared_ptr<::arrow::RecordBatch>& rb) {
  std::lock_guard<std::mutex> lck(thread_mtx_);
  record_batch_buffer_list_.push_back(rb);
  return ::arrow::Status::OK();
}

::arrow::Status ParquetWriter::MakeRecordBatch(
    const std::shared_ptr<::arrow::Schema>& schema, int num_rows, int64_t* in_buf_addrs,
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
