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

#include "jni/parquet/parquet_reader.h"

#include <arrow/record_batch.h>
#include <stdlib.h>
#include <iostream>
#include <memory>
#include "jni/parquet/file_connector.h"
#include "jni/parquet/hdfs_connector.h"

namespace jni {
namespace parquet {

ParquetReader::ParquetReader(std::string path)
    : pool_(::arrow::default_memory_pool()), properties_(false) {
  if (path.find("hdfs") != std::string::npos) {
    connector_ = new HdfsConnector(path);
  } else {
    connector_ = new FileConnector(path);
  }
}

ParquetReader::~ParquetReader() {
  connector_->TearDown();
  delete connector_;
}

::arrow::Status ParquetReader::Initialize(const std::vector<int>& column_indices,
                                          const std::vector<int>& row_group_indices,
                                          int64_t batch_size, bool use_hdfs3) {
  ::arrow::Status msg;
  msg = connector_->OpenReadable(use_hdfs3);
  if (!msg.ok()) {
    std::cerr << "Create connector failed, error msg: " << msg << std::endl;
    return msg;
  }
  properties_.set_batch_size(batch_size);

  msg = ::parquet::arrow::FileReader::Make(
      pool_, ::parquet::ParquetFileReader::Open(connector_->GetReader()), properties_,
      &arrow_reader_);
  if (!msg.ok()) {
    std::cerr << "Open parquet file failed, error msg: " << msg << std::endl;
    return msg;
  }

  msg = GetRecordBatchReader(row_group_indices, column_indices);
  if (!msg.ok()) {
    std::cerr << "GetRecordBatchReader failed, error msg: " << msg << std::endl;
    return msg;
  }
  return msg;
}

::arrow::Status ParquetReader::Initialize(const std::vector<int>& column_indices,
                                          int64_t start_pos, int64_t end_pos,
                                          int64_t batch_size, bool use_hdfs3) {
  ::arrow::Status msg;
  msg = connector_->OpenReadable(use_hdfs3);
  if (!msg.ok()) {
    std::cerr << "Create connector failed, error msg: " << msg << std::endl;
    return msg;
  }
  properties_.set_batch_size(batch_size);

  msg = ::parquet::arrow::FileReader::Make(
      pool_, ::parquet::ParquetFileReader::Open(connector_->GetReader()), properties_,
      &arrow_reader_);
  if (!msg.ok()) {
    std::cerr << "Open parquet file failed, error msg: " << msg << std::endl;
    return msg;
  }

  std::vector<int> row_group_indices =
      GetRowGroupIndices(arrow_reader_->num_row_groups(), start_pos, end_pos);
  msg = GetRecordBatchReader(row_group_indices, column_indices);
  if (!msg.ok()) {
    std::cerr << "GetRecordBatchReader failed, error msg: " << msg << std::endl;
    return msg;
  }
  return msg;
}

std::vector<int> ParquetReader::GetRowGroupIndices(int num_row_groups, int64_t start_pos,
                                                   int64_t end_pos) {
  std::unique_ptr<::parquet::ParquetFileReader> reader =
      ::parquet::ParquetFileReader::Open(connector_->GetReader());
  std::vector<int> row_group_indices;
  int64_t pos = 0;
  for (int i = 0; i < num_row_groups; i++) {
    if (pos >= start_pos && pos < end_pos) {
      row_group_indices.push_back(i);
      break;
    }
    pos += reader->RowGroup(i)->metadata()->total_byte_size();
  }
  if (row_group_indices.empty()) {
    row_group_indices.push_back(num_row_groups - 1);
  }
  return row_group_indices;
}

::arrow::Status ParquetReader::GetRecordBatchReader(
    const std::vector<int>& row_group_indices, const std::vector<int>& column_indices) {
  if (column_indices.empty()) {
    return arrow_reader_->GetRecordBatchReader(row_group_indices, &rb_reader_);
  } else {
    return arrow_reader_->GetRecordBatchReader(row_group_indices, column_indices,
                                               &rb_reader_);
  }
}

::arrow::Status ParquetReader::ReadNext(std::shared_ptr<::arrow::RecordBatch>* out) {
  std::lock_guard<std::mutex> lck(thread_mtx_);
  return rb_reader_->ReadNext(out);
}

std::shared_ptr<::arrow::Schema> ParquetReader::GetSchema() {
  return rb_reader_->schema();
}

}  // namespace parquet
}  // namespace jni
