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

#ifndef PARQUET_WRITER_H
#define PARQUET_WRITER_H

#include <arrow/table.h>
#include <parquet/arrow/schema.h>
#include <parquet/arrow/writer.h>
#include <parquet/file_writer.h>
#include <parquet/properties.h>
#include <memory>
#include <mutex>
#include <string>
#include <vector>
#include "jni/parquet/FileConnector.h"
#include "jni/parquet/HdfsConnector.h"

namespace jni {
namespace parquet {

class ParquetWriter {
 public:
  ParquetWriter(std::string path, std::shared_ptr<::arrow::Schema>& schema);
  ~ParquetWriter();
  ::arrow::Status initialize(bool useHdfs3 = true, int replication = 1);
  ::arrow::Status writeNext(int num_rows, int64_t* in_buf_addrs, int64_t* in_buf_sizes,
                            int in_bufs_len);
  ::arrow::Status writeNext(std::shared_ptr<::arrow::RecordBatch>& rb);
  ::arrow::Status flush();

 private:
  ::arrow::MemoryPool* pool;
  Connector* connector;
  std::mutex threadMtx;
  std::unique_ptr<::parquet::arrow::FileWriter> arrow_writer;
  std::shared_ptr<::arrow::Schema> schema;
  std::shared_ptr<::parquet::SchemaDescriptor> schema_description;
  std::vector<std::shared_ptr<::arrow::RecordBatch>> record_batch_buffer_list;

  ::arrow::Status makeRecordBatch(std::shared_ptr<::arrow::Schema>& schema, int num_rows,
                                  int64_t* in_buf_addrs, int64_t* in_buf_sizes,
                                  int in_bufs_len,
                                  std::shared_ptr<::arrow::RecordBatch>* batch);
};
}  // namespace parquet
}  // namespace jni

#endif
