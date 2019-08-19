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

#ifndef PARQUET_READER_H
#define PARQUET_READER_H

#include <arrow/record_batch.h>
#include <parquet/arrow/reader.h>
#include <parquet/file_reader.h>
#include <parquet/properties.h>
#include <memory>
#include <mutex>
#include <string>
#include <vector>
#include "jni/parquet/FileConnector.h"
#include "jni/parquet/HdfsConnector.h"

namespace jni {
namespace parquet {

class ParquetReader {
 public:
  explicit ParquetReader(std::string path);
  ::arrow::Status initialize(std::vector<int>& column_indices,
                             std::vector<int>& row_group_indices, int64_t batch_size,
                             bool useHdfs3 = true);
  ::arrow::Status initialize(std::vector<int>& column_indices, int64_t start_pos,
                             int64_t end_pos, int64_t batch_size, bool useHdfs3 = true);
  ~ParquetReader();
  ::arrow::Status readNext(std::shared_ptr<::arrow::RecordBatch>* out);
  std::shared_ptr<::arrow::Schema> schema();
  Connector* connector;

 private:
  ::arrow::MemoryPool* pool;
  std::mutex threadMtx;

  std::unique_ptr<::parquet::arrow::FileReader> arrow_reader;
  ::parquet::ArrowReaderProperties properties;
  std::shared_ptr<::arrow::RecordBatchReader> rb_reader;

  std::vector<int> getRowGroupIndices(int num_row_groups, int64_t start_pos,
                                      int64_t end_pos);
  ::arrow::Status getRecordBatch(std::vector<int>& row_group_indices,
                                 std::vector<int>& column_indices);
};
}  // namespace parquet
}  // namespace jni

#endif
