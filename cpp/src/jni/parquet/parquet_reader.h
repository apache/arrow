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
#include "jni/parquet/file_connector.h"
#include "jni/parquet/hdfs_connector.h"

namespace jni {
namespace parquet {

/// \brief An Reader instance of one parquet file
///
/// This class is used by jni_wrapper to hold a reader handler for
/// continuous record batch reading.
class ParquetReader {
 public:
  /// \brief Construction of ParquetReader
  /// \param[in] path ParquetReader will open difference connector according to file path
  explicit ParquetReader(std::string path);

  ~ParquetReader();

  /// \brief Initialization of ParquetReader
  /// \param[in] column_indices indexes of columns expected to be read
  /// \param[in] row_group_indices indexes of row_groups expected to be read
  /// \param[in] batch_size batch size, default is 4096
  /// \param[in] use_hdfs3 option used by HdfsConnector
  ::arrow::Status Initialize(const std::vector<int>& column_indices,
                             const std::vector<int>& row_group_indices,
                             int64_t batch_size, bool use_hdfs3 = true);

  /// \brief Initialization of ParquetReader
  /// \param[in] column_indices indexes of columns expected to be read
  /// \param[in] start_pos use offset to indicate which row_group is expected
  /// \param[in] end_pos use offset to indicate which row_group is expected
  /// \param[in] batch_size batch size, default is 4096
  /// \param[in] use_hdfs3 option used by HdfsConnector
  ::arrow::Status Initialize(const std::vector<int>& column_indices, int64_t start_pos,
                             int64_t end_pos, int64_t batch_size, bool use_hdfs3 = true);

  /// \brief Read next batch
  /// \param[out] out readed batch will be returned as RecordBatch
  ::arrow::Status ReadNext(std::shared_ptr<::arrow::RecordBatch>* out);

  /// \brief Get Parquet Schema
  std::shared_ptr<::arrow::Schema> GetSchema();

  Connector* connector_;

 private:
  ::arrow::MemoryPool* pool_;
  std::mutex thread_mtx_;

  std::unique_ptr<::parquet::arrow::FileReader> arrow_reader_;
  ::parquet::ArrowReaderProperties properties_;
  std::shared_ptr<::arrow::RecordBatchReader> rb_reader_;

  std::vector<int> GetRowGroupIndices(int num_row_groups, int64_t start_pos,
                                      int64_t end_pos);

  ::arrow::Status GetRecordBatchReader(const std::vector<int>& row_group_indices,
                                       const std::vector<int>& column_indices);
};
}  // namespace parquet
}  // namespace jni

#endif
