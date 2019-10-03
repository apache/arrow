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

#ifndef HDFS_CONNECTOR_H
#define HDFS_CONNECTOR_H

#include <arrow/io/hdfs.h>
#include <memory>
#include <string>
#include "jni/parquet/connector.h"

namespace jni {
namespace parquet {

/// \brief A connector to handle hdfs parquet file
///
/// This class is derived from base class "Connector", used to provide
/// an unified interface for ParquetReader and ParquetWriter to handle
/// files from different sources.
/// Member methods are wrappers of some methods under arrow/io/hdfs.h
class HdfsConnector : public Connector {
 public:
  /// \brief Construction of this class
  /// \param[in] hdfs_path hdfs:// is expected at the beginning of the string
  explicit HdfsConnector(std::string hdfs_path);
  ~HdfsConnector();

  /// \brief Open hdfs parquet file as readable handler
  /// \param[in] use_hdfs3 an option to indicate if libhdfs3 or libhdfs should be expected
  ::arrow::Status OpenReadable(bool use_hdfs3);

  /// \brief Open hdfs parquet file as writable handler
  /// \param[in] use_hdfs3 an option to indicate if libhdfs3 or libhdfs should be expected
  /// \param[in] replication an option to indicate the repliation number of HDFS
  ::arrow::Status OpenWritable(bool use_hdfs3, int replication);

  /// \brief Get reader handler
  std::shared_ptr<::arrow::io::RandomAccessFile> GetReader() { return file_reader_; }

  /// \brief Get writer handler
  std::shared_ptr<::arrow::io::OutputStream> GetWriter() { return file_writer_; }

  /// \brief Tear down connection and handlers
  void TearDown();

 protected:
  std::shared_ptr<::arrow::io::HadoopFileSystem> hdfs_client_;
  std::shared_ptr<::arrow::io::HdfsReadableFile> file_reader_;
  std::shared_ptr<::arrow::io::HdfsOutputStream> file_writer_;
  ::arrow::io::HdfsConnectionConfig hdfs_config_;
  bool driver_loaded_ = false;
  int32_t buffer_size_ = 0;
  int64_t default_block_size_ = 0;

  ::arrow::Status SetupHdfsClient(bool use_hdfs3);
  ::arrow::Status GetHdfsHostAndPort(std::string hdfs_path,
                                     ::arrow::io::HdfsConnectionConfig* hdfs_conf);
  std::string GetFileName(std::string file_path);
  ::arrow::Status Mkdir(std::string path);
};
}  // namespace parquet
}  // namespace jni

#endif
