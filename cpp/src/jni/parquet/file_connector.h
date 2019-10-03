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

#ifndef FILE_CONNECTOR_H
#define FILE_CONNECTOR_H

#include <arrow/io/file.h>
#include <memory>
#include <string>
#include "jni/parquet/connector.h"

namespace jni {
namespace parquet {

/// \brief A connector to handle local parquet file
///
/// This class is derived from base class "Connector", used to provide
/// an unified interface for ParquetReader and ParquetWriter to handle
/// files from different sources.
/// Member methods are wrappers of some methods under arrow/io/file.h
class FileConnector : public Connector {
 public:
  /// \brief Construction of this class
  /// \param[in] path local file path
  explicit FileConnector(std::string path);
  ~FileConnector();

  /// \brief Open local parquet file as readable handler
  /// \param[in] option a param holder, not used
  ::arrow::Status OpenReadable(bool option);

  /// \brief Open local parquet file as writable handler
  /// \param[in] option a param holder, not used
  /// \param[in] replication a param holder, not used
  ::arrow::Status OpenWritable(bool option, int replication);

  /// \brief Get reader handler
  std::shared_ptr<::arrow::io::RandomAccessFile> GetReader() { return file_reader_; }

  /// \brief Get writer handler
  std::shared_ptr<::arrow::io::OutputStream> GetWriter() { return file_writer_; }

  /// \brief Tear down connection and handlers
  void TearDown();

 protected:
  ::arrow::Status Mkdir(std::string path);
  std::shared_ptr<::arrow::io::ReadableFile> file_reader_;
  std::shared_ptr<::arrow::io::OutputStream> file_writer_;
};
}  // namespace parquet
}  // namespace jni

#endif
