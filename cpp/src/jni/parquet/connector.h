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

#ifndef CONNECTOR_H
#define CONNECTOR_H

#include <arrow/array.h>
#include <arrow/buffer.h>
#include <arrow/io/interfaces.h>
#include <arrow/status.h>
#include <cstring>
#include <iostream>
#include <memory>
#include <string>

namespace jni {
namespace parquet {

/// \brief A base class for Connectors
///
/// This class is used by ParquetReader and ParquetWriter to handle
/// parquet files from different resources, local and hdfs for now using
/// unified API.
class Connector {
 public:
  Connector() = default;
  virtual ~Connector() {}
  std::string GetFileName() { return file_path_; }
  virtual ::arrow::Status OpenReadable(bool option) = 0;
  virtual ::arrow::Status OpenWritable(bool option, int replication) = 0;
  virtual std::shared_ptr<::arrow::io::RandomAccessFile> GetReader() = 0;
  virtual std::shared_ptr<::arrow::io::OutputStream> GetWriter() = 0;
  virtual void TearDown() = 0;

 protected:
  std::string file_path_;
  std::string dir_path_;
  virtual ::arrow::Status Mkdir(std::string path) = 0;
};
}  // namespace parquet
}  // namespace jni

#endif
