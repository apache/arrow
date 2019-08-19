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
#include "jni/parquet/Connector.h"

namespace jni {
namespace parquet {

class HdfsConnector : public Connector {
 public:
  explicit HdfsConnector(std::string hdfsPath);
  ~HdfsConnector();

  ::arrow::Status openReadable(bool useHdfs3);
  ::arrow::Status openWritable(bool useHdfs3, int replication);
  std::shared_ptr<::arrow::io::RandomAccessFile> getReader() { return fileReader; }
  std::shared_ptr<::arrow::io::OutputStream> getWriter() { return fileWriter; }
  void teardown();

 protected:
  std::shared_ptr<::arrow::io::HadoopFileSystem> hdfsClient;
  std::shared_ptr<::arrow::io::HdfsReadableFile> fileReader;
  std::shared_ptr<::arrow::io::HdfsOutputStream> fileWriter;
  ::arrow::io::HdfsConnectionConfig hdfsConfig;
  bool driverLoaded = false;
  int32_t buffer_size = 0;
  int64_t default_block_size = 0;

  ::arrow::Status setupHdfsClient(bool useHdfs3);
  ::arrow::Status getHdfsHostAndPort(std::string hdfsPath,
                                     ::arrow::io::HdfsConnectionConfig* hdfs_conf);
  std::string getFileName(std::string filePath);
  ::arrow::Status mkdir(std::string path);
};
}  // namespace parquet
}  // namespace jni

#endif
