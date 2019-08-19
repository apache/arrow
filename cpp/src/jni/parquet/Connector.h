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

class Connector {
 public:
  Connector() {}
  std::string getFileName() { return filePath; }
  virtual ::arrow::Status openReadable(bool useHdfs3) = 0;
  virtual ::arrow::Status openWritable(bool useHdfs3, int replication) = 0;
  virtual std::shared_ptr<::arrow::io::RandomAccessFile> getReader() = 0;
  virtual std::shared_ptr<::arrow::io::OutputStream> getWriter() = 0;
  virtual void teardown() = 0;

 protected:
  std::string filePath;
  std::string dirPath;
  std::string getPathDir(std::string path) {
    std::string delimiter = "/";
    size_t pos = 0;
    size_t last_pos = pos;
    std::string token;
    while ((pos = path.find(delimiter, pos)) != std::string::npos) {
      last_pos = pos;
      pos += 1;
    }
    if (last_pos == 0) {
      return std::string();
    }
    return path.substr(0, last_pos + delimiter.length());
  }
  virtual ::arrow::Status mkdir(std::string path) = 0;
};
}  // namespace parquet
}  // namespace jni

#endif
