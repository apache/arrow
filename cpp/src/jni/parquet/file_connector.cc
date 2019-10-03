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

#include "jni/parquet/file_connector.h"

#include <atomic>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <iostream>
#include <numeric>
#include <sstream>  // IWYU pragma: keep
#include <string>
#include <vector>

namespace jni {
namespace parquet {

FileConnector::FileConnector(std::string path) {
  filePath = path;
  dirPath = getPathDir(path);
}

FileConnector::~FileConnector() {}

void FileConnector::teardown() {
  if (fileWriter) {
    fileWriter->Close();
  }
}

::arrow::Status FileConnector::openReadable(bool useHdfs3) {
  ::arrow::Status msg = ::arrow::io::ReadableFile::Open(filePath, &fileReader);
  if (!msg.ok()) {
    std::cerr << "Open file failed, file name is " << filePath << ", error is : " << msg
              << std::endl;
    return msg;
  }
  return msg;
}

::arrow::Status FileConnector::openWritable(bool useHdfs3, int replication) {
  ::arrow::Status msg;
  if (!dirPath.empty()) {
    msg = mkdir(dirPath);
    if (!msg.ok()) {
      std::cerr << "mkdir for path failed " << dirPath << ", error is : " << msg
                << std::endl;
      return msg;
    }
  }

  msg = ::arrow::io::FileOutputStream::Open(filePath, false, &fileWriter);
  if (!msg.ok()) {
    std::cerr << "Open file failed, file name is " << filePath << ", error is : " << msg
              << std::endl;
    return msg;
  }
  return msg;
}

::arrow::Status FileConnector::mkdir(std::string path) {
  std::string cmd = "mkdir -p ";
  cmd.append(path);
  const int ret = system(cmd.c_str());
  if (ret < 0 && ret != EEXIST) {
    return ::arrow::Status::IOError(strerror(ret));
  }
  return ::arrow::Status::OK();
}

}  // namespace parquet
}  // namespace jni
