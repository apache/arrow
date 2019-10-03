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
#include "arrow/filesystem/localfs.h"
#include "arrow/filesystem/path_util.h"

namespace jni {
namespace parquet {

FileConnector::FileConnector(std::string path) {
  file_path_ = path;
  auto path_pair = arrow::fs::internal::GetAbstractPathParent(file_path_);
  dir_path_ = path_pair.first;
}

FileConnector::~FileConnector() {}

void FileConnector::TearDown() {
  if (file_writer_) {
    file_writer_->Close();
  }
}

::arrow::Status FileConnector::OpenReadable(bool option) {
  ::arrow::Status msg = ::arrow::io::ReadableFile::Open(file_path_, &file_reader_);
  if (!msg.ok()) {
    std::cerr << "Open file failed, file name is " << file_path_ << ", error is : " << msg
              << std::endl;
    return msg;
  }
  return msg;
}

::arrow::Status FileConnector::OpenWritable(bool option, int replication) {
  ::arrow::Status msg;
  if (!dir_path_.empty()) {
    msg = Mkdir(dir_path_);
    if (!msg.ok()) {
      std::cerr << "Mkdir for path failed " << dir_path_ << ", error is : " << msg
                << std::endl;
      return msg;
    }
  }

  msg = ::arrow::io::FileOutputStream::Open(file_path_, false, &file_writer_);
  if (!msg.ok()) {
    std::cerr << "Open file failed, file name is " << file_path_ << ", error is : " << msg
              << std::endl;
    return msg;
  }
  return msg;
}

::arrow::Status FileConnector::Mkdir(std::string path) {
  std::shared_ptr<::arrow::fs::LocalFileSystem> local_fs =
      std::make_shared<::arrow::fs::LocalFileSystem>();
  ::arrow::Status msg = local_fs->CreateDir(path);
  if (!msg.ok()) {
    std::cerr << "Make Directory for " << path << " failed, error is: " << msg
              << std::endl;
    return msg;
  }
  return msg;
}

}  // namespace parquet
}  // namespace jni
