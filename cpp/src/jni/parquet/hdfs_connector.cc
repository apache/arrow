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

#include "jni/parquet/hdfs_connector.h"

#include <stdlib.h>
#include <atomic>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <iostream>
#include <numeric>
#include <sstream>  // IWYU pragma: keep
#include <string>
#include <vector>
#include "arrow/filesystem/path_util.h"

namespace jni {
namespace parquet {

HdfsConnector::HdfsConnector(std::string hdfs_path) : driver_loaded_(false) {
  GetHdfsHostAndPort(hdfs_path, &hdfs_config_);
  file_path_ = GetFileName(hdfs_path);
  auto path_pair = arrow::fs::internal::GetAbstractPathParent(file_path_);
  dir_path_ = path_pair.first;
}

HdfsConnector::~HdfsConnector() {}

::arrow::Status HdfsConnector::SetupHdfsClient(bool use_hdfs3) {
  if (hdfs_client_) {
    return ::arrow::Status::OK();
  }
  if (!driver_loaded_) {
    ::arrow::Status msg;
    if (use_hdfs3) {
      hdfs_config_.driver = ::arrow::io::HdfsDriver::LIBHDFS3;
    } else {
      hdfs_config_.driver = ::arrow::io::HdfsDriver::LIBHDFS;
    }
    driver_loaded_ = true;
  }

  return ::arrow::io::HadoopFileSystem::Connect(&hdfs_config_, &hdfs_client_);
}

::arrow::Status HdfsConnector::GetHdfsHostAndPort(
    std::string hdfs_path, ::arrow::io::HdfsConnectionConfig* hdfs_conf) {
  std::string search_str0 = std::string(":");
  std::string::size_type pos0 = hdfs_path.find_first_of(search_str0, 7);

  std::string search_str1 = std::string("/");
  std::string::size_type pos1 = hdfs_path.find_first_of(search_str1, pos0);

  if ((pos0 == std::string::npos) || (pos1 == std::string::npos)) {
    std::cerr << "No host and port information. Use default hdfs port!";
    hdfs_conf->host = "localhost";
    hdfs_conf->port = 20500;
  } else {
    hdfs_conf->host = hdfs_path.substr(7, pos0 - 7);
    hdfs_conf->port = std::stoul(hdfs_path.substr(pos0 + 1, pos1 - pos0 - 1));
  }
  return ::arrow::Status::OK();
}

std::string HdfsConnector::GetFileName(std::string path) {
  std::string search_str0 = std::string(":");
  std::string::size_type pos0 = path.find_first_of(search_str0, 7);
  std::string file_name;
  if (pos0 == std::string::npos) {
    file_name = path.substr(7, std::string::npos);
  } else {
    std::string search_str1 = std::string("/");
    std::string::size_type pos1 = path.find_first_of(search_str1, 7);
    file_name = path.substr(pos1, std::string::npos);
  }
  return file_name;
}

void HdfsConnector::TearDown() {
  if (file_writer_) {
    file_writer_->Close();
  }
  if (file_reader_) {
    file_reader_->Close();
  }
  if (hdfs_client_) {
    hdfs_client_->Disconnect();
    hdfs_client_ = nullptr;
  }
}

::arrow::Status HdfsConnector::OpenReadable(bool use_hdfs3) {
  ::arrow::Status msg;
  if (!hdfs_client_) {
    msg = SetupHdfsClient(use_hdfs3);
    if (!msg.ok()) {
      std::cerr << "connect HDFS failed, error is : " << msg << std::endl;
      return msg;
    }
  }
  msg = hdfs_client_->OpenReadable(file_path_, &file_reader_);
  if (!msg.ok()) {
    std::cerr << "Open HDFS file failed, file name is " << file_path_
              << ", error is : " << msg << std::endl;
    return msg;
  }
  return msg;
}

::arrow::Status HdfsConnector::OpenWritable(bool use_hdfs3, int replication) {
  ::arrow::Status msg;
  if (!hdfs_client_) {
    msg = SetupHdfsClient(use_hdfs3);
    if (!msg.ok()) {
      std::cerr << "connect HDFS failed, error is : " << msg << std::endl;
      return msg;
    }
  }
  if (!dir_path_.empty()) {
    msg = Mkdir(dir_path_);
    if (!msg.ok()) {
      std::cerr << "Mkdir for HDFS path failed " << dir_path_ << ", error is : " << msg
                << std::endl;
      return msg;
    }
  }

  msg = hdfs_client_->OpenWritable(file_path_, false, buffer_size_, (int16_t)replication,
                                   default_block_size_, &file_writer_);
  if (!msg.ok()) {
    std::cerr << "Open HDFS file failed, file name is " << file_path_
              << ", error is : " << msg << std::endl;
    return msg;
  }
  return msg;
}

::arrow::Status HdfsConnector::Mkdir(std::string path) {
  return hdfs_client_->MakeDirectory(path);
}
}  // namespace parquet
}  // namespace jni
