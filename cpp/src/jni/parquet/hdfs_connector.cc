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

namespace jni {
namespace parquet {

HdfsConnector::HdfsConnector(std::string hdfsPath) : driverLoaded(false) {
  getHdfsHostAndPort(hdfsPath, &hdfsConfig);
  filePath = getFileName(hdfsPath);
  dirPath = getPathDir(filePath);
}

HdfsConnector::~HdfsConnector() {}

::arrow::Status HdfsConnector::setupHdfsClient(bool useHdfs3) {
  if (hdfsClient) {
    return ::arrow::Status::OK();
  }
  if (!driverLoaded) {
    ::arrow::Status msg;
    if (useHdfs3) {
      hdfsConfig.driver = ::arrow::io::HdfsDriver::LIBHDFS3;
    } else {
      hdfsConfig.driver = ::arrow::io::HdfsDriver::LIBHDFS;
    }
    driverLoaded = true;
  }

  return ::arrow::io::HadoopFileSystem::Connect(&hdfsConfig, &hdfsClient);
}

::arrow::Status HdfsConnector::getHdfsHostAndPort(
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

std::string HdfsConnector::getFileName(std::string path) {
  std::string search_str0 = std::string(":");
  std::string::size_type pos0 = path.find_first_of(search_str0, 7);
  std::string fileName;
  if (pos0 == std::string::npos) {
    fileName = path.substr(7, std::string::npos);
  } else {
    std::string search_str1 = std::string("/");
    std::string::size_type pos1 = path.find_first_of(search_str1, 7);
    fileName = path.substr(pos1, std::string::npos);
  }
  return fileName;
}

void HdfsConnector::teardown() {
  if (fileWriter) {
    fileWriter->Close();
  }
  if (fileReader) {
    fileReader->Close();
  }
  if (hdfsClient) {
    hdfsClient->Disconnect();
    hdfsClient = nullptr;
  }
}

::arrow::Status HdfsConnector::openReadable(bool useHdfs3) {
  ::arrow::Status msg;
  if (!hdfsClient) {
    msg = setupHdfsClient(useHdfs3);
    if (!msg.ok()) {
      std::cerr << "connect HDFS failed, error is : " << msg << std::endl;
      return msg;
    }
  }
  msg = hdfsClient->OpenReadable(filePath, &fileReader);
  if (!msg.ok()) {
    std::cerr << "Open HDFS file failed, file name is " << filePath
              << ", error is : " << msg << std::endl;
    return msg;
  }
  return msg;
}

::arrow::Status HdfsConnector::openWritable(bool useHdfs3, int replication) {
  ::arrow::Status msg;
  if (!hdfsClient) {
    msg = setupHdfsClient(useHdfs3);
    if (!msg.ok()) {
      std::cerr << "connect HDFS failed, error is : " << msg << std::endl;
      return msg;
    }
  }
  if (!dirPath.empty()) {
    msg = mkdir(dirPath);
    if (!msg.ok()) {
      std::cerr << "Mkdir for HDFS path failed " << dirPath << ", error is : " << msg
                << std::endl;
      return msg;
    }
  }

  msg = hdfsClient->OpenWritable(filePath, false, buffer_size, (int16_t)replication,
                                 default_block_size, &fileWriter);
  if (!msg.ok()) {
    std::cerr << "Open HDFS file failed, file name is " << filePath
              << ", error is : " << msg << std::endl;
    return msg;
  }
  return msg;
}

::arrow::Status HdfsConnector::mkdir(std::string path) {
  return hdfsClient->MakeDirectory(path);
}
}  // namespace parquet
}  // namespace jni
