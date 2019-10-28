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

#include <memory>
#include <string>

#include <gtest/gtest.h>

#include "arrow/filesystem/hdfs.h"
#include "arrow/filesystem/test_util.h"
#include "arrow/testing/gtest_util.h"
#include "arrow/util/io_util.h"

namespace arrow {
namespace fs {
namespace internal {

#ifndef _WIN32

#define SOME_DATA_SIZE 9

using HdfsDriver = arrow::io::HdfsDriver;

struct JNIDriver {
  static HdfsDriver type;
};

struct PivotalDriver {
  static HdfsDriver type;
};

template <typename DRIVER>
class TestHadoopFileSystem : public ::testing::Test {
 public:
  void SetUp() override {
    const char* host = std::getenv("ARROW_HDFS_TEST_HOST");
    const char* port = std::getenv("ARROW_HDFS_TEST_PORT");
    const char* user = std::getenv("ARROW_HDFS_TEST_USER");

    std::string hdfs_host = host == nullptr ? "localhost" : std::string(host);
    int hdfs_port = port == nullptr ? 20500 : atoi(port);
    std::string hdfs_user = user == nullptr ? "root" : std::string(user);

    bool use_hdfs3;
    if (DRIVER::type == HdfsDriver::LIBHDFS) {
      use_hdfs3 = false;
    } else {
      use_hdfs3 = true;
    }

    HdfsOptions hdfs_options;
    hdfs_options.ConfigureEndPoint(hdfs_host, hdfs_port);
    hdfs_options.ConfigureHdfsUser(hdfs_user);
    hdfs_options.ConfigureHdfsDriver(use_hdfs3);
    hdfs_options.ConfigureHdfsReplication(0);

    std::shared_ptr<HadoopFileSystem> hdfs;
    auto status = HadoopFileSystem::Make(hdfs_options, &hdfs);
    if (!status.ok()) {
      loaded_driver_ = false;
      return;
    }
    loaded_driver_ = true;
    fs_ = std::make_shared<SubTreeFileSystem>("", hdfs);
  }

 protected:
  std::shared_ptr<FileSystem> fs_;
  bool loaded_driver_ = false;
};

HdfsDriver JNIDriver::type = HdfsDriver::LIBHDFS;
HdfsDriver PivotalDriver::type = HdfsDriver::LIBHDFS3;

typedef ::testing::Types<JNIDriver, PivotalDriver> DriverTypes;

TYPED_TEST_CASE(TestHadoopFileSystem, DriverTypes);

#define SKIP_IF_NO_DRIVER()                                  \
  if (!this->loaded_driver_) {                               \
    std::cout << "Driver not loaded, skipping" << std::endl; \
    return;                                                  \
  }

TYPED_TEST(TestHadoopFileSystem, CreateDir) {
  SKIP_IF_NO_DRIVER();

  std::string directory_name = "AB";
  ASSERT_OK(this->fs_->CreateDir(directory_name));
}

TYPED_TEST(TestHadoopFileSystem, GetTargetStats) {
  SKIP_IF_NO_DRIVER();

  std::string directory_name = "AB";
  FileStats stat;
  ASSERT_OK(this->fs_->GetTargetStats(directory_name, &stat));
  AssertFileStats(stat, "AB", FileType::Directory);
}

TYPED_TEST(TestHadoopFileSystem, MoveDir) {
  SKIP_IF_NO_DRIVER();

  std::string directory_name_src = "AB";
  std::string directory_name_dest = "CD";
  ASSERT_OK(this->fs_->Move(directory_name_src, directory_name_dest));
  FileStats stat;
  ASSERT_OK(this->fs_->GetTargetStats(directory_name_dest, &stat));
  AssertFileStats(stat, "CD", FileType::Directory);
}

TYPED_TEST(TestHadoopFileSystem, WriteReadFile) {
  SKIP_IF_NO_DRIVER();

  std::string file_name = "CD/abc";
  std::string data = "some data";
  std::shared_ptr<io::OutputStream> stream;
  ASSERT_OK(this->fs_->OpenOutputStream(file_name, &stream));
  auto data_size = static_cast<int64_t>(data.size());
  ASSERT_OK(stream->Write(data.data(), data_size));
  ASSERT_OK(stream->Close());

  std::shared_ptr<io::RandomAccessFile> file;
  ASSERT_OK(this->fs_->OpenInputFile(file_name, &file));
  int64_t file_size;
  ASSERT_OK(file->GetSize(&file_size));
  ASSERT_EQ(SOME_DATA_SIZE, file_size);
  uint8_t buffer[SOME_DATA_SIZE];
  int64_t bytes_read = 0;
  ASSERT_OK(file->Read(SOME_DATA_SIZE, &bytes_read, buffer));
  ASSERT_EQ(0, std::memcmp(buffer, data.c_str(), SOME_DATA_SIZE));
}

TYPED_TEST(TestHadoopFileSystem, DeleteFile) {
  SKIP_IF_NO_DRIVER();

  std::string file_name = "CD/abc";
  ASSERT_OK(this->fs_->DeleteFile(file_name));
  FileStats stat;
  ASSERT_TRUE(Status::OK() != this->fs_->GetTargetStats(file_name, &stat));
}

TYPED_TEST(TestHadoopFileSystem, DeleteDir) {
  SKIP_IF_NO_DRIVER();

  std::string directory_name = "CD";
  ASSERT_OK(this->fs_->DeleteDir(directory_name));
  FileStats stat;
  ASSERT_TRUE(Status::OK() != this->fs_->GetTargetStats(directory_name, &stat));
}
#endif  // _WIN32

}  // namespace internal
}  // namespace fs
}  // namespace arrow
