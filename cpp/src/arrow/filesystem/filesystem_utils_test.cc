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

#include <chrono>
#include <memory>
#include <sstream>
#include <string>
#include <utility>
#include <vector>

#include <gtest/gtest.h>

#include "arrow/filesystem/filesystem_utils.h"
#include "arrow/filesystem/path_util.h"
#include "arrow/filesystem/test_util.h"
#include "arrow/testing/gtest_util.h"
#include "arrow/util/io_util.h"
#include "arrow/util/uri.h"
#ifdef ARROW_HDFS
#include "arrow/io/hdfs.h"
#endif

namespace arrow {
namespace fs {

#define SOME_DATA_SIZE 9

struct CommonPathFormatter {
  std::string operator()(const arrow::internal::PlatformFilename& fn) {
    return fn.ToString();
  }
};

#ifndef _WIN32
using PathFormatters = ::testing::Types<CommonPathFormatter>;

class TestFileSystem : public ::testing::Test {
 public:
  void SetUp() override {}

  void TestFileReadWrite() {
    std::string data = "some data";
    // Test that the right location on disk is accessed
    std::shared_ptr<io::OutputStream> stream;
    ASSERT_OK(fs_->OpenOutputStream(file_name_, &stream));
    auto data_size = static_cast<int64_t>(data.size());
    ASSERT_OK(stream->Write(data.data(), data_size));
    ASSERT_OK(stream->Close());

    std::shared_ptr<io::RandomAccessFile> file;
    ASSERT_OK(fs_->OpenInputFile(file_name_, &file));
    int64_t file_size;
    ASSERT_OK(file->GetSize(&file_size));
    ASSERT_EQ(SOME_DATA_SIZE, file_size);
    uint8_t buffer[SOME_DATA_SIZE];
    int64_t bytes_read = 0;
    ASSERT_OK(file->Read(SOME_DATA_SIZE, &bytes_read, buffer));
    ASSERT_EQ(0, std::memcmp(buffer, data.c_str(), SOME_DATA_SIZE));
  }

  std::string GetDirectoryName(const std::string& file_name) {
    auto dir_file = internal::GetAbstractPathParent(file_name);
    return dir_file.first;
  }

  void TestCreateDir() { ASSERT_OK(fs_->CreateDir(GetDirectoryName(file_name_))); }

  void TestDeleteDir() { ASSERT_OK(fs_->DeleteDir(GetDirectoryName(file_name_))); }

 protected:
  std::shared_ptr<FileSystem> fs_;
  std::string file_name_;
};

template <typename PathFormatter>
class TestLocalFileSystem : public TestFileSystem {
 public:
  void SetUp() override {
    ASSERT_OK(arrow::internal::TemporaryDir::Make("test-localfs-", &temp_dir_));
    std::string full_path = PathFormatter()(temp_dir_->path());
    full_path.append("/AB/abc");
    arrow::internal::Uri uri;
    ASSERT_OK(uri.Parse(full_path));
    file_name_ = uri.path();
    ASSERT_OK(MakeFileSystem(full_path, &fs_));
  }

 protected:
  std::unique_ptr<arrow::internal::TemporaryDir> temp_dir_;
};

TYPED_TEST_CASE(TestLocalFileSystem, PathFormatters);

TYPED_TEST(TestLocalFileSystem, MakeFileSystemLocal) {
  this->TestCreateDir();
  this->TestFileReadWrite();
  this->TestDeleteDir();
}
#endif

}  // namespace fs
}  // namespace arrow
