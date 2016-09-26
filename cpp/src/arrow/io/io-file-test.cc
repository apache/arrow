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

#include <cstdint>
#include <cstdio>
#include <cstring>
#include <fcntl.h>
#include <fstream>
#include <memory>
#include <sstream>
#include <string>

#include "gtest/gtest.h"

#include "arrow/io/file.h"
#include "arrow/io/test-common.h"

namespace arrow {
namespace io {

static bool FileExists(const std::string& path) {
  return std::ifstream(path.c_str()).good();
}

static bool FileIsClosed(int fd) {
  if (-1 != fcntl(fd, F_GETFD)) { return false; }
  return errno == EBADF;
}

class FileTestFixture : public ::testing::Test {
 public:
  void SetUp() {
    path_ = "arrow-test-io-file-output-stream.txt";
    EnsureFileDeleted();
  }

  void TearDown() { EnsureFileDeleted(); }

  void EnsureFileDeleted() {
    if (FileExists(path_)) { std::remove(path_.c_str()); }
  }

 protected:
  std::string path_;
};

// ----------------------------------------------------------------------
// File output tests

class TestFileOutputStream : public FileTestFixture {
 public:
  void OpenFile() { ASSERT_OK(FileOutputStream::Open(path_, &file_)); }

 protected:
  std::shared_ptr<FileOutputStream> file_;
};

TEST_F(TestFileOutputStream, DestructorClosesFile) {
  int fd;
  {
    std::shared_ptr<FileOutputStream> file;
    ASSERT_OK(FileOutputStream::Open(path_, &file));
    fd = file->file_descriptor();
  }
  ASSERT_TRUE(FileIsClosed(fd));
}

TEST_F(TestFileOutputStream, Close) {
  OpenFile();

  const char* data = "testdata";
  ASSERT_OK(file_->Write(reinterpret_cast<const uint8_t*>(data), strlen(data)));

  int fd = file_->file_descriptor();
  file_->Close();

  ASSERT_TRUE(FileIsClosed(fd));

  // Idempotent
  file_->Close();

  std::shared_ptr<ReadableFile> rd_file;
  ASSERT_OK(ReadableFile::Open(path_, &rd_file));

  int64_t size = 0;
  ASSERT_OK(rd_file->GetSize(&size));
  ASSERT_EQ(strlen(data), size);
}

TEST_F(TestFileOutputStream, InvalidWrites) {
  OpenFile();

  const char* data = "";

  ASSERT_RAISES(IOError, file_->Write(reinterpret_cast<const uint8_t*>(data), -1));
}

TEST_F(TestFileOutputStream, Tell) {
  OpenFile();

  int64_t position;

  ASSERT_OK(file_->Tell(&position));
  ASSERT_EQ(0, position);

  const char* data = "testdata";
  ASSERT_OK(file_->Write(reinterpret_cast<const uint8_t*>(data), 8));
  ASSERT_OK(file_->Tell(&position));
  ASSERT_EQ(8, position);
}

// ----------------------------------------------------------------------
// File input tests

class TestReadableFile : public FileTestFixture {
 public:
  void OpenFile() { ASSERT_OK(ReadableFile::Open(path_, &file_)); }

  void MakeTestFile(const std::string& data) {
    std::ofstream stream;
    stream.open(path_.c_str());
    stream << data;
  }

 protected:
  std::shared_ptr<ReadableFile> file_;
};

TEST_F(TestReadableFile, NonExistentFile) {
  ASSERT_RAISES(IOError, ReadableFile::Open("0xDEADBEEF.txt", &file_));
}

}  // namespace io
}  // namespace arrow
