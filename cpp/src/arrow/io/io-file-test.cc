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
#ifndef _MSC_VER
# include <fcntl.h>
#endif
#include <fstream>
#include <memory>
#include <sstream>
#include <string>

#include "gtest/gtest.h"

#include "arrow/io/file.h"
#include "arrow/io/test-common.h"
#include "arrow/util/memory-pool.h"

namespace arrow {
namespace io {

static bool FileExists(const std::string& path) {
  return std::ifstream(path.c_str()).good();
}

static bool FileIsClosed(int fd) {
#ifdef _MSC_VER
  // Close file a second time, this should set errno to EBADF
  close(fd);
#else
  if (-1 != fcntl(fd, F_GETFD)) { return false; }
#endif
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

  void MakeTestFile() {
    std::string data = "testdata";
    std::ofstream stream;
    stream.open(path_.c_str());
    stream << data;
  }

 protected:
  std::shared_ptr<ReadableFile> file_;
};

TEST_F(TestReadableFile, DestructorClosesFile) {
  MakeTestFile();

  int fd;
  {
    std::shared_ptr<ReadableFile> file;
    ASSERT_OK(ReadableFile::Open(path_, &file));
    fd = file->file_descriptor();
  }
  ASSERT_TRUE(FileIsClosed(fd));
}

TEST_F(TestReadableFile, Close) {
  MakeTestFile();
  OpenFile();

  int fd = file_->file_descriptor();
  file_->Close();

  ASSERT_TRUE(FileIsClosed(fd));

  // Idempotent
  file_->Close();
}

TEST_F(TestReadableFile, SeekTellSize) {
  MakeTestFile();
  OpenFile();

  int64_t position;
  ASSERT_OK(file_->Tell(&position));
  ASSERT_EQ(0, position);

  ASSERT_OK(file_->Seek(4));
  ASSERT_OK(file_->Tell(&position));
  ASSERT_EQ(4, position);

  ASSERT_OK(file_->Seek(100));
  ASSERT_OK(file_->Tell(&position));

  // now at EOF
  ASSERT_EQ(8, position);

  int64_t size;
  ASSERT_OK(file_->GetSize(&size));
  ASSERT_EQ(8, size);

  // does not support zero copy
  ASSERT_FALSE(file_->supports_zero_copy());
}

TEST_F(TestReadableFile, Read) {
  uint8_t buffer[50];

  MakeTestFile();
  OpenFile();

  int64_t bytes_read;
  ASSERT_OK(file_->Read(4, &bytes_read, buffer));
  ASSERT_EQ(4, bytes_read);
  ASSERT_EQ(0, std::memcmp(buffer, "test", 4));

  ASSERT_OK(file_->Read(10, &bytes_read, buffer));
  ASSERT_EQ(4, bytes_read);
  ASSERT_EQ(0, std::memcmp(buffer, "data", 4));
}

TEST_F(TestReadableFile, ReadAt) {
  uint8_t buffer[50];
  const char* test_data = "testdata";

  MakeTestFile();
  OpenFile();

  int64_t bytes_read;
  int64_t position;

  ASSERT_OK(file_->ReadAt(0, 4, &bytes_read, buffer));
  ASSERT_EQ(4, bytes_read);
  ASSERT_EQ(0, std::memcmp(buffer, "test", 4));

  // position advanced
  ASSERT_OK(file_->Tell(&position));
  ASSERT_EQ(4, position);

  ASSERT_OK(file_->ReadAt(4, 10, &bytes_read, buffer));
  ASSERT_EQ(4, bytes_read);
  ASSERT_EQ(0, std::memcmp(buffer, "data", 4));

  // position advanced to EOF
  ASSERT_OK(file_->Tell(&position));
  ASSERT_EQ(8, position);

  // Check buffer API
  std::shared_ptr<Buffer> buffer2;

  ASSERT_OK(file_->ReadAt(0, 4, &buffer2));
  ASSERT_EQ(4, buffer2->size());

  Buffer expected(reinterpret_cast<const uint8_t*>(test_data), 4);
  ASSERT_TRUE(buffer2->Equals(expected));

  // position advanced
  ASSERT_OK(file_->Tell(&position));
  ASSERT_EQ(4, position);
}

TEST_F(TestReadableFile, NonExistentFile) {
  ASSERT_RAISES(IOError, ReadableFile::Open("0xDEADBEEF.txt", &file_));
}

class MyMemoryPool : public MemoryPool {
 public:
  MyMemoryPool() : num_allocations_(0) {}

  Status Allocate(int64_t size, uint8_t** out) override {
    *out = reinterpret_cast<uint8_t*>(std::malloc(size));
    ++num_allocations_;
    return Status::OK();
  }

  void Free(uint8_t* buffer, int64_t size) override { std::free(buffer); }

  int64_t bytes_allocated() const override { return -1; }

  int64_t num_allocations() const { return num_allocations_; }

 private:
  int64_t num_allocations_;
};

TEST_F(TestReadableFile, CustomMemoryPool) {
  MakeTestFile();

  MyMemoryPool pool;
  ASSERT_OK(ReadableFile::Open(path_, &pool, &file_));

  std::shared_ptr<Buffer> buffer;
  ASSERT_OK(file_->ReadAt(0, 4, &buffer));
  ASSERT_OK(file_->ReadAt(4, 8, &buffer));

  ASSERT_EQ(2, pool.num_allocations());
}

}  // namespace io
}  // namespace arrow
