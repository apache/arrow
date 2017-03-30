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

#include <atomic>
#include <cstdint>
#include <cstdio>
#include <cstring>
#ifndef _MSC_VER
#include <fcntl.h>
#endif
#include <fstream>
#include <memory>
#include <sstream>
#include <string>
#include <thread>

#include "gtest/gtest.h"

#include "arrow/io/file.h"
#include "arrow/io/test-common.h"
#include "arrow/memory_pool.h"

namespace arrow {
namespace io {

static bool FileExists(const std::string& path) {
  return std::ifstream(path.c_str()).good();
}

#if defined(_MSC_VER)
void InvalidParamHandler(const wchar_t* expr, const wchar_t* func,
    const wchar_t* source_file, unsigned int source_line, uintptr_t reserved) {
  wprintf(L"Invalid parameter in funcion %s. Source: %s line %d expression %s", func,
      source_file, source_line, expr);
}
#endif

static bool FileIsClosed(int fd) {
#if defined(_MSC_VER)
  // Disables default behavior on wrong params which causes the application to crash
  // https://msdn.microsoft.com/en-us/library/ksazx244.aspx
  _set_invalid_parameter_handler(InvalidParamHandler);

  // Disables possible assertion alert box on invalid input arguments
  _CrtSetReportMode(_CRT_ASSERT, 0);

  int ret = static_cast<int>(_close(fd));
  return (ret == -1);
#else
  if (-1 != fcntl(fd, F_GETFD)) { return false; }
  return errno == EBADF;
#endif
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
  void OpenFile(bool append = false) {
    ASSERT_OK(FileOutputStream::Open(path_, append, &file_));
  }

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

TEST_F(TestFileOutputStream, TruncatesNewFile) {
  ASSERT_OK(FileOutputStream::Open(path_, &file_));

  const char* data = "testdata";
  ASSERT_OK(file_->Write(reinterpret_cast<const uint8_t*>(data), strlen(data)));
  ASSERT_OK(file_->Close());

  ASSERT_OK(FileOutputStream::Open(path_, &file_));
  ASSERT_OK(file_->Close());

  std::shared_ptr<ReadableFile> rd_file;
  ASSERT_OK(ReadableFile::Open(path_, &rd_file));

  int64_t size;
  ASSERT_OK(rd_file->GetSize(&size));
  ASSERT_EQ(0, size);
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

  // Can seek past end of file
  ASSERT_EQ(100, position);

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

  Status Reallocate(int64_t old_size, int64_t new_size, uint8_t** ptr) override {
    *ptr = reinterpret_cast<uint8_t*>(std::realloc(*ptr, new_size));

    if (*ptr == NULL) {
      std::stringstream ss;
      ss << "realloc of size " << new_size << " failed";
      return Status::OutOfMemory(ss.str());
    }

    return Status::OK();
  }

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

TEST_F(TestReadableFile, ThreadSafety) {
  std::string data = "foobar";
  {
    std::ofstream stream;
    stream.open(path_.c_str());
    stream << data;
  }

  MyMemoryPool pool;
  ASSERT_OK(ReadableFile::Open(path_, &pool, &file_));

  std::atomic<int> correct_count(0);
  const int niter = 10000;

  auto ReadData = [&correct_count, &data, niter, this]() {
    std::shared_ptr<Buffer> buffer;

    for (int i = 0; i < niter; ++i) {
      ASSERT_OK(file_->ReadAt(0, 3, &buffer));
      if (0 == memcmp(data.c_str(), buffer->data(), 3)) { correct_count += 1; }
    }
  };

  std::thread thread1(ReadData);
  std::thread thread2(ReadData);

  thread1.join();
  thread2.join();

  ASSERT_EQ(niter * 2, correct_count);
}

// ----------------------------------------------------------------------
// Memory map tests

class TestMemoryMappedFile : public ::testing::Test, public MemoryMapFixture {
 public:
  void TearDown() { MemoryMapFixture::TearDown(); }
};

TEST_F(TestMemoryMappedFile, InvalidUsages) {}

TEST_F(TestMemoryMappedFile, WriteRead) {
  const int64_t buffer_size = 1024;
  std::vector<uint8_t> buffer(buffer_size);

  test::random_bytes(1024, 0, buffer.data());

  const int reps = 5;

  std::string path = "ipc-write-read-test";
  CreateFile(path, reps * buffer_size);

  std::shared_ptr<MemoryMappedFile> result;
  ASSERT_OK(MemoryMappedFile::Open(path, FileMode::READWRITE, &result));

  int64_t position = 0;
  std::shared_ptr<Buffer> out_buffer;
  for (int i = 0; i < reps; ++i) {
    ASSERT_OK(result->Write(buffer.data(), buffer_size));
    ASSERT_OK(result->ReadAt(position, buffer_size, &out_buffer));

    ASSERT_EQ(0, memcmp(out_buffer->data(), buffer.data(), buffer_size));

    position += buffer_size;
  }
}

TEST_F(TestMemoryMappedFile, ReadOnly) {
  const int64_t buffer_size = 1024;
  std::vector<uint8_t> buffer(buffer_size);

  test::random_bytes(1024, 0, buffer.data());

  const int reps = 5;

  std::string path = "ipc-read-only-test";
  CreateFile(path, reps * buffer_size);

  std::shared_ptr<MemoryMappedFile> rwmmap;
  ASSERT_OK(MemoryMappedFile::Open(path, FileMode::READWRITE, &rwmmap));

  int64_t position = 0;
  for (int i = 0; i < reps; ++i) {
    ASSERT_OK(rwmmap->Write(buffer.data(), buffer_size));
    position += buffer_size;
  }
  rwmmap->Close();

  std::shared_ptr<MemoryMappedFile> rommap;
  ASSERT_OK(MemoryMappedFile::Open(path, FileMode::READ, &rommap));

  position = 0;
  std::shared_ptr<Buffer> out_buffer;
  for (int i = 0; i < reps; ++i) {
    ASSERT_OK(rommap->ReadAt(position, buffer_size, &out_buffer));

    ASSERT_EQ(0, memcmp(out_buffer->data(), buffer.data(), buffer_size));
    position += buffer_size;
  }
  rommap->Close();
}

TEST_F(TestMemoryMappedFile, RetainMemoryMapReference) {
  // ARROW-494

  const int64_t buffer_size = 1024;
  std::vector<uint8_t> buffer(buffer_size);

  test::random_bytes(1024, 0, buffer.data());

  std::string path = "ipc-read-only-test";
  CreateFile(path, buffer_size);

  {
    std::shared_ptr<MemoryMappedFile> rwmmap;
    ASSERT_OK(MemoryMappedFile::Open(path, FileMode::READWRITE, &rwmmap));
    ASSERT_OK(rwmmap->Write(buffer.data(), buffer_size));
    ASSERT_OK(rwmmap->Close());
  }

  std::shared_ptr<Buffer> out_buffer;

  {
    std::shared_ptr<MemoryMappedFile> rommap;
    ASSERT_OK(MemoryMappedFile::Open(path, FileMode::READ, &rommap));
    ASSERT_OK(rommap->Read(buffer_size, &out_buffer));
    ASSERT_OK(rommap->Close());
  }

  // valgrind will catch if memory is unmapped
  ASSERT_EQ(0, memcmp(out_buffer->data(), buffer.data(), buffer_size));
}

TEST_F(TestMemoryMappedFile, InvalidMode) {
  const int64_t buffer_size = 1024;
  std::vector<uint8_t> buffer(buffer_size);

  test::random_bytes(1024, 0, buffer.data());

  std::string path = "ipc-invalid-mode-test";
  CreateFile(path, buffer_size);

  std::shared_ptr<MemoryMappedFile> rommap;
  ASSERT_OK(MemoryMappedFile::Open(path, FileMode::READ, &rommap));

  ASSERT_RAISES(IOError, rommap->Write(buffer.data(), buffer_size));
}

TEST_F(TestMemoryMappedFile, InvalidFile) {
  std::string non_existent_path = "invalid-file-name-asfd";

  std::shared_ptr<MemoryMappedFile> result;
  ASSERT_RAISES(
      IOError, MemoryMappedFile::Open(non_existent_path, FileMode::READ, &result));
}

TEST_F(TestMemoryMappedFile, CastableToFileInterface) {
  std::shared_ptr<MemoryMappedFile> memory_mapped_file;
  std::shared_ptr<FileInterface> file = memory_mapped_file;
}

TEST_F(TestMemoryMappedFile, ThreadSafety) {
  std::string data = "foobar";
  std::string path = "ipc-multithreading-test";
  CreateFile(path, static_cast<int>(data.size()));

  std::shared_ptr<MemoryMappedFile> file;
  ASSERT_OK(MemoryMappedFile::Open(path, FileMode::READWRITE, &file));
  ASSERT_OK(file->Write(
      reinterpret_cast<const uint8_t*>(data.c_str()), static_cast<int64_t>(data.size())));

  std::atomic<int> correct_count(0);
  const int niter = 10000;

  auto ReadData = [&correct_count, &data, niter, &file]() {
    std::shared_ptr<Buffer> buffer;

    for (int i = 0; i < niter; ++i) {
      ASSERT_OK(file->ReadAt(0, 3, &buffer));
      if (0 == memcmp(data.c_str(), buffer->data(), 3)) { correct_count += 1; }
    }
  };

  std::thread thread1(ReadData);
  std::thread thread2(ReadData);

  thread1.join();
  thread2.join();

  ASSERT_EQ(niter * 2, correct_count);
}

}  // namespace io
}  // namespace arrow
