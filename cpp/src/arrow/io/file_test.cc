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

#ifndef _WIN32
#include <fcntl.h>  // IWYU pragma: keep
#include <unistd.h>
#endif

#include <atomic>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <fstream>
#include <memory>
#include <string>
#include <thread>
#include <vector>

#include <gtest/gtest.h>

#include "arrow/buffer.h"
#include "arrow/io/file.h"
#include "arrow/io/interfaces.h"
#include "arrow/io/test_common.h"
#include "arrow/memory_pool.h"
#include "arrow/status.h"
#include "arrow/testing/gtest_util.h"
#include "arrow/testing/util.h"
#include "arrow/util/future.h"
#include "arrow/util/io_util.h"

namespace arrow {

using internal::CreatePipe;
using internal::FileClose;
using internal::FileDescriptor;
using internal::FileGetSize;
using internal::FileOpenReadable;
using internal::FileOpenWritable;
using internal::FileRead;
using internal::FileSeek;
using internal::PlatformFilename;
using internal::TemporaryDir;

namespace io {

class FileTestFixture : public ::testing::Test {
 public:
  void SetUp() {
    ASSERT_OK_AND_ASSIGN(temp_dir_, TemporaryDir::Make("file-test-"));
    path_ = TempFile("arrow-test-io-file.txt");
    EnsureFileDeleted();
  }

  std::string TempFile(std::string_view path) {
    return temp_dir_->path().Join(std::string(path)).ValueOrDie().ToString();
  }

  void TearDown() { EnsureFileDeleted(); }

  void EnsureFileDeleted() {
    if (FileExists(path_)) {
      ARROW_UNUSED(std::remove(path_.c_str()));
    }
  }

 protected:
  std::unique_ptr<TemporaryDir> temp_dir_;
  std::string path_;
};

// ----------------------------------------------------------------------
// File output tests

class TestFileOutputStream : public FileTestFixture {
 public:
  void OpenFile(bool append = false) {
    ASSERT_OK_AND_ASSIGN(file_, FileOutputStream::Open(path_, append));
  }

  void OpenFileDescriptor() {
    ASSERT_OK_AND_ASSIGN(auto file_name, PlatformFilename::FromString(path_));
    ASSERT_OK_AND_ASSIGN(
        FileDescriptor fd,
        FileOpenWritable(file_name, true /* write_only */, false /* truncate */));
    ASSERT_OK_AND_ASSIGN(file_, FileOutputStream::Open(fd.Detach()));
  }

 protected:
  std::shared_ptr<FileOutputStream> file_;
};

#if defined(_MSC_VER)
TEST_F(TestFileOutputStream, FileNameWideCharConversionRangeException) {
  // Invalid utf-8 filename
  std::string file_name = "\x80";
  ASSERT_RAISES(Invalid, FileOutputStream::Open(file_name));
  ASSERT_RAISES(Invalid, ReadableFile::Open(file_name));
}

// TODO add a test with a valid utf-8 filename
#endif

TEST_F(TestFileOutputStream, DestructorClosesFile) {
  int fd_file;

  OpenFile();
  fd_file = file_->file_descriptor();
  ASSERT_FALSE(FileIsClosed(fd_file));
  file_.reset();
  ASSERT_TRUE(FileIsClosed(fd_file));

  OpenFileDescriptor();
  fd_file = file_->file_descriptor();
  ASSERT_FALSE(FileIsClosed(fd_file));
  file_.reset();
  ASSERT_TRUE(FileIsClosed(fd_file));
}

TEST_F(TestFileOutputStream, Close) {
  OpenFile();

  const char* data = "testdata";
  ASSERT_OK(file_->Write(data, strlen(data)));

  int fd = file_->file_descriptor();
  ASSERT_FALSE(file_->closed());
  ASSERT_OK(file_->Close());
  ASSERT_TRUE(file_->closed());
  ASSERT_TRUE(FileIsClosed(fd));
  ASSERT_RAISES(Invalid, file_->Write(data, strlen(data)));

  // Idempotent
  ASSERT_OK(file_->Close());

  AssertFileContents(path_, data);
}

TEST_F(TestFileOutputStream, FromFileDescriptor) {
  OpenFileDescriptor();

  std::string data1 = "test";
  ASSERT_OK(file_->Write(data1.data(), data1.size()));
  int raw_fd = file_->file_descriptor();
  ASSERT_OK(file_->Close());
  ASSERT_TRUE(FileIsClosed(raw_fd));

  AssertFileContents(path_, data1);

  // Re-open at end of file
  ASSERT_OK_AND_ASSIGN(auto file_name, PlatformFilename::FromString(path_));
  ASSERT_OK_AND_ASSIGN(
      FileDescriptor fd,
      FileOpenWritable(file_name, true /* write_only */, false /* truncate */));
  raw_fd = fd.Detach();
  ASSERT_OK(FileSeek(raw_fd, 0, SEEK_END));
  ASSERT_OK_AND_ASSIGN(file_, FileOutputStream::Open(raw_fd));

  std::string data2 = "data";
  ASSERT_OK(file_->Write(data2.data(), data2.size()));
  ASSERT_OK(file_->Close());

  AssertFileContents(path_, data1 + data2);
}

TEST_F(TestFileOutputStream, InvalidWrites) {
  OpenFile();

  const char* data = "";

  ASSERT_RAISES(IOError, file_->Write(data, -1));
}

TEST_F(TestFileOutputStream, Tell) {
  OpenFile();

  ASSERT_OK_AND_EQ(0, file_->Tell());

  const char* data = "testdata";
  ASSERT_OK(file_->Write(data, 8));
  ASSERT_OK_AND_EQ(8, file_->Tell());
}

TEST_F(TestFileOutputStream, TruncatesNewFile) {
  ASSERT_OK_AND_ASSIGN(file_, FileOutputStream::Open(path_));

  const char* data = "testdata";
  ASSERT_OK(file_->Write(data, strlen(data)));
  ASSERT_OK(file_->Close());

  ASSERT_OK_AND_ASSIGN(file_, FileOutputStream::Open(path_));
  ASSERT_OK(file_->Close());

  AssertFileContents(path_, "");
}

TEST_F(TestFileOutputStream, Append) {
  ASSERT_OK_AND_ASSIGN(file_, FileOutputStream::Open(path_));
  {
    const char* data = "test";
    ASSERT_OK(file_->Write(data, strlen(data)));
  }
  ASSERT_OK(file_->Close());
  ASSERT_OK_AND_ASSIGN(file_, FileOutputStream::Open(path_, true /* append */));
  {
    const char* data = "data";
    ASSERT_OK(file_->Write(data, strlen(data)));
  }
  ASSERT_OK(file_->Close());
  AssertFileContents(path_, "testdata");
}

// ----------------------------------------------------------------------
// File input tests

class TestReadableFile : public FileTestFixture {
 public:
  void OpenFile() { ASSERT_OK_AND_ASSIGN(file_, ReadableFile::Open(path_)); }

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
    ASSERT_OK_AND_ASSIGN(auto file, ReadableFile::Open(path_));
    fd = file->file_descriptor();
  }
  ASSERT_TRUE(FileIsClosed(fd));
}

TEST_F(TestReadableFile, Close) {
  MakeTestFile();
  OpenFile();

  int fd = file_->file_descriptor();
  ASSERT_FALSE(file_->closed());
  ASSERT_OK(file_->Close());
  ASSERT_TRUE(file_->closed());

  ASSERT_TRUE(FileIsClosed(fd));

  // Idempotent
  ASSERT_OK(file_->Close());
  ASSERT_TRUE(FileIsClosed(fd));
}

TEST_F(TestReadableFile, FromFileDescriptor) {
  MakeTestFile();

  ASSERT_OK_AND_ASSIGN(auto file_name, PlatformFilename::FromString(path_));
  ASSERT_OK_AND_ASSIGN(FileDescriptor fd, FileOpenReadable(file_name));
  int raw_fd = fd.fd();
  ASSERT_GE(raw_fd, 0);
  ASSERT_OK(FileSeek(raw_fd, 4));

  ASSERT_OK_AND_ASSIGN(file_, ReadableFile::Open(fd.Detach()));
  ASSERT_EQ(file_->file_descriptor(), raw_fd);
  ASSERT_OK_AND_ASSIGN(auto buf, file_->Read(5));
  ASSERT_EQ(buf->size(), 4);
  ASSERT_TRUE(buf->Equals(Buffer("data")));

  ASSERT_FALSE(FileIsClosed(raw_fd));
  ASSERT_OK(file_->Close());
  ASSERT_TRUE(FileIsClosed(raw_fd));
  // Idempotent
  ASSERT_OK(file_->Close());
  ASSERT_TRUE(FileIsClosed(raw_fd));
}

TEST_F(TestReadableFile, Peek) {
  MakeTestFile();
  OpenFile();

  // Cannot peek
  ASSERT_RAISES(NotImplemented, file_->Peek(4));
}

TEST_F(TestReadableFile, SeekTellSize) {
  MakeTestFile();
  OpenFile();

  ASSERT_OK_AND_EQ(0, file_->Tell());

  ASSERT_OK(file_->Seek(4));
  ASSERT_OK_AND_EQ(4, file_->Tell());

  // Can seek past end of file
  ASSERT_OK(file_->Seek(100));
  ASSERT_OK_AND_EQ(100, file_->Tell());

  ASSERT_OK_AND_EQ(8, file_->GetSize());

  ASSERT_OK_AND_EQ(100, file_->Tell());

  // does not support zero copy
  ASSERT_FALSE(file_->supports_zero_copy());
}

TEST_F(TestReadableFile, Read) {
  uint8_t buffer[50];

  MakeTestFile();
  OpenFile();

  ASSERT_OK_AND_EQ(4, file_->Read(4, buffer));
  ASSERT_EQ(0, std::memcmp(buffer, "test", 4));

  ASSERT_OK_AND_EQ(4, file_->Read(10, buffer));
  ASSERT_EQ(0, std::memcmp(buffer, "data", 4));

  // Test incomplete read, ARROW-1094
  ASSERT_OK_AND_ASSIGN(int64_t size, file_->GetSize());

  ASSERT_OK(file_->Seek(1));
  ASSERT_OK_AND_ASSIGN(auto buf, file_->Read(size));
  ASSERT_EQ(size - 1, buf->size());

  ASSERT_OK(file_->Close());
  ASSERT_RAISES(Invalid, file_->Read(1));
}

TEST_F(TestReadableFile, ReadAt) {
  uint8_t buffer[50];
  const char* test_data = "testdata";

  MakeTestFile();
  OpenFile();

  ASSERT_OK_AND_EQ(4, file_->ReadAt(0, 4, buffer));
  ASSERT_EQ(0, std::memcmp(buffer, "test", 4));

  ASSERT_OK_AND_EQ(7, file_->ReadAt(1, 10, buffer));
  ASSERT_EQ(0, std::memcmp(buffer, "estdata", 7));

  // Check buffer API
  ASSERT_OK_AND_ASSIGN(auto buffer2, file_->ReadAt(2, 5));
  ASSERT_EQ(5, buffer2->size());

  Buffer expected(reinterpret_cast<const uint8_t*>(test_data + 2), 5);
  ASSERT_TRUE(buffer2->Equals(expected));

  // Invalid reads
  ASSERT_RAISES(Invalid, file_->ReadAt(-1, 1));
  ASSERT_RAISES(Invalid, file_->ReadAt(1, -1));
  ASSERT_RAISES(Invalid, file_->ReadAt(-1, 1, buffer));
  ASSERT_RAISES(Invalid, file_->ReadAt(1, -1, buffer));

  ASSERT_OK(file_->Close());
  ASSERT_RAISES(Invalid, file_->ReadAt(0, 1));
}

TEST_F(TestReadableFile, ReadAsync) {
  MakeTestFile();
  OpenFile();

  auto fut1 = file_->ReadAsync({}, 1, 10);
  auto fut2 = file_->ReadAsync({}, 0, 4);
  ASSERT_OK_AND_ASSIGN(auto buf1, fut1.result());
  ASSERT_OK_AND_ASSIGN(auto buf2, fut2.result());
  AssertBufferEqual(*buf1, "estdata");
  AssertBufferEqual(*buf2, "test");
}

TEST_F(TestReadableFile, ReadManyAsync) {
  MakeTestFile();
  OpenFile();

  std::vector<ReadRange> ranges = {{1, 3}, {2, 5}, {4, 2}};
  auto futs = file_->ReadManyAsync(std::move(ranges));

  ASSERT_EQ(futs.size(), 3);
  ASSERT_OK_AND_ASSIGN(auto buf1, futs[0].result());
  ASSERT_OK_AND_ASSIGN(auto buf2, futs[1].result());
  ASSERT_OK_AND_ASSIGN(auto buf3, futs[2].result());
  AssertBufferEqual(*buf1, "est");
  AssertBufferEqual(*buf2, "stdat");
  AssertBufferEqual(*buf3, "da");
}

TEST_F(TestReadableFile, SeekingRequired) {
  MakeTestFile();
  OpenFile();

  ASSERT_OK_AND_ASSIGN(auto buffer, file_->ReadAt(0, 4));
  AssertBufferEqual(*buffer, "test");

  ASSERT_RAISES(Invalid, file_->Read(4));
  ASSERT_OK(file_->Seek(0));
  ASSERT_OK_AND_ASSIGN(buffer, file_->Read(4));
  AssertBufferEqual(*buffer, "test");
}

TEST_F(TestReadableFile, WillNeed) {
  MakeTestFile();
  OpenFile();

  ASSERT_OK(file_->WillNeed({}));
  ASSERT_OK(file_->WillNeed({{0, 3}, {4, 6}}));
  ASSERT_OK(file_->WillNeed({{10, 0}}));

  ASSERT_RAISES(Invalid, file_->WillNeed({{-1, -1}}));
}

TEST_F(TestReadableFile, NonexistentFile) {
  std::string path = "0xDEADBEEF.txt";
  auto maybe_file = ReadableFile::Open(path);
  ASSERT_RAISES(IOError, maybe_file);
  std::string message = maybe_file.status().message();
  ASSERT_NE(std::string::npos, message.find(path));
}

class MyMemoryPool : public MemoryPool {
 public:
  MyMemoryPool() : num_allocations_(0) {}

  Status Allocate(int64_t size, int64_t /*alignment*/, uint8_t** out) override {
    *out = reinterpret_cast<uint8_t*>(std::malloc(size));
    ++num_allocations_;
    return Status::OK();
  }

  void Free(uint8_t* buffer, int64_t size, int64_t /*alignment*/) override {
    std::free(buffer);
  }

  Status Reallocate(int64_t old_size, int64_t new_size, int64_t /*alignment*/,
                    uint8_t** ptr) override {
    *ptr = reinterpret_cast<uint8_t*>(std::realloc(*ptr, new_size));

    if (*ptr == NULL) {
      return Status::OutOfMemory("realloc of size ", new_size, " failed");
    }

    return Status::OK();
  }

  int64_t bytes_allocated() const override { return -1; }

  int64_t total_bytes_allocated() const override { return -1; }

  std::string backend_name() const override { return "my"; }

  int64_t num_allocations() const override { return num_allocations_.load(); }

 private:
  std::atomic<int64_t> num_allocations_;
};

TEST_F(TestReadableFile, CustomMemoryPool) {
  MakeTestFile();

  MyMemoryPool pool;
  ASSERT_OK_AND_ASSIGN(file_, ReadableFile::Open(path_, &pool));

  ASSERT_OK_AND_ASSIGN(auto buffer, file_->ReadAt(0, 4));
  ASSERT_OK_AND_ASSIGN(buffer, file_->ReadAt(4, 8));

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
  ASSERT_OK_AND_ASSIGN(file_, ReadableFile::Open(path_, &pool));

  std::atomic<int> correct_count(0);
  int niter = 30000;

  auto ReadData = [&correct_count, &data, &niter, this]() {
    for (int i = 0; i < niter; ++i) {
      const int offset = i % 3;
      ASSERT_OK_AND_ASSIGN(auto buffer, file_->ReadAt(offset, 3));
      if (0 == memcmp(data.c_str() + offset, buffer->data(), 3)) {
        correct_count += 1;
      }
    }
  };

  std::thread thread1(ReadData);
  std::thread thread2(ReadData);

  thread1.join();
  thread2.join();

  ASSERT_EQ(niter * 2, correct_count);
}

// ----------------------------------------------------------------------
// Pipe I/O tests using FileOutputStream
// (cannot test using ReadableFile as it currently requires seeking)

class TestPipeIO : public ::testing::Test {
 public:
  void MakePipe() {
    ASSERT_OK_AND_ASSIGN(pipe_, CreatePipe());
    ASSERT_GE(pipe_.rfd.fd(), 0);
    ASSERT_GE(pipe_.rfd.fd(), 0);
  }
  void ClosePipe() {
    ASSERT_OK(pipe_.rfd.Close());
    ASSERT_OK(pipe_.wfd.Close());
  }
  void TearDown() { ClosePipe(); }

 protected:
  ::arrow::internal::Pipe pipe_;
};

TEST_F(TestPipeIO, TestWrite) {
  std::string data1 = "test", data2 = "data!";
  std::shared_ptr<FileOutputStream> file;
  uint8_t buffer[10];
  int64_t bytes_read;

  MakePipe();
  ASSERT_OK_AND_ASSIGN(file, FileOutputStream::Open(pipe_.wfd.Detach()));

  ASSERT_OK(file->Write(data1.data(), data1.size()));
  ASSERT_OK_AND_ASSIGN(bytes_read, FileRead(pipe_.rfd.fd(), buffer, 4));
  ASSERT_EQ(bytes_read, 4);
  ASSERT_EQ(0, std::memcmp(buffer, "test", 4));

  ASSERT_OK(file->Write(Buffer::FromString(std::string(data2))));
  ASSERT_OK_AND_ASSIGN(bytes_read, FileRead(pipe_.rfd.fd(), buffer, 4));
  ASSERT_EQ(bytes_read, 4);
  ASSERT_EQ(0, std::memcmp(buffer, "data", 4));

  ASSERT_FALSE(file->closed());
  ASSERT_OK(file->Close());
  ASSERT_TRUE(file->closed());
  ASSERT_OK_AND_ASSIGN(bytes_read, FileRead(pipe_.rfd.fd(), buffer, 2));
  ASSERT_EQ(bytes_read, 1);
  ASSERT_EQ(0, std::memcmp(buffer, "!", 1));
  // EOF reached
  ASSERT_OK_AND_ASSIGN(bytes_read, FileRead(pipe_.rfd.fd(), buffer, 2));
  ASSERT_EQ(bytes_read, 0);
}

TEST_F(TestPipeIO, ReadableFileFails) {
  // ReadableFile fails on non-seekable fd
  ASSERT_RAISES(IOError, ReadableFile::Open(pipe_.rfd.fd()));
}

// ----------------------------------------------------------------------
// Memory map tests

class TestMemoryMappedFile : public ::testing::Test, public MemoryMapFixture {
 public:
  void SetUp() override {
    ASSERT_OK_AND_ASSIGN(temp_dir_, TemporaryDir::Make("memory-map-test-"));
  }

  void TearDown() override { MemoryMapFixture::TearDown(); }

  std::string TempFile(std::string_view path) {
    return temp_dir_->path().Join(std::string(path)).ValueOrDie().ToString();
  }

 protected:
  std::unique_ptr<TemporaryDir> temp_dir_;
};

TEST_F(TestMemoryMappedFile, InvalidUsages) {}

TEST_F(TestMemoryMappedFile, ZeroSizeFile) {
  std::string path = TempFile("io-memory-map-zero-size");
  ASSERT_OK_AND_ASSIGN(auto result, InitMemoryMap(0, path));

  ASSERT_OK_AND_EQ(0, result->Tell());
}

TEST_F(TestMemoryMappedFile, MapPartFile) {
  const int64_t buffer_size = 1024;
  const int64_t unalign_offset = 1024;
  const int64_t offset = 65536;  // make WIN32 happy
  std::vector<uint8_t> buffer(buffer_size);

  random_bytes(1024, 0, buffer.data());

  const int reps = 128;

  std::string path = TempFile("io-memory-map-offset");

  // file size = 128k
  CreateFile(path, reps * buffer_size);

  // map failed with unaligned offset
  ASSERT_RAISES(IOError,
                MemoryMappedFile::Open(path, FileMode::READWRITE, unalign_offset, 4096));

  // map failed if length is greater than file size
  ASSERT_RAISES(Invalid,
                MemoryMappedFile::Open(path, FileMode::READWRITE, offset, 409600));

  // map succeeded with valid file region <64k-68k>
  ASSERT_OK_AND_ASSIGN(auto result,
                       MemoryMappedFile::Open(path, FileMode::READWRITE, offset, 4096));

  ASSERT_OK_AND_EQ(4096, result->GetSize());

  ASSERT_OK_AND_EQ(0, result->Tell());

  ASSERT_OK(result->Write(buffer.data(), buffer_size));
  ASSERT_OK_AND_ASSIGN(auto out_buffer, result->ReadAt(0, buffer_size));
  ASSERT_EQ(0, memcmp(out_buffer->data(), buffer.data(), buffer_size));

  ASSERT_OK_AND_EQ(buffer_size, result->Tell());

  ASSERT_OK(result->Seek(4096));
  ASSERT_OK_AND_EQ(4096, result->Tell());

  // Resize is not supported
  ASSERT_RAISES(IOError, result->Resize(4096));

  // Write beyond memory mapped length
  ASSERT_RAISES(IOError, result->WriteAt(4096, buffer.data(), buffer_size));
}

TEST_F(TestMemoryMappedFile, WriteRead) {
  const int64_t buffer_size = 1024;
  std::vector<uint8_t> buffer(buffer_size);
  random_bytes(1024, 0, buffer.data());

  const int reps = 5;

  std::string path = TempFile("io-memory-map-write-read-test");
  ASSERT_OK_AND_ASSIGN(auto result, InitMemoryMap(reps * buffer_size, path));

  int64_t position = 0;
  for (int i = 0; i < reps; ++i) {
    ASSERT_OK(result->Write(buffer.data(), buffer_size));
    ASSERT_OK_AND_ASSIGN(auto out_buffer, result->ReadAt(position, buffer_size));

    ASSERT_EQ(0, memcmp(out_buffer->data(), buffer.data(), buffer_size));

    position += buffer_size;
  }
}

TEST_F(TestMemoryMappedFile, ReadAsync) {
  const int64_t buffer_size = 1024;
  std::vector<uint8_t> buffer(buffer_size);
  random_bytes(1024, 0, buffer.data());

  std::string path = TempFile("io-memory-map-read-async-test");
  ASSERT_OK_AND_ASSIGN(auto mmap, InitMemoryMap(buffer_size, path));
  ASSERT_OK(mmap->Write(buffer.data(), buffer_size));

  auto fut1 = mmap->ReadAsync({}, 1, 1000);
  auto fut2 = mmap->ReadAsync({}, 3, 4);
  ASSERT_EQ(fut1.state(), FutureState::SUCCESS);
  ASSERT_EQ(fut2.state(), FutureState::SUCCESS);
  ASSERT_OK_AND_ASSIGN(auto buf1, fut1.result());
  ASSERT_OK_AND_ASSIGN(auto buf2, fut2.result());

  AssertBufferEqual(*buf1, Buffer(buffer.data() + 1, 1000));
  AssertBufferEqual(*buf2, Buffer(buffer.data() + 3, 4));
}

TEST_F(TestMemoryMappedFile, WillNeed) {
  const int64_t buffer_size = 1024;
  std::vector<uint8_t> buffer(buffer_size);
  random_bytes(1024, 0, buffer.data());

  std::string path = TempFile("io-memory-map-will-need-test");
  ASSERT_OK_AND_ASSIGN(auto mmap, InitMemoryMap(buffer_size, path));
  ASSERT_OK(mmap->Write(buffer.data(), buffer_size));

  ASSERT_OK(mmap->WillNeed({}));
  ASSERT_OK(mmap->WillNeed({{0, 4}, {100, 924}}));
  ASSERT_OK(mmap->WillNeed({{1024, 0}}));
  ASSERT_RAISES(IOError, mmap->WillNeed({{1025, 1}}));  // Out of bounds
}

TEST_F(TestMemoryMappedFile, InvalidReads) {
  std::string path = TempFile("io-memory-map-invalid-reads-test");
  ASSERT_OK_AND_ASSIGN(auto result, InitMemoryMap(4096, path));

  uint8_t buffer[10];

  ASSERT_RAISES(Invalid, result->ReadAt(-1, 1));
  ASSERT_RAISES(Invalid, result->ReadAt(1, -1));
  ASSERT_RAISES(Invalid, result->ReadAt(-1, 1, buffer));
  ASSERT_RAISES(Invalid, result->ReadAt(1, -1, buffer));
}

TEST_F(TestMemoryMappedFile, WriteResizeRead) {
  const int64_t buffer_size = 1024;
  const int reps = 5;
  std::vector<std::vector<uint8_t>> buffers(reps);
  for (auto& b : buffers) {
    b.resize(buffer_size);
    random_bytes(buffer_size, 0, b.data());
  }

  std::string path = TempFile("io-memory-map-write-read-test");
  ASSERT_OK_AND_ASSIGN(auto result, InitMemoryMap(buffer_size, path));

  int64_t position = 0;
  for (int i = 0; i < reps; ++i) {
    if (i != 0) {
      ASSERT_OK(result->Resize(buffer_size * (i + 1)));
    }
    ASSERT_OK(result->Write(buffers[i].data(), buffer_size));
    ASSERT_OK_AND_ASSIGN(auto out_buffer, result->ReadAt(position, buffer_size));

    ASSERT_EQ(out_buffer->size(), buffer_size);
    ASSERT_EQ(0, memcmp(out_buffer->data(), buffers[i].data(), buffer_size));
    out_buffer.reset();

    position += buffer_size;
  }
}

TEST_F(TestMemoryMappedFile, ResizeRaisesOnExported) {
  const int64_t buffer_size = 1024;
  std::vector<uint8_t> buffer(buffer_size);
  random_bytes(buffer_size, 0, buffer.data());

  std::string path = TempFile("io-memory-map-write-read-test");
  ASSERT_OK_AND_ASSIGN(auto result, InitMemoryMap(buffer_size, path));

  ASSERT_OK(result->Write(buffer.data(), buffer_size));
  ASSERT_OK_AND_ASSIGN(auto out_buffer1, result->ReadAt(0, buffer_size));
  ASSERT_OK_AND_ASSIGN(auto out_buffer2, result->ReadAt(0, buffer_size));
  ASSERT_EQ(0, memcmp(out_buffer1->data(), buffer.data(), buffer_size));
  ASSERT_EQ(0, memcmp(out_buffer2->data(), buffer.data(), buffer_size));

  // attempt resize
  ASSERT_RAISES(IOError, result->Resize(2 * buffer_size));

  out_buffer1.reset();

  ASSERT_RAISES(IOError, result->Resize(2 * buffer_size));

  out_buffer2.reset();

  ASSERT_OK(result->Resize(2 * buffer_size));

  ASSERT_OK_AND_EQ(buffer_size * 2, result->GetSize());
  ASSERT_OK_AND_EQ(buffer_size * 2, FileGetSize(result->file_descriptor()));
}

TEST_F(TestMemoryMappedFile, WriteReadZeroInitSize) {
  const int64_t buffer_size = 1024;
  std::vector<uint8_t> buffer(buffer_size);
  random_bytes(buffer_size, 0, buffer.data());

  std::string path = TempFile("io-memory-map-write-read-test");
  ASSERT_OK_AND_ASSIGN(auto result, InitMemoryMap(0, path));

  ASSERT_OK(result->Resize(buffer_size));
  ASSERT_OK(result->Write(buffer.data(), buffer_size));
  ASSERT_OK_AND_ASSIGN(auto out_buffer, result->ReadAt(0, buffer_size));
  ASSERT_EQ(0, memcmp(out_buffer->data(), buffer.data(), buffer_size));

  ASSERT_OK_AND_EQ(buffer_size, result->GetSize());
}

TEST_F(TestMemoryMappedFile, WriteThenShrink) {
  const int64_t buffer_size = 1024;
  std::vector<uint8_t> buffer(buffer_size);
  random_bytes(buffer_size, 0, buffer.data());

  std::string path = TempFile("io-memory-map-write-read-test");
  ASSERT_OK_AND_ASSIGN(auto result, InitMemoryMap(buffer_size * 2, path));

  ASSERT_OK(result->Resize(buffer_size));
  ASSERT_OK(result->Write(buffer.data(), buffer_size));
  ASSERT_OK(result->Resize(buffer_size / 2));

  ASSERT_OK_AND_ASSIGN(auto out_buffer, result->ReadAt(0, buffer_size / 2));
  ASSERT_EQ(0, memcmp(out_buffer->data(), buffer.data(), buffer_size / 2));

  ASSERT_OK_AND_EQ(buffer_size / 2, result->GetSize());
  ASSERT_OK_AND_EQ(buffer_size / 2, FileGetSize(result->file_descriptor()));
}

TEST_F(TestMemoryMappedFile, WriteThenShrinkToHalfThenWrite) {
  const int64_t buffer_size = 1024;
  std::vector<uint8_t> buffer(buffer_size);
  random_bytes(buffer_size, 0, buffer.data());

  std::string path = TempFile("io-memory-map-write-read-test");
  ASSERT_OK_AND_ASSIGN(auto result, InitMemoryMap(buffer_size, path));

  ASSERT_OK(result->Write(buffer.data(), buffer_size));
  ASSERT_OK(result->Resize(buffer_size / 2));

  ASSERT_OK_AND_EQ(buffer_size / 2, result->Tell());

  ASSERT_OK_AND_ASSIGN(auto out_buffer, result->ReadAt(0, buffer_size / 2));
  ASSERT_EQ(0, memcmp(out_buffer->data(), buffer.data(), buffer_size / 2));
  out_buffer.reset();

  // should resume writing directly at the seam
  ASSERT_OK(result->Resize(buffer_size));
  ASSERT_OK(result->Write(buffer.data() + buffer_size / 2, buffer_size / 2));

  ASSERT_OK_AND_ASSIGN(out_buffer, result->ReadAt(0, buffer_size));
  ASSERT_EQ(0, memcmp(out_buffer->data(), buffer.data(), buffer_size));

  ASSERT_OK_AND_EQ(buffer_size, result->GetSize());
  ASSERT_OK_AND_EQ(buffer_size, FileGetSize(result->file_descriptor()));
}

TEST_F(TestMemoryMappedFile, ResizeToZeroThanWrite) {
  const int64_t buffer_size = 1024;
  std::vector<uint8_t> buffer(buffer_size);
  random_bytes(buffer_size, 0, buffer.data());

  std::string path = TempFile("io-memory-map-write-read-test");
  ASSERT_OK_AND_ASSIGN(auto result, InitMemoryMap(buffer_size, path));

  // just a sanity check that writing works ook
  ASSERT_OK(result->Write(buffer.data(), buffer_size));
  ASSERT_OK_AND_ASSIGN(auto out_buffer, result->ReadAt(0, buffer_size));
  ASSERT_EQ(0, memcmp(out_buffer->data(), buffer.data(), buffer_size));
  out_buffer.reset();

  ASSERT_OK(result->Resize(0));
  ASSERT_OK_AND_EQ(0, result->GetSize());

  ASSERT_OK_AND_EQ(0, result->Tell());

  ASSERT_OK_AND_EQ(0, FileGetSize(result->file_descriptor()));

  // provision a vector to the buffer size in case ReadAt decides
  // to read even though it shouldn't
  std::vector<uint8_t> should_remain_empty(buffer_size);
  ASSERT_OK_AND_EQ(0, result->ReadAt(0, 1, should_remain_empty.data()));

  // just a sanity check that writing works ook
  ASSERT_OK(result->Resize(buffer_size));
  ASSERT_OK(result->Write(buffer.data(), buffer_size));
  ASSERT_OK_AND_ASSIGN(out_buffer, result->ReadAt(0, buffer_size));
  ASSERT_EQ(0, memcmp(out_buffer->data(), buffer.data(), buffer_size));
}

TEST_F(TestMemoryMappedFile, WriteAt) {
  const int64_t buffer_size = 1024;
  std::vector<uint8_t> buffer(buffer_size);
  random_bytes(buffer_size, 0, buffer.data());

  std::string path = TempFile("io-memory-map-write-read-test");
  ASSERT_OK_AND_ASSIGN(auto result, InitMemoryMap(buffer_size, path));

  ASSERT_OK(result->WriteAt(0, buffer.data(), buffer_size / 2));

  ASSERT_OK(
      result->WriteAt(buffer_size / 2, buffer.data() + buffer_size / 2, buffer_size / 2));

  ASSERT_OK_AND_ASSIGN(auto out_buffer, result->ReadAt(0, buffer_size));

  ASSERT_EQ(memcmp(out_buffer->data(), buffer.data(), buffer_size), 0);
}

TEST_F(TestMemoryMappedFile, WriteBeyondEnd) {
  const int64_t buffer_size = 1024;
  std::vector<uint8_t> buffer(buffer_size);
  random_bytes(buffer_size, 0, buffer.data());

  std::string path = TempFile("io-memory-map-write-read-test");
  ASSERT_OK_AND_ASSIGN(auto result, InitMemoryMap(buffer_size, path));

  ASSERT_OK(result->Seek(1));
  // Attempt to write beyond end of memory map
  ASSERT_RAISES(IOError, result->Write(buffer.data(), buffer_size));

  // The position should remain unchanged afterwards
  ASSERT_OK_AND_EQ(1, result->Tell());
}

TEST_F(TestMemoryMappedFile, WriteAtBeyondEnd) {
  const int64_t buffer_size = 1024;
  std::vector<uint8_t> buffer(buffer_size);
  random_bytes(buffer_size, 0, buffer.data());

  std::string path = TempFile("io-memory-map-write-read-test");
  ASSERT_OK_AND_ASSIGN(auto result, InitMemoryMap(buffer_size, path));

  // Attempt to write beyond end of memory map
  ASSERT_RAISES(IOError, result->WriteAt(1, buffer.data(), buffer_size));

  // The position should remain unchanged afterwards
  ASSERT_OK_AND_EQ(0, result->Tell());
}

TEST_F(TestMemoryMappedFile, GetSize) {
  std::string path = TempFile("io-memory-map-get-size");
  ASSERT_OK_AND_ASSIGN(auto result, InitMemoryMap(16384, path));

  ASSERT_OK_AND_EQ(16384, result->GetSize());

  ASSERT_OK_AND_EQ(0, result->Tell());
}

TEST_F(TestMemoryMappedFile, ReadOnly) {
  const int64_t buffer_size = 1024;
  std::vector<uint8_t> buffer(buffer_size);

  random_bytes(1024, 0, buffer.data());

  const int reps = 5;

  std::string path = TempFile("ipc-read-only-test");
  ASSERT_OK_AND_ASSIGN(auto rwmmap, InitMemoryMap(reps * buffer_size, path));

  int64_t position = 0;
  for (int i = 0; i < reps; ++i) {
    ASSERT_OK(rwmmap->Write(buffer.data(), buffer_size));
    position += buffer_size;
  }
  ASSERT_OK(rwmmap->Close());

  ASSERT_OK_AND_ASSIGN(auto rommap, MemoryMappedFile::Open(path, FileMode::READ));

  position = 0;
  for (int i = 0; i < reps; ++i) {
    ASSERT_OK_AND_ASSIGN(auto out_buffer, rommap->ReadAt(position, buffer_size));

    ASSERT_EQ(0, memcmp(out_buffer->data(), buffer.data(), buffer_size));
    position += buffer_size;
  }
  ASSERT_OK(rommap->Close());
}

TEST_F(TestMemoryMappedFile, LARGE_MEMORY_TEST(ReadWriteOver4GbFile)) {
  // ARROW-1096
  const int64_t buffer_size = 1000 * 1000;
  std::vector<uint8_t> buffer(buffer_size);

  random_bytes(buffer_size, 0, buffer.data());

  const int64_t reps = 5000;

  std::string path = TempFile("ipc-read-over-4gb-file-test");
  ASSERT_OK_AND_ASSIGN(auto rwmmap, InitMemoryMap(reps * buffer_size, path));
  AppendFile(path);

  int64_t position = 0;
  for (int i = 0; i < reps; ++i) {
    ASSERT_OK(rwmmap->Write(buffer.data(), buffer_size));
    position += buffer_size;
  }
  ASSERT_OK(rwmmap->Close());

  ASSERT_OK_AND_ASSIGN(auto rommap, MemoryMappedFile::Open(path, FileMode::READ));

  position = 0;
  for (int i = 0; i < reps; ++i) {
    ASSERT_OK_AND_ASSIGN(auto out_buffer, rommap->ReadAt(position, buffer_size));

    ASSERT_EQ(0, memcmp(out_buffer->data(), buffer.data(), buffer_size));
    position += buffer_size;
  }
  ASSERT_OK(rommap->Close());
}

TEST_F(TestMemoryMappedFile, RetainMemoryMapReference) {
  // ARROW-494

  const int64_t buffer_size = 1024;
  std::vector<uint8_t> buffer(buffer_size);

  random_bytes(1024, 0, buffer.data());

  std::string path = TempFile("ipc-read-only-test");
  CreateFile(path, buffer_size);

  {
    ASSERT_OK_AND_ASSIGN(auto rwmmap, MemoryMappedFile::Open(path, FileMode::READWRITE));
    ASSERT_OK(rwmmap->Write(buffer.data(), buffer_size));
    ASSERT_FALSE(rwmmap->closed());
    ASSERT_OK(rwmmap->Close());
    ASSERT_TRUE(rwmmap->closed());
  }

  std::shared_ptr<Buffer> out_buffer;

  {
    ASSERT_OK_AND_ASSIGN(auto rommap, MemoryMappedFile::Open(path, FileMode::READ));
    ASSERT_OK_AND_ASSIGN(out_buffer, rommap->Read(buffer_size));
    ASSERT_FALSE(rommap->closed());
    ASSERT_OK(rommap->Close());
    ASSERT_TRUE(rommap->closed());
  }

  // valgrind will catch if memory is unmapped
  ASSERT_EQ(0, memcmp(out_buffer->data(), buffer.data(), buffer_size));
}

TEST_F(TestMemoryMappedFile, InvalidMode) {
  const int64_t buffer_size = 1024;
  std::vector<uint8_t> buffer(buffer_size);

  random_bytes(1024, 0, buffer.data());

  std::string path = TempFile("ipc-invalid-mode-test");
  CreateFile(path, buffer_size);

  ASSERT_OK_AND_ASSIGN(auto rommap, MemoryMappedFile::Open(path, FileMode::READ));
  ASSERT_RAISES(IOError, rommap->Write(buffer.data(), buffer_size));
}

TEST_F(TestMemoryMappedFile, InvalidFile) {
  std::string nonexistent_path = "invalid-file-name-asfd";

  ASSERT_RAISES(IOError, MemoryMappedFile::Open(nonexistent_path, FileMode::READ));
}

TEST_F(TestMemoryMappedFile, CastableToFileInterface) {
  std::shared_ptr<MemoryMappedFile> memory_mapped_file;
  std::shared_ptr<FileInterface> file = memory_mapped_file;
}

TEST_F(TestMemoryMappedFile, ThreadSafety) {
  std::string data = "foobar";
  std::string path = TempFile("ipc-multithreading-test");
  CreateFile(path, static_cast<int>(data.size()));

  ASSERT_OK_AND_ASSIGN(auto file, MemoryMappedFile::Open(path, FileMode::READWRITE));
  ASSERT_OK(file->Write(data.c_str(), static_cast<int64_t>(data.size())));

  std::atomic<int> correct_count(0);
  int niter = 10000;

  auto ReadData = [&correct_count, &data, &file, &niter]() {
    for (int i = 0; i < niter; ++i) {
      ASSERT_OK_AND_ASSIGN(auto buffer, file->ReadAt(0, 3));
      if (0 == memcmp(data.c_str(), buffer->data(), 3)) {
        correct_count += 1;
      }
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
