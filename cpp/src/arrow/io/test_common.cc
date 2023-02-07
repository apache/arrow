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

#include "arrow/io/test_common.h"

#include <algorithm>
#include <cstdint>
#include <fstream>  // IWYU pragma: keep
#include <vector>

#ifndef _WIN32
#include <fcntl.h>
#endif

#include "arrow/buffer.h"
#include "arrow/io/file.h"
#include "arrow/io/memory.h"
#include "arrow/memory_pool.h"
#include "arrow/testing/gtest_util.h"
#include "arrow/util/future.h"
#include "arrow/util/io_util.h"

namespace arrow {

using internal::IOErrorFromErrno;

namespace io {

void AssertFileContents(const std::string& path, const std::string& contents) {
  ASSERT_OK_AND_ASSIGN(auto rf, ReadableFile::Open(path));
  ASSERT_OK_AND_ASSIGN(int64_t size, rf->GetSize());
  ASSERT_EQ(size, contents.size());

  ASSERT_OK_AND_ASSIGN(auto actual_data, rf->Read(size));
  ASSERT_TRUE(actual_data->Equals(Buffer(contents)));
}

bool FileExists(const std::string& path) { return std::ifstream(path.c_str()).good(); }

Status PurgeLocalFileFromOsCache(const std::string& path) {
#if defined(POSIX_FADV_WILLNEED)
  int fd = open(path.c_str(), O_WRONLY);
  if (fd < 0) {
    return IOErrorFromErrno(errno, "open on ", path,
                            " to clear from cache did not succeed.");
  }
  int err = posix_fadvise(fd, 0, 0, POSIX_FADV_DONTNEED);
  if (err != 0) {
    return IOErrorFromErrno(err, "fadvise on ", path,
                            " to clear from cache did not succeed");
  }
  err = close(fd);
  if (err == 0) {
    return Status::OK();
  }
  return IOErrorFromErrno(err, "close on ", path, " to clear from cache did not succeed");
#else
  return Status::NotImplemented("posix_fadvise is not implemented on this machine");
#endif
}

Status ZeroMemoryMap(MemoryMappedFile* file) {
  constexpr int64_t kBufferSize = 512;
  static constexpr uint8_t kZeroBytes[kBufferSize] = {0};

  RETURN_NOT_OK(file->Seek(0));
  int64_t position = 0;
  ARROW_ASSIGN_OR_RAISE(int64_t file_size, file->GetSize());

  int64_t chunksize;
  while (position < file_size) {
    chunksize = std::min(kBufferSize, file_size - position);
    RETURN_NOT_OK(file->Write(kZeroBytes, chunksize));
    position += chunksize;
  }
  return Status::OK();
}

void MemoryMapFixture::TearDown() {
  for (auto path : tmp_files_) {
    ARROW_UNUSED(std::remove(path.c_str()));
  }
}

void MemoryMapFixture::CreateFile(const std::string& path, int64_t size) {
  ASSERT_OK(MemoryMappedFile::Create(path, size));
  tmp_files_.push_back(path);
}

Result<std::shared_ptr<MemoryMappedFile>> MemoryMapFixture::InitMemoryMap(
    int64_t size, const std::string& path) {
  ARROW_ASSIGN_OR_RAISE(auto mmap, MemoryMappedFile::Create(path, size));
  tmp_files_.push_back(path);
  return mmap;
}

void MemoryMapFixture::AppendFile(const std::string& path) { tmp_files_.push_back(path); }

class TrackedRandomAccessFileImpl : public TrackedRandomAccessFile {
 public:
  explicit TrackedRandomAccessFileImpl(io::RandomAccessFile* delegate)
      : delegate_(delegate) {}

  Status Close() override { return delegate_->Close(); }
  bool closed() const override { return delegate_->closed(); }
  Result<int64_t> Tell() const override { return delegate_->Tell(); }
  Status Seek(int64_t position) override { return delegate_->Seek(position); }
  Result<int64_t> Read(int64_t nbytes, void* out) override {
    ARROW_ASSIGN_OR_RAISE(auto position, delegate_->Tell());
    SaveReadRange(position, nbytes);
    return delegate_->Read(nbytes, out);
  }
  Result<std::shared_ptr<Buffer>> Read(int64_t nbytes) override {
    ARROW_ASSIGN_OR_RAISE(auto position, delegate_->Tell());
    SaveReadRange(position, nbytes);
    return delegate_->Read(nbytes);
  }
  bool supports_zero_copy() const override { return delegate_->supports_zero_copy(); }
  Result<int64_t> GetSize() override { return delegate_->GetSize(); }
  Result<int64_t> ReadAt(int64_t position, int64_t nbytes, void* out) override {
    SaveReadRange(position, nbytes);
    return delegate_->ReadAt(position, nbytes, out);
  }
  Result<std::shared_ptr<Buffer>> ReadAt(int64_t position, int64_t nbytes) override {
    SaveReadRange(position, nbytes);
    return delegate_->ReadAt(position, nbytes);
  }
  Future<std::shared_ptr<Buffer>> ReadAsync(const io::IOContext& io_context,
                                            int64_t position, int64_t nbytes) override {
    SaveReadRange(position, nbytes);
    return delegate_->ReadAsync(io_context, position, nbytes);
  }

  int64_t num_reads() const override { return read_ranges_.size(); }
  int64_t bytes_read() const override {
    int64_t sum = 0;
    for (const auto& range : read_ranges_) {
      sum += range.length;
    }
    return sum;
  }

  const std::vector<io::ReadRange>& get_read_ranges() const override {
    return read_ranges_;
  }

 private:
  io::RandomAccessFile* delegate_;
  std::vector<io::ReadRange> read_ranges_;

  void SaveReadRange(int64_t offset, int64_t length) {
    read_ranges_.emplace_back(io::ReadRange{offset, length});
  }
};

std::unique_ptr<TrackedRandomAccessFile> TrackedRandomAccessFile::Make(
    io::RandomAccessFile* target) {
  return std::make_unique<TrackedRandomAccessFileImpl>(target);
}

}  // namespace io
}  // namespace arrow
