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

#ifdef _WIN32
#include <crtdbg.h>
#include <io.h>
#else
#include <fcntl.h>
#endif

#include "arrow/buffer.h"
#include "arrow/io/file.h"
#include "arrow/io/memory.h"
#include "arrow/memory_pool.h"
#include "arrow/testing/gtest_util.h"
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

#if defined(_WIN32)
static void InvalidParamHandler(const wchar_t* expr, const wchar_t* func,
                                const wchar_t* source_file, unsigned int source_line,
                                uintptr_t reserved) {
  wprintf(L"Invalid parameter in function '%s'. Source: '%s' line %d expression '%s'\n",
          func, source_file, source_line, expr);
}
#endif

bool FileIsClosed(int fd) {
#if defined(_WIN32)
  // Disables default behavior on wrong params which causes the application to crash
  // https://msdn.microsoft.com/en-us/library/ksazx244.aspx
  _set_invalid_parameter_handler(InvalidParamHandler);

  // Disables possible assertion alert box on invalid input arguments
  _CrtSetReportMode(_CRT_ASSERT, 0);

  int new_fd = _dup(fd);
  if (new_fd == -1) {
    return errno == EBADF;
  }
  _close(new_fd);
  return false;
#else
  if (-1 != fcntl(fd, F_GETFD)) {
    return false;
  }
  return errno == EBADF;
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

}  // namespace io
}  // namespace arrow
