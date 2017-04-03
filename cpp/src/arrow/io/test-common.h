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

#ifndef ARROW_IO_TEST_COMMON_H
#define ARROW_IO_TEST_COMMON_H

#include <algorithm>
#include <cstdint>
#include <memory>
#include <string>
#include <vector>

#if defined(__MINGW32__)  // MinGW
// nothing
#elif defined(_MSC_VER)  // Visual Studio
#include <io.h>
#else  // POSIX / Linux
// nothing
#endif

#include "arrow/buffer.h"
#include "arrow/io/file.h"
#include "arrow/io/memory.h"
#include "arrow/memory_pool.h"
#include "arrow/test-util.h"

namespace arrow {
namespace io {

static inline Status ZeroMemoryMap(MemoryMappedFile* file) {
  constexpr int64_t kBufferSize = 512;
  static constexpr uint8_t kZeroBytes[kBufferSize] = {0};

  RETURN_NOT_OK(file->Seek(0));
  int64_t position = 0;
  int64_t file_size;
  RETURN_NOT_OK(file->GetSize(&file_size));

  int64_t chunksize;
  while (position < file_size) {
    chunksize = std::min(kBufferSize, file_size - position);
    RETURN_NOT_OK(file->Write(kZeroBytes, chunksize));
    position += chunksize;
  }
  return Status::OK();
}

class MemoryMapFixture {
 public:
  void TearDown() {
    for (auto path : tmp_files_) {
      std::remove(path.c_str());
    }
  }

  void CreateFile(const std::string& path, int64_t size) {
    std::shared_ptr<MemoryMappedFile> file;
    ASSERT_OK(MemoryMappedFile::Create(path, size, &file));
    tmp_files_.push_back(path);
  }

  Status InitMemoryMap(
      int64_t size, const std::string& path, std::shared_ptr<MemoryMappedFile>* mmap) {
    RETURN_NOT_OK(MemoryMappedFile::Create(path, size, mmap));
    tmp_files_.push_back(path);
    return Status::OK();
  }

 private:
  std::vector<std::string> tmp_files_;
};

}  // namespace io
}  // namespace arrow

#endif  // ARROW_IO_TEST_COMMON_H
