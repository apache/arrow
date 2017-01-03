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

class MemoryMapFixture {
 public:
  void TearDown() {
    for (auto path : tmp_files_) {
      std::remove(path.c_str());
    }
  }

  void CreateFile(const std::string path, int64_t size) {
    FILE* file = fopen(path.c_str(), "w");
    if (file != nullptr) { tmp_files_.push_back(path); }
#ifdef _MSC_VER
    _chsize(fileno(file), size);
#else
    ftruncate(fileno(file), size);
#endif
    fclose(file);
  }

  Status InitMemoryMap(
      int64_t size, const std::string& path, std::shared_ptr<MemoryMappedFile>* mmap) {
    CreateFile(path, size);
    return MemoryMappedFile::Open(path, FileMode::READWRITE, mmap);
  }

 private:
  std::vector<std::string> tmp_files_;
};

}  // namespace io
}  // namespace arrow

#endif  // ARROW_IO_TEST_COMMON_H
