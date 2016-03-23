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

#ifndef ARROW_IPC_TEST_COMMON_H
#define ARROW_IPC_TEST_COMMON_H

#include <cstdint>
#include <memory>
#include <string>
#include <vector>

namespace arrow {
namespace ipc {

class MemoryMapFixture {
 public:
  void TearDown() {
    for (auto path : tmp_files_) {
      std::remove(path.c_str());
    }
  }

  void CreateFile(const std::string path, int64_t size) {
    FILE* file = fopen(path.c_str(), "w");
    if (file != nullptr) {
      tmp_files_.push_back(path);
    }
    ftruncate(fileno(file), size);
    fclose(file);
  }

 private:
  std::vector<std::string> tmp_files_;
};

} // namespace ipc
} // namespace arrow

#endif // ARROW_IPC_TEST_COMMON_H
