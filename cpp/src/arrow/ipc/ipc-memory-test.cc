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
#include <memory>
#include <string>
#include <vector>

#include "gtest/gtest.h"

#include "arrow/ipc/memory.h"
#include "arrow/ipc/test-common.h"
#include "arrow/test-util.h"
#include "arrow/util/buffer.h"
#include "arrow/util/status.h"

namespace arrow {
namespace ipc {

class TestMemoryMappedSource : public ::testing::Test, public MemoryMapFixture {
 public:
  void TearDown() { MemoryMapFixture::TearDown(); }
};

TEST_F(TestMemoryMappedSource, InvalidUsages) {}

TEST_F(TestMemoryMappedSource, WriteRead) {
  const int64_t buffer_size = 1024;
  std::vector<uint8_t> buffer(buffer_size);

  test::random_bytes(1024, 0, buffer.data());

  const int reps = 5;

  std::string path = "ipc-write-read-test";
  CreateFile(path, reps * buffer_size);

  std::shared_ptr<MemoryMappedSource> result;
  ASSERT_OK(MemoryMappedSource::Open(path, MemorySource::READ_WRITE, &result));

  int64_t position = 0;

  std::shared_ptr<Buffer> out_buffer;
  for (int i = 0; i < reps; ++i) {
    ASSERT_OK(result->Write(position, buffer.data(), buffer_size));
    ASSERT_OK(result->ReadAt(position, buffer_size, &out_buffer));

    ASSERT_EQ(0, memcmp(out_buffer->data(), buffer.data(), buffer_size));

    position += buffer_size;
  }
}

TEST_F(TestMemoryMappedSource, InvalidFile) {
  std::string non_existent_path = "invalid-file-name-asfd";

  std::shared_ptr<MemoryMappedSource> result;
  ASSERT_RAISES(IOError,
      MemoryMappedSource::Open(non_existent_path, MemorySource::READ_ONLY, &result));
}

}  // namespace ipc
}  // namespace arrow
