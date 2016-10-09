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

#include "arrow/io/memory.h"
#include "arrow/io/test-common.h"

namespace arrow {
namespace io {

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

class TestBufferOutputStream : public ::testing::Test {
 public:
  void SetUp() {
    buffer_.reset(new PoolBuffer(default_memory_pool()));
    stream_.reset(new BufferOutputStream(buffer_));
  }

 protected:
  std::shared_ptr<PoolBuffer> buffer_;
  std::unique_ptr<OutputStream> stream_;
};

TEST_F(TestBufferOutputStream, CloseResizes) {
  std::string data = "data123456";

  const int64_t nbytes = static_cast<int64_t>(data.size());
  const int K = 100;
  for (int i = 0; i < K; ++i) {
    EXPECT_OK(stream_->Write(reinterpret_cast<const uint8_t*>(data.c_str()), nbytes));
  }

  ASSERT_OK(stream_->Close());
  ASSERT_EQ(K * nbytes, buffer_->size());
}

}  // namespace io
}  // namespace arrow
