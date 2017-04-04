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

TEST_F(TestBufferOutputStream, DtorCloses) {
  std::string data = "data123456";

  const int K = 100;
  for (int i = 0; i < K; ++i) {
    EXPECT_OK(stream_->Write(data));
  }

  stream_ = nullptr;
  ASSERT_EQ(static_cast<int64_t>(K * data.size()), buffer_->size());
}

TEST_F(TestBufferOutputStream, CloseResizes) {
  std::string data = "data123456";

  const int K = 100;
  for (int i = 0; i < K; ++i) {
    EXPECT_OK(stream_->Write(data));
  }

  ASSERT_OK(stream_->Close());
  ASSERT_EQ(static_cast<int64_t>(K * data.size()), buffer_->size());
}

TEST(TestFixedSizeBufferWriter, Basics) {
  std::shared_ptr<MutableBuffer> buffer;
  ASSERT_OK(AllocateBuffer(default_memory_pool(), 1024, &buffer));

  FixedSizeBufferWriter writer(buffer);

  int64_t position;
  ASSERT_OK(writer.Tell(&position));
  ASSERT_EQ(0, position);

  std::string data = "data123456";
  auto nbytes = static_cast<int64_t>(data.size());
  ASSERT_OK(writer.Write(reinterpret_cast<const uint8_t*>(data.c_str()), nbytes));

  ASSERT_OK(writer.Tell(&position));
  ASSERT_EQ(nbytes, position);

  ASSERT_OK(writer.Seek(4));
  ASSERT_OK(writer.Tell(&position));
  ASSERT_EQ(4, position);

  ASSERT_RAISES(IOError, writer.Seek(-1));
  ASSERT_RAISES(IOError, writer.Seek(1024));

  ASSERT_OK(writer.Close());
}

TEST(TestBufferReader, RetainParentReference) {
  // ARROW-387
  std::string data = "data123456";

  std::shared_ptr<Buffer> slice1;
  std::shared_ptr<Buffer> slice2;
  {
    std::shared_ptr<MutableBuffer> buffer;
    ASSERT_OK(AllocateBuffer(nullptr, static_cast<int64_t>(data.size()), &buffer));
    std::memcpy(buffer->mutable_data(), data.c_str(), data.size());
    BufferReader reader(buffer);
    ASSERT_OK(reader.Read(4, &slice1));
    ASSERT_OK(reader.Read(6, &slice2));
  }

  ASSERT_TRUE(slice1->parent() != nullptr);

  ASSERT_EQ(0, std::memcmp(slice1->data(), data.c_str(), 4));
  ASSERT_EQ(0, std::memcmp(slice2->data(), data.c_str() + 4, 6));
}

}  // namespace io
}  // namespace arrow
