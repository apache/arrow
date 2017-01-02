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
