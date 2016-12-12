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
#include <limits>
#include <string>

#include "gtest/gtest.h"

#include "arrow/buffer.h"
#include "arrow/status.h"
#include "arrow/test-util.h"

using std::string;

namespace arrow {

class TestBuffer : public ::testing::Test {};

TEST_F(TestBuffer, IsMutableFlag) {
  Buffer buf(nullptr, 0);

  ASSERT_FALSE(buf.is_mutable());

  MutableBuffer mbuf(nullptr, 0);
  ASSERT_TRUE(mbuf.is_mutable());

  PoolBuffer pbuf;
  ASSERT_TRUE(pbuf.is_mutable());
}

TEST_F(TestBuffer, Resize) {
  PoolBuffer buf;

  ASSERT_EQ(0, buf.size());
  ASSERT_OK(buf.Resize(100));
  ASSERT_EQ(100, buf.size());
  ASSERT_OK(buf.Resize(200));
  ASSERT_EQ(200, buf.size());

  // Make it smaller, too
  ASSERT_OK(buf.Resize(50));
  ASSERT_EQ(50, buf.size());
}

TEST_F(TestBuffer, ResizeOOM) {
  // realloc fails, even though there may be no explicit limit
  PoolBuffer buf;
  ASSERT_OK(buf.Resize(100));
  int64_t to_alloc = std::numeric_limits<int64_t>::max();
  ASSERT_RAISES(OutOfMemory, buf.Resize(to_alloc));
}

TEST_F(TestBuffer, EqualsWithSameContent) {
  MemoryPool* pool = default_memory_pool();
  const int32_t bufferSize = 128 * 1024;
  uint8_t* rawBuffer1;
  ASSERT_OK(pool->Allocate(bufferSize, &rawBuffer1));
  memset(rawBuffer1, 12, bufferSize);
  uint8_t* rawBuffer2;
  ASSERT_OK(pool->Allocate(bufferSize, &rawBuffer2));
  memset(rawBuffer2, 12, bufferSize);
  uint8_t* rawBuffer3;
  ASSERT_OK(pool->Allocate(bufferSize, &rawBuffer3));
  memset(rawBuffer3, 3, bufferSize);

  Buffer buffer1(rawBuffer1, bufferSize);
  Buffer buffer2(rawBuffer2, bufferSize);
  Buffer buffer3(rawBuffer3, bufferSize);
  ASSERT_TRUE(buffer1.Equals(buffer2));
  ASSERT_FALSE(buffer1.Equals(buffer3));

  pool->Free(rawBuffer1, bufferSize);
  pool->Free(rawBuffer2, bufferSize);
  pool->Free(rawBuffer3, bufferSize);
}

TEST_F(TestBuffer, EqualsWithSameBuffer) {
  MemoryPool* pool = default_memory_pool();
  const int32_t bufferSize = 128 * 1024;
  uint8_t* rawBuffer;
  ASSERT_OK(pool->Allocate(bufferSize, &rawBuffer));
  memset(rawBuffer, 111, bufferSize);

  Buffer buffer1(rawBuffer, bufferSize);
  Buffer buffer2(rawBuffer, bufferSize);
  ASSERT_TRUE(buffer1.Equals(buffer2));

  const int64_t nbytes = bufferSize / 2;
  Buffer buffer3(rawBuffer, nbytes);
  ASSERT_TRUE(buffer1.Equals(buffer3, nbytes));
  ASSERT_FALSE(buffer1.Equals(buffer3, nbytes + 1));

  pool->Free(rawBuffer, bufferSize);
}

TEST_F(TestBuffer, Copy) {
  std::string data_str = "some data to copy";

  auto data = reinterpret_cast<const uint8_t*>(data_str.c_str());

  Buffer buf(data, data_str.size());

  std::shared_ptr<Buffer> out;

  ASSERT_OK(buf.Copy(5, 4, &out));

  Buffer expected(data + 5, 4);
  ASSERT_TRUE(out->Equals(expected));
}

TEST_F(TestBuffer, SliceBuffer) {
  std::string data_str = "some data to slice";

  auto data = reinterpret_cast<const uint8_t*>(data_str.c_str());

  auto buf = std::make_shared<Buffer>(data, data_str.size());

  std::shared_ptr<Buffer> out = SliceBuffer(buf, 5, 4);
  Buffer expected(data + 5, 4);
  ASSERT_TRUE(out->Equals(expected));

  ASSERT_EQ(2, buf.use_count());
}

}  // namespace arrow
