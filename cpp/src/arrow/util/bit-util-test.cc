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

#include "arrow/util/bit-util.h"

#include <cstdint>
#include <vector>

#include "gtest/gtest.h"

#include "arrow/buffer.h"
#include "arrow/test-util.h"

namespace arrow {

TEST(BitUtilTests, TestIsMultipleOf64) {
  using BitUtil::IsMultipleOf64;
  EXPECT_TRUE(IsMultipleOf64(64));
  EXPECT_TRUE(IsMultipleOf64(0));
  EXPECT_TRUE(IsMultipleOf64(128));
  EXPECT_TRUE(IsMultipleOf64(192));
  EXPECT_FALSE(IsMultipleOf64(23));
  EXPECT_FALSE(IsMultipleOf64(32));
}

TEST(BitUtilTests, TestNextPower2) {
  using BitUtil::NextPower2;

  ASSERT_EQ(8, NextPower2(6));
  ASSERT_EQ(8, NextPower2(8));

  ASSERT_EQ(1, NextPower2(1));
  ASSERT_EQ(256, NextPower2(131));

  ASSERT_EQ(1024, NextPower2(1000));

  ASSERT_EQ(4096, NextPower2(4000));

  ASSERT_EQ(65536, NextPower2(64000));

  ASSERT_EQ(1LL << 32, NextPower2((1LL << 32) - 1));
  ASSERT_EQ(1LL << 31, NextPower2((1LL << 31) - 1));
  ASSERT_EQ(1LL << 62, NextPower2((1LL << 62) - 1));
}

static inline int64_t SlowCountBits(
    const uint8_t* data, int64_t bit_offset, int64_t length) {
  int64_t count = 0;
  for (int64_t i = bit_offset; i < bit_offset + length; ++i) {
    if (BitUtil::GetBit(data, i)) { ++count; }
  }
  return count;
}

TEST(BitUtilTests, TestCountSetBits) {
  const int kBufferSize = 1000;
  uint8_t buffer[kBufferSize] = {0};

  test::random_bytes(kBufferSize, 0, buffer);

  const int num_bits = kBufferSize * 8;

  std::vector<int64_t> offsets = {
      0, 12, 16, 32, 37, 63, 64, 128, num_bits - 30, num_bits - 64};
  for (int64_t offset : offsets) {
    int64_t result = CountSetBits(buffer, offset, num_bits - offset);
    int64_t expected = SlowCountBits(buffer, offset, num_bits - offset);

    ASSERT_EQ(expected, result);
  }
}

TEST(BitUtilTests, TestCopyBitmap) {
  const int kBufferSize = 1000;

  std::shared_ptr<MutableBuffer> buffer;
  ASSERT_OK(AllocateBuffer(default_memory_pool(), kBufferSize, &buffer));
  memset(buffer->mutable_data(), 0, kBufferSize);
  test::random_bytes(kBufferSize, 0, buffer->mutable_data());

  const int num_bits = kBufferSize * 8;

  const uint8_t* src = buffer->data();

  std::vector<int64_t> offsets = {0, 12, 16, 32, 37, 63, 64, 128};
  for (int64_t offset : offsets) {
    const int64_t copy_length = num_bits - offset;

    std::shared_ptr<Buffer> copy;
    ASSERT_OK(CopyBitmap(default_memory_pool(), src, offset, copy_length, &copy));

    for (int64_t i = 0; i < copy_length; ++i) {
      ASSERT_EQ(BitUtil::GetBit(src, i + offset), BitUtil::GetBit(copy->data(), i));
    }
  }
}

}  // namespace arrow
