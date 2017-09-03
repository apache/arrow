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

#include <climits>
#include <cstdint>
#include <cstring>
#include <limits>
#include <memory>
#include <vector>

#include <gtest/gtest.h>

#include <boost/utility.hpp>  // IWYU pragma: export

#include "arrow/buffer.h"
#include "arrow/memory_pool.h"
#include "arrow/status.h"
#include "arrow/test-util.h"
#include "arrow/util/bit-stream-utils.h"
#include "arrow/util/bit-util.h"
#include "arrow/util/cpu-info.h"

namespace arrow {

static void EnsureCpuInfoInitialized() {
  if (!CpuInfo::initialized()) {
    CpuInfo::Init();
  }
}

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

static inline int64_t SlowCountBits(const uint8_t* data, int64_t bit_offset,
                                    int64_t length) {
  int64_t count = 0;
  for (int64_t i = bit_offset; i < bit_offset + length; ++i) {
    if (BitUtil::GetBit(data, i)) {
      ++count;
    }
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

  std::shared_ptr<Buffer> buffer;
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

TEST(BitUtil, Ceil) {
  EXPECT_EQ(BitUtil::Ceil(0, 1), 0);
  EXPECT_EQ(BitUtil::Ceil(1, 1), 1);
  EXPECT_EQ(BitUtil::Ceil(1, 2), 1);
  EXPECT_EQ(BitUtil::Ceil(1, 8), 1);
  EXPECT_EQ(BitUtil::Ceil(7, 8), 1);
  EXPECT_EQ(BitUtil::Ceil(8, 8), 1);
  EXPECT_EQ(BitUtil::Ceil(9, 8), 2);
  EXPECT_EQ(BitUtil::Ceil(9, 9), 1);
  EXPECT_EQ(BitUtil::Ceil(10000000000, 10), 1000000000);
  EXPECT_EQ(BitUtil::Ceil(10, 10000000000), 1);
  EXPECT_EQ(BitUtil::Ceil(100000000000, 10000000000), 10);
}

TEST(BitUtil, RoundUp) {
  EXPECT_EQ(BitUtil::RoundUp(0, 1), 0);
  EXPECT_EQ(BitUtil::RoundUp(1, 1), 1);
  EXPECT_EQ(BitUtil::RoundUp(1, 2), 2);
  EXPECT_EQ(BitUtil::RoundUp(6, 2), 6);
  EXPECT_EQ(BitUtil::RoundUp(7, 3), 9);
  EXPECT_EQ(BitUtil::RoundUp(9, 9), 9);
  EXPECT_EQ(BitUtil::RoundUp(10000000001, 10), 10000000010);
  EXPECT_EQ(BitUtil::RoundUp(10, 10000000000), 10000000000);
  EXPECT_EQ(BitUtil::RoundUp(100000000000, 10000000000), 100000000000);
}

TEST(BitUtil, RoundDown) {
  EXPECT_EQ(BitUtil::RoundDown(0, 1), 0);
  EXPECT_EQ(BitUtil::RoundDown(1, 1), 1);
  EXPECT_EQ(BitUtil::RoundDown(1, 2), 0);
  EXPECT_EQ(BitUtil::RoundDown(6, 2), 6);
  EXPECT_EQ(BitUtil::RoundDown(7, 3), 6);
  EXPECT_EQ(BitUtil::RoundDown(9, 9), 9);
  EXPECT_EQ(BitUtil::RoundDown(10000000001, 10), 10000000000);
  EXPECT_EQ(BitUtil::RoundDown(10, 10000000000), 0);
  EXPECT_EQ(BitUtil::RoundDown(100000000000, 10000000000), 100000000000);
}

TEST(BitUtil, Popcount) {
  EnsureCpuInfoInitialized();

  EXPECT_EQ(BitUtil::Popcount(BOOST_BINARY(0 1 0 1 0 1 0 1)), 4);
  EXPECT_EQ(BitUtil::PopcountNoHw(BOOST_BINARY(0 1 0 1 0 1 0 1)), 4);
  EXPECT_EQ(BitUtil::Popcount(BOOST_BINARY(1 1 1 1 0 1 0 1)), 6);
  EXPECT_EQ(BitUtil::PopcountNoHw(BOOST_BINARY(1 1 1 1 0 1 0 1)), 6);
  EXPECT_EQ(BitUtil::Popcount(BOOST_BINARY(1 1 1 1 1 1 1 1)), 8);
  EXPECT_EQ(BitUtil::PopcountNoHw(BOOST_BINARY(1 1 1 1 1 1 1 1)), 8);
  EXPECT_EQ(BitUtil::Popcount(0), 0);
  EXPECT_EQ(BitUtil::PopcountNoHw(0), 0);
}

TEST(BitUtil, TrailingBits) {
  EXPECT_EQ(BitUtil::TrailingBits(BOOST_BINARY(1 1 1 1 1 1 1 1), 0), 0);
  EXPECT_EQ(BitUtil::TrailingBits(BOOST_BINARY(1 1 1 1 1 1 1 1), 1), 1);
  EXPECT_EQ(BitUtil::TrailingBits(BOOST_BINARY(1 1 1 1 1 1 1 1), 64),
            BOOST_BINARY(1 1 1 1 1 1 1 1));
  EXPECT_EQ(BitUtil::TrailingBits(BOOST_BINARY(1 1 1 1 1 1 1 1), 100),
            BOOST_BINARY(1 1 1 1 1 1 1 1));
  EXPECT_EQ(BitUtil::TrailingBits(0, 1), 0);
  EXPECT_EQ(BitUtil::TrailingBits(0, 64), 0);
  EXPECT_EQ(BitUtil::TrailingBits(1LL << 63, 0), 0);
  EXPECT_EQ(BitUtil::TrailingBits(1LL << 63, 63), 0);
  EXPECT_EQ(BitUtil::TrailingBits(1LL << 63, 64), 1LL << 63);
}

TEST(BitUtil, ByteSwap) {
  EXPECT_EQ(BitUtil::ByteSwap(static_cast<uint32_t>(0)), 0);
  EXPECT_EQ(BitUtil::ByteSwap(static_cast<uint32_t>(0x11223344)), 0x44332211);

  EXPECT_EQ(BitUtil::ByteSwap(static_cast<int32_t>(0)), 0);
  EXPECT_EQ(BitUtil::ByteSwap(static_cast<int32_t>(0x11223344)), 0x44332211);

  EXPECT_EQ(BitUtil::ByteSwap(static_cast<uint64_t>(0)), 0);
  EXPECT_EQ(BitUtil::ByteSwap(static_cast<uint64_t>(0x1122334455667788)),
            0x8877665544332211);

  EXPECT_EQ(BitUtil::ByteSwap(static_cast<int64_t>(0)), 0);
  EXPECT_EQ(BitUtil::ByteSwap(static_cast<int64_t>(0x1122334455667788)),
            0x8877665544332211);

  EXPECT_EQ(BitUtil::ByteSwap(static_cast<int16_t>(0)), 0);
  EXPECT_EQ(BitUtil::ByteSwap(static_cast<int16_t>(0x1122)), 0x2211);

  EXPECT_EQ(BitUtil::ByteSwap(static_cast<uint16_t>(0)), 0);
  EXPECT_EQ(BitUtil::ByteSwap(static_cast<uint16_t>(0x1122)), 0x2211);
}

TEST(BitUtil, Log2) {
  EXPECT_EQ(BitUtil::Log2(1), 0);
  EXPECT_EQ(BitUtil::Log2(2), 1);
  EXPECT_EQ(BitUtil::Log2(3), 2);
  EXPECT_EQ(BitUtil::Log2(4), 2);
  EXPECT_EQ(BitUtil::Log2(5), 3);
  EXPECT_EQ(BitUtil::Log2(INT_MAX), 31);
  EXPECT_EQ(BitUtil::Log2(UINT_MAX), 32);
  EXPECT_EQ(BitUtil::Log2(ULLONG_MAX), 64);
}

TEST(BitUtil, RoundUpToPowerOf2) {
  EXPECT_EQ(BitUtil::RoundUpToPowerOf2(7, 8), 8);
  EXPECT_EQ(BitUtil::RoundUpToPowerOf2(8, 8), 8);
  EXPECT_EQ(BitUtil::RoundUpToPowerOf2(9, 8), 16);
}

TEST(BitUtil, RoundDownToPowerOf2) {
  EXPECT_EQ(BitUtil::RoundDownToPowerOf2(7, 8), 0);
  EXPECT_EQ(BitUtil::RoundDownToPowerOf2(8, 8), 8);
  EXPECT_EQ(BitUtil::RoundDownToPowerOf2(9, 8), 8);
}

TEST(BitUtil, RoundUpDown) {
  EXPECT_EQ(BitUtil::RoundUpNumBytes(7), 1);
  EXPECT_EQ(BitUtil::RoundUpNumBytes(8), 1);
  EXPECT_EQ(BitUtil::RoundUpNumBytes(9), 2);
  EXPECT_EQ(BitUtil::RoundDownNumBytes(7), 0);
  EXPECT_EQ(BitUtil::RoundDownNumBytes(8), 1);
  EXPECT_EQ(BitUtil::RoundDownNumBytes(9), 1);

  EXPECT_EQ(BitUtil::RoundUpNumi32(31), 1);
  EXPECT_EQ(BitUtil::RoundUpNumi32(32), 1);
  EXPECT_EQ(BitUtil::RoundUpNumi32(33), 2);
  EXPECT_EQ(BitUtil::RoundDownNumi32(31), 0);
  EXPECT_EQ(BitUtil::RoundDownNumi32(32), 1);
  EXPECT_EQ(BitUtil::RoundDownNumi32(33), 1);

  EXPECT_EQ(BitUtil::RoundUpNumi64(63), 1);
  EXPECT_EQ(BitUtil::RoundUpNumi64(64), 1);
  EXPECT_EQ(BitUtil::RoundUpNumi64(65), 2);
  EXPECT_EQ(BitUtil::RoundDownNumi64(63), 0);
  EXPECT_EQ(BitUtil::RoundDownNumi64(64), 1);
  EXPECT_EQ(BitUtil::RoundDownNumi64(65), 1);
}

void TestZigZag(int32_t v) {
  uint8_t buffer[BitReader::MAX_VLQ_BYTE_LEN];
  BitWriter writer(buffer, sizeof(buffer));
  BitReader reader(buffer, sizeof(buffer));
  writer.PutZigZagVlqInt(v);
  int32_t result;
  EXPECT_TRUE(reader.GetZigZagVlqInt(&result));
  EXPECT_EQ(v, result);
}

TEST(BitStreamUtil, ZigZag) {
  TestZigZag(0);
  TestZigZag(1);
  TestZigZag(-1);
  TestZigZag(std::numeric_limits<int32_t>::max());
  TestZigZag(-std::numeric_limits<int32_t>::max());
}

}  // namespace arrow
