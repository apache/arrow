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
#include <functional>
#include <initializer_list>
#include <limits>
#include <memory>
#include <valarray>
#include <vector>

#include <gtest/gtest.h>

#include <boost/utility.hpp>  // IWYU pragma: export

#include "arrow/buffer.h"
#include "arrow/memory_pool.h"
#include "arrow/test-common.h"
#include "arrow/test-util.h"
#include "arrow/util/bit-stream-utils.h"
#include "arrow/util/bit-util.h"
#include "arrow/util/cpu-info.h"

namespace arrow {

template <class BitmapWriter>
void WriteVectorToWriter(BitmapWriter& writer, const std::vector<int> values) {
  for (const auto& value : values) {
    if (value) {
      writer.Set();
    } else {
      writer.Clear();
    }
    writer.Next();
  }
  writer.Finish();
}

void BitmapFromVector(const std::vector<int>& values, int64_t bit_offset,
                      std::shared_ptr<Buffer>* out_buffer, int64_t* out_length) {
  const int64_t length = values.size();
  *out_length = length;
  ASSERT_OK(AllocateEmptyBitmap(length + bit_offset, out_buffer));
  auto writer = internal::BitmapWriter((*out_buffer)->mutable_data(), bit_offset, length);
  WriteVectorToWriter(writer, values);
}

#define ASSERT_READER_SET(reader)    \
  do {                               \
    ASSERT_TRUE(reader.IsSet());     \
    ASSERT_FALSE(reader.IsNotSet()); \
    reader.Next();                   \
  } while (false)

#define ASSERT_READER_NOT_SET(reader) \
  do {                                \
    ASSERT_FALSE(reader.IsSet());     \
    ASSERT_TRUE(reader.IsNotSet());   \
    reader.Next();                    \
  } while (false)

// Assert that a BitmapReader yields the given bit values
void ASSERT_READER_VALUES(internal::BitmapReader& reader, std::vector<int> values) {
  for (const auto& value : values) {
    if (value) {
      ASSERT_READER_SET(reader);
    } else {
      ASSERT_READER_NOT_SET(reader);
    }
  }
}

// Assert equal contents of a memory area and a vector of bytes
void ASSERT_BYTES_EQ(const uint8_t* left, const std::vector<uint8_t>& right) {
  auto left_array = std::vector<uint8_t>(left, left + right.size());
  ASSERT_EQ(left_array, right);
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

TEST(BitmapReader, NormalOperation) {
  std::shared_ptr<Buffer> buffer;
  int64_t length;

  for (int64_t offset : {0, 1, 3, 5, 7, 8, 12, 13, 21, 38, 75, 120}) {
    BitmapFromVector({0, 1, 1, 1, 0, 0, 0, 1, 0, 1, 0, 1, 0, 1}, offset, &buffer,
                     &length);
    ASSERT_EQ(length, 14);

    auto reader = internal::BitmapReader(buffer->mutable_data(), offset, length);
    ASSERT_READER_VALUES(reader, {0, 1, 1, 1, 0, 0, 0, 1, 0, 1, 0, 1, 0, 1});
  }
}

TEST(BitmapReader, DoesNotReadOutOfBounds) {
  uint8_t bitmap[16] = {0};

  const int length = 128;

  internal::BitmapReader r1(bitmap, 0, length);

  // If this were to read out of bounds, valgrind would tell us
  for (int i = 0; i < length; ++i) {
    ASSERT_TRUE(r1.IsNotSet());
    r1.Next();
  }

  internal::BitmapReader r2(bitmap, 5, length - 5);

  for (int i = 0; i < (length - 5); ++i) {
    ASSERT_TRUE(r2.IsNotSet());
    r2.Next();
  }

  // Does not access invalid memory
  internal::BitmapReader r3(nullptr, 0, 0);
}

TEST(BitmapWriter, NormalOperation) {
  {
    uint8_t bitmap[] = {0, 0, 0, 0};
    auto writer = internal::BitmapWriter(bitmap, 0, 12);
    WriteVectorToWriter(writer, {0, 1, 1, 0, 1, 1, 0, 0, 0, 1, 0, 1});
    //                      {0b00110110, 0b1010, 0, 0}
    ASSERT_BYTES_EQ(bitmap, {0x36, 0x0a, 0, 0});
  }
  {
    uint8_t bitmap[] = {0xff, 0xff, 0xff, 0xff};
    auto writer = internal::BitmapWriter(bitmap, 0, 12);
    WriteVectorToWriter(writer, {0, 1, 1, 0, 1, 1, 0, 0, 0, 1, 0, 1});
    //                      {0b00110110, 0b11111010, 0xff, 0xff}
    ASSERT_BYTES_EQ(bitmap, {0x36, 0xfa, 0xff, 0xff});
  }
  {
    uint8_t bitmap[] = {0, 0, 0, 0};
    auto writer = internal::BitmapWriter(bitmap, 3, 12);
    WriteVectorToWriter(writer, {0, 1, 1, 0, 1, 1, 0, 0, 0, 1, 0, 1});
    //                      {0b10110000, 0b01010001, 0, 0}
    ASSERT_BYTES_EQ(bitmap, {0xb0, 0x51, 0, 0});
  }
  {
    uint8_t bitmap[] = {0, 0, 0, 0};
    auto writer = internal::BitmapWriter(bitmap, 20, 12);
    WriteVectorToWriter(writer, {0, 1, 1, 0, 1, 1, 0, 0, 0, 1, 0, 1});
    //                      {0, 0, 0b01100000, 0b10100011}
    ASSERT_BYTES_EQ(bitmap, {0, 0, 0x60, 0xa3});
  }
}

TEST(BitmapWriter, DoesNotWriteOutOfBounds) {
  uint8_t bitmap[16] = {0};

  const int length = 128;

  int64_t num_values = 0;

  internal::BitmapWriter r1(bitmap, 0, length);

  // If this were to write out of bounds, valgrind would tell us
  for (int i = 0; i < length; ++i) {
    r1.Set();
    r1.Clear();
    r1.Next();
  }
  r1.Finish();
  num_values = r1.position();

  ASSERT_EQ(length, num_values);

  internal::BitmapWriter r2(bitmap, 5, length - 5);

  for (int i = 0; i < (length - 5); ++i) {
    r2.Set();
    r2.Clear();
    r2.Next();
  }
  r2.Finish();
  num_values = r2.position();

  ASSERT_EQ((length - 5), num_values);
}

TEST(FirstTimeBitmapWriter, NormalOperation) {
  for (const auto fill_byte_int : {0x00, 0xff}) {
    const uint8_t fill_byte = static_cast<uint8_t>(fill_byte_int);
    {
      uint8_t bitmap[] = {fill_byte, fill_byte, fill_byte, fill_byte};
      auto writer = internal::FirstTimeBitmapWriter(bitmap, 0, 12);
      WriteVectorToWriter(writer, {0, 1, 1, 0, 1, 1, 0, 0, 0, 1, 0, 1});
      //                      {0b00110110, 0b1010, 0, 0}
      ASSERT_BYTES_EQ(bitmap, {0x36, 0x0a});
    }
    {
      uint8_t bitmap[] = {fill_byte, fill_byte, fill_byte, fill_byte};
      auto writer = internal::FirstTimeBitmapWriter(bitmap, 4, 12);
      WriteVectorToWriter(writer, {0, 1, 1, 0, 1, 1, 0, 0, 0, 1, 0, 1});
      //                      {0b00110110, 0b1010, 0, 0}
      ASSERT_BYTES_EQ(bitmap, {static_cast<uint8_t>(0x60 | (fill_byte & 0x0f)), 0xa3});
    }
    // Consecutive write chunks
    {
      uint8_t bitmap[] = {fill_byte, fill_byte, fill_byte, fill_byte};
      {
        auto writer = internal::FirstTimeBitmapWriter(bitmap, 0, 6);
        WriteVectorToWriter(writer, {0, 1, 1, 0, 1, 1});
      }
      {
        auto writer = internal::FirstTimeBitmapWriter(bitmap, 6, 3);
        WriteVectorToWriter(writer, {0, 0, 0});
      }
      {
        auto writer = internal::FirstTimeBitmapWriter(bitmap, 9, 3);
        WriteVectorToWriter(writer, {1, 0, 1});
      }
      ASSERT_BYTES_EQ(bitmap, {0x36, 0x0a});
    }
    {
      uint8_t bitmap[] = {fill_byte, fill_byte, fill_byte, fill_byte};
      {
        auto writer = internal::FirstTimeBitmapWriter(bitmap, 4, 6);
        WriteVectorToWriter(writer, {0, 1, 1, 0, 1, 1});
      }
      {
        auto writer = internal::FirstTimeBitmapWriter(bitmap, 10, 3);
        WriteVectorToWriter(writer, {0, 0, 0});
      }
      {
        auto writer = internal::FirstTimeBitmapWriter(bitmap, 13, 3);
        WriteVectorToWriter(writer, {1, 0, 1});
      }
      ASSERT_BYTES_EQ(bitmap, {static_cast<uint8_t>(0x60 | (fill_byte & 0x0f)), 0xa3});
    }
  }
}

// Tests for GenerateBits and GenerateBitsUnrolled

struct GenerateBitsFunctor {
  template <class Generator>
  void operator()(uint8_t* bitmap, int64_t start_offset, int64_t length, Generator&& g) {
    return internal::GenerateBits(bitmap, start_offset, length, g);
  }
};

struct GenerateBitsUnrolledFunctor {
  template <class Generator>
  void operator()(uint8_t* bitmap, int64_t start_offset, int64_t length, Generator&& g) {
    return internal::GenerateBitsUnrolled(bitmap, start_offset, length, g);
  }
};

template <typename T>
class TestGenerateBits : public ::testing::Test {};

typedef ::testing::Types<GenerateBitsFunctor, GenerateBitsUnrolledFunctor>
    GenerateBitsTypes;
TYPED_TEST_CASE(TestGenerateBits, GenerateBitsTypes);

TYPED_TEST(TestGenerateBits, NormalOperation) {
  const int kSourceSize = 256;
  uint8_t source[kSourceSize];
  random_bytes(kSourceSize, 0, source);

  const int64_t start_offsets[] = {0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 21, 31, 32};
  const int64_t lengths[] = {0,  1,  2,  3,  4,   5,   6,   7,   8,   9,   12,  16,
                             17, 21, 31, 32, 100, 201, 202, 203, 204, 205, 206, 207};
  const uint8_t fill_bytes[] = {0x00, 0xff};

  for (const int64_t start_offset : start_offsets) {
    for (const int64_t length : lengths) {
      for (const uint8_t fill_byte : fill_bytes) {
        uint8_t bitmap[kSourceSize];
        memset(bitmap, fill_byte, kSourceSize);
        // First call GenerateBits
        {
          int64_t ncalled = 0;
          internal::BitmapReader reader(source, 0, length);
          TypeParam()(bitmap, start_offset, length, [&]() -> bool {
            bool b = reader.IsSet();
            reader.Next();
            ++ncalled;
            return b;
          });
          ASSERT_EQ(ncalled, length);
        }
        // Then check generated contents
        {
          internal::BitmapReader source_reader(source, 0, length);
          internal::BitmapReader result_reader(bitmap, start_offset, length);
          for (int64_t i = 0; i < length; ++i) {
            ASSERT_EQ(source_reader.IsSet(), result_reader.IsSet())
                << "mismatch at bit #" << i;
            source_reader.Next();
            result_reader.Next();
          }
        }
        // Check bits preceding and following generated contents weren't clobbered
        {
          internal::BitmapReader reader_before(bitmap, 0, start_offset);
          for (int64_t i = 0; i < start_offset; ++i) {
            ASSERT_EQ(reader_before.IsSet(), fill_byte == 0xff)
                << "mismatch at preceding bit #" << start_offset - i;
          }
        }
      }
    }
  }
}

struct BitmapOperation {
  virtual Status Call(MemoryPool* pool, const uint8_t* left, int64_t left_offset,
                      const uint8_t* right, int64_t right_offset, int64_t length,
                      int64_t out_offset, std::shared_ptr<Buffer>* out_buffer) const = 0;

  virtual ~BitmapOperation() = default;
};

struct BitmapAndOp : public BitmapOperation {
  Status Call(MemoryPool* pool, const uint8_t* left, int64_t left_offset,
              const uint8_t* right, int64_t right_offset, int64_t length,
              int64_t out_offset, std::shared_ptr<Buffer>* out_buffer) const override {
    return BitmapAnd(pool, left, left_offset, right, right_offset, length, out_offset,
                     out_buffer);
  }
};

struct BitmapOrOp : public BitmapOperation {
  Status Call(MemoryPool* pool, const uint8_t* left, int64_t left_offset,
              const uint8_t* right, int64_t right_offset, int64_t length,
              int64_t out_offset, std::shared_ptr<Buffer>* out_buffer) const override {
    return BitmapOr(pool, left, left_offset, right, right_offset, length, out_offset,
                    out_buffer);
  }
};

struct BitmapXorOp : public BitmapOperation {
  Status Call(MemoryPool* pool, const uint8_t* left, int64_t left_offset,
              const uint8_t* right, int64_t right_offset, int64_t length,
              int64_t out_offset, std::shared_ptr<Buffer>* out_buffer) const override {
    return BitmapXor(pool, left, left_offset, right, right_offset, length, out_offset,
                     out_buffer);
  }
};

class BitmapOp : public TestBase {
 public:
  void TestAligned(const BitmapOperation& op, const std::vector<int>& left_bits,
                   const std::vector<int>& right_bits,
                   const std::vector<int>& result_bits) {
    std::shared_ptr<Buffer> left, right, out;
    int64_t length;

    for (int64_t left_offset : {0, 1, 3, 5, 7, 8, 13, 21, 38, 75, 120}) {
      BitmapFromVector(left_bits, left_offset, &left, &length);
      for (int64_t right_offset : {left_offset, left_offset + 8, left_offset + 40}) {
        BitmapFromVector(right_bits, right_offset, &right, &length);
        for (int64_t out_offset : {left_offset, left_offset + 16, left_offset + 24}) {
          ASSERT_OK(op.Call(default_memory_pool(), left->mutable_data(), left_offset,
                            right->mutable_data(), right_offset, length, out_offset,
                            &out));
          auto reader = internal::BitmapReader(out->mutable_data(), out_offset, length);
          ASSERT_READER_VALUES(reader, result_bits);
        }
      }
    }
  }

  void TestUnaligned(const BitmapOperation& op, const std::vector<int>& left_bits,
                     const std::vector<int>& right_bits,
                     const std::vector<int>& result_bits) {
    std::shared_ptr<Buffer> left, right, out;
    int64_t length;
    auto offset_values = {0, 1, 3, 5, 7, 8, 13, 21, 38, 75, 120};

    for (int64_t left_offset : offset_values) {
      BitmapFromVector(left_bits, left_offset, &left, &length);

      for (int64_t right_offset : offset_values) {
        BitmapFromVector(right_bits, right_offset, &right, &length);

        for (int64_t out_offset : offset_values) {
          ASSERT_OK(op.Call(default_memory_pool(), left->mutable_data(), left_offset,
                            right->mutable_data(), right_offset, length, out_offset,
                            &out));
          auto reader = internal::BitmapReader(out->mutable_data(), out_offset, length);
          ASSERT_READER_VALUES(reader, result_bits);
        }
      }
    }
  }
};

TEST_F(BitmapOp, And) {
  BitmapAndOp op;
  std::vector<int> left = {0, 1, 1, 1, 0, 0, 0, 1, 0, 1, 0, 1, 0, 1};
  std::vector<int> right = {0, 0, 1, 0, 1, 1, 0, 0, 1, 1, 1, 0, 1, 0};
  std::vector<int> result = {0, 0, 1, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0};

  TestAligned(op, left, right, result);
  TestUnaligned(op, left, right, result);
}

TEST_F(BitmapOp, Or) {
  BitmapOrOp op;
  std::vector<int> left = {0, 1, 1, 1, 0, 0, 0, 1, 0, 1, 0, 0, 0, 0};
  std::vector<int> right = {0, 0, 1, 0, 1, 1, 0, 0, 1, 1, 1, 0, 1, 0};
  std::vector<int> result = {0, 1, 1, 1, 1, 1, 0, 1, 1, 1, 1, 0, 1, 0};

  TestAligned(op, left, right, result);
  TestUnaligned(op, left, right, result);
}

TEST_F(BitmapOp, XorAligned) {
  BitmapXorOp op;
  std::vector<int> left = {0, 1, 1, 1, 0, 0, 0, 1, 0, 1, 0, 1, 0, 1};
  std::vector<int> right = {0, 0, 1, 0, 1, 1, 0, 0, 1, 1, 1, 0, 1, 0};
  std::vector<int> result = {0, 1, 0, 1, 1, 1, 0, 1, 1, 0, 1, 1, 1, 1};

  TestAligned(op, left, right, result);
  TestUnaligned(op, left, right, result);
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

  random_bytes(kBufferSize, 0, buffer);

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
  ASSERT_OK(AllocateBuffer(kBufferSize, &buffer));
  memset(buffer->mutable_data(), 0, kBufferSize);
  random_bytes(kBufferSize, 0, buffer->mutable_data());

  const uint8_t* src = buffer->data();

  std::vector<int64_t> lengths = {kBufferSize * 8 - 4, kBufferSize * 8};
  std::vector<int64_t> offsets = {0, 12, 16, 32, 37, 63, 64, 128};
  for (int64_t num_bits : lengths) {
    for (int64_t offset : offsets) {
      const int64_t copy_length = num_bits - offset;

      std::shared_ptr<Buffer> copy;
      ASSERT_OK(CopyBitmap(default_memory_pool(), src, offset, copy_length, &copy));

      for (int64_t i = 0; i < copy_length; ++i) {
        ASSERT_EQ(BitUtil::GetBit(src, i + offset), BitUtil::GetBit(copy->data(), i));
      }
    }
  }
}

TEST(BitUtil, CeilDiv) {
  EXPECT_EQ(BitUtil::CeilDiv(0, 1), 0);
  EXPECT_EQ(BitUtil::CeilDiv(1, 1), 1);
  EXPECT_EQ(BitUtil::CeilDiv(1, 2), 1);
  EXPECT_EQ(BitUtil::CeilDiv(1, 8), 1);
  EXPECT_EQ(BitUtil::CeilDiv(7, 8), 1);
  EXPECT_EQ(BitUtil::CeilDiv(8, 8), 1);
  EXPECT_EQ(BitUtil::CeilDiv(9, 8), 2);
  EXPECT_EQ(BitUtil::CeilDiv(9, 9), 1);
  EXPECT_EQ(BitUtil::CeilDiv(10000000000, 10), 1000000000);
  EXPECT_EQ(BitUtil::CeilDiv(10, 10000000000), 1);
  EXPECT_EQ(BitUtil::CeilDiv(100000000000, 10000000000), 10);
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
  EXPECT_EQ(BitUtil::Log2(8), 3);
  EXPECT_EQ(BitUtil::Log2(9), 4);
  EXPECT_EQ(BitUtil::Log2(INT_MAX), 31);
  EXPECT_EQ(BitUtil::Log2(UINT_MAX), 32);
  EXPECT_EQ(BitUtil::Log2(ULLONG_MAX), 64);
}

TEST(BitUtil, NumRequiredBits) {
  EXPECT_EQ(BitUtil::NumRequiredBits(0), 0);
  EXPECT_EQ(BitUtil::NumRequiredBits(1), 1);
  EXPECT_EQ(BitUtil::NumRequiredBits(2), 2);
  EXPECT_EQ(BitUtil::NumRequiredBits(3), 2);
  EXPECT_EQ(BitUtil::NumRequiredBits(4), 3);
  EXPECT_EQ(BitUtil::NumRequiredBits(5), 3);
  EXPECT_EQ(BitUtil::NumRequiredBits(7), 3);
  EXPECT_EQ(BitUtil::NumRequiredBits(8), 4);
  EXPECT_EQ(BitUtil::NumRequiredBits(9), 4);
  EXPECT_EQ(BitUtil::NumRequiredBits(UINT_MAX - 1), 32);
  EXPECT_EQ(BitUtil::NumRequiredBits(UINT_MAX), 32);
  EXPECT_EQ(BitUtil::NumRequiredBits(static_cast<uint64_t>(UINT_MAX) + 1), 33);
  EXPECT_EQ(BitUtil::NumRequiredBits(ULLONG_MAX / 2), 63);
  EXPECT_EQ(BitUtil::NumRequiredBits(ULLONG_MAX / 2 + 1), 64);
  EXPECT_EQ(BitUtil::NumRequiredBits(ULLONG_MAX - 1), 64);
  EXPECT_EQ(BitUtil::NumRequiredBits(ULLONG_MAX), 64);
}

#define U32(x) static_cast<uint32_t>(x)
#define U64(x) static_cast<uint64_t>(x)

TEST(BitUtil, CountLeadingZeros) {
  EXPECT_EQ(BitUtil::CountLeadingZeros(U32(0)), 32);
  EXPECT_EQ(BitUtil::CountLeadingZeros(U32(1)), 31);
  EXPECT_EQ(BitUtil::CountLeadingZeros(U32(2)), 30);
  EXPECT_EQ(BitUtil::CountLeadingZeros(U32(3)), 30);
  EXPECT_EQ(BitUtil::CountLeadingZeros(U32(4)), 29);
  EXPECT_EQ(BitUtil::CountLeadingZeros(U32(7)), 29);
  EXPECT_EQ(BitUtil::CountLeadingZeros(U32(8)), 28);
  EXPECT_EQ(BitUtil::CountLeadingZeros(U32(UINT_MAX / 2)), 1);
  EXPECT_EQ(BitUtil::CountLeadingZeros(U32(UINT_MAX / 2 + 1)), 0);
  EXPECT_EQ(BitUtil::CountLeadingZeros(U32(UINT_MAX)), 0);

  EXPECT_EQ(BitUtil::CountLeadingZeros(U64(0)), 64);
  EXPECT_EQ(BitUtil::CountLeadingZeros(U64(1)), 63);
  EXPECT_EQ(BitUtil::CountLeadingZeros(U64(2)), 62);
  EXPECT_EQ(BitUtil::CountLeadingZeros(U64(3)), 62);
  EXPECT_EQ(BitUtil::CountLeadingZeros(U64(4)), 61);
  EXPECT_EQ(BitUtil::CountLeadingZeros(U64(7)), 61);
  EXPECT_EQ(BitUtil::CountLeadingZeros(U64(8)), 60);
  EXPECT_EQ(BitUtil::CountLeadingZeros(U64(UINT_MAX)), 32);
  EXPECT_EQ(BitUtil::CountLeadingZeros(U64(UINT_MAX) + 1), 31);
  EXPECT_EQ(BitUtil::CountLeadingZeros(U64(ULLONG_MAX / 2)), 1);
  EXPECT_EQ(BitUtil::CountLeadingZeros(U64(ULLONG_MAX / 2 + 1)), 0);
  EXPECT_EQ(BitUtil::CountLeadingZeros(U64(ULLONG_MAX)), 0);
}

#undef U32
#undef U64

TEST(BitUtil, RoundUpToPowerOf2) {
  EXPECT_EQ(BitUtil::RoundUpToPowerOf2(7, 8), 8);
  EXPECT_EQ(BitUtil::RoundUpToPowerOf2(8, 8), 8);
  EXPECT_EQ(BitUtil::RoundUpToPowerOf2(9, 8), 16);
}

static void TestZigZag(int32_t v) {
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

TEST(BitUtil, RoundTripLittleEndianTest) {
  uint64_t value = 0xFF;

#if ARROW_LITTLE_ENDIAN
  uint64_t expected = value;
#else
  uint64_t expected = std::numeric_limits<uint64_t>::max() << 56;
#endif

  uint64_t little_endian_result = BitUtil::ToLittleEndian(value);
  ASSERT_EQ(expected, little_endian_result);

  uint64_t from_little_endian = BitUtil::FromLittleEndian(little_endian_result);
  ASSERT_EQ(value, from_little_endian);
}

TEST(BitUtil, RoundTripBigEndianTest) {
  uint64_t value = 0xFF;

#if ARROW_LITTLE_ENDIAN
  uint64_t expected = std::numeric_limits<uint64_t>::max() << 56;
#else
  uint64_t expected = value;
#endif

  uint64_t big_endian_result = BitUtil::ToBigEndian(value);
  ASSERT_EQ(expected, big_endian_result);

  uint64_t from_big_endian = BitUtil::FromBigEndian(big_endian_result);
  ASSERT_EQ(value, from_big_endian);
}

}  // namespace arrow
