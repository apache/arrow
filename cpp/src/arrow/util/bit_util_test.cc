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

#include <algorithm>
#include <array>
#include <climits>
#include <cstdint>
#include <cstring>
#include <limits>
#include <memory>
#include <ostream>
#include <string>
#include <utility>
#include <vector>

#include <gmock/gmock-matchers.h>
#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "arrow/array/array_base.h"
#include "arrow/array/data.h"
#include "arrow/buffer.h"
#include "arrow/buffer_builder.h"
#include "arrow/result.h"
#include "arrow/status.h"
#include "arrow/testing/gtest_compat.h"
#include "arrow/testing/gtest_util.h"
#include "arrow/testing/random.h"
#include "arrow/testing/util.h"
#include "arrow/type_fwd.h"
#include "arrow/util/bit_run_reader.h"
#include "arrow/util/bit_stream_utils.h"
#include "arrow/util/bitmap.h"
#include "arrow/util/bitmap_generate.h"
#include "arrow/util/bitmap_ops.h"
#include "arrow/util/bitmap_reader.h"
#include "arrow/util/bitmap_visit.h"
#include "arrow/util/bitmap_writer.h"
#include "arrow/util/bitset_stack.h"
#include "arrow/util/endian.h"
#include "arrow/util/ubsan.h"

namespace arrow {

using internal::BitmapAnd;
using internal::BitmapAndNot;
using internal::BitmapOr;
using internal::BitmapXor;
using internal::BitsetStack;
using internal::CopyBitmap;
using internal::CountSetBits;
using internal::InvertBitmap;
using internal::ReverseBitmap;
using util::SafeCopy;

using ::testing::ElementsAreArray;

namespace internal {

void PrintTo(const BitRun& run, std::ostream* os) { *os << run.ToString(); }
void PrintTo(const SetBitRun& run, std::ostream* os) { *os << run.ToString(); }

}  // namespace internal

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
  ASSERT_OK_AND_ASSIGN(*out_buffer, AllocateEmptyBitmap(length + bit_offset));
  auto writer = internal::BitmapWriter((*out_buffer)->mutable_data(), bit_offset, length);
  WriteVectorToWriter(writer, values);
}

std::shared_ptr<Buffer> BitmapFromString(const std::string& s) {
  TypedBufferBuilder<bool> builder;
  ABORT_NOT_OK(builder.Reserve(s.size()));
  for (const char c : s) {
    switch (c) {
      case '0':
        builder.UnsafeAppend(false);
        break;
      case '1':
        builder.UnsafeAppend(true);
        break;
      case ' ':
      case '\t':
      case '\n':
      case '\r':
        break;
      default:
        ARROW_LOG(FATAL) << "Unexpected character in bitmap string";
    }
  }
  std::shared_ptr<Buffer> buffer;
  ABORT_NOT_OK(builder.Finish(&buffer));
  return buffer;
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
  using bit_util::IsMultipleOf64;
  EXPECT_TRUE(IsMultipleOf64(64));
  EXPECT_TRUE(IsMultipleOf64(0));
  EXPECT_TRUE(IsMultipleOf64(128));
  EXPECT_TRUE(IsMultipleOf64(192));
  EXPECT_FALSE(IsMultipleOf64(23));
  EXPECT_FALSE(IsMultipleOf64(32));
}

TEST(BitUtilTests, TestNextPower2) {
  using bit_util::NextPower2;

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

TEST(BitUtilTests, BytesForBits) {
  using bit_util::BytesForBits;

  ASSERT_EQ(BytesForBits(0), 0);
  ASSERT_EQ(BytesForBits(1), 1);
  ASSERT_EQ(BytesForBits(7), 1);
  ASSERT_EQ(BytesForBits(8), 1);
  ASSERT_EQ(BytesForBits(9), 2);
  ASSERT_EQ(BytesForBits(0xffff), 8192);
  ASSERT_EQ(BytesForBits(0x10000), 8192);
  ASSERT_EQ(BytesForBits(0x10001), 8193);
  ASSERT_EQ(BytesForBits(0x7ffffffffffffff8ll), 0x0fffffffffffffffll);
  ASSERT_EQ(BytesForBits(0x7ffffffffffffff9ll), 0x1000000000000000ll);
  ASSERT_EQ(BytesForBits(0x7fffffffffffffffll), 0x1000000000000000ll);
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

class TestBitmapUInt64Reader : public ::testing::Test {
 public:
  void AssertWords(const Buffer& buffer, int64_t start_offset, int64_t length,
                   const std::vector<uint64_t>& expected) {
    internal::BitmapUInt64Reader reader(buffer.data(), start_offset, length);
    ASSERT_EQ(reader.position(), 0);
    ASSERT_EQ(reader.length(), length);
    for (const uint64_t word : expected) {
      ASSERT_EQ(reader.NextWord(), word);
    }
    ASSERT_EQ(reader.position(), length);
  }

  void Check(const Buffer& buffer, int64_t start_offset, int64_t length) {
    internal::BitmapUInt64Reader reader(buffer.data(), start_offset, length);
    for (int64_t i = 0; i < length; i += 64) {
      ASSERT_EQ(reader.position(), i);
      const auto nbits = std::min<int64_t>(64, length - i);
      uint64_t word = reader.NextWord();
      for (int64_t j = 0; j < nbits; ++j) {
        ASSERT_EQ(word & 1, bit_util::GetBit(buffer.data(), start_offset + i + j));
        word >>= 1;
      }
    }
    ASSERT_EQ(reader.position(), length);
  }

  void CheckExtensive(const Buffer& buffer) {
    for (const int64_t offset : kTestOffsets) {
      for (int64_t length : kTestOffsets) {
        if (offset + length <= buffer.size()) {
          Check(buffer, offset, length);
          length = buffer.size() - offset - length;
          if (offset + length <= buffer.size()) {
            Check(buffer, offset, length);
          }
        }
      }
    }
  }

 protected:
  const std::vector<int64_t> kTestOffsets = {0, 1, 6, 7, 8, 33, 62, 63, 64, 65};
};

TEST_F(TestBitmapUInt64Reader, Empty) {
  for (const int64_t offset : kTestOffsets) {
    // Does not access invalid memory
    internal::BitmapUInt64Reader reader(nullptr, offset, 0);
    ASSERT_EQ(reader.position(), 0);
    ASSERT_EQ(reader.length(), 0);
  }
}

TEST_F(TestBitmapUInt64Reader, Small) {
  auto buffer = BitmapFromString(
      "11111111 10000000 00000000 00000000 00000000 00000000 00000001 11111111"
      "11111111 10000000 00000000 00000000 00000000 00000000 00000001 11111111"
      "11111111 10000000 00000000 00000000 00000000 00000000 00000001 11111111"
      "11111111 10000000 00000000 00000000 00000000 00000000 00000001 11111111");

  // One word
  AssertWords(*buffer, 0, 9, {0x1ff});
  AssertWords(*buffer, 1, 9, {0xff});
  AssertWords(*buffer, 7, 9, {0x3});
  AssertWords(*buffer, 8, 9, {0x1});
  AssertWords(*buffer, 9, 9, {0x0});

  AssertWords(*buffer, 54, 10, {0x3fe});
  AssertWords(*buffer, 54, 9, {0x1fe});
  AssertWords(*buffer, 54, 8, {0xfe});

  AssertWords(*buffer, 55, 9, {0x1ff});
  AssertWords(*buffer, 56, 8, {0xff});
  AssertWords(*buffer, 57, 7, {0x7f});
  AssertWords(*buffer, 63, 1, {0x1});

  AssertWords(*buffer, 0, 64, {0xff800000000001ffULL});

  // One straddling word
  AssertWords(*buffer, 54, 12, {0xffe});
  AssertWords(*buffer, 63, 2, {0x3});

  // One word (start_offset >= 64)
  AssertWords(*buffer, 96, 64, {0x000001ffff800000ULL});

  // Two words
  AssertWords(*buffer, 0, 128, {0xff800000000001ffULL, 0xff800000000001ffULL});
  AssertWords(*buffer, 0, 127, {0xff800000000001ffULL, 0x7f800000000001ffULL});
  AssertWords(*buffer, 1, 127, {0xffc00000000000ffULL, 0x7fc00000000000ffULL});
  AssertWords(*buffer, 1, 128, {0xffc00000000000ffULL, 0xffc00000000000ffULL});
  AssertWords(*buffer, 63, 128, {0xff000000000003ffULL, 0xff000000000003ffULL});
  AssertWords(*buffer, 63, 65, {0xff000000000003ffULL, 0x1});

  // More than two words
  AssertWords(*buffer, 0, 256,
              {0xff800000000001ffULL, 0xff800000000001ffULL, 0xff800000000001ffULL,
               0xff800000000001ffULL});
  AssertWords(*buffer, 1, 255,
              {0xffc00000000000ffULL, 0xffc00000000000ffULL, 0xffc00000000000ffULL,
               0x7fc00000000000ffULL});
  AssertWords(*buffer, 63, 193,
              {0xff000000000003ffULL, 0xff000000000003ffULL, 0xff000000000003ffULL, 0x1});
  AssertWords(*buffer, 63, 192,
              {0xff000000000003ffULL, 0xff000000000003ffULL, 0xff000000000003ffULL});

  CheckExtensive(*buffer);
}

TEST_F(TestBitmapUInt64Reader, Random) {
  random::RandomArrayGenerator rng(42);
  auto buffer = rng.NullBitmap(500, 0.5);
  CheckExtensive(*buffer);
}

class TestSetBitRunReader : public ::testing::Test {
 public:
  std::vector<internal::SetBitRun> ReferenceBitRuns(const uint8_t* data,
                                                    int64_t start_offset,
                                                    int64_t length) {
    std::vector<internal::SetBitRun> runs;
    internal::BitRunReaderLinear reader(data, start_offset, length);
    int64_t position = 0;
    while (position < length) {
      const auto br = reader.NextRun();
      if (br.set) {
        runs.push_back({position, br.length});
      }
      position += br.length;
    }
    return runs;
  }

  template <typename SetBitRunReaderType>
  std::vector<internal::SetBitRun> AllBitRuns(SetBitRunReaderType* reader) {
    std::vector<internal::SetBitRun> runs;
    auto run = reader->NextRun();
    while (!run.AtEnd()) {
      runs.push_back(run);
      run = reader->NextRun();
    }
    return runs;
  }

  template <typename SetBitRunReaderType>
  void AssertBitRuns(SetBitRunReaderType* reader,
                     const std::vector<internal::SetBitRun>& expected) {
    ASSERT_EQ(AllBitRuns(reader), expected);
  }

  void AssertBitRuns(const uint8_t* data, int64_t start_offset, int64_t length,
                     const std::vector<internal::SetBitRun>& expected) {
    {
      internal::SetBitRunReader reader(data, start_offset, length);
      AssertBitRuns(&reader, expected);
    }
    {
      internal::ReverseSetBitRunReader reader(data, start_offset, length);
      auto reversed_expected = expected;
      std::reverse(reversed_expected.begin(), reversed_expected.end());
      AssertBitRuns(&reader, reversed_expected);
    }
  }

  void AssertBitRuns(const Buffer& buffer, int64_t start_offset, int64_t length,
                     const std::vector<internal::SetBitRun>& expected) {
    AssertBitRuns(buffer.data(), start_offset, length, expected);
  }

  void CheckAgainstReference(const Buffer& buffer, int64_t start_offset, int64_t length) {
    const auto expected = ReferenceBitRuns(buffer.data(), start_offset, length);
    AssertBitRuns(buffer.data(), start_offset, length, expected);
  }

  struct Range {
    int64_t offset;
    int64_t length;

    int64_t end_offset() const { return offset + length; }
  };

  std::vector<Range> BufferTestRanges(const Buffer& buffer) {
    const int64_t buffer_size = buffer.size() * 8;  // in bits
    std::vector<Range> ranges;
    for (const int64_t offset : kTestOffsets) {
      for (const int64_t length_adjust : kTestOffsets) {
        int64_t length = std::min(buffer_size - offset, length_adjust);
        EXPECT_GE(length, 0);
        ranges.push_back({offset, length});
        length = std::min(buffer_size - offset, buffer_size - length_adjust);
        EXPECT_GE(length, 0);
        ranges.push_back({offset, length});
      }
    }
    return ranges;
  }

 protected:
  const std::vector<int64_t> kTestOffsets = {0, 1, 6, 7, 8, 33, 63, 64, 65, 71};
};

TEST_F(TestSetBitRunReader, Empty) {
  for (const int64_t offset : kTestOffsets) {
    // Does not access invalid memory
    AssertBitRuns(nullptr, offset, 0, {});
  }
}

TEST_F(TestSetBitRunReader, OneByte) {
  auto buffer = BitmapFromString("01101101");
  AssertBitRuns(*buffer, 0, 8, {{1, 2}, {4, 2}, {7, 1}});

  for (const char* bitmap_string : {"01101101", "10110110", "00000000", "11111111"}) {
    auto buffer = BitmapFromString(bitmap_string);
    for (int64_t offset = 0; offset < 8; ++offset) {
      for (int64_t length = 0; length <= 8 - offset; ++length) {
        CheckAgainstReference(*buffer, offset, length);
      }
    }
  }
}

TEST_F(TestSetBitRunReader, Tiny) {
  auto buffer = BitmapFromString("11100011 10001110 00111000 11100011 10001110 00111000");

  AssertBitRuns(*buffer, 0, 48,
                {{0, 3}, {6, 3}, {12, 3}, {18, 3}, {24, 3}, {30, 3}, {36, 3}, {42, 3}});
  AssertBitRuns(*buffer, 0, 46,
                {{0, 3}, {6, 3}, {12, 3}, {18, 3}, {24, 3}, {30, 3}, {36, 3}, {42, 3}});
  AssertBitRuns(*buffer, 0, 45,
                {{0, 3}, {6, 3}, {12, 3}, {18, 3}, {24, 3}, {30, 3}, {36, 3}, {42, 3}});
  AssertBitRuns(*buffer, 0, 42,
                {{0, 3}, {6, 3}, {12, 3}, {18, 3}, {24, 3}, {30, 3}, {36, 3}});
  AssertBitRuns(*buffer, 3, 45,
                {{3, 3}, {9, 3}, {15, 3}, {21, 3}, {27, 3}, {33, 3}, {39, 3}});
  AssertBitRuns(*buffer, 3, 43,
                {{3, 3}, {9, 3}, {15, 3}, {21, 3}, {27, 3}, {33, 3}, {39, 3}});
  AssertBitRuns(*buffer, 3, 42,
                {{3, 3}, {9, 3}, {15, 3}, {21, 3}, {27, 3}, {33, 3}, {39, 3}});
  AssertBitRuns(*buffer, 3, 39, {{3, 3}, {9, 3}, {15, 3}, {21, 3}, {27, 3}, {33, 3}});
}

TEST_F(TestSetBitRunReader, AllZeros) {
  const int64_t kBufferSize = 256;
  ASSERT_OK_AND_ASSIGN(auto buffer, AllocateEmptyBitmap(kBufferSize));

  for (const auto range : BufferTestRanges(*buffer)) {
    AssertBitRuns(*buffer, range.offset, range.length, {});
  }
}

TEST_F(TestSetBitRunReader, AllOnes) {
  const int64_t kBufferSize = 256;
  ASSERT_OK_AND_ASSIGN(auto buffer, AllocateEmptyBitmap(kBufferSize));
  bit_util::SetBitsTo(buffer->mutable_data(), 0, kBufferSize, true);

  for (const auto range : BufferTestRanges(*buffer)) {
    if (range.length > 0) {
      AssertBitRuns(*buffer, range.offset, range.length, {{0, range.length}});
    } else {
      AssertBitRuns(*buffer, range.offset, range.length, {});
    }
  }
}

TEST_F(TestSetBitRunReader, Small) {
  // Ones then zeros then ones
  const int64_t kBufferSize = 256;
  const int64_t kOnesLength = 64;
  const int64_t kSecondOnesStart = kBufferSize - kOnesLength;
  ASSERT_OK_AND_ASSIGN(auto buffer, AllocateEmptyBitmap(kBufferSize));
  bit_util::SetBitsTo(buffer->mutable_data(), 0, kBufferSize, false);
  bit_util::SetBitsTo(buffer->mutable_data(), 0, kOnesLength, true);
  bit_util::SetBitsTo(buffer->mutable_data(), kSecondOnesStart, kOnesLength, true);

  for (const auto range : BufferTestRanges(*buffer)) {
    std::vector<internal::SetBitRun> expected;
    if (range.offset < kOnesLength && range.length > 0) {
      expected.push_back({0, std::min(kOnesLength - range.offset, range.length)});
    }
    if (range.offset + range.length > kSecondOnesStart) {
      expected.push_back({kSecondOnesStart - range.offset,
                          range.length + range.offset - kSecondOnesStart});
    }
    AssertBitRuns(*buffer, range.offset, range.length, expected);
  }
}

TEST_F(TestSetBitRunReader, SingleRun) {
  // One single run of ones, at varying places in the buffer
  const int64_t kBufferSize = 512;
  ASSERT_OK_AND_ASSIGN(auto buffer, AllocateEmptyBitmap(kBufferSize));

  for (const auto ones_range : BufferTestRanges(*buffer)) {
    bit_util::SetBitsTo(buffer->mutable_data(), 0, kBufferSize, false);
    bit_util::SetBitsTo(buffer->mutable_data(), ones_range.offset, ones_range.length,
                        true);
    for (const auto range : BufferTestRanges(*buffer)) {
      std::vector<internal::SetBitRun> expected;

      if (range.length && ones_range.length && range.offset < ones_range.end_offset() &&
          ones_range.offset < range.end_offset()) {
        // The two ranges intersect
        const int64_t intersect_start = std::max(range.offset, ones_range.offset);
        const int64_t intersect_stop =
            std::min(range.end_offset(), ones_range.end_offset());
        expected.push_back(
            {intersect_start - range.offset, intersect_stop - intersect_start});
      }
      AssertBitRuns(*buffer, range.offset, range.length, expected);
    }
  }
}

TEST_F(TestSetBitRunReader, Random) {
  const int64_t kBufferSize = 4096;
  arrow::random::RandomArrayGenerator rng(42);
  for (const double set_probability : {0.003, 0.01, 0.1, 0.5, 0.9, 0.99, 0.997}) {
    auto arr = rng.Boolean(kBufferSize, set_probability);
    auto buffer = arr->data()->buffers[1];
    for (const auto range : BufferTestRanges(*buffer)) {
      CheckAgainstReference(*buffer, range.offset, range.length);
    }
  }
}

TEST(BitRunReader, ZeroLength) {
  internal::BitRunReader reader(nullptr, /*start_offset=*/0, /*length=*/0);

  EXPECT_EQ(reader.NextRun().length, 0);
}

TEST(BitRunReader, NormalOperation) {
  std::vector<int> bm_vector = {1, 0, 1};                   // size: 3
  bm_vector.insert(bm_vector.end(), /*n=*/5, /*val=*/0);    // size: 8
  bm_vector.insert(bm_vector.end(), /*n=*/7, /*val=*/1);    // size: 15
  bm_vector.insert(bm_vector.end(), /*n=*/3, /*val=*/0);    // size: 18
  bm_vector.insert(bm_vector.end(), /*n=*/25, /*val=*/1);   // size: 43
  bm_vector.insert(bm_vector.end(), /*n=*/21, /*val=*/0);   // size: 64
  bm_vector.insert(bm_vector.end(), /*n=*/26, /*val=*/1);   // size: 90
  bm_vector.insert(bm_vector.end(), /*n=*/130, /*val=*/0);  // size: 220
  bm_vector.insert(bm_vector.end(), /*n=*/65, /*val=*/1);   // size: 285
  std::shared_ptr<Buffer> bitmap;
  int64_t length;
  BitmapFromVector(bm_vector, /*bit_offset=*/0, &bitmap, &length);

  internal::BitRunReader reader(bitmap->data(), /*start_offset=*/0, /*length=*/length);
  std::vector<internal::BitRun> results;
  internal::BitRun rl;
  do {
    rl = reader.NextRun();
    results.push_back(rl);
  } while (rl.length != 0);
  EXPECT_EQ(results.back().length, 0);
  results.pop_back();
  EXPECT_THAT(results, ElementsAreArray(
                           std::vector<internal::BitRun>{{/*length=*/1, /*set=*/true},
                                                         {/*length=*/1, /*set=*/false},
                                                         {/*length=*/1, /*set=*/true},
                                                         {/*length=*/5, /*set=*/false},
                                                         {/*length=*/7, /*set=*/true},
                                                         {/*length=*/3, /*set=*/false},
                                                         {/*length=*/25, /*set=*/true},
                                                         {/*length=*/21, /*set=*/false},
                                                         {/*length=*/26, /*set=*/true},
                                                         {/*length=*/130, /*set=*/false},
                                                         {/*length=*/65, /*set=*/true}}));
}

TEST(BitRunReader, AllFirstByteCombos) {
  for (int offset = 0; offset < 8; offset++) {
    for (int64_t x = 0; x < (1 << 8) - 1; x++) {
      int64_t bits = bit_util::ToLittleEndian(x);
      internal::BitRunReader reader(reinterpret_cast<uint8_t*>(&bits),
                                    /*start_offset=*/offset,
                                    /*length=*/8 - offset);
      std::vector<internal::BitRun> results;
      internal::BitRun rl;
      do {
        rl = reader.NextRun();
        results.push_back(rl);
      } while (rl.length != 0);
      EXPECT_EQ(results.back().length, 0);
      results.pop_back();
      int64_t sum = 0;
      for (const auto& result : results) {
        sum += result.length;
      }
      ASSERT_EQ(sum, 8 - offset);
    }
  }
}

TEST(BitRunReader, TruncatedAtWord) {
  std::vector<int> bm_vector;
  bm_vector.insert(bm_vector.end(), /*n=*/7, /*val=*/1);
  bm_vector.insert(bm_vector.end(), /*n=*/58, /*val=*/0);

  std::shared_ptr<Buffer> bitmap;
  int64_t length;
  BitmapFromVector(bm_vector, /*bit_offset=*/0, &bitmap, &length);

  internal::BitRunReader reader(bitmap->data(), /*start_offset=*/1,
                                /*length=*/63);
  std::vector<internal::BitRun> results;
  internal::BitRun rl;
  do {
    rl = reader.NextRun();
    results.push_back(rl);
  } while (rl.length != 0);
  EXPECT_EQ(results.back().length, 0);
  results.pop_back();
  EXPECT_THAT(results,
              ElementsAreArray(std::vector<internal::BitRun>{
                  {/*length=*/6, /*set=*/true}, {/*length=*/57, /*set=*/false}}));
}

TEST(BitRunReader, ScalarComparison) {
  ::arrow::random::RandomArrayGenerator rag(/*seed=*/23);
  constexpr int64_t kNumBits = 1000000;
  std::shared_ptr<Buffer> buffer =
      rag.Boolean(kNumBits, /*set_probability=*/.4)->data()->buffers[1];

  const uint8_t* bitmap = buffer->data();

  internal::BitRunReader reader(bitmap, 0, kNumBits);
  internal::BitRunReaderLinear scalar_reader(bitmap, 0, kNumBits);
  internal::BitRun br, brs;
  int64_t br_bits = 0;
  int64_t brs_bits = 0;
  do {
    br = reader.NextRun();
    brs = scalar_reader.NextRun();
    br_bits += br.length;
    brs_bits += brs.length;
    EXPECT_EQ(br.length, brs.length);
    if (br.length > 0) {
      EXPECT_EQ(br, brs) << internal::Bitmap(bitmap, 0, kNumBits).ToString() << br_bits
                         << " " << brs_bits;
    }
  } while (brs.length != 0);
  EXPECT_EQ(br_bits, brs_bits);
}

TEST(BitRunReader, TruncatedWithinWordMultipleOf8Bits) {
  std::vector<int> bm_vector;
  bm_vector.insert(bm_vector.end(), /*n=*/7, /*val=*/1);
  bm_vector.insert(bm_vector.end(), /*n=*/5, /*val=*/0);

  std::shared_ptr<Buffer> bitmap;
  int64_t length;
  BitmapFromVector(bm_vector, /*bit_offset=*/0, &bitmap, &length);

  internal::BitRunReader reader(bitmap->data(), /*start_offset=*/1,
                                /*length=*/7);
  std::vector<internal::BitRun> results;
  internal::BitRun rl;
  do {
    rl = reader.NextRun();
    results.push_back(rl);
  } while (rl.length != 0);
  EXPECT_EQ(results.back().length, 0);
  results.pop_back();
  EXPECT_THAT(results, ElementsAreArray(std::vector<internal::BitRun>{
                           {/*length=*/6, /*set=*/true}, {/*length=*/1, /*set=*/false}}));
}

TEST(BitRunReader, TruncatedWithinWord) {
  std::vector<int> bm_vector;
  bm_vector.insert(bm_vector.end(), /*n=*/37 + 40, /*val=*/0);
  bm_vector.insert(bm_vector.end(), /*n=*/23, /*val=*/1);

  std::shared_ptr<Buffer> bitmap;
  int64_t length;
  BitmapFromVector(bm_vector, /*bit_offset=*/0, &bitmap, &length);

  constexpr int64_t kOffset = 37;
  internal::BitRunReader reader(bitmap->data(), /*start_offset=*/kOffset,
                                /*length=*/53);
  std::vector<internal::BitRun> results;
  internal::BitRun rl;
  do {
    rl = reader.NextRun();
    results.push_back(rl);
  } while (rl.length != 0);
  EXPECT_EQ(results.back().length, 0);
  results.pop_back();
  EXPECT_THAT(results,
              ElementsAreArray(std::vector<internal::BitRun>{
                  {/*length=*/40, /*set=*/false}, {/*length=*/13, /*set=*/true}}));
}

TEST(BitRunReader, TruncatedMultipleWords) {
  std::vector<int> bm_vector = {1, 0, 1};                  // size: 3
  bm_vector.insert(bm_vector.end(), /*n=*/5, /*val=*/0);   // size: 8
  bm_vector.insert(bm_vector.end(), /*n=*/30, /*val=*/1);  // size: 38
  bm_vector.insert(bm_vector.end(), /*n=*/95, /*val=*/0);  // size: 133
  std::shared_ptr<Buffer> bitmap;
  int64_t length;
  BitmapFromVector(bm_vector, /*bit_offset=*/0, &bitmap, &length);

  constexpr int64_t kOffset = 5;
  internal::BitRunReader reader(bitmap->data(), /*start_offset=*/kOffset,
                                /*length=*/length - (kOffset + 3));
  std::vector<internal::BitRun> results;
  internal::BitRun rl;
  do {
    rl = reader.NextRun();
    results.push_back(rl);
  } while (rl.length != 0);
  EXPECT_EQ(results.back().length, 0);
  results.pop_back();
  EXPECT_THAT(results, ElementsAreArray(std::vector<internal::BitRun>{
                           {/*length=*/3, /*set=*/false},
                           {/*length=*/30, /*set=*/true},
                           {/*length=*/92, /*set=*/false}}));
}

TEST(BitmapWriter, NormalOperation) {
  for (const auto fill_byte_int : {0x00, 0xff}) {
    const uint8_t fill_byte = static_cast<uint8_t>(fill_byte_int);
    {
      uint8_t bitmap[] = {fill_byte, fill_byte, fill_byte, fill_byte};
      auto writer = internal::BitmapWriter(bitmap, 0, 12);
      WriteVectorToWriter(writer, {0, 1, 1, 0, 1, 1, 0, 0, 0, 1, 0, 1});
      //                      {0b00110110, 0b....1010, ........, ........}
      ASSERT_BYTES_EQ(bitmap, {0x36, static_cast<uint8_t>(0x0a | (fill_byte & 0xf0)),
                               fill_byte, fill_byte});
    }
    {
      uint8_t bitmap[] = {fill_byte, fill_byte, fill_byte, fill_byte};
      auto writer = internal::BitmapWriter(bitmap, 3, 12);
      WriteVectorToWriter(writer, {0, 1, 1, 0, 1, 1, 0, 0, 0, 1, 0, 1});
      //                      {0b10110..., 0b.1010001, ........, ........}
      ASSERT_BYTES_EQ(bitmap, {static_cast<uint8_t>(0xb0 | (fill_byte & 0x07)),
                               static_cast<uint8_t>(0x51 | (fill_byte & 0x80)), fill_byte,
                               fill_byte});
    }
    {
      uint8_t bitmap[] = {fill_byte, fill_byte, fill_byte, fill_byte};
      auto writer = internal::BitmapWriter(bitmap, 20, 12);
      WriteVectorToWriter(writer, {0, 1, 1, 0, 1, 1, 0, 0, 0, 1, 0, 1});
      //                      {........, ........, 0b0110...., 0b10100011}
      ASSERT_BYTES_EQ(bitmap, {fill_byte, fill_byte,
                               static_cast<uint8_t>(0x60 | (fill_byte & 0x0f)), 0xa3});
    }
    // 0-length writes
    for (int64_t pos = 0; pos < 32; ++pos) {
      uint8_t bitmap[] = {fill_byte, fill_byte, fill_byte, fill_byte};
      auto writer = internal::BitmapWriter(bitmap, pos, 0);
      WriteVectorToWriter(writer, {});
      ASSERT_BYTES_EQ(bitmap, {fill_byte, fill_byte, fill_byte, fill_byte});
    }
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
        auto writer = internal::FirstTimeBitmapWriter(bitmap, 4, 0);
        WriteVectorToWriter(writer, {});
      }
      {
        auto writer = internal::FirstTimeBitmapWriter(bitmap, 4, 6);
        WriteVectorToWriter(writer, {0, 1, 1, 0, 1, 1});
      }
      {
        auto writer = internal::FirstTimeBitmapWriter(bitmap, 10, 3);
        WriteVectorToWriter(writer, {0, 0, 0});
      }
      {
        auto writer = internal::FirstTimeBitmapWriter(bitmap, 13, 0);
        WriteVectorToWriter(writer, {});
      }
      {
        auto writer = internal::FirstTimeBitmapWriter(bitmap, 13, 3);
        WriteVectorToWriter(writer, {1, 0, 1});
      }
      ASSERT_BYTES_EQ(bitmap, {static_cast<uint8_t>(0x60 | (fill_byte & 0x0f)), 0xa3});
    }
  }
}

std::string BitmapToString(const uint8_t* bitmap, int64_t bit_count) {
  return arrow::internal::Bitmap(bitmap, /*offset*/ 0, /*length=*/bit_count).ToString();
}

std::string BitmapToString(const std::vector<uint8_t>& bitmap, int64_t bit_count) {
  return BitmapToString(bitmap.data(), bit_count);
}

TEST(FirstTimeBitmapWriter, AppendWordOffsetOverwritesCorrectBitsOnExistingByte) {
  auto check_append = [](const std::string& expected_bits, int64_t offset) {
    std::vector<uint8_t> valid_bits = {0x00};
    constexpr int64_t kBitsAfterAppend = 8;
    internal::FirstTimeBitmapWriter writer(valid_bits.data(), offset,
                                           /*length=*/(8 * valid_bits.size()) - offset);
    writer.AppendWord(/*word=*/0xFF, /*number_of_bits=*/kBitsAfterAppend - offset);
    writer.Finish();
    EXPECT_EQ(BitmapToString(valid_bits, kBitsAfterAppend), expected_bits);
  };
  check_append("11111111", 0);
  check_append("01111111", 1);
  check_append("00111111", 2);
  check_append("00011111", 3);
  check_append("00001111", 4);
  check_append("00000111", 5);
  check_append("00000011", 6);
  check_append("00000001", 7);

  auto check_with_set = [](const std::string& expected_bits, int64_t offset) {
    std::vector<uint8_t> valid_bits = {0x1};
    constexpr int64_t kBitsAfterAppend = 8;
    internal::FirstTimeBitmapWriter writer(valid_bits.data(), offset,
                                           /*length=*/(8 * valid_bits.size()) - offset);
    writer.AppendWord(/*word=*/0xFF, /*number_of_bits=*/kBitsAfterAppend - offset);
    writer.Finish();
    EXPECT_EQ(BitmapToString(valid_bits, kBitsAfterAppend), expected_bits);
  };
  // 0ffset zero would not be a valid mask.
  check_with_set("11111111", 1);
  check_with_set("10111111", 2);
  check_with_set("10011111", 3);
  check_with_set("10001111", 4);
  check_with_set("10000111", 5);
  check_with_set("10000011", 6);
  check_with_set("10000001", 7);

  auto check_with_preceding = [](const std::string& expected_bits, int64_t offset) {
    std::vector<uint8_t> valid_bits = {0xFF};
    constexpr int64_t kBitsAfterAppend = 8;
    internal::FirstTimeBitmapWriter writer(valid_bits.data(), offset,
                                           /*length=*/(8 * valid_bits.size()) - offset);
    writer.AppendWord(/*word=*/0xFF, /*number_of_bits=*/kBitsAfterAppend - offset);
    writer.Finish();
    EXPECT_EQ(BitmapToString(valid_bits, kBitsAfterAppend), expected_bits);
  };
  check_with_preceding("11111111", 0);
  check_with_preceding("11111111", 1);
  check_with_preceding("11111111", 2);
  check_with_preceding("11111111", 3);
  check_with_preceding("11111111", 4);
  check_with_preceding("11111111", 5);
  check_with_preceding("11111111", 6);
  check_with_preceding("11111111", 7);
}

TEST(FirstTimeBitmapWriter, AppendZeroBitsHasNoImpact) {
  std::vector<uint8_t> valid_bits(/*count=*/1, 0);
  internal::FirstTimeBitmapWriter writer(valid_bits.data(), /*start_offset=*/1,
                                         /*length=*/valid_bits.size() * 8);
  writer.AppendWord(/*word=*/0xFF, /*number_of_bits=*/0);
  writer.AppendWord(/*word=*/0xFF, /*number_of_bits=*/0);
  writer.AppendWord(/*word=*/0x01, /*number_of_bits=*/1);
  writer.Finish();
  EXPECT_EQ(valid_bits[0], 0x2);
}

TEST(FirstTimeBitmapWriter, AppendLessThanByte) {
  {
    std::vector<uint8_t> valid_bits(/*count*/ 8, 0);
    internal::FirstTimeBitmapWriter writer(valid_bits.data(), /*start_offset=*/1,
                                           /*length=*/8);
    writer.AppendWord(0xB, 4);
    writer.Finish();
    EXPECT_EQ(BitmapToString(valid_bits, /*bit_count=*/8), "01101000");
  }
  {
    // Test with all bits initially set.
    std::vector<uint8_t> valid_bits(/*count*/ 8, 0xFF);
    internal::FirstTimeBitmapWriter writer(valid_bits.data(), /*start_offset=*/1,
                                           /*length=*/8);
    writer.AppendWord(0xB, 4);
    writer.Finish();
    EXPECT_EQ(BitmapToString(valid_bits, /*bit_count=*/8), "11101000");
  }
}

TEST(FirstTimeBitmapWriter, AppendByteThenMore) {
  {
    std::vector<uint8_t> valid_bits(/*count*/ 8, 0);
    internal::FirstTimeBitmapWriter writer(valid_bits.data(), /*start_offset=*/0,
                                           /*length=*/9);
    writer.AppendWord(0xC3, 8);
    writer.AppendWord(0x01, 1);
    writer.Finish();
    EXPECT_EQ(BitmapToString(valid_bits, /*bit_count=*/9), "11000011 1");
  }
  {
    std::vector<uint8_t> valid_bits(/*count*/ 8, 0xFF);
    internal::FirstTimeBitmapWriter writer(valid_bits.data(), /*start_offset=*/0,
                                           /*length=*/9);
    writer.AppendWord(0xC3, 8);
    writer.AppendWord(0x01, 1);
    writer.Finish();
    EXPECT_EQ(BitmapToString(valid_bits, /*bit_count=*/9), "11000011 1");
  }
}

TEST(FirstTimeBitmapWriter, AppendWordShiftsBitsCorrectly) {
  constexpr uint64_t kPattern = 0x9A9A9A9A9A9A9A9A;
  auto check_append = [&](const std::string& leading_bits, const std::string& middle_bits,
                          const std::string& trailing_bits, int64_t offset,
                          bool preset_buffer_bits = false) {
    ASSERT_GE(offset, 8);
    std::vector<uint8_t> valid_bits(/*count=*/10, preset_buffer_bits ? 0xFF : 0);
    valid_bits[0] = 0x99;
    internal::FirstTimeBitmapWriter writer(valid_bits.data(), offset,
                                           /*length=*/(9 * sizeof(kPattern)) - offset);
    writer.AppendWord(/*word=*/kPattern, /*number_of_bits=*/64);
    writer.Finish();
    EXPECT_EQ(valid_bits[0], 0x99);  // shouldn't get changed.
    EXPECT_EQ(BitmapToString(valid_bits.data() + 1, /*num_bits=*/8), leading_bits);
    for (int x = 2; x < 9; x++) {
      EXPECT_EQ(BitmapToString(valid_bits.data() + x, /*num_bits=*/8), middle_bits)
          << "x: " << x << " " << offset << " " << BitmapToString(valid_bits.data(), 80);
    }
    EXPECT_EQ(BitmapToString(valid_bits.data() + 9, /*num_bits=*/8), trailing_bits);
  };
  // Original Pattern = "01011001"
  check_append(/*leading_bits= */ "01011001", /*middle_bits=*/"01011001",
               /*trailing_bits=*/"00000000", /*offset=*/8);
  check_append("00101100", "10101100", "10000000", 9);
  check_append("00010110", "01010110", "01000000", 10);
  check_append("00001011", "00101011", "00100000", 11);
  check_append("00000101", "10010101", "10010000", 12);
  check_append("00000010", "11001010", "11001000", 13);
  check_append("00000001", "01100101", "01100100", 14);
  check_append("00000000", "10110010", "10110010", 15);

  check_append(/*leading_bits= */ "01011001", /*middle_bits=*/"01011001",
               /*trailing_bits=*/"11111111", /*offset=*/8, /*preset_buffer_bits=*/true);
  check_append("10101100", "10101100", "10000000", 9, true);
  check_append("11010110", "01010110", "01000000", 10, true);
  check_append("11101011", "00101011", "00100000", 11, true);
  check_append("11110101", "10010101", "10010000", 12, true);
  check_append("11111010", "11001010", "11001000", 13, true);
  check_append("11111101", "01100101", "01100100", 14, true);
  check_append("11111110", "10110010", "10110010", 15, true);
}

TEST(TestAppendBitmap, AppendWordOnlyAppropriateBytesWritten) {
  std::vector<uint8_t> valid_bits = {0x00, 0x00};

  uint64_t bitmap = 0x1FF;
  {
    internal::FirstTimeBitmapWriter writer(valid_bits.data(), /*start_offset=*/1,
                                           /*length=*/(8 * valid_bits.size()) - 1);
    writer.AppendWord(bitmap, /*number_of_bits*/ 7);
    writer.Finish();
    EXPECT_THAT(valid_bits, ElementsAreArray(std::vector<uint8_t>{0xFE, 0x00}));
  }
  {
    internal::FirstTimeBitmapWriter writer(valid_bits.data(), /*start_offset=*/1,
                                           /*length=*/(8 * valid_bits.size()) - 1);
    writer.AppendWord(bitmap, /*number_of_bits*/ 8);
    writer.Finish();
    EXPECT_THAT(valid_bits, ElementsAreArray(std::vector<uint8_t>{0xFE, 0x03}));
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
TYPED_TEST_SUITE(TestGenerateBits, GenerateBitsTypes);

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
        uint8_t bitmap[kSourceSize + 1];
        memset(bitmap, fill_byte, kSourceSize + 1);
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
        // Check bits preceding generated contents weren't clobbered
        {
          internal::BitmapReader reader_before(bitmap, 0, start_offset);
          for (int64_t i = 0; i < start_offset; ++i) {
            ASSERT_EQ(reader_before.IsSet(), fill_byte == 0xff)
                << "mismatch at preceding bit #" << start_offset - i;
          }
        }
        // Check the byte following generated contents wasn't clobbered
        auto byte_after = bitmap[bit_util::CeilDiv(start_offset + length, 8)];
        ASSERT_EQ(byte_after, fill_byte);
      }
    }
  }
}

// Tests for VisitBits and VisitBitsUnrolled. Based on the tests for GenerateBits and
// GenerateBitsUnrolled.
struct VisitBitsFunctor {
  void operator()(const uint8_t* bitmap, int64_t start_offset, int64_t length,
                  bool* destination) {
    auto writer = [&](const bool& bit_value) { *destination++ = bit_value; };
    return internal::VisitBits(bitmap, start_offset, length, writer);
  }
};

struct VisitBitsUnrolledFunctor {
  void operator()(const uint8_t* bitmap, int64_t start_offset, int64_t length,
                  bool* destination) {
    auto writer = [&](const bool& bit_value) { *destination++ = bit_value; };
    return internal::VisitBitsUnrolled(bitmap, start_offset, length, writer);
  }
};

/* Define a typed test class with some utility members. */
template <typename T>
class TestVisitBits : public ::testing::Test {
 protected:
  // The bitmap size that will be used throughout the VisitBits tests.
  static const int64_t kBitmapSizeInBytes = 32;

  // Typedefs for the source and expected destination types in this test.
  using PackedBitmapType = std::array<uint8_t, kBitmapSizeInBytes>;
  using UnpackedBitmapType = std::array<bool, 8 * kBitmapSizeInBytes>;

  // Helper functions to generate the source bitmap and expected destination
  // arrays.
  static PackedBitmapType generate_packed_bitmap() {
    PackedBitmapType bitmap;
    // Assign random values into the source array.
    random_bytes(kBitmapSizeInBytes, 0, bitmap.data());
    return bitmap;
  }

  static UnpackedBitmapType generate_unpacked_bitmap(PackedBitmapType bitmap) {
    // Use a BitmapReader (tested earlier) to populate the expected
    // unpacked bitmap.
    UnpackedBitmapType result;
    internal::BitmapReader reader(bitmap.data(), 0, 8 * kBitmapSizeInBytes);
    for (int64_t index = 0; index < 8 * kBitmapSizeInBytes; ++index) {
      result[index] = reader.IsSet();
      reader.Next();
    }
    return result;
  }

  // A pre-defined packed bitmap for use in test cases.
  const PackedBitmapType packed_bitmap_;

  // The expected unpacked bitmap that would be generated if each bit in
  // the entire source bitmap was correctly unpacked to bytes.
  const UnpackedBitmapType expected_unpacked_bitmap_;

  // Define a test constructor that populates the packed bitmap and the expected
  // unpacked bitmap.
  TestVisitBits()
      : packed_bitmap_(generate_packed_bitmap()),
        expected_unpacked_bitmap_(generate_unpacked_bitmap(packed_bitmap_)) {}
};

using VisitBitsTestTypes = ::testing::Types<VisitBitsFunctor, VisitBitsUnrolledFunctor>;
TYPED_TEST_SUITE(TestVisitBits, VisitBitsTestTypes);

/* Test bit-unpacking when reading less than eight bits from the input */
TYPED_TEST(TestVisitBits, NormalOperation) {
  typename TestFixture::UnpackedBitmapType unpacked_bitmap;
  const int64_t start_offsets[] = {0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 21, 31, 32};
  const int64_t lengths[] = {0,  1,  2,  3,  4,   5,   6,   7,   8,   9,   12,  16,
                             17, 21, 31, 32, 100, 201, 202, 203, 204, 205, 206, 207};
  const bool fill_values[] = {false, true};

  for (const bool fill_value : fill_values) {
    auto is_unmodified = [=](bool value) -> bool { return value == fill_value; };

    for (const int64_t start_offset : start_offsets) {
      for (const int64_t length : lengths) {
        std::string failure_info = std::string("fill value: ") +
                                   std::to_string(fill_value) +
                                   ", start offset: " + std::to_string(start_offset) +
                                   ", length: " + std::to_string(length);
        // Pre-fill the unpacked_bitmap array.
        unpacked_bitmap.fill(fill_value);

        // Attempt to read bits from the input bitmap into the unpacked_bitmap bitmap.
        using VisitBitsFunctor = TypeParam;
        VisitBitsFunctor()(this->packed_bitmap_.data(), start_offset, length,
                           unpacked_bitmap.data() + start_offset);

        // Verify that the correct values have been written in the [start_offset,
        // start_offset+length) range.
        EXPECT_TRUE(std::equal(unpacked_bitmap.begin() + start_offset,
                               unpacked_bitmap.begin() + start_offset + length,
                               this->expected_unpacked_bitmap_.begin() + start_offset))
            << "Invalid bytes unpacked when using " << failure_info;

        // Verify that the unpacked_bitmap array has not changed before or after
        // the [start_offset, start_offset+length) range.
        EXPECT_TRUE(std::all_of(unpacked_bitmap.begin(),
                                unpacked_bitmap.begin() + start_offset, is_unmodified))
            << "Unexpected modification to unpacked_bitmap array before written range "
               "when using "
            << failure_info;
        EXPECT_TRUE(std::all_of(unpacked_bitmap.begin() + start_offset + length,
                                unpacked_bitmap.end(), is_unmodified))
            << "Unexpected modification to unpacked_bitmap array after written range "
               "when using "
            << failure_info;
      }
    }
  }
}

struct BitmapOperation {
  virtual Result<std::shared_ptr<Buffer>> Call(MemoryPool* pool, const uint8_t* left,
                                               int64_t left_offset, const uint8_t* right,
                                               int64_t right_offset, int64_t length,
                                               int64_t out_offset) const = 0;

  virtual Status Call(const uint8_t* left, int64_t left_offset, const uint8_t* right,
                      int64_t right_offset, int64_t length, int64_t out_offset,
                      uint8_t* out_buffer) const = 0;

  virtual ~BitmapOperation() = default;
};

struct BitmapAndOp : public BitmapOperation {
  Result<std::shared_ptr<Buffer>> Call(MemoryPool* pool, const uint8_t* left,
                                       int64_t left_offset, const uint8_t* right,
                                       int64_t right_offset, int64_t length,
                                       int64_t out_offset) const override {
    return BitmapAnd(pool, left, left_offset, right, right_offset, length, out_offset);
  }

  Status Call(const uint8_t* left, int64_t left_offset, const uint8_t* right,
              int64_t right_offset, int64_t length, int64_t out_offset,
              uint8_t* out_buffer) const override {
    BitmapAnd(left, left_offset, right, right_offset, length, out_offset, out_buffer);
    return Status::OK();
  }
};

struct BitmapOrOp : public BitmapOperation {
  Result<std::shared_ptr<Buffer>> Call(MemoryPool* pool, const uint8_t* left,
                                       int64_t left_offset, const uint8_t* right,
                                       int64_t right_offset, int64_t length,
                                       int64_t out_offset) const override {
    return BitmapOr(pool, left, left_offset, right, right_offset, length, out_offset);
  }

  Status Call(const uint8_t* left, int64_t left_offset, const uint8_t* right,
              int64_t right_offset, int64_t length, int64_t out_offset,
              uint8_t* out_buffer) const override {
    BitmapOr(left, left_offset, right, right_offset, length, out_offset, out_buffer);
    return Status::OK();
  }
};

struct BitmapXorOp : public BitmapOperation {
  Result<std::shared_ptr<Buffer>> Call(MemoryPool* pool, const uint8_t* left,
                                       int64_t left_offset, const uint8_t* right,
                                       int64_t right_offset, int64_t length,
                                       int64_t out_offset) const override {
    return BitmapXor(pool, left, left_offset, right, right_offset, length, out_offset);
  }

  Status Call(const uint8_t* left, int64_t left_offset, const uint8_t* right,
              int64_t right_offset, int64_t length, int64_t out_offset,
              uint8_t* out_buffer) const override {
    BitmapXor(left, left_offset, right, right_offset, length, out_offset, out_buffer);
    return Status::OK();
  }
};

struct BitmapAndNotOp : public BitmapOperation {
  Result<std::shared_ptr<Buffer>> Call(MemoryPool* pool, const uint8_t* left,
                                       int64_t left_offset, const uint8_t* right,
                                       int64_t right_offset, int64_t length,
                                       int64_t out_offset) const override {
    return BitmapAndNot(pool, left, left_offset, right, right_offset, length, out_offset);
  }

  Status Call(const uint8_t* left, int64_t left_offset, const uint8_t* right,
              int64_t right_offset, int64_t length, int64_t out_offset,
              uint8_t* out_buffer) const override {
    BitmapAndNot(left, left_offset, right, right_offset, length, out_offset, out_buffer);
    return Status::OK();
  }
};

class BitmapOp : public ::testing::Test {
 public:
  void TestAligned(const BitmapOperation& op, const std::vector<int>& left_bits,
                   const std::vector<int>& right_bits,
                   const std::vector<int>& result_bits) {
    std::shared_ptr<Buffer> left, right, out;
    int64_t length;

    for (int64_t left_offset : {0, 1, 3, 5, 7, 8, 13, 21, 38, 75, 120, 65536}) {
      BitmapFromVector(left_bits, left_offset, &left, &length);
      for (int64_t right_offset : {left_offset, left_offset + 8, left_offset + 40}) {
        BitmapFromVector(right_bits, right_offset, &right, &length);
        for (int64_t out_offset : {left_offset, left_offset + 16, left_offset + 24}) {
          ASSERT_OK_AND_ASSIGN(
              out, op.Call(default_memory_pool(), left->mutable_data(), left_offset,
                           right->mutable_data(), right_offset, length, out_offset));
          auto reader = internal::BitmapReader(out->mutable_data(), out_offset, length);
          ASSERT_READER_VALUES(reader, result_bits);

          // Clear out buffer and try non-allocating version
          std::memset(out->mutable_data(), 0, out->size());
          ASSERT_OK(op.Call(left->mutable_data(), left_offset, right->mutable_data(),
                            right_offset, length, out_offset, out->mutable_data()));
          reader = internal::BitmapReader(out->mutable_data(), out_offset, length);
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
    auto offset_values = {0, 1, 3, 5, 7, 8, 13, 21, 38, 75, 120, 65536};

    for (int64_t left_offset : offset_values) {
      BitmapFromVector(left_bits, left_offset, &left, &length);

      for (int64_t right_offset : offset_values) {
        BitmapFromVector(right_bits, right_offset, &right, &length);

        for (int64_t out_offset : offset_values) {
          ASSERT_OK_AND_ASSIGN(
              out, op.Call(default_memory_pool(), left->mutable_data(), left_offset,
                           right->mutable_data(), right_offset, length, out_offset));
          auto reader = internal::BitmapReader(out->mutable_data(), out_offset, length);
          ASSERT_READER_VALUES(reader, result_bits);

          // Clear out buffer and try non-allocating version
          std::memset(out->mutable_data(), 0, out->size());
          ASSERT_OK(op.Call(left->mutable_data(), left_offset, right->mutable_data(),
                            right_offset, length, out_offset, out->mutable_data()));
          reader = internal::BitmapReader(out->mutable_data(), out_offset, length);
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

TEST_F(BitmapOp, Xor) {
  BitmapXorOp op;
  std::vector<int> left = {0, 1, 1, 1, 0, 0, 0, 1, 0, 1, 0, 1, 0, 1};
  std::vector<int> right = {0, 0, 1, 0, 1, 1, 0, 0, 1, 1, 1, 0, 1, 0};
  std::vector<int> result = {0, 1, 0, 1, 1, 1, 0, 1, 1, 0, 1, 1, 1, 1};

  TestAligned(op, left, right, result);
  TestUnaligned(op, left, right, result);
}

TEST_F(BitmapOp, AndNot) {
  BitmapAndNotOp op;
  std::vector<int> left = {0, 1, 1, 1, 0, 0, 0, 1, 0, 1, 0, 1, 0, 1};
  std::vector<int> right = {0, 0, 1, 0, 1, 1, 0, 0, 1, 1, 1, 0, 1, 0};
  std::vector<int> result = {0, 1, 0, 1, 0, 0, 0, 1, 0, 0, 0, 1, 0, 1};

  TestAligned(op, left, right, result);
  TestUnaligned(op, left, right, result);
}

TEST_F(BitmapOp, RandomXor) {
  const int kBitCount = 1000;
  uint8_t buffer[kBitCount * 2] = {0};

  random_bytes(kBitCount * 2, 0, buffer);

  std::vector<int> left(kBitCount);
  std::vector<int> right(kBitCount);
  std::vector<int> result(kBitCount);

  for (int i = 0; i < kBitCount; ++i) {
    left[i] = buffer[i] & 1;
    right[i] = buffer[i + kBitCount] & 1;
    result[i] = left[i] ^ right[i];
  }

  BitmapXorOp op;
  for (int i = 0; i < 3; ++i) {
    TestAligned(op, left, right, result);
    TestUnaligned(op, left, right, result);

    left.resize(left.size() * 5 / 11);
    right.resize(left.size());
    result.resize(left.size());
  }
}

static inline int64_t SlowCountBits(const uint8_t* data, int64_t bit_offset,
                                    int64_t length) {
  int64_t count = 0;
  for (int64_t i = bit_offset; i < bit_offset + length; ++i) {
    if (bit_util::GetBit(data, i)) {
      ++count;
    }
  }
  return count;
}

TEST(BitUtilTests, TestCountSetBits) {
  const int kBufferSize = 1000;
  alignas(8) uint8_t buffer[kBufferSize] = {0};
  const int buffer_bits = kBufferSize * 8;

  random_bytes(kBufferSize, 0, buffer);

  // Check start addresses with 64-bit alignment and without
  for (const uint8_t* data : {buffer, buffer + 1, buffer + 7}) {
    for (const int num_bits : {buffer_bits - 96, buffer_bits - 101, buffer_bits - 127}) {
      std::vector<int64_t> offsets = {
          0, 12, 16, 32, 37, 63, 64, 128, num_bits - 30, num_bits - 64};
      for (const int64_t offset : offsets) {
        int64_t result = CountSetBits(data, offset, num_bits - offset);
        int64_t expected = SlowCountBits(data, offset, num_bits - offset);

        ASSERT_EQ(expected, result);
      }
    }
  }
}

TEST(BitUtilTests, TestSetBitsTo) {
  using bit_util::SetBitsTo;
  for (const auto fill_byte_int : {0x00, 0xff}) {
    const uint8_t fill_byte = static_cast<uint8_t>(fill_byte_int);
    {
      // test set within a byte
      uint8_t bitmap[] = {fill_byte, fill_byte, fill_byte, fill_byte};
      SetBitsTo(bitmap, 2, 2, true);
      SetBitsTo(bitmap, 4, 2, false);
      ASSERT_BYTES_EQ(bitmap, {static_cast<uint8_t>((fill_byte & ~0x3C) | 0xC)});
    }
    {
      // test straddling a single byte boundary
      uint8_t bitmap[] = {fill_byte, fill_byte, fill_byte, fill_byte};
      SetBitsTo(bitmap, 4, 7, true);
      SetBitsTo(bitmap, 11, 7, false);
      ASSERT_BYTES_EQ(bitmap, {static_cast<uint8_t>((fill_byte & 0xF) | 0xF0), 0x7,
                               static_cast<uint8_t>(fill_byte & ~0x3)});
    }
    {
      // test byte aligned end
      uint8_t bitmap[] = {fill_byte, fill_byte, fill_byte, fill_byte};
      SetBitsTo(bitmap, 4, 4, true);
      SetBitsTo(bitmap, 8, 8, false);
      ASSERT_BYTES_EQ(bitmap,
                      {static_cast<uint8_t>((fill_byte & 0xF) | 0xF0), 0x00, fill_byte});
    }
    {
      // test byte aligned end, multiple bytes
      uint8_t bitmap[] = {fill_byte, fill_byte, fill_byte, fill_byte};
      SetBitsTo(bitmap, 0, 24, false);
      uint8_t false_byte = static_cast<uint8_t>(0);
      ASSERT_BYTES_EQ(bitmap, {false_byte, false_byte, false_byte, fill_byte});
    }
  }
}

TEST(BitUtilTests, TestSetBitmap) {
  using bit_util::SetBitsTo;
  for (const auto fill_byte_int : {0xff}) {
    const uint8_t fill_byte = static_cast<uint8_t>(fill_byte_int);
    {
      // test set within a byte
      uint8_t bitmap[] = {fill_byte, fill_byte, fill_byte, fill_byte};
      bit_util::SetBitmap(bitmap, 2, 2);
      bit_util::ClearBitmap(bitmap, 4, 2);
      ASSERT_BYTES_EQ(bitmap, {static_cast<uint8_t>((fill_byte & ~0x3C) | 0xC)});
    }
    {
      // test straddling a single byte boundary
      uint8_t bitmap[] = {fill_byte, fill_byte, fill_byte, fill_byte};
      bit_util::SetBitmap(bitmap, 4, 7);
      bit_util::ClearBitmap(bitmap, 11, 7);
      ASSERT_BYTES_EQ(bitmap, {static_cast<uint8_t>((fill_byte & 0xF) | 0xF0), 0x7,
                               static_cast<uint8_t>(fill_byte & ~0x3)});
    }
    {
      // test byte aligned end
      uint8_t bitmap[] = {fill_byte, fill_byte, fill_byte, fill_byte};
      bit_util::SetBitmap(bitmap, 4, 4);
      bit_util::ClearBitmap(bitmap, 8, 8);
      ASSERT_BYTES_EQ(bitmap,
                      {static_cast<uint8_t>((fill_byte & 0xF) | 0xF0), 0x00, fill_byte});
    }
    {
      // test byte aligned end, multiple bytes
      uint8_t bitmap[] = {fill_byte, fill_byte, fill_byte, fill_byte};
      bit_util::ClearBitmap(bitmap, 0, 24);
      uint8_t false_byte = static_cast<uint8_t>(0);
      ASSERT_BYTES_EQ(bitmap, {false_byte, false_byte, false_byte, fill_byte});
    }
    {
      // ASAN test against out of bound access (ARROW-13803)
      uint8_t bitmap[1] = {fill_byte};
      bit_util::ClearBitmap(bitmap, 0, 8);
      ASSERT_EQ(bitmap[0], 0);
    }
  }
}

TEST(BitUtilTests, TestCopyBitmap) {
  const int kBufferSize = 1000;

  ASSERT_OK_AND_ASSIGN(auto buffer, AllocateBuffer(kBufferSize));
  memset(buffer->mutable_data(), 0, kBufferSize);
  random_bytes(kBufferSize, 0, buffer->mutable_data());

  const uint8_t* src = buffer->data();

  std::vector<int64_t> lengths = {kBufferSize * 8 - 4, kBufferSize * 8};
  std::vector<int64_t> offsets = {0, 12, 16, 32, 37, 63, 64, 128};
  for (int64_t num_bits : lengths) {
    for (int64_t offset : offsets) {
      const int64_t copy_length = num_bits - offset;

      std::shared_ptr<Buffer> copy;
      ASSERT_OK_AND_ASSIGN(copy,
                           CopyBitmap(default_memory_pool(), src, offset, copy_length));

      for (int64_t i = 0; i < copy_length; ++i) {
        ASSERT_EQ(bit_util::GetBit(src, i + offset), bit_util::GetBit(copy->data(), i));
      }
    }
  }
}

TEST(BitUtilTests, TestCopyBitmapPreAllocated) {
  const int kBufferSize = 1000;
  std::vector<int64_t> lengths = {kBufferSize * 8 - 4, kBufferSize * 8};
  std::vector<int64_t> offsets = {0, 12, 16, 32, 37, 63, 64, 128};

  ASSERT_OK_AND_ASSIGN(auto buffer, AllocateBuffer(kBufferSize));
  memset(buffer->mutable_data(), 0, kBufferSize);
  random_bytes(kBufferSize, 0, buffer->mutable_data());
  const uint8_t* src = buffer->data();

  // Add 16 byte padding on both sides
  ASSERT_OK_AND_ASSIGN(auto other_buffer, AllocateBuffer(kBufferSize + 32));
  memset(other_buffer->mutable_data(), 0, kBufferSize + 32);
  random_bytes(kBufferSize + 32, 0, other_buffer->mutable_data());
  const uint8_t* other = other_buffer->data();

  for (int64_t num_bits : lengths) {
    for (int64_t offset : offsets) {
      for (int64_t dest_offset : offsets) {
        const int64_t copy_length = num_bits - offset;

        ASSERT_OK_AND_ASSIGN(auto copy, AllocateBuffer(other_buffer->size()));
        memcpy(copy->mutable_data(), other_buffer->data(), other_buffer->size());
        CopyBitmap(src, offset, copy_length, copy->mutable_data(), dest_offset);

        for (int64_t i = 0; i < dest_offset; ++i) {
          ASSERT_EQ(bit_util::GetBit(other, i), bit_util::GetBit(copy->data(), i));
        }
        for (int64_t i = 0; i < copy_length; ++i) {
          ASSERT_EQ(bit_util::GetBit(src, i + offset),
                    bit_util::GetBit(copy->data(), i + dest_offset));
        }
        for (int64_t i = dest_offset + copy_length; i < (other_buffer->size() * 8); ++i) {
          ASSERT_EQ(bit_util::GetBit(other, i), bit_util::GetBit(copy->data(), i));
        }
      }
    }
  }
}

TEST(BitUtilTests, TestCopyAndInvertBitmapPreAllocated) {
  const int kBufferSize = 1000;
  std::vector<int64_t> lengths = {kBufferSize * 8 - 4, kBufferSize * 8};
  std::vector<int64_t> offsets = {0, 12, 16, 32, 37, 63, 64, 128};

  ASSERT_OK_AND_ASSIGN(auto buffer, AllocateBuffer(kBufferSize));
  memset(buffer->mutable_data(), 0, kBufferSize);
  random_bytes(kBufferSize, 0, buffer->mutable_data());
  const uint8_t* src = buffer->data();

  // Add 16 byte padding on both sides
  ASSERT_OK_AND_ASSIGN(auto other_buffer, AllocateBuffer(kBufferSize + 32));
  memset(other_buffer->mutable_data(), 0, kBufferSize + 32);
  random_bytes(kBufferSize + 32, 0, other_buffer->mutable_data());
  const uint8_t* other = other_buffer->data();

  for (int64_t num_bits : lengths) {
    for (int64_t offset : offsets) {
      for (int64_t dest_offset : offsets) {
        const int64_t copy_length = num_bits - offset;

        ASSERT_OK_AND_ASSIGN(auto copy, AllocateBuffer(other_buffer->size()));
        memcpy(copy->mutable_data(), other_buffer->data(), other_buffer->size());
        InvertBitmap(src, offset, copy_length, copy->mutable_data(), dest_offset);

        for (int64_t i = 0; i < dest_offset; ++i) {
          ASSERT_EQ(bit_util::GetBit(other, i), bit_util::GetBit(copy->data(), i));
        }
        for (int64_t i = 0; i < copy_length; ++i) {
          ASSERT_EQ(bit_util::GetBit(src, i + offset),
                    !bit_util::GetBit(copy->data(), i + dest_offset));
        }
        for (int64_t i = dest_offset + copy_length; i < (other_buffer->size() * 8); ++i) {
          ASSERT_EQ(bit_util::GetBit(other, i), bit_util::GetBit(copy->data(), i));
        }
      }
    }
  }
}

TEST(BitUtilTests, TestCopyAndReverseBitmapPreAllocated) {
  const int kBufferSize = 1000;
  std::vector<int64_t> lengths = {kBufferSize * 8 - 4, kBufferSize * 8};
  std::vector<int64_t> offsets = {0, 12, 16, 32, 37, 63, 64, 128};

  ASSERT_OK_AND_ASSIGN(auto buffer, AllocateBuffer(kBufferSize));
  memset(buffer->mutable_data(), 0, kBufferSize);
  random_bytes(kBufferSize, 0, buffer->mutable_data());
  const uint8_t* src = buffer->data();

  // Add 16 byte padding on both sides
  ASSERT_OK_AND_ASSIGN(auto other_buffer, AllocateBuffer(kBufferSize + 32));
  memset(other_buffer->mutable_data(), 0, kBufferSize + 32);
  random_bytes(kBufferSize + 32, 0, other_buffer->mutable_data());
  const uint8_t* other = other_buffer->data();

  for (int64_t num_bits : lengths) {
    for (int64_t offset : offsets) {
      for (int64_t dest_offset : offsets) {
        const int64_t copy_length = num_bits - offset;

        ASSERT_OK_AND_ASSIGN(auto copy, AllocateBuffer(other_buffer->size()));
        memcpy(copy->mutable_data(), other_buffer->data(), other_buffer->size());
        ReverseBitmap(src, offset, copy_length, copy->mutable_data(), dest_offset);

        for (int64_t i = 0; i < dest_offset; ++i) {
          ASSERT_EQ(bit_util::GetBit(other, i), bit_util::GetBit(copy->data(), i));
        }
        for (int64_t i = 0; i < copy_length; ++i) {
          ASSERT_EQ(bit_util::GetBit(src, offset + i),
                    bit_util::GetBit(copy->data(), dest_offset + (copy_length - 1) - i));
        }
        for (int64_t i = dest_offset + copy_length; i < (other_buffer->size() * 8); ++i) {
          ASSERT_EQ(bit_util::GetBit(other, i), bit_util::GetBit(copy->data(), i));
        }
      }
    }
  }
}

TEST(BitUtilTests, TestBitmapEquals) {
  const int srcBufferSize = 1000;

  ASSERT_OK_AND_ASSIGN(auto src_buffer, AllocateBuffer(srcBufferSize));
  memset(src_buffer->mutable_data(), 0, srcBufferSize);
  random_bytes(srcBufferSize, 0, src_buffer->mutable_data());
  const uint8_t* src = src_buffer->data();

  std::vector<int64_t> lengths = {srcBufferSize * 8 - 4, srcBufferSize * 8};
  std::vector<int64_t> offsets = {0, 12, 16, 32, 37, 63, 64, 128};

  const auto dstBufferSize = srcBufferSize + bit_util::BytesForBits(*std::max_element(
                                                 offsets.cbegin(), offsets.cend()));
  ASSERT_OK_AND_ASSIGN(auto dst_buffer, AllocateBuffer(dstBufferSize))
  uint8_t* dst = dst_buffer->mutable_data();

  for (int64_t num_bits : lengths) {
    for (int64_t offset_src : offsets) {
      for (int64_t offset_dst : offsets) {
        const auto bit_length = num_bits - offset_src;

        internal::CopyBitmap(src, offset_src, bit_length, dst, offset_dst);
        ASSERT_TRUE(internal::BitmapEquals(src, offset_src, dst, offset_dst, bit_length));

        // test negative cases by flip some bit at head and tail
        for (int64_t offset_flip : offsets) {
          const auto offset_flip_head = offset_dst + offset_flip;
          dst[offset_flip_head / 8] ^= 1 << (offset_flip_head % 8);
          ASSERT_FALSE(
              internal::BitmapEquals(src, offset_src, dst, offset_dst, bit_length));
          dst[offset_flip_head / 8] ^= 1 << (offset_flip_head % 8);

          const auto offset_flip_tail = offset_dst + bit_length - offset_flip - 1;
          dst[offset_flip_tail / 8] ^= 1 << (offset_flip_tail % 8);
          ASSERT_FALSE(
              internal::BitmapEquals(src, offset_src, dst, offset_dst, bit_length));
          dst[offset_flip_tail / 8] ^= 1 << (offset_flip_tail % 8);
        }
      }
    }
  }
}

TEST(BitUtil, CeilDiv) {
  EXPECT_EQ(bit_util::CeilDiv(0, 1), 0);
  EXPECT_EQ(bit_util::CeilDiv(1, 1), 1);
  EXPECT_EQ(bit_util::CeilDiv(1, 2), 1);
  EXPECT_EQ(bit_util::CeilDiv(0, 8), 0);
  EXPECT_EQ(bit_util::CeilDiv(1, 8), 1);
  EXPECT_EQ(bit_util::CeilDiv(7, 8), 1);
  EXPECT_EQ(bit_util::CeilDiv(8, 8), 1);
  EXPECT_EQ(bit_util::CeilDiv(9, 8), 2);
  EXPECT_EQ(bit_util::CeilDiv(9, 9), 1);
  EXPECT_EQ(bit_util::CeilDiv(10000000000, 10), 1000000000);
  EXPECT_EQ(bit_util::CeilDiv(10, 10000000000), 1);
  EXPECT_EQ(bit_util::CeilDiv(100000000000, 10000000000), 10);

  // test overflow
  int64_t value = std::numeric_limits<int64_t>::max() - 1;
  int64_t divisor = std::numeric_limits<int64_t>::max();
  EXPECT_EQ(bit_util::CeilDiv(value, divisor), 1);

  value = std::numeric_limits<int64_t>::max();
  EXPECT_EQ(bit_util::CeilDiv(value, divisor), 1);
}

TEST(BitUtil, RoundUp) {
  EXPECT_EQ(bit_util::RoundUp(0, 1), 0);
  EXPECT_EQ(bit_util::RoundUp(1, 1), 1);
  EXPECT_EQ(bit_util::RoundUp(1, 2), 2);
  EXPECT_EQ(bit_util::RoundUp(6, 2), 6);
  EXPECT_EQ(bit_util::RoundUp(0, 3), 0);
  EXPECT_EQ(bit_util::RoundUp(7, 3), 9);
  EXPECT_EQ(bit_util::RoundUp(9, 9), 9);
  EXPECT_EQ(bit_util::RoundUp(10000000001, 10), 10000000010);
  EXPECT_EQ(bit_util::RoundUp(10, 10000000000), 10000000000);
  EXPECT_EQ(bit_util::RoundUp(100000000000, 10000000000), 100000000000);

  // test overflow
  int64_t value = std::numeric_limits<int64_t>::max() - 1;
  int64_t divisor = std::numeric_limits<int64_t>::max();
  EXPECT_EQ(bit_util::RoundUp(value, divisor), divisor);

  value = std::numeric_limits<int64_t>::max();
  EXPECT_EQ(bit_util::RoundUp(value, divisor), divisor);
}

TEST(BitUtil, RoundDown) {
  EXPECT_EQ(bit_util::RoundDown(0, 1), 0);
  EXPECT_EQ(bit_util::RoundDown(1, 1), 1);
  EXPECT_EQ(bit_util::RoundDown(1, 2), 0);
  EXPECT_EQ(bit_util::RoundDown(6, 2), 6);
  EXPECT_EQ(bit_util::RoundDown(5, 7), 0);
  EXPECT_EQ(bit_util::RoundDown(10, 7), 7);
  EXPECT_EQ(bit_util::RoundDown(7, 3), 6);
  EXPECT_EQ(bit_util::RoundDown(9, 9), 9);
  EXPECT_EQ(bit_util::RoundDown(10000000001, 10), 10000000000);
  EXPECT_EQ(bit_util::RoundDown(10, 10000000000), 0);
  EXPECT_EQ(bit_util::RoundDown(100000000000, 10000000000), 100000000000);

  for (int i = 0; i < 100; i++) {
    for (int j = 1; j < 100; j++) {
      EXPECT_EQ(bit_util::RoundDown(i, j), i - (i % j));
    }
  }
}

TEST(BitUtil, CoveringBytes) {
  EXPECT_EQ(bit_util::CoveringBytes(0, 8), 1);
  EXPECT_EQ(bit_util::CoveringBytes(0, 9), 2);
  EXPECT_EQ(bit_util::CoveringBytes(1, 7), 1);
  EXPECT_EQ(bit_util::CoveringBytes(1, 8), 2);
  EXPECT_EQ(bit_util::CoveringBytes(2, 19), 3);
  EXPECT_EQ(bit_util::CoveringBytes(7, 18), 4);
}

TEST(BitUtil, TrailingBits) {
  EXPECT_EQ(bit_util::TrailingBits(0xFF, 0), 0);
  EXPECT_EQ(bit_util::TrailingBits(0xFF, 1), 1);
  EXPECT_EQ(bit_util::TrailingBits(0xFF, 64), 0xFF);
  EXPECT_EQ(bit_util::TrailingBits(0xFF, 100), 0xFF);
  EXPECT_EQ(bit_util::TrailingBits(0, 1), 0);
  EXPECT_EQ(bit_util::TrailingBits(0, 64), 0);
  EXPECT_EQ(bit_util::TrailingBits(1LL << 63, 0), 0);
  EXPECT_EQ(bit_util::TrailingBits(1LL << 63, 63), 0);
  EXPECT_EQ(bit_util::TrailingBits(1LL << 63, 64), 1LL << 63);
}

TEST(BitUtil, ByteSwap) {
  EXPECT_EQ(bit_util::ByteSwap(static_cast<uint32_t>(0)), 0);
  EXPECT_EQ(bit_util::ByteSwap(static_cast<uint32_t>(0x11223344)), 0x44332211);

  EXPECT_EQ(bit_util::ByteSwap(static_cast<int32_t>(0)), 0);
  EXPECT_EQ(bit_util::ByteSwap(static_cast<int32_t>(0x11223344)), 0x44332211);

  EXPECT_EQ(bit_util::ByteSwap(static_cast<uint64_t>(0)), 0);
  EXPECT_EQ(bit_util::ByteSwap(static_cast<uint64_t>(0x1122334455667788)),
            0x8877665544332211);

  EXPECT_EQ(bit_util::ByteSwap(static_cast<int64_t>(0)), 0);
  EXPECT_EQ(bit_util::ByteSwap(static_cast<int64_t>(0x1122334455667788)),
            0x8877665544332211);

  EXPECT_EQ(bit_util::ByteSwap(static_cast<int16_t>(0)), 0);
  EXPECT_EQ(bit_util::ByteSwap(static_cast<int16_t>(0x1122)), 0x2211);

  EXPECT_EQ(bit_util::ByteSwap(static_cast<uint16_t>(0)), 0);
  EXPECT_EQ(bit_util::ByteSwap(static_cast<uint16_t>(0x1122)), 0x2211);

  EXPECT_EQ(bit_util::ByteSwap(static_cast<int8_t>(0)), 0);
  EXPECT_EQ(bit_util::ByteSwap(static_cast<int8_t>(0x11)), 0x11);

  EXPECT_EQ(bit_util::ByteSwap(static_cast<uint8_t>(0)), 0);
  EXPECT_EQ(bit_util::ByteSwap(static_cast<uint8_t>(0x11)), 0x11);

  EXPECT_EQ(bit_util::ByteSwap(static_cast<float>(0)), 0);
  uint32_t srci32 = 0xaabbccdd, expectedi32 = 0xddccbbaa;
  float srcf32 = SafeCopy<float>(srci32);
  float expectedf32 = SafeCopy<float>(expectedi32);
  EXPECT_EQ(bit_util::ByteSwap(srcf32), expectedf32);
  uint64_t srci64 = 0xaabb11223344ccdd, expectedi64 = 0xddcc44332211bbaa;
  double srcd64 = SafeCopy<double>(srci64);
  double expectedd64 = SafeCopy<double>(expectedi64);
  EXPECT_EQ(bit_util::ByteSwap(srcd64), expectedd64);
}

TEST(BitUtil, Log2) {
  EXPECT_EQ(bit_util::Log2(1), 0);
  EXPECT_EQ(bit_util::Log2(2), 1);
  EXPECT_EQ(bit_util::Log2(3), 2);
  EXPECT_EQ(bit_util::Log2(4), 2);
  EXPECT_EQ(bit_util::Log2(5), 3);
  EXPECT_EQ(bit_util::Log2(8), 3);
  EXPECT_EQ(bit_util::Log2(9), 4);
  EXPECT_EQ(bit_util::Log2(INT_MAX), 31);
  EXPECT_EQ(bit_util::Log2(UINT_MAX), 32);
  EXPECT_EQ(bit_util::Log2(ULLONG_MAX), 64);
}

TEST(BitUtil, NumRequiredBits) {
  EXPECT_EQ(bit_util::NumRequiredBits(0), 0);
  EXPECT_EQ(bit_util::NumRequiredBits(1), 1);
  EXPECT_EQ(bit_util::NumRequiredBits(2), 2);
  EXPECT_EQ(bit_util::NumRequiredBits(3), 2);
  EXPECT_EQ(bit_util::NumRequiredBits(4), 3);
  EXPECT_EQ(bit_util::NumRequiredBits(5), 3);
  EXPECT_EQ(bit_util::NumRequiredBits(7), 3);
  EXPECT_EQ(bit_util::NumRequiredBits(8), 4);
  EXPECT_EQ(bit_util::NumRequiredBits(9), 4);
  EXPECT_EQ(bit_util::NumRequiredBits(UINT_MAX - 1), 32);
  EXPECT_EQ(bit_util::NumRequiredBits(UINT_MAX), 32);
  EXPECT_EQ(bit_util::NumRequiredBits(static_cast<uint64_t>(UINT_MAX) + 1), 33);
  EXPECT_EQ(bit_util::NumRequiredBits(ULLONG_MAX / 2), 63);
  EXPECT_EQ(bit_util::NumRequiredBits(ULLONG_MAX / 2 + 1), 64);
  EXPECT_EQ(bit_util::NumRequiredBits(ULLONG_MAX - 1), 64);
  EXPECT_EQ(bit_util::NumRequiredBits(ULLONG_MAX), 64);
}

#define U32(x) static_cast<uint32_t>(x)
#define U64(x) static_cast<uint64_t>(x)
#define S64(x) static_cast<int64_t>(x)

TEST(BitUtil, CountLeadingZeros) {
  EXPECT_EQ(bit_util::CountLeadingZeros(U32(0)), 32);
  EXPECT_EQ(bit_util::CountLeadingZeros(U32(1)), 31);
  EXPECT_EQ(bit_util::CountLeadingZeros(U32(2)), 30);
  EXPECT_EQ(bit_util::CountLeadingZeros(U32(3)), 30);
  EXPECT_EQ(bit_util::CountLeadingZeros(U32(4)), 29);
  EXPECT_EQ(bit_util::CountLeadingZeros(U32(7)), 29);
  EXPECT_EQ(bit_util::CountLeadingZeros(U32(8)), 28);
  EXPECT_EQ(bit_util::CountLeadingZeros(U32(UINT_MAX / 2)), 1);
  EXPECT_EQ(bit_util::CountLeadingZeros(U32(UINT_MAX / 2 + 1)), 0);
  EXPECT_EQ(bit_util::CountLeadingZeros(U32(UINT_MAX)), 0);

  EXPECT_EQ(bit_util::CountLeadingZeros(U64(0)), 64);
  EXPECT_EQ(bit_util::CountLeadingZeros(U64(1)), 63);
  EXPECT_EQ(bit_util::CountLeadingZeros(U64(2)), 62);
  EXPECT_EQ(bit_util::CountLeadingZeros(U64(3)), 62);
  EXPECT_EQ(bit_util::CountLeadingZeros(U64(4)), 61);
  EXPECT_EQ(bit_util::CountLeadingZeros(U64(7)), 61);
  EXPECT_EQ(bit_util::CountLeadingZeros(U64(8)), 60);
  EXPECT_EQ(bit_util::CountLeadingZeros(U64(UINT_MAX)), 32);
  EXPECT_EQ(bit_util::CountLeadingZeros(U64(UINT_MAX) + 1), 31);
  EXPECT_EQ(bit_util::CountLeadingZeros(U64(ULLONG_MAX / 2)), 1);
  EXPECT_EQ(bit_util::CountLeadingZeros(U64(ULLONG_MAX / 2 + 1)), 0);
  EXPECT_EQ(bit_util::CountLeadingZeros(U64(ULLONG_MAX)), 0);
}

TEST(BitUtil, CountTrailingZeros) {
  EXPECT_EQ(bit_util::CountTrailingZeros(U32(0)), 32);
  EXPECT_EQ(bit_util::CountTrailingZeros(U32(1) << 31), 31);
  EXPECT_EQ(bit_util::CountTrailingZeros(U32(1) << 30), 30);
  EXPECT_EQ(bit_util::CountTrailingZeros(U32(1) << 29), 29);
  EXPECT_EQ(bit_util::CountTrailingZeros(U32(1) << 28), 28);
  EXPECT_EQ(bit_util::CountTrailingZeros(U32(8)), 3);
  EXPECT_EQ(bit_util::CountTrailingZeros(U32(4)), 2);
  EXPECT_EQ(bit_util::CountTrailingZeros(U32(2)), 1);
  EXPECT_EQ(bit_util::CountTrailingZeros(U32(1)), 0);
  EXPECT_EQ(bit_util::CountTrailingZeros(U32(ULONG_MAX)), 0);

  EXPECT_EQ(bit_util::CountTrailingZeros(U64(0)), 64);
  EXPECT_EQ(bit_util::CountTrailingZeros(U64(1) << 63), 63);
  EXPECT_EQ(bit_util::CountTrailingZeros(U64(1) << 62), 62);
  EXPECT_EQ(bit_util::CountTrailingZeros(U64(1) << 61), 61);
  EXPECT_EQ(bit_util::CountTrailingZeros(U64(1) << 60), 60);
  EXPECT_EQ(bit_util::CountTrailingZeros(U64(8)), 3);
  EXPECT_EQ(bit_util::CountTrailingZeros(U64(4)), 2);
  EXPECT_EQ(bit_util::CountTrailingZeros(U64(2)), 1);
  EXPECT_EQ(bit_util::CountTrailingZeros(U64(1)), 0);
  EXPECT_EQ(bit_util::CountTrailingZeros(U64(ULLONG_MAX)), 0);
}

TEST(BitUtil, RoundUpToPowerOf2) {
  EXPECT_EQ(bit_util::RoundUpToPowerOf2(S64(7), 8), 8);
  EXPECT_EQ(bit_util::RoundUpToPowerOf2(S64(8), 8), 8);
  EXPECT_EQ(bit_util::RoundUpToPowerOf2(S64(9), 8), 16);

  EXPECT_EQ(bit_util::RoundUpToPowerOf2(U64(7), 8), 8);
  EXPECT_EQ(bit_util::RoundUpToPowerOf2(U64(8), 8), 8);
  EXPECT_EQ(bit_util::RoundUpToPowerOf2(U64(9), 8), 16);
}

#undef U32
#undef U64
#undef S64

static void TestZigZag(int32_t v, std::array<uint8_t, 5> buffer_expect) {
  uint8_t buffer[bit_util::BitReader::kMaxVlqByteLength] = {};
  bit_util::BitWriter writer(buffer, sizeof(buffer));
  bit_util::BitReader reader(buffer, sizeof(buffer));
  writer.PutZigZagVlqInt(v);
  EXPECT_THAT(buffer, testing::ElementsAreArray(buffer_expect));
  int32_t result;
  EXPECT_TRUE(reader.GetZigZagVlqInt(&result));
  EXPECT_EQ(v, result);
}

TEST(BitStreamUtil, ZigZag) {
  TestZigZag(0, {0, 0, 0, 0, 0});
  TestZigZag(1, {2, 0, 0, 0, 0});
  TestZigZag(1234, {164, 19, 0, 0, 0});
  TestZigZag(-1, {1, 0, 0, 0, 0});
  TestZigZag(-1234, {163, 19, 0, 0, 0});
  TestZigZag(std::numeric_limits<int32_t>::max(), {254, 255, 255, 255, 15});
  TestZigZag(-std::numeric_limits<int32_t>::max(), {253, 255, 255, 255, 15});
  TestZigZag(std::numeric_limits<int32_t>::min(), {255, 255, 255, 255, 15});
}

static void TestZigZag64(int64_t v, std::array<uint8_t, 10> buffer_expect) {
  uint8_t buffer[bit_util::BitReader::kMaxVlqByteLengthForInt64] = {};
  bit_util::BitWriter writer(buffer, sizeof(buffer));
  bit_util::BitReader reader(buffer, sizeof(buffer));
  writer.PutZigZagVlqInt(v);
  EXPECT_THAT(buffer, testing::ElementsAreArray(buffer_expect));
  int64_t result;
  EXPECT_TRUE(reader.GetZigZagVlqInt(&result));
  EXPECT_EQ(v, result);
}

TEST(BitStreamUtil, ZigZag64) {
  TestZigZag64(0, {0, 0, 0, 0, 0, 0, 0, 0, 0, 0});
  TestZigZag64(1, {2, 0, 0, 0, 0, 0, 0, 0, 0, 0});
  TestZigZag64(1234, {164, 19, 0, 0, 0, 0, 0, 0, 0, 0});
  TestZigZag64(-1, {1, 0, 0, 0, 0, 0, 0, 0, 0, 0});
  TestZigZag64(-1234, {163, 19, 0, 0, 0, 0, 0, 0, 0, 0});
  TestZigZag64(std::numeric_limits<int64_t>::max(),
               {254, 255, 255, 255, 255, 255, 255, 255, 255, 1});
  TestZigZag64(-std::numeric_limits<int64_t>::max(),
               {253, 255, 255, 255, 255, 255, 255, 255, 255, 1});
  TestZigZag64(std::numeric_limits<int64_t>::min(),
               {255, 255, 255, 255, 255, 255, 255, 255, 255, 1});
}

TEST(BitUtil, RoundTripLittleEndianTest) {
  uint64_t value = 0xFF;

#if ARROW_LITTLE_ENDIAN
  uint64_t expected = value;
#else
  uint64_t expected = std::numeric_limits<uint64_t>::max() << 56;
#endif

  uint64_t little_endian_result = bit_util::ToLittleEndian(value);
  ASSERT_EQ(expected, little_endian_result);

  uint64_t from_little_endian = bit_util::FromLittleEndian(little_endian_result);
  ASSERT_EQ(value, from_little_endian);
}

TEST(BitUtil, RoundTripBigEndianTest) {
  uint64_t value = 0xFF;

#if ARROW_LITTLE_ENDIAN
  uint64_t expected = std::numeric_limits<uint64_t>::max() << 56;
#else
  uint64_t expected = value;
#endif

  uint64_t big_endian_result = bit_util::ToBigEndian(value);
  ASSERT_EQ(expected, big_endian_result);

  uint64_t from_big_endian = bit_util::FromBigEndian(big_endian_result);
  ASSERT_EQ(value, from_big_endian);
}

TEST(BitUtil, BitsetStack) {
  BitsetStack stack;
  ASSERT_EQ(stack.TopSize(), 0);
  stack.Push(3, false);
  ASSERT_EQ(stack.TopSize(), 3);
  stack[1] = true;
  stack.Push(5, true);
  ASSERT_EQ(stack.TopSize(), 5);
  stack[1] = false;
  for (int i = 0; i != 5; ++i) {
    ASSERT_EQ(stack[i], i != 1);
  }
  stack.Pop();
  ASSERT_EQ(stack.TopSize(), 3);
  for (int i = 0; i != 3; ++i) {
    ASSERT_EQ(stack[i], i == 1);
  }
  stack.Pop();
  ASSERT_EQ(stack.TopSize(), 0);
}

TEST(SpliceWord, SpliceWord) {
  static_assert(
      bit_util::PrecedingWordBitmask<uint8_t>(0) == bit_util::kPrecedingBitmask[0], "");
  static_assert(
      bit_util::PrecedingWordBitmask<uint8_t>(5) == bit_util::kPrecedingBitmask[5], "");
  static_assert(bit_util::PrecedingWordBitmask<uint8_t>(8) == UINT8_MAX, "");

  static_assert(bit_util::PrecedingWordBitmask<uint64_t>(0) == uint64_t(0), "");
  static_assert(bit_util::PrecedingWordBitmask<uint64_t>(33) == 8589934591, "");
  static_assert(bit_util::PrecedingWordBitmask<uint64_t>(64) == UINT64_MAX, "");
  static_assert(bit_util::PrecedingWordBitmask<uint64_t>(65) == UINT64_MAX, "");

  ASSERT_EQ(bit_util::SpliceWord<uint8_t>(0, 0x12, 0xef), 0xef);
  ASSERT_EQ(bit_util::SpliceWord<uint8_t>(8, 0x12, 0xef), 0x12);
  ASSERT_EQ(bit_util::SpliceWord<uint8_t>(3, 0x12, 0xef), 0xea);

  ASSERT_EQ(bit_util::SpliceWord<uint32_t>(0, 0x12345678, 0xfedcba98), 0xfedcba98);
  ASSERT_EQ(bit_util::SpliceWord<uint32_t>(32, 0x12345678, 0xfedcba98), 0x12345678);
  ASSERT_EQ(bit_util::SpliceWord<uint32_t>(24, 0x12345678, 0xfedcba98), 0xfe345678);

  ASSERT_EQ(bit_util::SpliceWord<uint64_t>(0, 0x0123456789abcdef, 0xfedcba9876543210),
            0xfedcba9876543210);
  ASSERT_EQ(bit_util::SpliceWord<uint64_t>(64, 0x0123456789abcdef, 0xfedcba9876543210),
            0x0123456789abcdef);
  ASSERT_EQ(bit_util::SpliceWord<uint64_t>(48, 0x0123456789abcdef, 0xfedcba9876543210),
            0xfedc456789abcdef);
}

// test the basic assumption of word level Bitmap::Visit
TEST(Bitmap, ShiftingWordsOptimization) {
  // single word
  {
    uint64_t word;
    auto bytes = reinterpret_cast<uint8_t*>(&word);
    constexpr size_t kBitWidth = sizeof(word) * 8;

    for (int seed = 0; seed < 64; ++seed) {
      random_bytes(sizeof(word), seed, bytes);
      uint64_t native_word = bit_util::FromLittleEndian(word);

      // bits are accessible through simple bit shifting of the word
      for (size_t i = 0; i < kBitWidth; ++i) {
        ASSERT_EQ(bit_util::GetBit(bytes, i), bool((native_word >> i) & 1));
      }

      // bit offset can therefore be accommodated by shifting the word
      for (size_t offset = 0; offset < (kBitWidth * 3) / 4; ++offset) {
        uint64_t shifted_word = arrow::bit_util::ToLittleEndian(native_word >> offset);
        auto shifted_bytes = reinterpret_cast<uint8_t*>(&shifted_word);
        ASSERT_TRUE(
            internal::BitmapEquals(bytes, offset, shifted_bytes, 0, kBitWidth - offset));
      }
    }
  }

  // two words
  {
    uint64_t words[2];
    auto bytes = reinterpret_cast<uint8_t*>(words);
    constexpr size_t kBitWidth = sizeof(words[0]) * 8;

    for (int seed = 0; seed < 64; ++seed) {
      random_bytes(sizeof(words), seed, bytes);
      uint64_t native_words0 = bit_util::FromLittleEndian(words[0]);
      uint64_t native_words1 = bit_util::FromLittleEndian(words[1]);

      // bits are accessible through simple bit shifting of a word
      for (size_t i = 0; i < kBitWidth; ++i) {
        ASSERT_EQ(bit_util::GetBit(bytes, i), bool((native_words0 >> i) & 1));
      }
      for (size_t i = 0; i < kBitWidth; ++i) {
        ASSERT_EQ(bit_util::GetBit(bytes, i + kBitWidth), bool((native_words1 >> i) & 1));
      }

      // bit offset can therefore be accommodated by shifting the word
      for (size_t offset = 1; offset < (kBitWidth * 3) / 4; offset += 3) {
        uint64_t shifted_words[2];
        shifted_words[0] = arrow::bit_util::ToLittleEndian(
            native_words0 >> offset | (native_words1 << (kBitWidth - offset)));
        shifted_words[1] = arrow::bit_util::ToLittleEndian(native_words1 >> offset);
        auto shifted_bytes = reinterpret_cast<uint8_t*>(shifted_words);

        // from offset to unshifted word boundary
        ASSERT_TRUE(
            internal::BitmapEquals(bytes, offset, shifted_bytes, 0, kBitWidth - offset));

        // from unshifted word boundary to shifted word boundary
        ASSERT_TRUE(internal::BitmapEquals(bytes, kBitWidth, shifted_bytes,
                                           kBitWidth - offset, offset));

        // from shifted word boundary to end
        ASSERT_TRUE(internal::BitmapEquals(bytes, kBitWidth + offset, shifted_bytes,
                                           kBitWidth, kBitWidth - offset));
      }
    }
  }
}

namespace internal {

static Bitmap Copy(const Bitmap& bitmap, std::shared_ptr<Buffer> storage) {
  int64_t i = 0;
  Bitmap bitmaps[] = {bitmap};
  auto min_offset = Bitmap::VisitWords(bitmaps, [&](std::array<uint64_t, 1> uint64s) {
    reinterpret_cast<uint64_t*>(storage->mutable_data())[i++] = uint64s[0];
  });
  return Bitmap(std::move(storage), min_offset, bitmap.length());
}

// reconstruct a bitmap from a word-wise visit
TEST(Bitmap, VisitWords) {
  constexpr int64_t nbytes = 1 << 10;
  std::shared_ptr<Buffer> buffer, actual_buffer;
  for (std::shared_ptr<Buffer>* b : {&buffer, &actual_buffer}) {
    ASSERT_OK_AND_ASSIGN(*b, AllocateBuffer(nbytes));
    memset((*b)->mutable_data(), 0, nbytes);
  }
  random_bytes(nbytes, 0, buffer->mutable_data());

  constexpr int64_t kBitWidth = 8 * sizeof(uint64_t);

  for (int64_t offset : {0, 1, 2, 5, 17}) {
    for (int64_t num_bits :
         {int64_t(13), int64_t(9), kBitWidth - 1, kBitWidth, kBitWidth + 1,
          nbytes * 8 - offset, nbytes * 6, nbytes * 4}) {
      Bitmap actual = Copy({buffer, offset, num_bits}, actual_buffer);
      ASSERT_EQ(actual, Bitmap(buffer->data(), offset, num_bits))
          << "offset:" << offset << "  bits:" << num_bits << std::endl
          << Bitmap(actual_buffer, 0, num_bits).Diff({buffer, offset, num_bits});
    }
  }
}

#ifndef ARROW_VALGRIND

// This test reads uninitialized memory
TEST(Bitmap, VisitPartialWords) {
  uint64_t words[2] = {0};
  constexpr auto nbytes = sizeof(words);
  constexpr auto nbits = nbytes * 8;

  auto buffer = Buffer::Wrap(words, 2);
  Bitmap bitmap(buffer, 0, nbits);
  ASSERT_OK_AND_ASSIGN(std::shared_ptr<Buffer> storage, AllocateBuffer(nbytes));

  // words partially outside the buffer are not accessible, but they are loaded bitwise
  auto first_byte_was_missing = Bitmap(SliceBuffer(buffer, 1), 0, nbits - 8);
  ASSERT_EQ(Copy(first_byte_was_missing, storage), bitmap.Slice(8));

  auto last_byte_was_missing = Bitmap(SliceBuffer(buffer, 0, nbytes - 1), 0, nbits - 8);
  ASSERT_EQ(Copy(last_byte_was_missing, storage), bitmap.Slice(0, nbits - 8));
}

#endif  // ARROW_VALGRIND

TEST(Bitmap, ToString) {
  uint8_t bitmap[8] = {0xAC, 0xCA, 0, 0, 0, 0, 0, 0};
  EXPECT_EQ(Bitmap(bitmap, /*bit_offset*/ 0, /*length=*/34).ToString(),
            "00110101 01010011 00000000 00000000 00");
  EXPECT_EQ(Bitmap(bitmap, /*bit_offset*/ 0, /*length=*/16).ToString(),
            "00110101 01010011");
  EXPECT_EQ(Bitmap(bitmap, /*bit_offset*/ 0, /*length=*/11).ToString(), "00110101 010");
  EXPECT_EQ(Bitmap(bitmap, /*bit_offset*/ 3, /*length=*/8).ToString(), "10101010");
}

// compute bitwise AND of bitmaps using word-wise visit
TEST(Bitmap, VisitWordsAnd) {
  constexpr int64_t nbytes = 1 << 10;
  std::shared_ptr<Buffer> buffer, actual_buffer, expected_buffer;
  for (std::shared_ptr<Buffer>* b : {&buffer, &actual_buffer, &expected_buffer}) {
    ASSERT_OK_AND_ASSIGN(*b, AllocateBuffer(nbytes));
    memset((*b)->mutable_data(), 0, nbytes);
  }
  random_bytes(nbytes, 0, buffer->mutable_data());

  constexpr int64_t kBitWidth = 8 * sizeof(uint64_t);

  for (int64_t left_offset :
       {0, 1, 2, 5, 17, int(kBitWidth - 1), int(kBitWidth + 1), int(kBitWidth + 17)}) {
    for (int64_t right_offset = 0; right_offset < left_offset; ++right_offset) {
      for (int64_t num_bits :
           {int64_t(13), int64_t(9), kBitWidth - 1, kBitWidth, kBitWidth + 1,
            2 * kBitWidth - 1, 2 * kBitWidth, 2 * kBitWidth + 1, nbytes * 8 - left_offset,
            3 * kBitWidth - 1, 3 * kBitWidth, 3 * kBitWidth + 1, nbytes * 6,
            nbytes * 4}) {
        Bitmap bitmaps[] = {{buffer, left_offset, num_bits},
                            {buffer, right_offset, num_bits}};

        int64_t i = 0;
        auto min_offset =
            Bitmap::VisitWords(bitmaps, [&](std::array<uint64_t, 2> uint64s) {
              reinterpret_cast<uint64_t*>(actual_buffer->mutable_data())[i++] =
                  uint64s[0] & uint64s[1];
            });

        BitmapAnd(bitmaps[0].buffer()->data(), bitmaps[0].offset(),
                  bitmaps[1].buffer()->data(), bitmaps[1].offset(), bitmaps[0].length(),
                  0, expected_buffer->mutable_data());

        ASSERT_TRUE(BitmapEquals(actual_buffer->data(), min_offset,
                                 expected_buffer->data(), 0, num_bits))
            << "left_offset:" << left_offset << "  bits:" << num_bits
            << "  right_offset:" << right_offset << std::endl
            << Bitmap(actual_buffer, 0, num_bits).Diff({expected_buffer, 0, num_bits});
      }
    }
  }
}

void DoBitmapVisitAndWrite(int64_t part, bool with_offset) {
  int64_t bits = part * 4;

  random::RandomArrayGenerator rand(/*seed=*/0);
  auto arrow_data = rand.ArrayOf(boolean(), bits, 0);

  std::shared_ptr<Buffer>& arrow_buffer = arrow_data->data()->buffers[1];

  Bitmap bm0(arrow_buffer, 0, part);
  Bitmap bm1(arrow_buffer, part * 1, part);
  Bitmap bm2(arrow_buffer, part * 2, part);

  std::array<Bitmap, 2> out_bms;
  if (with_offset) {
    ASSERT_OK_AND_ASSIGN(auto out, AllocateBitmap(part * 4));
    out_bms[0] = Bitmap(out, part, part);
    out_bms[1] = Bitmap(out, part * 2, part);
  } else {
    ASSERT_OK_AND_ASSIGN(auto out0, AllocateBitmap(part));
    ASSERT_OK_AND_ASSIGN(auto out1, AllocateBitmap(part));
    out_bms[0] = Bitmap(out0, 0, part);
    out_bms[1] = Bitmap(out1, 0, part);
  }

  // out0 = bm0 & bm1, out1= bm0 | bm2
  std::array<Bitmap, 3> in_bms{bm0, bm1, bm2};
  Bitmap::VisitWordsAndWrite(
      in_bms, &out_bms,
      [](const std::array<uint64_t, 3>& in, std::array<uint64_t, 2>* out) {
        out->at(0) = in[0] & in[1];
        out->at(1) = in[0] | in[2];
      });

  auto pool = MemoryPool::CreateDefault();
  ASSERT_OK_AND_ASSIGN(auto exp_0,
                       BitmapAnd(pool.get(), bm0.buffer()->data(), bm0.offset(),
                                 bm1.buffer()->data(), bm1.offset(), part, 0));
  ASSERT_OK_AND_ASSIGN(auto exp_1,
                       BitmapOr(pool.get(), bm0.buffer()->data(), bm0.offset(),
                                bm2.buffer()->data(), bm2.offset(), part, 0));

  ASSERT_TRUE(BitmapEquals(exp_0->data(), 0, out_bms[0].buffer()->data(),
                           out_bms[0].offset(), part))
      << "exp: " << Bitmap(exp_0->data(), 0, part).ToString() << std::endl
      << "got: " << out_bms[0].ToString();

  ASSERT_TRUE(BitmapEquals(exp_1->data(), 0, out_bms[1].buffer()->data(),
                           out_bms[1].offset(), part))
      << "exp: " << Bitmap(exp_1->data(), 0, part).ToString() << std::endl
      << "got: " << out_bms[1].ToString();
}

class TestBitmapVisitAndWrite : public ::testing::TestWithParam<int32_t> {};

INSTANTIATE_TEST_SUITE_P(VisitWriteGeneral, TestBitmapVisitAndWrite,
                         testing::Values(199, 256, 1000));

INSTANTIATE_TEST_SUITE_P(VisitWriteEdgeCases, TestBitmapVisitAndWrite,
                         testing::Values(5, 13, 21, 29, 37, 41, 51, 59, 64, 97));

INSTANTIATE_TEST_SUITE_P(VisitWriteEdgeCases2, TestBitmapVisitAndWrite,
                         testing::Values(8, 16, 24, 32, 40, 48, 56, 64));

TEST_P(TestBitmapVisitAndWrite, NoOffset) { DoBitmapVisitAndWrite(GetParam(), false); }

TEST_P(TestBitmapVisitAndWrite, WithOffset) { DoBitmapVisitAndWrite(GetParam(), true); }

}  // namespace internal
}  // namespace arrow
