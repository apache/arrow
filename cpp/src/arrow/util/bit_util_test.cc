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
#include <gtest/gtest.h>

#include "arrow/buffer.h"
#include "arrow/buffer_builder.h"
#include "arrow/result.h"
#include "arrow/status.h"
#include "arrow/testing/gtest_compat.h"
#include "arrow/testing/gtest_util.h"
#include "arrow/testing/util.h"
#include "arrow/type_fwd.h"
#include "arrow/util/bit_stream_utils_internal.h"
#include "arrow/util/bitmap_reader.h"
#include "arrow/util/bitmap_visit.h"
#include "arrow/util/bitset_stack_internal.h"
#include "arrow/util/endian.h"
#include "arrow/util/test_common.h"
#include "arrow/util/ubsan.h"

namespace arrow {

using internal::BitsetStack;
using internal::CopyBitmap;
using internal::CountSetBits;
using internal::InvertBitmap;
using internal::ReverseBitmap;
using util::SafeCopy;

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
      AssertBytesEqual(bitmap, {static_cast<uint8_t>((fill_byte & ~0x3C) | 0xC)});
    }
    {
      // test straddling a single byte boundary
      uint8_t bitmap[] = {fill_byte, fill_byte, fill_byte, fill_byte};
      SetBitsTo(bitmap, 4, 7, true);
      SetBitsTo(bitmap, 11, 7, false);
      AssertBytesEqual(bitmap, {static_cast<uint8_t>((fill_byte & 0xF) | 0xF0), 0x7,
                                static_cast<uint8_t>(fill_byte & ~0x3)});
    }
    {
      // test byte aligned end
      uint8_t bitmap[] = {fill_byte, fill_byte, fill_byte, fill_byte};
      SetBitsTo(bitmap, 4, 4, true);
      SetBitsTo(bitmap, 8, 8, false);
      AssertBytesEqual(bitmap,
                       {static_cast<uint8_t>((fill_byte & 0xF) | 0xF0), 0x00, fill_byte});
    }
    {
      // test byte aligned end, multiple bytes
      uint8_t bitmap[] = {fill_byte, fill_byte, fill_byte, fill_byte};
      SetBitsTo(bitmap, 0, 24, false);
      uint8_t false_byte = static_cast<uint8_t>(0);
      AssertBytesEqual(bitmap, {false_byte, false_byte, false_byte, fill_byte});
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
      AssertBytesEqual(bitmap, {static_cast<uint8_t>((fill_byte & ~0x3C) | 0xC)});
    }
    {
      // test straddling a single byte boundary
      uint8_t bitmap[] = {fill_byte, fill_byte, fill_byte, fill_byte};
      bit_util::SetBitmap(bitmap, 4, 7);
      bit_util::ClearBitmap(bitmap, 11, 7);
      AssertBytesEqual(bitmap, {static_cast<uint8_t>((fill_byte & 0xF) | 0xF0), 0x7,
                                static_cast<uint8_t>(fill_byte & ~0x3)});
    }
    {
      // test byte aligned end
      uint8_t bitmap[] = {fill_byte, fill_byte, fill_byte, fill_byte};
      bit_util::SetBitmap(bitmap, 4, 4);
      bit_util::ClearBitmap(bitmap, 8, 8);
      AssertBytesEqual(bitmap,
                       {static_cast<uint8_t>((fill_byte & 0xF) | 0xF0), 0x00, fill_byte});
    }
    {
      // test byte aligned end, multiple bytes
      uint8_t bitmap[] = {fill_byte, fill_byte, fill_byte, fill_byte};
      bit_util::ClearBitmap(bitmap, 0, 24);
      uint8_t false_byte = static_cast<uint8_t>(0);
      AssertBytesEqual(bitmap, {false_byte, false_byte, false_byte, fill_byte});
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
      std::vector<int64_t> dest_offsets = {0, offset - 1, offset + 1};
      for (int64_t dest_offset : dest_offsets) {
        if (dest_offset < 0) {
          continue;
        }
        const int64_t copy_length = num_bits - offset;

        std::shared_ptr<Buffer> copy;
        ASSERT_OK_AND_ASSIGN(copy, CopyBitmap(default_memory_pool(), src, offset,
                                              copy_length, dest_offset));

        for (int64_t i = 0; i < copy_length; ++i) {
          ASSERT_EQ(bit_util::GetBit(src, i + offset),
                    bit_util::GetBit(copy->data(), i + dest_offset));
        }
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

/// Test the maximum number of bytes needed to write a LEB128 of a give size.
TEST(LEB128, MaxLEB128ByteLenFor) {
  EXPECT_EQ(bit_util::kMaxLEB128ByteLenFor<int8_t>, 2);
  EXPECT_EQ(bit_util::kMaxLEB128ByteLenFor<uint8_t>, 2);
  EXPECT_EQ(bit_util::kMaxLEB128ByteLenFor<int16_t>, 3);
  EXPECT_EQ(bit_util::kMaxLEB128ByteLenFor<uint16_t>, 3);
  EXPECT_EQ(bit_util::kMaxLEB128ByteLenFor<int32_t>, 5);
  EXPECT_EQ(bit_util::kMaxLEB128ByteLenFor<uint32_t>, 5);
  EXPECT_EQ(bit_util::kMaxLEB128ByteLenFor<int64_t>, 10);
  EXPECT_EQ(bit_util::kMaxLEB128ByteLenFor<uint64_t>, 10);
}

/// Utility function to test LEB128 encoding with known input value and expected byte
/// array
template <typename Int>
void TestLEB128Encode(Int input_value, const std::vector<uint8_t>& expected_data,
                      std::size_t buffer_size) {
  std::vector<uint8_t> buffer(buffer_size);
  auto bytes_written = bit_util::WriteLEB128(input_value, buffer.data(),
                                             static_cast<int32_t>(buffer.size()));

  EXPECT_EQ(bytes_written, expected_data.size());
  // Encoded data
  for (std::size_t i = 0; i < expected_data.size(); ++i) {
    EXPECT_EQ(buffer.at(i), expected_data.at(i));
  }

  // When the value is successfully encoded, the remaining of the buffer is untouched
  if (bytes_written > 0) {
    for (std::size_t i = bytes_written; i < buffer.size(); ++i) {
      EXPECT_EQ(buffer.at(i), 0);
    }
  }
}

/// Test encoding to known LEB128 byte sequences with edge cases parameters.
/// \see LEB128.KnownSuccessfulValues for other known values tested.
TEST(LEB128, WriteEdgeCases) {
  // Single byte value 0
  TestLEB128Encode(0U, {0x00}, 1);
  // Single byte value 127
  TestLEB128Encode(127U, {0x7F}, 1);
  // Three byte value 16384, encoded in larger buffer
  TestLEB128Encode(16384U, {0x80, 0x80, 0x01}, 10);
  // Two byte boundary values
  TestLEB128Encode(128U, {0x80, 0x01}, 2);
  TestLEB128Encode(129U, {0x81, 0x01}, 2);
  TestLEB128Encode(16383U, {0xFF, 0x7F}, 2);
  // Error case: Buffer too small for value 128 (needs 2 bytes but only 1 provided)
  TestLEB128Encode(128U, {}, 1);
  // Error case: Buffer too small for uint32_t max (needs 5 bytes but only 4 provided)
  TestLEB128Encode(4294967295U, {}, 4);
  // Error case: Zero buffer size
  TestLEB128Encode(52U, {}, 0);
  // Error case: Negative value
  TestLEB128Encode(-3, {}, 1);
}

/// Utility function to test LEB128 decoding with known byte array and expected result
template <typename Int>
void TestLEB128Decode(const std::vector<uint8_t>& data, Int expected_value,
                      int32_t expected_bytes_read) {
  Int result = 0;
  auto bytes_read = bit_util::ParseLeadingLEB128(
      data.data(), static_cast<int32_t>(data.size()), &result);
  EXPECT_EQ(bytes_read, expected_bytes_read);
  if (expected_bytes_read > 0) {
    EXPECT_EQ(result, expected_value);
  }
}

/// Test decoding from known LEB128 byte sequences with edge case parameters.
/// \see LEB128.KnownSuccessfulValues for other known values tested.
TEST(LEB128, ReadEdgeCases) {
  // Single byte value 0
  TestLEB128Decode({0x00}, 0U, 1);
  // Single byte value 127
  TestLEB128Decode({0x7F}, 127U, 1);
  // Three byte value 16384, with remaining data
  TestLEB128Decode({0x80, 0x80, 0x01, 0x80, 0x00}, 16384U, 3);
  // Four byte value 268435455
  TestLEB128Decode({0xFF, 0xFF, 0xFF, 0x7F}, 268435455U, 4);
  // Error case: Truncated sequence (continuation bit set but no more data)
  TestLEB128Decode({0x80}, 0U, 0);
  // Error case: Input has exactly the maximum number of bytes for a int32_t (5),
  // but the decoded value overflows nonetheless (7 * 5 = 35 bits of data).
  TestLEB128Decode({0xFF, 0xFF, 0xFF, 0xFF, 0x7F}, int32_t{}, 0);
  // Error case: Oversized sequence for uint32_t (too many bytes)
  TestLEB128Decode({0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0x01}, 0U, 0);
}

struct KnownLEB128Encoding {
  uint64_t value;
  std::vector<uint8_t> bytes;
};

static const std::vector<KnownLEB128Encoding> KnownLEB128EncodingValues{
    {0, {0x00}},
    {1, {0x01}},
    {63, {0x3F}},
    {64, {0x40}},
    {127U, {0x7F}},
    {128, {0x80, 0x01}},
    {300, {0xAC, 0x02}},
    {16384, {0x80, 0x80, 0x01}},
    {268435455, {0xFF, 0xFF, 0xFF, 0x7F}},
    {static_cast<uint64_t>(std::numeric_limits<uint8_t>::max()), {0xFF, 0x01}},
    {static_cast<uint64_t>(std::numeric_limits<int8_t>::max()), {0x7F}},
    {static_cast<uint64_t>(std::numeric_limits<uint16_t>::max()), {0xFF, 0xFF, 0x03}},
    {static_cast<uint64_t>(std::numeric_limits<int16_t>::max()), {0xFF, 0xFF, 0x01}},
    {static_cast<uint64_t>(std::numeric_limits<uint32_t>::max()),
     {0xFF, 0xFF, 0xFF, 0xFF, 0x0F}},
    {static_cast<uint64_t>(std::numeric_limits<int32_t>::max()),
     {0xFF, 0xFF, 0xFF, 0xFF, 0x7}},
    {static_cast<uint64_t>(std::numeric_limits<uint64_t>::max()),
     {0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0x01}},
    {static_cast<uint64_t>(std::numeric_limits<int64_t>::max()),
     {0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0x7F}},
};

/// Test encoding and decoding to known LEB128 byte sequences with all possible
/// integer sizes and signess.
TEST(LEB128, KnownSuccessfulValues) {
  for (const auto& data : KnownLEB128EncodingValues) {
    SCOPED_TRACE("Testing value " + std::to_string(data.value));

    // 8 bits
    if (data.value <= static_cast<uint64_t>(std::numeric_limits<int8_t>::max())) {
      const auto val = static_cast<int8_t>(data.value);
      TestLEB128Encode(val, data.bytes, data.bytes.size());
      TestLEB128Decode(data.bytes, val, static_cast<int32_t>(data.bytes.size()));
    }
    if (data.value <= static_cast<uint64_t>(std::numeric_limits<uint8_t>::max())) {
      const auto val = static_cast<uint8_t>(data.value);
      TestLEB128Encode(val, data.bytes, data.bytes.size());
      TestLEB128Decode(data.bytes, val, static_cast<int32_t>(data.bytes.size()));
    }

    // 16 bits
    if (data.value <= static_cast<uint64_t>(std::numeric_limits<int16_t>::max())) {
      const auto val = static_cast<int16_t>(data.value);
      TestLEB128Encode(val, data.bytes, data.bytes.size());
      TestLEB128Decode(data.bytes, val, static_cast<int32_t>(data.bytes.size()));
    }
    if (data.value <= static_cast<uint64_t>(std::numeric_limits<uint16_t>::max())) {
      const auto val = static_cast<uint16_t>(data.value);
      TestLEB128Encode(val, data.bytes, data.bytes.size());
      TestLEB128Decode(data.bytes, val, static_cast<int32_t>(data.bytes.size()));
    }

    // 32 bits
    if (data.value <= static_cast<uint64_t>(std::numeric_limits<int32_t>::max())) {
      const auto val = static_cast<int32_t>(data.value);
      TestLEB128Encode(val, data.bytes, data.bytes.size());
      TestLEB128Decode(data.bytes, val, static_cast<int32_t>(data.bytes.size()));
    }
    if (data.value <= static_cast<uint64_t>(std::numeric_limits<uint32_t>::max())) {
      const auto val = static_cast<uint32_t>(data.value);
      TestLEB128Encode(val, data.bytes, data.bytes.size());
      TestLEB128Decode(data.bytes, val, static_cast<int32_t>(data.bytes.size()));
    }

    // 64 bits
    if (data.value <= static_cast<uint64_t>(std::numeric_limits<int64_t>::max())) {
      const auto val = static_cast<int64_t>(data.value);
      TestLEB128Encode(val, data.bytes, data.bytes.size());
      TestLEB128Decode(data.bytes, val, static_cast<int32_t>(data.bytes.size()));
    }
    if (data.value <= static_cast<uint64_t>(std::numeric_limits<uint64_t>::max())) {
      const auto val = static_cast<uint64_t>(data.value);
      TestLEB128Encode(val, data.bytes, data.bytes.size());
      TestLEB128Decode(data.bytes, val, static_cast<int32_t>(data.bytes.size()));
    }
  }
}

static void TestZigZag(int32_t v, std::array<uint8_t, 5> buffer_expect) {
  uint8_t buffer[bit_util::kMaxLEB128ByteLenFor<int32_t>] = {};
  bit_util::BitWriter writer(buffer, sizeof(buffer));
  writer.PutZigZagVlqInt(v);
  // WARNING: The reader reads and caches the input when created, so it must be created
  // after the data is written in the buffer.
  bit_util::BitReader reader(buffer, sizeof(buffer));
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
  uint8_t buffer[bit_util::kMaxLEB128ByteLenFor<int64_t>] = {};
  bit_util::BitWriter writer(buffer, sizeof(buffer));
  writer.PutZigZagVlqInt(v);
  // WARNING: The reader reads and caches the input when created, so it must be created
  // after the data is written in the buffer.
  bit_util::BitReader reader(buffer, sizeof(buffer));
  EXPECT_THAT(buffer, testing::ElementsAreArray(buffer_expect));
  int64_t result = 0;
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

}  // namespace arrow
