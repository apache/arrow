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

#include "parquet/level_conversion.h"

#include "parquet/level_comparison.h"
#include "parquet/test_util.h"

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <string>
#include <vector>

#include "arrow/testing/gtest_compat.h"
#include "arrow/util/bit_util.h"
#include "arrow/util/bitmap.h"
#include "arrow/util/ubsan.h"

namespace parquet::internal {

using ::arrow::internal::Bitmap;
using ::testing::ElementsAreArray;

std::string BitmapToString(const uint8_t* bitmap, int64_t bit_count) {
  return ::arrow::internal::Bitmap(bitmap, /*offset*/ 0, /*length=*/bit_count).ToString();
}

std::string BitmapToString(const std::vector<uint8_t>& bitmap, int64_t bit_count) {
  return BitmapToString(bitmap.data(), bit_count);
}

TEST(TestColumnReader, DefLevelsToBitmap) {
  // Bugs in this function were exposed in ARROW-3930
  std::vector<int16_t> def_levels = {3, 3, 3, 2, 3, 3, 3, 3, 3};

  std::vector<uint8_t> valid_bits(2, 0);

  LevelInfo level_info;
  level_info.def_level = 3;
  level_info.rep_level = 1;

  ValidityBitmapInputOutput io;
  io.values_read_upper_bound = def_levels.size();
  io.values_read = -1;
  io.valid_bits = valid_bits.data();

  internal::DefLevelsToBitmap(def_levels.data(), 9, level_info, &io);
  ASSERT_EQ(9, io.values_read);
  ASSERT_EQ(1, io.null_count);

  // Call again with 0 definition levels, make sure that valid_bits is unmodified
  const uint8_t current_byte = valid_bits[1];
  io.null_count = 0;
  internal::DefLevelsToBitmap(def_levels.data(), 0, level_info, &io);

  ASSERT_EQ(0, io.values_read);
  ASSERT_EQ(0, io.null_count);
  ASSERT_EQ(current_byte, valid_bits[1]);
}

TEST(TestColumnReader, DefLevelsToBitmapPowerOfTwo) {
  // PARQUET-1623: Invalid memory access when decoding a valid bits vector that has a
  // length equal to a power of two and also using a non-zero valid_bits_offset.  This
  // should not fail when run with ASAN or valgrind.
  std::vector<int16_t> def_levels = {3, 3, 3, 2, 3, 3, 3, 3};
  std::vector<uint8_t> valid_bits(1, 0);

  LevelInfo level_info;
  level_info.rep_level = 1;
  level_info.def_level = 3;

  ValidityBitmapInputOutput io;
  io.values_read_upper_bound = def_levels.size();
  io.values_read = -1;
  io.valid_bits = valid_bits.data();

  // Read the latter half of the validity bitmap
  internal::DefLevelsToBitmap(def_levels.data() + 4, 4, level_info, &io);
  ASSERT_EQ(4, io.values_read);
  ASSERT_EQ(0, io.null_count);
}

#if defined(ARROW_LITTLE_ENDIAN)
TEST(GreaterThanBitmap, GeneratesExpectedBitmasks) {
  std::vector<int16_t> levels = {0, 1, 2, 3, 4, 5, 6, 7, 0, 1, 2, 3, 4, 5, 6, 7,
                                 0, 1, 2, 3, 4, 5, 6, 7, 0, 1, 2, 3, 4, 5, 6, 7,
                                 0, 1, 2, 3, 4, 5, 6, 7, 0, 1, 2, 3, 4, 5, 6, 7,
                                 0, 1, 2, 3, 4, 5, 6, 7, 0, 1, 2, 3, 4, 5, 6, 7};
  EXPECT_EQ(GreaterThanBitmap(levels.data(), /*num_levels=*/0, /*rhs*/ 0), 0);
  EXPECT_EQ(GreaterThanBitmap(levels.data(), /*num_levels=*/64, /*rhs*/ 8), 0);
  EXPECT_EQ(GreaterThanBitmap(levels.data(), /*num_levels=*/64, /*rhs*/ -1),
            0xFFFFFFFFFFFFFFFF);
  // Should be zero padded.
  EXPECT_EQ(GreaterThanBitmap(levels.data(), /*num_levels=*/47, /*rhs*/ -1),
            0x7FFFFFFFFFFF);
  EXPECT_EQ(GreaterThanBitmap(levels.data(), /*num_levels=*/64, /*rhs*/ 6),
            0x8080808080808080);
}
#endif

TEST(DefLevelsToBitmap, WithRepetitionLevelFiltersOutEmptyListValues) {
  std::vector<uint8_t> validity_bitmap(/*count*/ 8, 0);

  ValidityBitmapInputOutput io;
  io.values_read_upper_bound = 64;
  io.values_read = 1;
  io.null_count = 5;
  io.valid_bits = validity_bitmap.data();
  io.valid_bits_offset = 1;

  LevelInfo level_info;
  level_info.repeated_ancestor_def_level = 1;
  level_info.def_level = 2;
  level_info.rep_level = 1;
  // All zeros should be ignored, ones should be unset in the bitmap and 2 should be set.
  std::vector<int16_t> def_levels = {0, 0, 0, 2, 2, 1, 0, 2};
  DefLevelsToBitmap(def_levels.data(), def_levels.size(), level_info, &io);

  EXPECT_EQ(BitmapToString(validity_bitmap, /*bit_count=*/8), "01101000");
  for (size_t x = 1; x < validity_bitmap.size(); x++) {
    EXPECT_EQ(validity_bitmap[x], 0) << "index: " << x;
  }
  EXPECT_EQ(io.null_count, /*5 + 1 =*/6);
  EXPECT_EQ(io.values_read, 4);  // value should get overwritten.
}

struct MultiLevelTestData {
 public:
  std::vector<int16_t> def_levels;
  std::vector<int16_t> rep_levels;
};

MultiLevelTestData TriplyNestedList() {
  // Triply nested list values borrow from write_path
  // [null, [[1 , null, 3], []], []],
  // [[[]], [[], [1, 2]], null, [[3]]],
  // null,
  // []
  return MultiLevelTestData{
      /*def_levels=*/std::vector<int16_t>{2, 7, 6, 7, 5, 3,  // first row
                                          5, 5, 7, 7, 2, 7,  // second row
                                          0,                 // third row
                                          1},
      /*rep_levels=*/std::vector<int16_t>{0, 1, 3, 3, 2, 1,  // first row
                                          0, 1, 2, 3, 1, 1,  // second row
                                          0, 0}};
}

template <typename ConverterType>
class NestedListTest : public testing::Test {
 public:
  void InitForLength(int length) {
    this->validity_bits_.clear();
    this->validity_bits_.insert(this->validity_bits_.end(), length, 0);
    validity_io_.valid_bits = validity_bits_.data();
    validity_io_.values_read_upper_bound = length;
    offsets_.clear();
    offsets_.insert(offsets_.end(), length + 1, 0);
  }

  typename ConverterType::OffsetsType* Run(const MultiLevelTestData& test_data,
                                           LevelInfo level_info) {
    return this->converter_.ComputeListInfo(test_data, level_info, &validity_io_,
                                            offsets_.data());
  }

  ConverterType converter_;
  ValidityBitmapInputOutput validity_io_;
  std::vector<uint8_t> validity_bits_;
  std::vector<typename ConverterType::OffsetsType> offsets_;
};

template <typename IndexType>
struct RepDefLevelConverter {
  using OffsetsType = IndexType;
  OffsetsType* ComputeListInfo(const MultiLevelTestData& test_data, LevelInfo level_info,
                               ValidityBitmapInputOutput* output, IndexType* offsets) {
    DefRepLevelsToList(test_data.def_levels.data(), test_data.rep_levels.data(),
                       test_data.def_levels.size(), level_info, output, offsets);
    return offsets + output->values_read;
  }
};

using ConverterTypes =
    ::testing::Types<RepDefLevelConverter</*list_length_type=*/int32_t>,
                     RepDefLevelConverter</*list_length_type=*/int64_t>>;
TYPED_TEST_SUITE(NestedListTest, ConverterTypes);

TYPED_TEST(NestedListTest, OuterMostTest) {
  // [null, [[1 , null, 3], []], []],
  // [[[]], [[], [1, 2]], null, [[3]]],
  // null,
  // []
  // -> 4 outer most lists (len(3), len(4), null, len(0))
  LevelInfo level_info;
  level_info.rep_level = 1;
  level_info.def_level = 2;

  this->InitForLength(4);
  typename TypeParam::OffsetsType* next_position =
      this->Run(TriplyNestedList(), level_info);

  EXPECT_EQ(next_position, this->offsets_.data() + 4);
  EXPECT_THAT(this->offsets_, testing::ElementsAre(0, 3, 7, 7, 7));

  EXPECT_EQ(this->validity_io_.values_read, 4);
  EXPECT_EQ(this->validity_io_.null_count, 1);
  EXPECT_EQ(BitmapToString(this->validity_io_.valid_bits, /*length=*/4), "1101");
}

TYPED_TEST(NestedListTest, MiddleListTest) {
  // [null, [[1 , null, 3], []], []],
  // [[[]], [[], [1, 2]], null, [[3]]],
  // null,
  // []
  // -> middle lists (null, len(2), len(0),
  //                  len(1), len(2), null, len(1),
  //                  N/A,
  //                  N/A
  LevelInfo level_info;
  level_info.rep_level = 2;
  level_info.def_level = 4;
  level_info.repeated_ancestor_def_level = 2;

  this->InitForLength(7);
  typename TypeParam::OffsetsType* next_position =
      this->Run(TriplyNestedList(), level_info);

  EXPECT_EQ(next_position, this->offsets_.data() + 7);
  EXPECT_THAT(this->offsets_, testing::ElementsAre(0, 0, 2, 2, 3, 5, 5, 6));

  EXPECT_EQ(this->validity_io_.values_read, 7);
  EXPECT_EQ(this->validity_io_.null_count, 2);
  EXPECT_EQ(BitmapToString(this->validity_io_.valid_bits, /*length=*/7), "0111101");
}

TYPED_TEST(NestedListTest, InnerMostListTest) {
  // [null, [[1, null, 3], []], []],
  // [[[]], [[], [1, 2]], null, [[3]]],
  // null,
  // []
  // -> 6 inner lists (N/A, [len(3), len(0)], N/A
  //                        len(0), [len(0), len(2)], N/A, len(1),
  //                        N/A,
  //                        N/A
  LevelInfo level_info;
  level_info.rep_level = 3;
  level_info.def_level = 6;
  level_info.repeated_ancestor_def_level = 4;

  this->InitForLength(6);
  typename TypeParam::OffsetsType* next_position =
      this->Run(TriplyNestedList(), level_info);

  EXPECT_EQ(next_position, this->offsets_.data() + 6);
  EXPECT_THAT(this->offsets_, testing::ElementsAre(0, 3, 3, 3, 3, 5, 6));

  EXPECT_EQ(this->validity_io_.values_read, 6);
  EXPECT_EQ(this->validity_io_.null_count, 0);
  EXPECT_EQ(BitmapToString(this->validity_io_.valid_bits, /*length=*/6), "111111");
}

TYPED_TEST(NestedListTest, SimpleLongList) {
  LevelInfo level_info;
  level_info.rep_level = 1;
  level_info.def_level = 2;
  level_info.repeated_ancestor_def_level = 0;

  MultiLevelTestData test_data;
  // No empty lists.
  test_data.def_levels = std::vector<int16_t>(65 * 9, 2);
  for (int x = 0; x < 65; x++) {
    test_data.rep_levels.push_back(0);
    test_data.rep_levels.insert(test_data.rep_levels.end(), 8,
                                /*rep_level=*/1);
  }

  std::vector<typename TypeParam::OffsetsType> expected_offsets(66, 0);
  for (size_t x = 1; x < expected_offsets.size(); x++) {
    expected_offsets[x] = static_cast<typename TypeParam::OffsetsType>(x) * 9;
  }
  this->InitForLength(65);
  typename TypeParam::OffsetsType* next_position = this->Run(test_data, level_info);

  EXPECT_EQ(next_position, this->offsets_.data() + 65);
  EXPECT_THAT(this->offsets_, testing::ElementsAreArray(expected_offsets));

  EXPECT_EQ(this->validity_io_.values_read, 65);
  EXPECT_EQ(this->validity_io_.null_count, 0);
  EXPECT_EQ(BitmapToString(this->validity_io_.valid_bits, /*length=*/65),
            "11111111 "
            "11111111 "
            "11111111 "
            "11111111 "
            "11111111 "
            "11111111 "
            "11111111 "
            "11111111 "
            "1");
}

TYPED_TEST(NestedListTest, TestOverflow) {
  LevelInfo level_info;
  level_info.rep_level = 1;
  level_info.def_level = 2;
  level_info.repeated_ancestor_def_level = 0;

  MultiLevelTestData test_data;
  test_data.def_levels = std::vector<int16_t>{2};
  test_data.rep_levels = std::vector<int16_t>{0};

  this->InitForLength(2);
  // Offsets is populated as the cumulative sum of all elements,
  // so populating the offsets[0] with max-value impacts the
  // other values populated.
  this->offsets_[0] = std::numeric_limits<typename TypeParam::OffsetsType>::max();
  this->offsets_[1] = std::numeric_limits<typename TypeParam::OffsetsType>::max();
  ASSERT_THROW(this->Run(test_data, level_info), ParquetException);

  ASSERT_THROW(this->Run(test_data, level_info), ParquetException);

  // Same thing should happen if the list already existed.
  test_data.rep_levels = std::vector<int16_t>{1};
  ASSERT_THROW(this->Run(test_data, level_info), ParquetException);

  // Should be OK because it shouldn't increment.
  test_data.def_levels = std::vector<int16_t>{0};
  test_data.rep_levels = std::vector<int16_t>{0};
  this->Run(test_data, level_info);
}

TEST(TestOnlyExtractBitsSoftware, BasicTest) {
  auto check = [](uint64_t bitmap, uint64_t selection, uint64_t expected) -> void {
    EXPECT_EQ(TestOnlyExtractBitsSoftware(bitmap, selection), expected);
  };
  check(0xFF, 0, 0);
  check(0xFF, ~uint64_t{0}, 0xFF);
  check(0xFF00FF, 0xAAAA, 0x000F);
  check(0xFF0AFF, 0xAFAA, 0x00AF);
  check(0xFFAAFF, 0xAFAA, 0x03AF);
  check(0xFECBDA9876543210ULL, 0xF00FF00FF00FF00FULL, 0xFBD87430ULL);
}

}  // namespace parquet::internal
