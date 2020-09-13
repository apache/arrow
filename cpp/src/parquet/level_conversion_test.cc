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

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <string>
#include <vector>

#include "arrow/util/bit_util.h"
#include "arrow/util/bitmap.h"
#include "arrow/util/ubsan.h"

namespace parquet {
namespace internal {

using ::arrow::internal::Bitmap;
using ::testing::ElementsAreArray;

std::string BitmapToString(const uint8_t* bitmap, int64_t bit_count) {
  return arrow::internal::Bitmap(bitmap, /*offset*/ 0, /*length=*/bit_count).ToString();
}

std::string BitmapToString(const std::vector<uint8_t>& bitmap, int64_t bit_count) {
  return BitmapToString(bitmap.data(), bit_count);
}

TEST(TestColumnReader, DefinitionLevelsToBitmap) {
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

  internal::DefinitionLevelsToBitmap(def_levels.data(), 9, level_info, &io);
  ASSERT_EQ(9, io.values_read);
  ASSERT_EQ(1, io.null_count);

  // Call again with 0 definition levels, make sure that valid_bits is unmodified
  const uint8_t current_byte = valid_bits[1];
  io.null_count = 0;
  internal::DefinitionLevelsToBitmap(def_levels.data(), 0, level_info, &io);

  ASSERT_EQ(0, io.values_read);
  ASSERT_EQ(0, io.null_count);
  ASSERT_EQ(current_byte, valid_bits[1]);
}

TEST(TestColumnReader, DefinitionLevelsToBitmapPowerOfTwo) {
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
  internal::DefinitionLevelsToBitmap(def_levels.data() + 4, 4, level_info, &io);
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

TEST(DefinitionLevelsToBitmap, WithRepetitionLevelFiltersOutEmptyListValues) {
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
  // All zeros should be ignored, ones should be unset in the bitmp and 2 should be set.
  std::vector<int16_t> def_levels = {0, 0, 0, 2, 2, 1, 0, 2};
  DefinitionLevelsToBitmap(def_levels.data(), def_levels.size(), level_info, &io);

  EXPECT_EQ(BitmapToString(validity_bitmap, /*bit_count=*/8), "01101000");
  for (size_t x = 1; x < validity_bitmap.size(); x++) {
    EXPECT_EQ(validity_bitmap[x], 0) << "index: " << x;
  }
  EXPECT_EQ(io.null_count, /*5 + 1 =*/6);
  EXPECT_EQ(io.values_read, 4);  // value should get overwritten.
}

class MultiLevelTestData {
 public:
  // Triply nested list values borrow from write_path
  // [null, [[1 , null, 3], []], []],
  // [[[]], [[], [1, 2]], null, [[3]]],
  // null,
  // []
  std::vector<int16_t> def_levels_{2, 7, 6, 7, 5, 3,  // first row
                                   5, 5, 7, 7, 2, 7,  // second row
                                   0,                 // third row
                                   1};
  std::vector<int16_t> rep_levels_{0, 1, 3, 3, 2, 1,  // first row
                                   0, 1, 2, 3, 1, 1,  // second row
                                   0, 0};
};

template <typename ConverterType>
class NestedListTest : public testing::Test {
 public:
  MultiLevelTestData test_data_;
  ConverterType converter_;
};

template <typename ListType>
struct RepDefLevelConverter {
  using ListLengthType = ListType;
  ListLengthType* ComputeListInfo(const MultiLevelTestData& test_data,
                                  LevelInfo level_info, ValidityBitmapInputOutput* output,
                                  ListType* lengths) {
    ConvertDefRepLevelsToList(test_data.def_levels_.data(), test_data.rep_levels_.data(),
                              test_data.def_levels_.size(), level_info, output, lengths);
    return lengths + output->values_read;
  }
};

using ConverterTypes =
    ::testing::Types<RepDefLevelConverter</*list_length_type=*/int32_t>,
                     RepDefLevelConverter</*list_length_type=*/int64_t>>;
TYPED_TEST_CASE(NestedListTest, ConverterTypes);

TYPED_TEST(NestedListTest, OuterMostTest) {
  // [null, [[1 , null, 3], []], []],
  // [[[]], [[], [1, 2]], null, [[3]]],
  // null,
  // []
  // -> 4 outer most lists (len(3), len(4), null, len(0))
  LevelInfo level_info;
  level_info.rep_level = 1;
  level_info.def_level = 2;

  std::vector<typename TypeParam::ListLengthType> lengths(5, 0);
  uint64_t validity_output;
  ValidityBitmapInputOutput validity_io;
  validity_io.values_read_upper_bound = 4;
  validity_io.valid_bits = reinterpret_cast<uint8_t*>(&validity_output);
  typename TypeParam::ListLengthType* next_position = this->converter_.ComputeListInfo(
      this->test_data_, level_info, &validity_io, lengths.data());

  EXPECT_THAT(next_position, lengths.data() + 4);
  EXPECT_THAT(lengths, testing::ElementsAre(0, 3, 7, 7, 7));

  EXPECT_EQ(validity_io.values_read, 4);
  EXPECT_EQ(validity_io.null_count, 1);
  EXPECT_EQ(BitmapToString(validity_io.valid_bits, /*length=*/4), "1101");
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

  std::vector<typename TypeParam::ListLengthType> lengths(8, 0);
  uint64_t validity_output;
  ValidityBitmapInputOutput validity_io;
  validity_io.values_read_upper_bound = 7;
  validity_io.valid_bits = reinterpret_cast<uint8_t*>(&validity_output);
  typename TypeParam::ListLengthType* next_position = this->converter_.ComputeListInfo(
      this->test_data_, level_info, &validity_io, lengths.data());

  EXPECT_THAT(next_position, lengths.data() + 7);
  EXPECT_THAT(lengths, testing::ElementsAre(0, 0, 2, 2, 3, 5, 5, 6));

  EXPECT_EQ(validity_io.values_read, 7);
  EXPECT_EQ(validity_io.null_count, 2);
  EXPECT_EQ(BitmapToString(validity_io.valid_bits, /*length=*/7), "0111101");
}

TYPED_TEST(NestedListTest, InnerMostListTest) {
  // [null, [[1, null, 3], []], []],
  // [[[]], [[], [1, 2]], null, [[3]]],
  // null,
  // []
  // -> 4 inner lists (N/A, [len(3), len(0)], N/A
  //                        len(0), [len(0), len(2)], N/A, len(1),
  //                        N/A,
  //                        N/A
  LevelInfo level_info;
  level_info.rep_level = 3;
  level_info.def_level = 6;
  level_info.repeated_ancestor_def_level = 4;

  std::vector<typename TypeParam::ListLengthType> lengths(7, 0);
  uint64_t validity_output;
  ValidityBitmapInputOutput validity_io;
  validity_io.values_read_upper_bound = 6;
  validity_io.valid_bits = reinterpret_cast<uint8_t*>(&validity_output);
  typename TypeParam::ListLengthType* next_position = this->converter_.ComputeListInfo(
      this->test_data_, level_info, &validity_io, lengths.data());

  EXPECT_THAT(next_position, lengths.data() + 6);
  EXPECT_THAT(lengths, testing::ElementsAre(0, 3, 3, 3, 3, 5, 6));

  EXPECT_EQ(validity_io.values_read, 6);
  EXPECT_EQ(validity_io.null_count, 0);
  EXPECT_EQ(BitmapToString(validity_io.valid_bits, /*length=*/6), "111111");
}

TYPED_TEST(NestedListTest, SimpleLongList) {
  LevelInfo level_info;
  level_info.rep_level = 1;
  level_info.def_level = 2;
  level_info.repeated_ancestor_def_level = 0;

  // No empty lists.
  this->test_data_.def_levels_ = std::vector<int16_t>(65 * 9, 2);
  this->test_data_.rep_levels_.clear();
  for (int x = 0; x < 65; x++) {
    this->test_data_.rep_levels_.push_back(0);
    this->test_data_.rep_levels_.insert(this->test_data_.rep_levels_.end(), 8,
                                        /*rep_level=*/1);
  }

  std::vector<typename TypeParam::ListLengthType> lengths(66, 0);
  std::vector<typename TypeParam::ListLengthType> expected_lengths(66, 0);
  for (size_t x = 1; x < expected_lengths.size(); x++) {
    expected_lengths[x] = x * 9;
  }
  std::vector<uint8_t> validity_output(9, 0);
  ValidityBitmapInputOutput validity_io;
  validity_io.values_read_upper_bound = 65;
  validity_io.valid_bits = validity_output.data();
  typename TypeParam::ListLengthType* next_position = this->converter_.ComputeListInfo(
      this->test_data_, level_info, &validity_io, lengths.data());

  EXPECT_THAT(next_position, lengths.data() + 65);
  EXPECT_THAT(lengths, testing::ElementsAreArray(expected_lengths));

  EXPECT_EQ(validity_io.values_read, 65);
  EXPECT_EQ(validity_io.null_count, 0);
  EXPECT_EQ(BitmapToString(validity_io.valid_bits, /*length=*/65),
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

TEST(RunBasedExtract, BasicTest) {
  EXPECT_EQ(RunBasedExtract(arrow::BitUtil::ToLittleEndian(0xFF), 0), 0);
  EXPECT_EQ(RunBasedExtract(arrow::BitUtil::ToLittleEndian(0xFF), ~uint64_t{0}),
            arrow::BitUtil::ToLittleEndian(0xFF));

  EXPECT_EQ(RunBasedExtract(arrow::BitUtil::ToLittleEndian(0xFF00FF),
                            arrow::BitUtil::ToLittleEndian(0xAAAA)),
            arrow::BitUtil::ToLittleEndian(0x000F));
  EXPECT_EQ(RunBasedExtract(arrow::BitUtil::ToLittleEndian(0xFF0AFF),
                            arrow::BitUtil::ToLittleEndian(0xAFAA)),
            arrow::BitUtil::ToLittleEndian(0x00AF));
  EXPECT_EQ(RunBasedExtract(arrow::BitUtil::ToLittleEndian(0xFFAAFF),
                            arrow::BitUtil::ToLittleEndian(0xAFAA)),
            arrow::BitUtil::ToLittleEndian(0x03AF));
}

}  // namespace internal
}  // namespace parquet
