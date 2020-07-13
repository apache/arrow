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

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <string>
#include <vector>

#include "arrow/util/bit_util.h"
#include "arrow/util/bitmap.h"

namespace parquet {
namespace internal {

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

  const int max_def_level = 3;
  const int max_rep_level = 1;

  int64_t values_read = -1;
  int64_t null_count = 0;
  internal::DefinitionLevelsToBitmap(def_levels.data(), 9, max_def_level, max_rep_level,
                                     &values_read, &null_count, valid_bits.data(),
                                     0 /* valid_bits_offset */);
  ASSERT_EQ(9, values_read);
  ASSERT_EQ(1, null_count);

  // Call again with 0 definition levels, make sure that valid_bits is unmodified
  const uint8_t current_byte = valid_bits[1];
  null_count = 0;
  internal::DefinitionLevelsToBitmap(def_levels.data(), 0, max_def_level, max_rep_level,
                                     &values_read, &null_count, valid_bits.data(),
                                     9 /* valid_bits_offset */);
  ASSERT_EQ(0, values_read);
  ASSERT_EQ(0, null_count);
  ASSERT_EQ(current_byte, valid_bits[1]);
}

TEST(TestColumnReader, DefinitionLevelsToBitmapPowerOfTwo) {
  // PARQUET-1623: Invalid memory access when decoding a valid bits vector that has a
  // length equal to a power of two and also using a non-zero valid_bits_offset.  This
  // should not fail when run with ASAN or valgrind.
  std::vector<int16_t> def_levels = {3, 3, 3, 2, 3, 3, 3, 3};
  std::vector<uint8_t> valid_bits(1, 0);

  const int max_def_level = 3;
  const int max_rep_level = 1;

  int64_t values_read = -1;
  int64_t null_count = 0;

  // Read the latter half of the validity bitmap
  internal::DefinitionLevelsToBitmap(def_levels.data() + 4, 4, max_def_level,
                                     max_rep_level, &values_read, &null_count,
                                     valid_bits.data(), 4 /* valid_bits_offset */);
  ASSERT_EQ(4, values_read);
  ASSERT_EQ(0, null_count);
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
  int64_t null_count = 5;
  int64_t values_read = 1;

  // All zeros should be ignored, ones should be unset in the bitmp and 2 should be set.
  std::vector<int16_t> def_levels = {0, 0, 0, 2, 2, 1, 0, 2};
  DefinitionLevelsToBitmap(
      def_levels.data(), def_levels.size(), /*max_definition_level=*/2,
      /*max_repetition_level=*/1, &values_read, &null_count, validity_bitmap.data(),
      /*valid_bits_offset=*/1);

  EXPECT_EQ(BitmapToString(validity_bitmap, /*bit_count=*/8), "01101000");
  for (size_t x = 1; x < validity_bitmap.size(); x++) {
    EXPECT_EQ(validity_bitmap[x], 0) << "index: " << x;
  }
  EXPECT_EQ(null_count, /*5 + 1 =*/6);
  EXPECT_EQ(values_read, 4);  // value should get overwritten.
}

}  // namespace internal
}  // namespace parquet
