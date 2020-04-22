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

#include "arrow/util/bit_util.h"

namespace parquet {
namespace internal {

using ::testing::ElementsAreArray;

std::string ToString(const std::vector<uint8_t>& bitmap, int64_t bit_count) {
  return arrow::internal::Bitmap(bitmap.data(), /*offset*/ 0, /*length=*/bit_count)
      .ToString();
}

TEST(TestAppendBitmap, TestOffsetOverwritesCorrectBitsOnExistingByte) {
  auto check_append = [](const std::string& expected_bits, int64_t offset) {
    std::vector<uint8_t> valid_bits = {0x00};
    constexpr int64_t kBitsAfterAppend = 8;
    ASSERT_EQ(
        AppendBitmap(/*new_bits=*/0xFF, /*number_of_bits*/ 8 - offset,
                     /*valid_bits_length=*/valid_bits.size(), offset, valid_bits.data()),
        kBitsAfterAppend);
    EXPECT_EQ(ToString(valid_bits, kBitsAfterAppend), expected_bits);
  };
  check_append("11111111", 0);
  check_append("01111111", 1);
  check_append("00111111", 2);
  check_append("00011111", 3);
  check_append("00001111", 4);
  check_append("00000111", 5);
  check_append("00000011", 6);
  check_append("00000001", 7);
}

TEST(TestAppendBitmap, AllBytesAreWrittenWithEnoughSpace) {
  std::vector<uint8_t> valid_bits(/*count=*/9, 0);

  uint64_t bitmap = 0xFFFFFFFFFFFFFFFF;
  AppendBitmap(bitmap, /*number_of_bits*/ 7,
               /*valid_bits_length=*/valid_bits.size(),
               /*valid_bits_offset=*/1,
               /*valid_bits=*/valid_bits.data());
  EXPECT_THAT(valid_bits, ElementsAreArray(std::vector<uint8_t>{
                              0xFE, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0x01}));
}

TEST(TestAppendBitmap, OnlyApproriateBytesWrittenWhenLessThen8BytesAvailable) {
  std::vector<uint8_t> valid_bits = {0x00, 0x00};

  uint64_t bitmap = 0x1FF;
  AppendBitmap(bitmap, /*number_of_bits*/ 7,
               /*valid_bits_length=*/2,
               /*valid_bits_offset=*/1,
               /*valid_bits=*/valid_bits.data());

  EXPECT_THAT(valid_bits, ElementsAreArray(std::vector<uint8_t>{0xFE, 0x00}));

  AppendBitmap(bitmap, /*number_of_bits*/ 9,
               /*valid_bits_length=*/2,
               /*valid_bits_offset=*/1,
               /*valid_bits=*/valid_bits.data());
  EXPECT_THAT(valid_bits, ElementsAreArray(std::vector<uint8_t>{0xFE, 0x03}));
}

}  // namespace internal
}  // namespace parquet
