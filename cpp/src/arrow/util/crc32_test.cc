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

#include <array>
#include <cstdint>
#include <limits>
#include <memory>

#include <gtest/gtest.h>

#include "arrow/util/crc32.h"

namespace arrow {

TEST(Crc32Test, Basic) {
  // use the string "123456789" in ASCII as test data.
  constexpr uint32_t TEST_CRC32_RESULT = 0xCBF43926;
  constexpr size_t TEST_CRC32_LENGTH = 9;
  std::array<unsigned char, TEST_CRC32_LENGTH> std_data = {0x31, 0x32, 0x33, 0x34, 0x35,
                                                           0x36, 0x37, 0x38, 0x39};
  size_t const std_data_len = sizeof(std_data) / sizeof(std_data[0]);
  EXPECT_EQ(TEST_CRC32_RESULT, internal::crc32(0, &std_data[0], std_data_len));

  for (size_t i = 1; i < std_data_len - 1; ++i) {
    uint32_t crc1 = internal::crc32(0, &std_data[0], i);
    EXPECT_EQ(TEST_CRC32_RESULT, internal::crc32(crc1, &std_data[i], std_data_len - i));
  }
}

}  // namespace arrow
