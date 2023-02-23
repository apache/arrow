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
#include <random>

#include <gtest/gtest.h>
#include <boost/crc.hpp>

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

TEST(Crc32Test, matchesBoost32Type) {
  const size_t BUFFER_SIZE = 512 * 1024 * sizeof(uint64_t);
  std::vector<uint8_t> buffer;
  buffer.resize(BUFFER_SIZE, 0);

  // Populate a buffer with a deterministic pattern
  // on which to compute checksums
  std::random_device r;
  std::seed_seq seed{r(), r(), r(), r(), r(), r(), r(), r()};
  std::mt19937 gen(seed);
  // N4659 29.6.1.1 [rand.req.genl]/1e requires one of short, int, long, long long,
  // unsigned short, unsigned int, unsigned long, or unsigned long long
  std::uniform_int_distribution<uint32_t> dist;

  for (size_t i = 0; i < BUFFER_SIZE; ++i) {
    buffer[i] = static_cast<uint8_t>(dist(gen));
  }

  struct TestCrcGroup {
    size_t offset;
    size_t length;
  };

  // NOLINTNEXTLINE
  TestCrcGroup testCrcGroups[] = {
      // Zero-byte input
      {0, 0},
      {8, 1},
      {8, 2},
      {8, 3},
      {8, 4},
      {8, 5},
      {8, 6},
      {8, 7},
      {9, 1},
      {10, 2},
      {11, 3},
      {12, 4},
      {13, 5},
      {14, 6},
      {15, 7},
      {8, 8},
      {8, 9},
      {8, 10},
      {8, 11},
      {8, 12},
      {8, 13},
      {8, 14},
      {8, 15},
      {8, 16},
      {8, 17},
      // Much larger inputs
      {0, BUFFER_SIZE},
      {1, BUFFER_SIZE / 2},
  };

  for (TestCrcGroup group : testCrcGroups) {
    uint32_t crc = internal::crc32(0, &buffer[group.offset], group.length);
    boost::crc_32_type boost_crc;
    boost_crc.process_bytes(&buffer[group.offset], group.length);
    EXPECT_EQ(boost_crc.checksum(), crc);
  }
}

}  // namespace arrow
