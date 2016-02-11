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

#include <cstdlib>
#include <iostream>
#include <sstream>
#include <string>

#include <gtest/gtest.h>

#include "parquet/thrift/parquet_types.h"
#include "parquet/column/levels.h"

using std::string;

namespace parquet_cpp {

class TestLevels : public ::testing::Test {
 public:
  int GenerateLevels(int min_repeat_factor, int max_repeat_factor,
      int max_level, std::vector<int16_t>& input_levels) {
    int total_count = 0;
    // for each repetition count upto max_repeat_factor
    for (int repeat = min_repeat_factor; repeat <= max_repeat_factor; repeat++) {
      // repeat count increase by a factor of 2 for every iteration
      int repeat_count = (1 << repeat);
      // generate levels for repetition count upto the maximum level
      int value = 0;
      int bwidth = 0;
      while (value <= max_level) {
        for (int i = 0; i < repeat_count; i++) {
          input_levels[total_count++] = value;
        }
        value = (2 << bwidth) - 1;
        bwidth++;
      }
    }
    return total_count;
  }

  void VerifyLevelsEncoding(parquet::Encoding::type encoding, int max_level,
      std::vector<int16_t>& input_levels) {
    LevelEncoder encoder;
    LevelDecoder decoder;
    int levels_count = 0;
    std::vector<int16_t> output_levels;
    std::vector<uint8_t> bytes;
    int num_levels = input_levels.size();
    output_levels.resize(num_levels);
    bytes.resize(2 * num_levels);
    ASSERT_EQ(num_levels, output_levels.size());
    ASSERT_EQ(2 * num_levels, bytes.size());
    // start encoding and decoding
    if (encoding == parquet::Encoding::RLE) {
      // leave space to write the rle length value
      encoder.Init(encoding, max_level, num_levels,
          bytes.data() + sizeof(uint32_t), bytes.size());

      levels_count = encoder.Encode(num_levels, input_levels.data());
      (reinterpret_cast<uint32_t*>(bytes.data()))[0] = encoder.len();

    } else {
      encoder.Init(encoding, max_level, num_levels,
          bytes.data(), bytes.size());
      levels_count = encoder.Encode(num_levels, input_levels.data());
    }

    ASSERT_EQ(num_levels, levels_count);

    decoder.Init(encoding, max_level, num_levels, bytes.data());
    levels_count = decoder.Decode(num_levels, output_levels.data());

    ASSERT_EQ(num_levels, levels_count);

    for (int i = 0; i < num_levels; i++) {
      EXPECT_EQ(input_levels[i], output_levels[i]);
    }
  }
};

// test levels with maximum bit-width from 1 to 8
// increase the repetition count for each iteration by a factor of 2
TEST_F(TestLevels, TestEncodeDecodeLevels) {
  int min_repeat_factor = 0;
  int max_repeat_factor = 7; // 128
  int max_bit_width = 8;
  std::vector<int16_t> input_levels;
  parquet::Encoding::type encodings[2] = {parquet::Encoding::RLE,
      parquet::Encoding::BIT_PACKED};

  // for each encoding
  for (int encode = 0; encode < 2; encode++) {
    parquet::Encoding::type encoding = encodings[encode];
    // BIT_PACKED requires a sequence of atleast 8
    if (encoding == parquet::Encoding::BIT_PACKED) min_repeat_factor = 3;

    // for each maximum bit-width
    for (int bit_width = 1; bit_width <= max_bit_width; bit_width++) {
      int num_levels_per_width = ((2 << max_repeat_factor) - (1 << min_repeat_factor));
      int num_levels = (bit_width + 1) * num_levels_per_width;
      input_levels.resize(num_levels);
      ASSERT_EQ(num_levels, input_levels.size());

      // find the maximum level for the current bit_width
      int max_level = (1 << bit_width) - 1;
      // Generate levels
      int total_count = GenerateLevels(min_repeat_factor, max_repeat_factor,
          max_level, input_levels);
      ASSERT_EQ(num_levels, total_count);
      VerifyLevelsEncoding(encoding, max_level, input_levels);
    }
  }
}

} // namespace parquet_cpp
