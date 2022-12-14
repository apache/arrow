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

// From Apache Impala (incubating) as of 2016-01-29

#include <cstdint>
#include <cstring>
#include <random>
#include <vector>

#include <gtest/gtest.h>

#include "arrow/array.h"
#include "arrow/buffer.h"
#include "arrow/testing/random.h"
#include "arrow/type.h"
#include "arrow/util/bit_stream_utils.h"
#include "arrow/util/bit_util.h"
#include "arrow/util/io_util.h"
#include "arrow/util/rle_encoding.h"

namespace arrow {
namespace util {

const int MAX_WIDTH = 32;

TEST(BitArray, TestBool) {
  const int len = 8;
  uint8_t buffer[len];

  bit_util::BitWriter writer(buffer, len);

  // Write alternating 0's and 1's
  for (int i = 0; i < 8; ++i) {
    EXPECT_TRUE(writer.PutValue(i % 2, 1));
  }
  writer.Flush();

  EXPECT_EQ(buffer[0], 0xAA /* 0b10101010 */);

  // Write 00110011
  for (int i = 0; i < 8; ++i) {
    bool result = false;
    switch (i) {
      case 0:
      case 1:
      case 4:
      case 5:
        result = writer.PutValue(false, 1);
        break;
      default:
        result = writer.PutValue(true, 1);
        break;
    }
    EXPECT_TRUE(result);
  }
  writer.Flush();

  // Validate the exact bit value
  EXPECT_EQ(buffer[0], 0xAA /* 0b10101010 */);
  EXPECT_EQ(buffer[1], 0xCC /* 0b11001100 */);

  // Use the reader and validate
  bit_util::BitReader reader(buffer, len);
  for (int i = 0; i < 8; ++i) {
    bool val = false;
    bool result = reader.GetValue(1, &val);
    EXPECT_TRUE(result);
    EXPECT_EQ(val, (i % 2) != 0);
  }

  for (int i = 0; i < 8; ++i) {
    bool val = false;
    bool result = reader.GetValue(1, &val);
    EXPECT_TRUE(result);
    switch (i) {
      case 0:
      case 1:
      case 4:
      case 5:
        EXPECT_EQ(val, false);
        break;
      default:
        EXPECT_EQ(val, true);
        break;
    }
  }
}

// Writes 'num_vals' values with width 'bit_width' and reads them back.
void TestBitArrayValues(int bit_width, int num_vals) {
  int len = static_cast<int>(bit_util::BytesForBits(bit_width * num_vals));
  EXPECT_GT(len, 0);
  const uint64_t mod = bit_width == 64 ? 1 : 1LL << bit_width;

  std::vector<uint8_t> buffer(len);
  bit_util::BitWriter writer(buffer.data(), len);
  for (int i = 0; i < num_vals; ++i) {
    bool result = writer.PutValue(i % mod, bit_width);
    EXPECT_TRUE(result);
  }
  writer.Flush();
  EXPECT_EQ(writer.bytes_written(), len);

  bit_util::BitReader reader(buffer.data(), len);
  for (int i = 0; i < num_vals; ++i) {
    int64_t val = 0;
    bool result = reader.GetValue(bit_width, &val);
    EXPECT_TRUE(result);
    EXPECT_EQ(val, i % mod);
  }
  EXPECT_EQ(reader.bytes_left(), 0);
}

TEST(BitArray, TestValues) {
  for (int width = 1; width <= MAX_WIDTH; ++width) {
    TestBitArrayValues(width, 1);
    TestBitArrayValues(width, 2);
    // Don't write too many values
    TestBitArrayValues(width, (width < 12) ? (1 << width) : 4096);
    TestBitArrayValues(width, 1024);
  }
}

// Test some mixed values
TEST(BitArray, TestMixed) {
  const int len = 1024;
  uint8_t buffer[len];
  bool parity = true;

  bit_util::BitWriter writer(buffer, len);
  for (int i = 0; i < len; ++i) {
    bool result;
    if (i % 2 == 0) {
      result = writer.PutValue(parity, 1);
      parity = !parity;
    } else {
      result = writer.PutValue(i, 10);
    }
    EXPECT_TRUE(result);
  }
  writer.Flush();

  parity = true;
  bit_util::BitReader reader(buffer, len);
  for (int i = 0; i < len; ++i) {
    bool result;
    if (i % 2 == 0) {
      bool val;
      result = reader.GetValue(1, &val);
      EXPECT_EQ(val, parity);
      parity = !parity;
    } else {
      int val;
      result = reader.GetValue(10, &val);
      EXPECT_EQ(val, i);
    }
    EXPECT_TRUE(result);
  }
}

// Write up to 'num_vals' values with width 'bit_width' and reads them back.
static void TestPutValue(int bit_width, uint64_t num_vals) {
  // The max value representable in `bit_width` bits.
  const uint64_t max = std::numeric_limits<uint64_t>::max() >> (64 - bit_width);
  num_vals = std::min(num_vals, max);
  int len = static_cast<int>(bit_util::BytesForBits(bit_width * num_vals));
  EXPECT_GT(len, 0);

  std::vector<uint8_t> buffer(len);
  bit_util::BitWriter writer(buffer.data(), len);
  for (uint64_t i = max - num_vals; i < max; i++) {
    bool result = writer.PutValue(i, bit_width);
    EXPECT_TRUE(result);
  }
  writer.Flush();
  EXPECT_EQ(writer.bytes_written(), len);

  bit_util::BitReader reader(buffer.data(), len);
  for (uint64_t i = max - num_vals; i < max; i++) {
    int64_t val = 0;
    bool result = reader.GetValue(bit_width, &val);
    EXPECT_TRUE(result);
    EXPECT_EQ(val, i);
  }
  EXPECT_EQ(reader.bytes_left(), 0);
}

TEST(BitUtil, RoundTripIntValues) {
  for (int width = 1; width < 64; width++) {
    TestPutValue(width, 1);
    TestPutValue(width, 1024);
  }
}

// Validates encoding of values by encoding and decoding them.  If
// expected_encoding != NULL, also validates that the encoded buffer is
// exactly 'expected_encoding'.
// if expected_len is not -1, it will validate the encoded size is correct.
void ValidateRle(const std::vector<int>& values, int bit_width,
                 uint8_t* expected_encoding, int expected_len) {
  const int len = 64 * 1024;
  uint8_t buffer[len];
  EXPECT_LE(expected_len, len);

  RleEncoder encoder(buffer, len, bit_width);
  for (size_t i = 0; i < values.size(); ++i) {
    bool result = encoder.Put(values[i]);
    EXPECT_TRUE(result);
  }
  int encoded_len = encoder.Flush();

  if (expected_len != -1) {
    EXPECT_EQ(encoded_len, expected_len);
  }
  if (expected_encoding != NULL) {
    EXPECT_EQ(memcmp(buffer, expected_encoding, encoded_len), 0);
  }

  // Verify read
  {
    RleDecoder decoder(buffer, len, bit_width);
    for (size_t i = 0; i < values.size(); ++i) {
      uint64_t val;
      bool result = decoder.Get(&val);
      EXPECT_TRUE(result);
      EXPECT_EQ(values[i], val);
    }
  }

  // Verify batch read
  {
    RleDecoder decoder(buffer, len, bit_width);
    std::vector<int> values_read(values.size());
    ASSERT_EQ(values.size(),
              decoder.GetBatch(values_read.data(), static_cast<int>(values.size())));
    EXPECT_EQ(values, values_read);
  }
}

// A version of ValidateRle that round-trips the values and returns false if
// the returned values are not all the same
bool CheckRoundTrip(const std::vector<int>& values, int bit_width) {
  const int len = 64 * 1024;
  uint8_t buffer[len];
  RleEncoder encoder(buffer, len, bit_width);
  for (size_t i = 0; i < values.size(); ++i) {
    bool result = encoder.Put(values[i]);
    if (!result) {
      return false;
    }
  }
  int encoded_len = encoder.Flush();
  int out = 0;

  {
    RleDecoder decoder(buffer, encoded_len, bit_width);
    for (size_t i = 0; i < values.size(); ++i) {
      EXPECT_TRUE(decoder.Get(&out));
      if (values[i] != out) {
        return false;
      }
    }
  }

  // Verify batch read
  {
    RleDecoder decoder(buffer, encoded_len, bit_width);
    std::vector<int> values_read(values.size());
    if (static_cast<int>(values.size()) !=
        decoder.GetBatch(values_read.data(), static_cast<int>(values.size()))) {
      return false;
    }

    if (values != values_read) {
      return false;
    }
  }

  return true;
}

TEST(Rle, SpecificSequences) {
  const int len = 1024;
  uint8_t expected_buffer[len];
  std::vector<int> values;

  // Test 50 0' followed by 50 1's
  values.resize(100);
  for (int i = 0; i < 50; ++i) {
    values[i] = 0;
  }
  for (int i = 50; i < 100; ++i) {
    values[i] = 1;
  }

  // expected_buffer valid for bit width <= 1 byte
  expected_buffer[0] = (50 << 1);
  expected_buffer[1] = 0;
  expected_buffer[2] = (50 << 1);
  expected_buffer[3] = 1;
  for (int width = 1; width <= 8; ++width) {
    ValidateRle(values, width, expected_buffer, 4);
  }

  for (int width = 9; width <= MAX_WIDTH; ++width) {
    ValidateRle(values, width, nullptr,
                2 * (1 + static_cast<int>(bit_util::CeilDiv(width, 8))));
  }

  // Test 100 0's and 1's alternating
  for (int i = 0; i < 100; ++i) {
    values[i] = i % 2;
  }
  int num_groups = static_cast<int>(bit_util::CeilDiv(100, 8));
  expected_buffer[0] = static_cast<uint8_t>((num_groups << 1) | 1);
  for (int i = 1; i <= 100 / 8; ++i) {
    expected_buffer[i] = 0xAA /* 0b10101010 */;
  }
  // Values for the last 4 0 and 1's. The upper 4 bits should be padded to 0.
  expected_buffer[100 / 8 + 1] = 0x0A /* 0b00001010 */;

  // num_groups and expected_buffer only valid for bit width = 1
  ValidateRle(values, 1, expected_buffer, 1 + num_groups);
  for (int width = 2; width <= MAX_WIDTH; ++width) {
    int num_values = static_cast<int>(bit_util::CeilDiv(100, 8)) * 8;
    ValidateRle(values, width, nullptr,
                1 + static_cast<int>(bit_util::CeilDiv(width * num_values, 8)));
  }

  // Test 16-bit values to confirm encoded values are stored in little endian
  values.resize(28);
  for (int i = 0; i < 16; ++i) {
    values[i] = 0x55aa;
  }
  for (int i = 16; i < 28; ++i) {
    values[i] = 0xaa55;
  }
  expected_buffer[0] = (16 << 1);
  expected_buffer[1] = 0xaa;
  expected_buffer[2] = 0x55;
  expected_buffer[3] = (12 << 1);
  expected_buffer[4] = 0x55;
  expected_buffer[5] = 0xaa;

  ValidateRle(values, 16, expected_buffer, 6);

  // Test 32-bit values to confirm encoded values are stored in little endian
  values.resize(28);
  for (int i = 0; i < 16; ++i) {
    values[i] = 0x555aaaa5;
  }
  for (int i = 16; i < 28; ++i) {
    values[i] = 0x5aaaa555;
  }
  expected_buffer[0] = (16 << 1);
  expected_buffer[1] = 0xa5;
  expected_buffer[2] = 0xaa;
  expected_buffer[3] = 0x5a;
  expected_buffer[4] = 0x55;
  expected_buffer[5] = (12 << 1);
  expected_buffer[6] = 0x55;
  expected_buffer[7] = 0xa5;
  expected_buffer[8] = 0xaa;
  expected_buffer[9] = 0x5a;

  ValidateRle(values, 32, expected_buffer, 10);
}

// ValidateRle on 'num_vals' values with width 'bit_width'. If 'value' != -1, that value
// is used, otherwise alternating values are used.
void TestRleValues(int bit_width, int num_vals, int value = -1) {
  const uint64_t mod = (bit_width == 64) ? 1 : 1LL << bit_width;
  std::vector<int> values;
  for (int v = 0; v < num_vals; ++v) {
    values.push_back((value != -1) ? value : static_cast<int>(v % mod));
  }
  ValidateRle(values, bit_width, NULL, -1);
}

TEST(Rle, TestValues) {
  for (int width = 1; width <= MAX_WIDTH; ++width) {
    TestRleValues(width, 1);
    TestRleValues(width, 1024);
    TestRleValues(width, 1024, 0);
    TestRleValues(width, 1024, 1);
  }
}

TEST(Rle, BitWidthZeroRepeated) {
  uint8_t buffer[1];
  const int num_values = 15;
  buffer[0] = num_values << 1;  // repeated indicator byte
  RleDecoder decoder(buffer, sizeof(buffer), 0);
  uint8_t val;
  for (int i = 0; i < num_values; ++i) {
    bool result = decoder.Get(&val);
    EXPECT_TRUE(result);
    EXPECT_EQ(val, 0);  // can only encode 0s with bit width 0
  }
  EXPECT_FALSE(decoder.Get(&val));
}

TEST(Rle, BitWidthZeroLiteral) {
  uint8_t buffer[1];
  const int num_groups = 4;
  buffer[0] = num_groups << 1 | 1;  // literal indicator byte
  RleDecoder decoder = RleDecoder(buffer, sizeof(buffer), 0);
  const int num_values = num_groups * 8;
  uint8_t val;
  for (int i = 0; i < num_values; ++i) {
    bool result = decoder.Get(&val);
    EXPECT_TRUE(result);
    EXPECT_EQ(val, 0);  // can only encode 0s with bit width 0
  }
  EXPECT_FALSE(decoder.Get(&val));
}

// Test that writes out a repeated group and then a literal
// group but flush before finishing.
TEST(BitRle, Flush) {
  std::vector<int> values;
  for (int i = 0; i < 16; ++i) values.push_back(1);
  values.push_back(0);
  ValidateRle(values, 1, NULL, -1);
  values.push_back(1);
  ValidateRle(values, 1, NULL, -1);
  values.push_back(1);
  ValidateRle(values, 1, NULL, -1);
  values.push_back(1);
  ValidateRle(values, 1, NULL, -1);
}

// Test some random sequences.
TEST(BitRle, Random) {
  int niters = 50;
  int ngroups = 1000;
  int max_group_size = 16;
  std::vector<int> values(ngroups + max_group_size);

  // prng setup
  const auto seed = ::arrow::internal::GetRandomSeed();
  std::default_random_engine gen(
      static_cast<std::default_random_engine::result_type>(seed));
  std::uniform_int_distribution<int> dist(1, 20);

  for (int iter = 0; iter < niters; ++iter) {
    // generate a seed with device entropy
    bool parity = 0;
    values.resize(0);

    for (int i = 0; i < ngroups; ++i) {
      int group_size = dist(gen);
      if (group_size > max_group_size) {
        group_size = 1;
      }
      for (int i = 0; i < group_size; ++i) {
        values.push_back(parity);
      }
      parity = !parity;
    }
    if (!CheckRoundTrip(values, bit_util::NumRequiredBits(values.size()))) {
      FAIL() << "failing seed: " << seed;
    }
  }
}

// Test a sequence of 1 0's, 2 1's, 3 0's. etc
// e.g. 011000111100000
TEST(BitRle, RepeatedPattern) {
  std::vector<int> values;
  const int min_run = 1;
  const int max_run = 32;

  for (int i = min_run; i <= max_run; ++i) {
    int v = i % 2;
    for (int j = 0; j < i; ++j) {
      values.push_back(v);
    }
  }

  // And go back down again
  for (int i = max_run; i >= min_run; --i) {
    int v = i % 2;
    for (int j = 0; j < i; ++j) {
      values.push_back(v);
    }
  }

  ValidateRle(values, 1, NULL, -1);
}

TEST(BitRle, Overflow) {
  for (int bit_width = 1; bit_width < 32; bit_width += 3) {
    int len = RleEncoder::MinBufferSize(bit_width);
    std::vector<uint8_t> buffer(len);
    int num_added = 0;
    bool parity = true;

    RleEncoder encoder(buffer.data(), len, bit_width);
    // Insert alternating true/false until there is no space left
    while (true) {
      bool result = encoder.Put(parity);
      parity = !parity;
      if (!result) break;
      ++num_added;
    }

    int bytes_written = encoder.Flush();
    EXPECT_LE(bytes_written, len);
    EXPECT_GT(num_added, 0);

    RleDecoder decoder(buffer.data(), bytes_written, bit_width);
    parity = true;
    uint32_t v;
    for (int i = 0; i < num_added; ++i) {
      bool result = decoder.Get(&v);
      EXPECT_TRUE(result);
      EXPECT_EQ(v != 0, parity);
      parity = !parity;
    }
    // Make sure we get false when reading past end a couple times.
    EXPECT_FALSE(decoder.Get(&v));
    EXPECT_FALSE(decoder.Get(&v));
  }
}

template <typename Type>
void CheckRoundTripSpaced(const Array& data, int bit_width) {
  using ArrayType = typename TypeTraits<Type>::ArrayType;
  using T = typename Type::c_type;

  int num_values = static_cast<int>(data.length());
  int buffer_size = RleEncoder::MaxBufferSize(bit_width, num_values);

  const T* values = static_cast<const ArrayType&>(data).raw_values();

  std::vector<uint8_t> buffer(buffer_size);
  RleEncoder encoder(buffer.data(), buffer_size, bit_width);
  for (int i = 0; i < num_values; ++i) {
    if (data.IsValid(i)) {
      if (!encoder.Put(static_cast<uint64_t>(values[i]))) {
        FAIL() << "Encoding failed";
      }
    }
  }
  int encoded_size = encoder.Flush();

  // Verify batch read
  RleDecoder decoder(buffer.data(), encoded_size, bit_width);
  std::vector<T> values_read(num_values);

  if (num_values != decoder.GetBatchSpaced(
                        num_values, static_cast<int>(data.null_count()),
                        data.null_bitmap_data(), data.offset(), values_read.data())) {
    FAIL();
  }

  for (int64_t i = 0; i < num_values; ++i) {
    if (data.IsValid(i)) {
      if (values_read[i] != values[i]) {
        FAIL() << "Index " << i << " read " << values_read[i] << " but should be "
               << values[i];
      }
    }
  }
}

template <typename T>
struct GetBatchSpacedTestCase {
  T max_value;
  int64_t size;
  double null_probability;
  int bit_width;
};

TEST(RleDecoder, GetBatchSpaced) {
  uint32_t kSeed = 1337;
  ::arrow::random::RandomArrayGenerator rand(kSeed);

  std::vector<GetBatchSpacedTestCase<int32_t>> int32_cases{
      {1, 100000, 0.01, 1}, {1, 100000, 0.1, 1},    {1, 100000, 0.5, 1},
      {4, 100000, 0.05, 3}, {100, 100000, 0.05, 7},
  };
  for (auto case_ : int32_cases) {
    auto arr = rand.Int32(case_.size, /*min=*/0, case_.max_value, case_.null_probability);
    CheckRoundTripSpaced<Int32Type>(*arr, case_.bit_width);
    CheckRoundTripSpaced<Int32Type>(*arr->Slice(1), case_.bit_width);
  }
}

}  // namespace util
}  // namespace arrow
