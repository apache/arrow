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
#include "arrow/array/concatenate.h"
#include "arrow/array/util.h"
#include "arrow/scalar.h"
#include "arrow/testing/gtest_util.h"
#include "arrow/testing/random.h"
#include "arrow/type.h"
#include "arrow/util/bit_stream_utils_internal.h"
#include "arrow/util/bit_util.h"
#include "arrow/util/io_util.h"
#include "arrow/util/rle_encoding_internal.h"

namespace arrow::util {

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

/// A Rle run is a simple class owning some data and a repetition count.
/// It does not know how to read such data.
TEST(Rle, RleRun) {
  const std::array<uint8_t, 4> value = {21, 2, 0, 0};

  const rle_size_t value_count = 12;

  // 12 times the value 21 fitting over 5 bits
  const rle_size_t value_bit_width_5 = 5;
  const auto run_5 = RleRun(value.data(), value_count, value_bit_width_5);
  EXPECT_EQ(run_5.values_count(), value_count);
  EXPECT_EQ(run_5.raw_data_size(value_bit_width_5), 1);  // 5 bits fit in one byte
  EXPECT_EQ(*run_5.raw_data_ptr(), 21);

  // 12 times the value 21 fitting over 8 bits
  const rle_size_t value_bit_width_8 = 8;
  const auto run_8 = RleRun(value.data(), value_count, value_bit_width_8);
  EXPECT_EQ(run_8.values_count(), value_count);
  EXPECT_EQ(run_8.raw_data_size(value_bit_width_8), 1);  // 8 bits fit in 1 byte
  EXPECT_EQ(*run_8.raw_data_ptr(), 21);

  // 12 times the value 533 (21 + 2 * 2^8) fitting over 10 bits
  const rle_size_t value_bit_width_10 = 10;
  const auto run_10 = RleRun(value.data(), value_count, value_bit_width_10);
  EXPECT_EQ(run_10.values_count(), value_count);
  EXPECT_EQ(run_10.raw_data_size(value_bit_width_10), 2);  // 10 bits fit in 2 bytes
  EXPECT_EQ(*(run_10.raw_data_ptr() + 0), 21);
  EXPECT_EQ(*(run_10.raw_data_ptr() + 1), 2);

  // 12 times the value 533 (21 + 2 * 2^8) fitting over 32 bits
  const rle_size_t value_bit_width_32 = 32;
  const auto run_32 = RleRun(value.data(), value_count, value_bit_width_32);
  EXPECT_EQ(run_32.values_count(), value_count);
  EXPECT_EQ(run_32.raw_data_size(value_bit_width_32), 4);  // 32 bits fit in 4 bytes
  EXPECT_EQ(*(run_32.raw_data_ptr() + 0), 21);
  EXPECT_EQ(*(run_32.raw_data_ptr() + 1), 2);
  EXPECT_EQ(*(run_32.raw_data_ptr() + 2), 0);
  EXPECT_EQ(*(run_32.raw_data_ptr() + 3), 0);
}

/// A BitPacked run is a simple class owning some data and its size.
/// It does not know how to read such data.
TEST(BitPacked, BitPackedRun) {
  const std::array<uint8_t, 4> value = {0b10101010, 0, 0, 0b1111111};

  // 16 values of 1 bit for a total of 16 bits
  const rle_size_t value_count_1 = 16;
  const rle_size_t value_bit_width_1 = 1;
  const auto run_1 = BitPackedRun(value.data(), value_count_1, value_bit_width_1);
  EXPECT_EQ(run_1.values_count(), value_count_1);
  EXPECT_EQ(run_1.raw_data_size(value_bit_width_1), 2);  // 16 bits fit in 2 bytes
  EXPECT_EQ(run_1.raw_data_ptr(), value.data());

  // 8 values of 3 bits for a total of 24 bits
  const rle_size_t value_count_3 = 8;
  const rle_size_t value_bit_width_3 = 3;
  const auto run_3 = BitPackedRun(value.data(), value_count_3, value_bit_width_3);
  EXPECT_EQ(run_3.values_count(), value_count_3);
  EXPECT_EQ(run_3.raw_data_size(value_bit_width_3), 3);  // 24 bits fit in 3 bytes
  EXPECT_EQ(run_3.raw_data_ptr(), value.data());
}

template <typename T>
void TestRleDecoder(std::vector<uint8_t> bytes, rle_size_t value_count,
                    rle_size_t bit_width, T expected_value) {
  // Pre-requisite for this test
  EXPECT_GT(value_count, 6);

  const auto run = RleRun(bytes.data(), value_count, bit_width);

  auto decoder = RleRunDecoder<T>(run, bit_width);
  std::vector<T> vals = {0, 0};

  EXPECT_EQ(decoder.remaining(), value_count);

  rle_size_t read = 0;
  EXPECT_EQ(decoder.Get(vals.data(), bit_width), 1);
  read += 1;
  EXPECT_EQ(vals.at(0), expected_value);
  EXPECT_EQ(decoder.remaining(), value_count - read);

  EXPECT_EQ(decoder.Advance(3, bit_width), 3);
  read += 3;
  EXPECT_EQ(decoder.remaining(), value_count - read);

  vals = {0, 0};
  EXPECT_EQ(decoder.GetBatch(vals.data(), 2, bit_width), vals.size());
  EXPECT_EQ(vals.at(0), expected_value);
  EXPECT_EQ(vals.at(1), expected_value);
  read += static_cast<decltype(read)>(vals.size());
  EXPECT_EQ(decoder.remaining(), value_count - read);

  // Exhaust iteration
  EXPECT_EQ(decoder.Advance(value_count - read, bit_width), value_count - read);
  EXPECT_EQ(decoder.remaining(), 0);
  EXPECT_EQ(decoder.Advance(1, bit_width), 0);
  vals = {0, 0};
  EXPECT_EQ(decoder.Get(vals.data(), bit_width), 0);
  EXPECT_EQ(vals.at(0), 0);

  // Reset the decoder
  decoder.Reset(run, bit_width);
  EXPECT_EQ(decoder.remaining(), value_count);
  vals = {0, 0};
  EXPECT_EQ(decoder.GetBatch(vals.data(), 2, bit_width), vals.size());
  EXPECT_EQ(vals.at(0), expected_value);
  EXPECT_EQ(vals.at(1), expected_value);
}

TEST(Rle, RleDecoder) {
  TestRleDecoder<uint8_t>({21, 0, 0}, /* value_count= */ 23, /* bit_width= */ 5,
                          /* expected_value= */ 21);
  TestRleDecoder<uint16_t>({1, 0}, /* value_count= */ 13, /* bit_width= */ 1,
                           /* expected_value= */ 1);
  TestRleDecoder<uint32_t>({21, 0, 0}, /* value_count= */ 23, /* bit_width= */ 5,
                           /* expected_value= */ 21);
  TestRleDecoder<int32_t>({21, 0, 0}, /* value_count= */ 23, /* bit_width= */ 5,
                          /* expected_value= */ 21);
  TestRleDecoder<uint64_t>({21, 2, 0, 1}, /* value_count= */ 20, /* bit_width= */ 30,
                           /* expected_value= */ 16777749);
}

template <typename T>
void TestBitPackedDecoder(std::vector<uint8_t> bytes, rle_size_t value_count,
                          rle_size_t bit_width, std::vector<T> expected) {
  // Pre-requisite for this test
  EXPECT_GT(value_count, 6);

  const auto run = BitPackedRun(bytes.data(), value_count, bit_width);

  auto decoder = BitPackedRunDecoder<T>(run, bit_width);
  std::vector<T> vals = {0, 0};

  EXPECT_EQ(decoder.remaining(), value_count);

  rle_size_t read = 0;
  EXPECT_EQ(decoder.Get(vals.data(), bit_width), 1);
  EXPECT_EQ(vals.at(0), expected.at(0 + read));
  read += 1;
  EXPECT_EQ(decoder.remaining(), value_count - read);

  EXPECT_EQ(decoder.Advance(3, bit_width), 3);
  read += 3;
  EXPECT_EQ(decoder.remaining(), value_count - read);

  vals = {0, 0};
  EXPECT_EQ(decoder.GetBatch(vals.data(), 2, bit_width), vals.size());
  EXPECT_EQ(vals.at(0), expected.at(0 + read));
  EXPECT_EQ(vals.at(1), expected.at(1 + read));
  read += static_cast<decltype(read)>(vals.size());
  EXPECT_EQ(decoder.remaining(), value_count - read);

  // Exhaust iteration
  EXPECT_EQ(decoder.Advance(value_count - read, bit_width), value_count - read);
  EXPECT_EQ(decoder.remaining(), 0);
  EXPECT_EQ(decoder.Advance(1, bit_width), 0);
  vals = {0, 0};
  EXPECT_EQ(decoder.Get(vals.data(), bit_width), 0);
  EXPECT_EQ(vals.at(0), 0);

  // Reset the decoder
  decoder.Reset(run, bit_width);
  read = 0;
  EXPECT_EQ(decoder.remaining(), value_count);
  vals = {0, 0};
  EXPECT_EQ(decoder.GetBatch(vals.data(), 2, bit_width), vals.size());
  EXPECT_EQ(vals.at(0), expected.at(0 + read));
  EXPECT_EQ(vals.at(1), expected.at(1 + read));
}

TEST(BitPacked, BitPackedDecoder) {
  // See parquet encoding for bytes layout
  TestBitPackedDecoder<uint16_t>(
      /* bytes= */ {0x88, 0xc6, 0xfa},
      /* values_count= */ 8,
      /* bit_width= */ 3,
      /* expected= */ {0, 1, 2, 3, 4, 5, 6, 7});
  TestBitPackedDecoder<uint8_t>(
      /* bytes= */ {0x0, 0x1, 0x2, 0x3, 0x4, 0x5, 0x6, 0x7},
      /* values_count= */ 8,
      /* bit_width= */ 8,
      /* expected= */ {0, 1, 2, 3, 4, 5, 6, 7});
  TestBitPackedDecoder<uint32_t>(
      /* bytes= */ {0x47, 0xc, 0x10, 0x35},
      /* values_count= */ 8,
      /* bit_width= */ 4,
      /* expected= */ {7, 4, 12, 0, 0, 1, 5, 3});
  TestBitPackedDecoder<uint64_t>(
      /* bytes= */ {0xe8, 0x7, 0x20, 0xc0, 0x0, 0x4, 0x14, 0x60, 0xc0, 0x1},
      /* values_count= */ 8,
      /* bit_width= */ 10,
      /* expected= */ {1000, 1, 2, 3, 4, 5, 6, 7});
}

template <typename T>
void TestRleBitPackedParser(std::vector<uint8_t> bytes, rle_size_t bit_width,
                            std::vector<T> expected) {
  auto parser =
      RleBitPackedParser(bytes.data(), static_cast<rle_size_t>(bytes.size()), bit_width);
  EXPECT_FALSE(parser.exhausted());

  // Try to decode all data of all runs in the decoded vector
  decltype(expected) decoded = {};
  auto rle_decoder = RleRunDecoder<T>();
  auto bit_packed_decoder = BitPackedRunDecoder<T>();

  struct {
    decltype(rle_decoder)* rle_decoder_ptr_;
    decltype(bit_packed_decoder)* bit_packed_decoder_ptr_;
    decltype(decoded)* decoded_ptr_;
    decltype(bit_width) bit_width_;

    auto OnRleRun(RleRun run) {
      rle_decoder_ptr_->Reset(run, bit_width_);

      const auto n_decoded = decoded_ptr_->size();
      const auto n_to_decode = rle_decoder_ptr_->remaining();
      decoded_ptr_->resize(n_decoded + n_to_decode);
      EXPECT_EQ(rle_decoder_ptr_->GetBatch(decoded_ptr_->data() + n_decoded, n_to_decode,
                                           bit_width_),
                n_to_decode);
      EXPECT_EQ(rle_decoder_ptr_->remaining(), 0);

      return RleBitPackedParser::ControlFlow::Continue;
    }

    auto OnBitPackedRun(BitPackedRun run) {
      bit_packed_decoder_ptr_->Reset(run, bit_width_);

      const auto n_decoded = decoded_ptr_->size();
      const auto n_to_decode = bit_packed_decoder_ptr_->remaining();
      decoded_ptr_->resize(n_decoded + n_to_decode);
      EXPECT_EQ(bit_packed_decoder_ptr_->GetBatch(decoded_ptr_->data() + n_decoded,
                                                  n_to_decode, bit_width_),
                n_to_decode);
      EXPECT_EQ(bit_packed_decoder_ptr_->remaining(), 0);

      return RleBitPackedParser::ControlFlow::Continue;
    }
  } handler{&rle_decoder, &bit_packed_decoder, &decoded, bit_width};

  // Iterate over all runs
  parser.Parse(handler);

  EXPECT_TRUE(parser.exhausted());
  EXPECT_EQ(decoded.size(), expected.size());
  EXPECT_EQ(decoded, expected);
}

TEST(RleBitPacked, RleBitPackedParser) {
  TestRleBitPackedParser<uint16_t>(
      /* bytes= */
      {/* LEB128 for 8 values bit packed marker */ 0x3,
       /* Bitpacked run */ 0x88, 0xc6, 0xfa},
      /* bit_width= */ 3,
      /* expected= */ {0, 1, 2, 3, 4, 5, 6, 7});

  {
    std::vector<uint32_t> expected = {0, 1, 2, 3, 4, 5, 6, 7};
    expected.resize(expected.size() + 200, 5);
    TestRleBitPackedParser<uint32_t>(
        /* bytes= */
        {/* LEB128 for 8 values bit packed marker */ 0x3,
         /* Bitpacked run */ 0x88, 0xc6, 0xfa,
         /* LEB128 for 200 RLE marker */ 0x90, 0x3,
         /* Value 5 over paded to a byte*/ 0x5},
        /* bit_width= */ 3,
        /* expected= */ expected);
  }

  {
    std::vector<uint16_t> expected = {0, 0, 0, 0, 1, 1, 1, 1};
    expected.resize(expected.size() + 200, 1);
    expected.resize(expected.size() + 10, 3);
    std::array<uint16_t, 16> run2 = {1, 2, 1, 2, 1, 2, 1, 2, 1, 2, 1, 2, 1, 2, 1, 2};
    expected.insert(expected.end(), run2.begin(), run2.end());
    TestRleBitPackedParser<uint16_t>(
        /* bytes= */
        {/* LEB128 for 8 values bit packed marker */ 0x3,
         /* Bitpacked run */ 0x0, 0x55,
         /* LEB128 for 200 RLE marker */ 0x90, 0x3,
         /* Value 1 over paded to a byte*/ 0x1,
         /* LEB128 for 10 RLE marker */ 0x14,
         /* Value 3 over paded to a byte*/ 0x3,
         /* LEB128 for 16 values bit packed marker */ 0x5,
         /* Bitpacked run */ 0x99, 0x99, 0x99, 0x99},
        /* bit_width= */ 2,
        /* expected= */ expected);
  }
}

// Validates encoding of values by encoding and decoding them.  If
// expected_encoding != NULL, also validates that the encoded buffer is
// exactly 'expected_encoding'.
// if expected_len is not -1, it will validate the encoded size is correct.
void ValidateRleBitPacked(const std::vector<int>& values, int bit_width,
                          uint8_t* expected_encoding, int expected_len) {
  const int len = 64 * 1024;
#ifdef __EMSCRIPTEN__
  // don't make this on the stack as it is
  // too big for emscripten
  std::vector<uint8_t> buffer_vec(static_cast<size_t>(len));
  uint8_t* buffer = buffer_vec.data();
#else
  uint8_t buffer[len];
#endif
  EXPECT_LE(expected_len, len);

  RleBitPackedEncoder encoder(buffer, len, bit_width);
  for (size_t i = 0; i < values.size(); ++i) {
    bool result = encoder.Put(values[i]);
    EXPECT_TRUE(result);
  }
  int encoded_len = encoder.Flush();

  if (expected_len != -1) {
    EXPECT_EQ(encoded_len, expected_len);
  }
  if (expected_encoding != NULL && encoded_len == expected_len) {
    EXPECT_EQ(memcmp(buffer, expected_encoding, encoded_len), 0);
  }

  // Verify read
  {
    RleBitPackedDecoder<uint64_t> decoder(buffer, len, bit_width);
    for (size_t i = 0; i < values.size(); ++i) {
      uint64_t val;
      bool result = decoder.Get(&val);
      EXPECT_TRUE(result);
      EXPECT_EQ(values[i], val);
    }
  }

  // Verify batch read
  {
    RleBitPackedDecoder<int> decoder(buffer, len, bit_width);
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
#ifdef __EMSCRIPTEN__
  // don't make this on the stack as it is
  // too big for emscripten
  std::vector<uint8_t> buffer_vec(static_cast<size_t>(len));
  uint8_t* buffer = buffer_vec.data();
#else
  uint8_t buffer[len];
#endif
  RleBitPackedEncoder encoder(buffer, len, bit_width);
  for (size_t i = 0; i < values.size(); ++i) {
    bool result = encoder.Put(values[i]);
    if (!result) {
      return false;
    }
  }
  int encoded_len = encoder.Flush();
  int out = 0;

  {
    RleBitPackedDecoder<int> decoder(buffer, encoded_len, bit_width);
    for (size_t i = 0; i < values.size(); ++i) {
      EXPECT_TRUE(decoder.Get(&out));
      if (values[i] != out) {
        return false;
      }
    }
  }

  // Verify batch read
  {
    RleBitPackedDecoder<int> decoder(buffer, encoded_len, bit_width);
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

TEST(RleBitPacked, SpecificSequences) {
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
    ValidateRleBitPacked(values, width, expected_buffer, 4);
  }

  for (int width = 9; width <= MAX_WIDTH; ++width) {
    ValidateRleBitPacked(values, width, nullptr,
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
  ValidateRleBitPacked(values, 1, expected_buffer, 1 + num_groups);
  for (int width = 2; width <= MAX_WIDTH; ++width) {
    int num_values = static_cast<int>(bit_util::CeilDiv(100, 8)) * 8;
    ValidateRleBitPacked(values, width, nullptr,
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

  ValidateRleBitPacked(values, 16, expected_buffer, 6);

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

  ValidateRleBitPacked(values, 32, expected_buffer, 10);
}

// ValidateRle on 'num_vals' values with width 'bit_width'. If 'value' != -1, that value
// is used, otherwise alternating values are used.
void TestRleValues(int bit_width, int num_vals, int value = -1) {
  const uint64_t mod = (bit_width == 64) ? 1 : 1LL << bit_width;
  std::vector<int> values;
  for (int v = 0; v < num_vals; ++v) {
    values.push_back((value != -1) ? value : static_cast<int>(v % mod));
  }
  ValidateRleBitPacked(values, bit_width, NULL, -1);
}

TEST(RleBitPacked, TestValues) {
  for (int width = 1; width <= MAX_WIDTH; ++width) {
    TestRleValues(width, 1);
    TestRleValues(width, 1024);
    TestRleValues(width, 1024, 0);
    TestRleValues(width, 1024, 1);
  }
}

TEST(RleBitPacked, BitWidthZeroRepeated) {
  uint8_t buffer[1];
  const int num_values = 15;
  buffer[0] = num_values << 1;  // repeated indicator byte
  RleBitPackedDecoder<uint8_t> decoder(buffer, sizeof(buffer), 0);
  uint8_t val;
  for (int i = 0; i < num_values; ++i) {
    bool result = decoder.Get(&val);
    EXPECT_TRUE(result);
    EXPECT_EQ(val, 0);  // can only encode 0s with bit width 0
  }
  EXPECT_FALSE(decoder.Get(&val));
}

TEST(RleBitPacked, BitWidthZeroLiteral) {
  uint8_t buffer[1];
  const int num_groups = 4;
  buffer[0] = num_groups << 1 | 1;  // literal indicator byte
  RleBitPackedDecoder<uint8_t> decoder = {buffer, sizeof(buffer), 0};
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
  ValidateRleBitPacked(values, 1, NULL, -1);
  values.push_back(1);
  ValidateRleBitPacked(values, 1, NULL, -1);
  values.push_back(1);
  ValidateRleBitPacked(values, 1, NULL, -1);
  values.push_back(1);
  ValidateRleBitPacked(values, 1, NULL, -1);
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

  ValidateRleBitPacked(values, 1, NULL, -1);
}

TEST(BitRle, Overflow) {
  for (int bit_width = 1; bit_width < 32; bit_width += 3) {
    int len = RleBitPackedEncoder::MinBufferSize(bit_width);
    std::vector<uint8_t> buffer(len);
    int num_added = 0;
    bool parity = true;

    RleBitPackedEncoder encoder(buffer.data(), len, bit_width);
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

    RleBitPackedDecoder<uint32_t> decoder(buffer.data(), bytes_written, bit_width);
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

/// Check RleBitPacked encoding/decoding round trip.
///
/// \param spaced If set to false, treat Nulls in the input array as regular data.
/// \param parts The number of parts in which the data will be decoded.
///         For number greater than one, this ensure that the decoder intermediate state
///         is valid.
template <typename Type>
void CheckRoundTrip(const Array& data, int bit_width, bool spaced, int32_t parts,
                    std::shared_ptr<FloatArray> dict = {}) {
  using ArrayType = typename TypeTraits<Type>::ArrayType;
  using value_type = typename Type::c_type;

  const int data_size = static_cast<int>(data.length());
  const int data_values_count =
      static_cast<int>(data.length() - spaced * data.null_count());
  const int buffer_size = RleBitPackedEncoder::MaxBufferSize(bit_width, data_size);
  ASSERT_GE(parts, 1);
  ASSERT_LE(parts, data_size);

  const value_type* data_values = static_cast<const ArrayType&>(data).raw_values();

  // Encode the data into `buffer` using the encoder.
  std::vector<uint8_t> buffer(buffer_size);
  RleBitPackedEncoder encoder(buffer.data(), buffer_size, bit_width);
  int32_t encoded_values_size = 0;
  for (int i = 0; i < data_size; ++i) {
    // Depending on `spaced` we treat nulls as regular values.
    if (data.IsValid(i) || !spaced) {
      bool success = encoder.Put(static_cast<uint64_t>(data_values[i]));
      ASSERT_TRUE(success) << "Encoding failed in pos " << i;
      ++encoded_values_size;
    }
  }
  int encoded_byte_size = encoder.Flush();
  ASSERT_EQ(encoded_values_size, data_values_count)
      << "All values input were not encoded successfully by the encoder";

  // Now we verify batch read
  RleBitPackedDecoder<value_type> decoder(buffer.data(), encoded_byte_size, bit_width);
  // We will only use one of them depending on whether this is a dictionary tests
  std::vector<float> dict_read;
  std::vector<value_type> values_read;
  if (dict) {
    dict_read.resize(data_size);
  } else {
    values_read.resize(data_size);
  }

  // We will read the data in `parts` calls to make sure intermediate states are valid
  rle_size_t total_read_count = 0;
  while (total_read_count < data_size) {
    const auto remaining = data_size - total_read_count;
    auto to_read = data_size / parts;
    if (remaining / to_read == 1) {
      to_read = remaining;
    }

    rle_size_t read = 0;
    if (spaced) {
      // We need to slice the input array get the proper null count and bitmap
      auto data_remaining = data.Slice(total_read_count, to_read);

      if (dict) {
        auto* out = dict_read.data() + total_read_count;
        read = decoder.GetBatchWithDictSpaced(
            dict->raw_values(), static_cast<int32_t>(dict->length()), out, to_read,
            static_cast<int32_t>(data_remaining->null_count()),
            data_remaining->null_bitmap_data(), data_remaining->offset());
      } else {
        auto* out = values_read.data() + total_read_count;
        read = decoder.GetBatchSpaced(
            to_read, static_cast<int32_t>(data_remaining->null_count()),
            data_remaining->null_bitmap_data(), data_remaining->offset(), out);
      }
    } else {
      if (dict) {
        auto* out = dict_read.data() + total_read_count;
        read = decoder.GetBatchWithDict(
            dict->raw_values(), static_cast<int32_t>(dict->length()), out, to_read);
      } else {
        auto* out = values_read.data() + total_read_count;
        read = decoder.GetBatch(out, to_read);
      }
    }
    ASSERT_EQ(read, to_read) << "Decoder did not read as many values as requested";

    total_read_count += read;
  }
  EXPECT_EQ(total_read_count, data_size) << "Total number of values read is off";

  // Verify the round trip: encoded-decoded values must equal the original one
  for (int64_t i = 0; i < data_size; ++i) {
    if (data.IsValid(i) || !spaced) {
      if (dict) {
        EXPECT_EQ(dict_read.at(i), dict->Value(data_values[i]))
            << "Encoded then decoded and mapped value at position " << i << " ("
            << values_read[i] << ") differs from original value (" << data_values[i]
            << " mapped to " << dict->Value(data_values[i]) << ")";
      } else {
        EXPECT_EQ(values_read.at(i), data_values[i])
            << "Encoded then decoded value at position " << i << " (" << values_read.at(i)
            << ") differs from original value (" << data_values[i] << ")";
      }
    }
  }
}

template <typename T>
struct DataTestRleBitPackedRandomPart {
  using value_type = T;

  value_type max;
  int32_t size;
  double null_probability;
};

template <typename T>
struct DataTestRleBitPackedRepeatPart {
  using value_type = T;

  value_type value;
  int32_t size;
  double null_probability;
};

template <typename T>
struct DataTestRleBitPackedNullPart {
  using value_type = T;

  int32_t size;
};

template <typename T>
struct DataTestRleBitPacked {
  using value_type = T;
  using ArrowType = typename arrow::CTypeTraits<value_type>::ArrowType;
  using RandomPart = DataTestRleBitPackedRandomPart<value_type>;
  using RepeatPart = DataTestRleBitPackedRepeatPart<value_type>;
  using NullPart = DataTestRleBitPackedNullPart<value_type>;
  using AnyPart = std::variant<RandomPart, RepeatPart, NullPart>;

  std::vector<AnyPart> parts;
  int32_t bit_width;

  std::shared_ptr<::arrow::Array> MakeArray(
      ::arrow::random::RandomArrayGenerator& rand) const {
    using Traits = arrow::TypeTraits<ArrowType>;

    std::vector<std::shared_ptr<::arrow::Array>> arrays = {};

    for (const auto& dyn_part : parts) {
      if (auto* part = std::get_if<RandomPart>(&dyn_part)) {
        auto arr = rand.Numeric<ArrowType>(part->size, /* min= */ value_type(0),
                                           part->max, part->null_probability);
        arrays.push_back(std::move(arr));

      } else if (auto* part = std::get_if<RepeatPart>(&dyn_part)) {
        auto arr =
            rand.Numeric<ArrowType>(part->size, /* min= */ part->value,
                                    /* max= */ part->value, part->null_probability);
        arrays.push_back(std::move(arr));

      } else if (auto* part = std::get_if<NullPart>(&dyn_part)) {
        EXPECT_OK_AND_ASSIGN(
            auto arr, ::arrow::MakeArrayOfNull(Traits::type_singleton(), part->size));
        arrays.push_back(std::move(arr));
      }
    }
    ARROW_DCHECK_EQ(parts.size(), arrays.size());

    return ::arrow::Concatenate(arrays).ValueOrDie();
  }
};

template <typename T>
void DoTestGetBatchSpacedRoundtrip() {
  using Data = DataTestRleBitPacked<T>;
  using ArrowType = typename Data::ArrowType;
  using RandomPart = typename Data::RandomPart;
  using NullPart = typename Data::NullPart;
  using RepeatPart = typename Data::RepeatPart;

  std::vector<Data> test_cases = {
      {
          {RandomPart{/* max=*/1, /* size=*/400, /* null_proba= */ 0.1}},
          /* bit_width= */ 1,
      },
      {
          {
              RandomPart{/* max=*/7, /* size=*/1037, /* null_proba= */ 0.0},
              NullPart{/* size= */ 1153},
              RandomPart{/* max=*/7, /* size=*/800, /* null_proba= */ 0.5},
          },
          /* bit_width= */ 3,
      },
      {
          {
              NullPart{/* size= */ 80},
              RandomPart{/* max=*/static_cast<T>(1023), /* size=*/800,
                         /* null_proba= */ 0.01},
              NullPart{/* size= */ 1023},
          },
          /* bit_width= */ 11,
      },
      {
          {RepeatPart{/* value=*/13, /* size=*/1024, /* null_proba= */ 0.01}},
          /* bit_width= */ 10,
      },
      {
          {
              NullPart{/* size= */ 1024},
              RepeatPart{/* value=*/static_cast<T>(10000), /* size=*/1025,
                         /* null_proba= */ 0.1},
              NullPart{/* size= */ 77},
          },
          /* bit_width= */ 23,
      },
      {
          {
              RepeatPart{/* value=*/13, /* size=*/1023, /* null_proba= */ 0.0},
              NullPart{/* size= */ 1153},
              RepeatPart{/* value=*/72, /* size=*/1799, /* null_proba= */ 0.5},
          },
          /* bit_width= */ 10,
      },
      {
          {
              RandomPart{/* max=*/1, /* size=*/1013, /* null_proba= */ 0.01},
              NullPart{/* size=*/8},
              RepeatPart{1, /* size= */ 256, /* null_proba= */ 0.1},
              NullPart{/* size=*/128},
              RepeatPart{0, /* size= */ 256, /* null_proba= */ 0.0},
              NullPart{/* size=*/15},
              RandomPart{/* max=*/1, /* size=*/1024, /* null_proba= */ 0.01},
          },
          /* bit_width= */ 1,
      },
  };

  ::arrow::random::RandomArrayGenerator rand(/* seed= */ 12);
  // FRAGILE: we create a dictionary large enough so that any encoded value from the
  // previous test cases can be used as an index in the dictionary.
  // Its size must be increased accordingly if larger values are encoded in the test
  // cases.
  auto dict = std::static_pointer_cast<arrow::FloatArray>(rand.Float32(20000, -1.0, 1.0));

  // Number of bits available in T to write a positive integer.
  constexpr int kBitsAvailable = 8 * sizeof(T) - (std::is_signed_v<T> ? 1 : 0);

  for (auto case_ : test_cases) {
    if (case_.bit_width > kBitsAvailable) {
      continue;
    }

    auto array = case_.MakeArray(rand);

    // Tests for GetBatch
    CheckRoundTrip<ArrowType>(*array, case_.bit_width, /* spaced= */ false,
                              /* parts= */ 1);
    CheckRoundTrip<ArrowType>(*array, case_.bit_width, /* spaced= */ false,
                              /* parts= */ 3);

    // Tests for GetBatchSpaced
    CheckRoundTrip<ArrowType>(*array, case_.bit_width, /* spaced= */ true,
                              /* parts= */ 1);
    CheckRoundTrip<ArrowType>(*array, case_.bit_width, /* spaced= */ true,
                              /* parts= */ 7);
    CheckRoundTrip<ArrowType>(*array->Slice(1), case_.bit_width, /* spaced= */ true,
                              /* parts= */ 1);

    // Cannot test GetBatchWithDict with this method since unknown null values

    // Tests for GetBatchWithDictSpaced
    CheckRoundTrip<ArrowType>(*array, case_.bit_width, /* spaced= */ true, /* parts= */ 1,
                              dict);
    CheckRoundTrip<ArrowType>(*array, case_.bit_width, /* spaced= */ true, /* parts= */ 5,
                              dict);
  }
}

TEST(RleBitPacked, GetBatchSpacedRoundtripUint8) {
  DoTestGetBatchSpacedRoundtrip<uint8_t>();
}
TEST(RleBitPacked, GetBatchSpacedRoundtripUint16) {
  DoTestGetBatchSpacedRoundtrip<uint16_t>();
}
TEST(RleBitPacked, GetBatchSpacedRoundtripInt32) {
  DoTestGetBatchSpacedRoundtrip<int32_t>();
}
TEST(RleBitPacked, GetBatchSpacedRoundtripUInt32) {
  DoTestGetBatchSpacedRoundtrip<uint32_t>();
}
TEST(RleBitPacked, GetBatchSpacedRoundtripUint64) {
  DoTestGetBatchSpacedRoundtrip<uint64_t>();
}

}  // namespace arrow::util
