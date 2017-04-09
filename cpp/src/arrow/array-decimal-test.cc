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

#include "arrow/type.h"
#include "gtest/gtest.h"

#include "arrow/builder.h"
#include "arrow/test-util.h"
#include "arrow/util/decimal.h"

namespace arrow {

TEST(TypesTest, TestDecimal32Type) {
  DecimalType t1(8, 4);

  ASSERT_EQ(t1.type, Type::DECIMAL);
  ASSERT_EQ(t1.precision, 8);
  ASSERT_EQ(t1.scale, 4);

  ASSERT_EQ(t1.ToString(), std::string("decimal(8, 4)"));

  // Test properties
  ASSERT_EQ(t1.byte_width(), 4);
  ASSERT_EQ(t1.bit_width(), 32);
}

TEST(TypesTest, TestDecimal64Type) {
  DecimalType t1(12, 5);

  ASSERT_EQ(t1.type, Type::DECIMAL);
  ASSERT_EQ(t1.precision, 12);
  ASSERT_EQ(t1.scale, 5);

  ASSERT_EQ(t1.ToString(), std::string("decimal(12, 5)"));

  // Test properties
  ASSERT_EQ(t1.byte_width(), 8);
  ASSERT_EQ(t1.bit_width(), 64);
}

TEST(TypesTest, TestDecimal128Type) {
  DecimalType t1(27, 7);

  ASSERT_EQ(t1.type, Type::DECIMAL);
  ASSERT_EQ(t1.precision, 27);
  ASSERT_EQ(t1.scale, 7);

  ASSERT_EQ(t1.ToString(), std::string("decimal(27, 7)"));

  // Test properties
  ASSERT_EQ(t1.byte_width(), 16);
  ASSERT_EQ(t1.bit_width(), 128);
}

template <typename T>
class DecimalTestBase {
 public:
  virtual std::vector<uint8_t> data(
      const std::vector<T>& input, size_t byte_width) const = 0;

  void test(int precision, const std::vector<T>& draw,
      const std::vector<uint8_t>& valid_bytes,
      const std::vector<uint8_t>& sign_bitmap = {}, int64_t offset = 0) const {
    auto type = std::make_shared<DecimalType>(precision, 4);
    int byte_width = type->byte_width();
    auto pool = default_memory_pool();
    auto builder = std::make_shared<DecimalBuilder>(pool, type);
    size_t null_count = 0;

    size_t size = draw.size();
    builder->Reserve(size);

    for (size_t i = 0; i < size; ++i) {
      if (valid_bytes[i]) {
        builder->Append(draw[i]);
      } else {
        builder->AppendNull();
        ++null_count;
      }
    }

    std::shared_ptr<Buffer> expected_sign_bitmap;
    if (!sign_bitmap.empty()) {
      BitUtil::BytesToBits(sign_bitmap, &expected_sign_bitmap);
    }

    auto raw_bytes = data(draw, byte_width);
    auto expected_data = std::make_shared<Buffer>(raw_bytes.data(), size * byte_width);
    auto expected_null_bitmap = test::bytes_to_null_buffer(valid_bytes);
    int64_t expected_null_count = test::null_count(valid_bytes);
    auto expected = std::make_shared<DecimalArray>(type, size, expected_data,
        expected_null_bitmap, expected_null_count, offset, expected_sign_bitmap);

    std::shared_ptr<Array> out;
    ASSERT_OK(builder->Finish(&out));
    ASSERT_TRUE(out->Equals(*expected));
  }
};

template <typename T>
class DecimalTest : public DecimalTestBase<T> {
 public:
  std::vector<uint8_t> data(
      const std::vector<T>& input, size_t byte_width) const override {
    std::vector<uint8_t> result;
    result.reserve(input.size() * byte_width);
    // TODO(phillipc): There's probably a better way to do this
    constexpr static const size_t bytes_per_element = sizeof(T);
    for (size_t i = 0, j = 0; i < input.size(); ++i, j += bytes_per_element) {
      *reinterpret_cast<typename T::value_type*>(&result[j]) = input[i].value;
    }
    return result;
  }
};

template <>
class DecimalTest<Decimal128> : public DecimalTestBase<Decimal128> {
 public:
  std::vector<uint8_t> data(
      const std::vector<Decimal128>& input, size_t byte_width) const override {
    std::vector<uint8_t> result;
    result.reserve(input.size() * byte_width);
    constexpr static const size_t bytes_per_element = 16;
    for (size_t i = 0; i < input.size(); ++i) {
      uint8_t stack_bytes[bytes_per_element] = {0};
      uint8_t* bytes = stack_bytes;
      bool is_negative;
      ToBytes(input[i], &bytes, &is_negative);

      for (size_t i = 0; i < bytes_per_element; ++i) {
        result.push_back(bytes[i]);
      }
    }
    return result;
  }
};

class Decimal32BuilderTest : public ::testing::TestWithParam<int>,
                             public DecimalTest<Decimal32> {};

class Decimal64BuilderTest : public ::testing::TestWithParam<int>,
                             public DecimalTest<Decimal64> {};

class Decimal128BuilderTest : public ::testing::TestWithParam<int>,
                              public DecimalTest<Decimal128> {};

TEST_P(Decimal32BuilderTest, NoNulls) {
  int precision = GetParam();
  std::vector<Decimal32> draw = {
      Decimal32(1), Decimal32(2), Decimal32(2389), Decimal32(4), Decimal32(-12348)};
  std::vector<uint8_t> valid_bytes = {true, true, true, true, true};
  this->test(precision, draw, valid_bytes);
}

TEST_P(Decimal64BuilderTest, NoNulls) {
  int precision = GetParam();
  std::vector<Decimal64> draw = {
      Decimal64(1), Decimal64(2), Decimal64(2389), Decimal64(4), Decimal64(-12348)};
  std::vector<uint8_t> valid_bytes = {true, true, true, true, true};
  this->test(precision, draw, valid_bytes);
}

TEST_P(Decimal128BuilderTest, NoNulls) {
  int precision = GetParam();
  std::vector<Decimal128> draw = {
      Decimal128(1), Decimal128(-2), Decimal128(2389), Decimal128(4), Decimal128(-12348)};
  std::vector<uint8_t> valid_bytes = {true, true, true, true, true};
  std::vector<uint8_t> sign_bitmap = {false, true, false, false, true};
  this->test(precision, draw, valid_bytes, sign_bitmap);
}

TEST_P(Decimal32BuilderTest, WithNulls) {
  int precision = GetParam();
  std::vector<Decimal32> draw = {
      Decimal32(1), Decimal32(2), Decimal32(-1), Decimal32(4), Decimal32(-1)};
  std::vector<uint8_t> valid_bytes = {true, true, false, true, false};
  this->test(precision, draw, valid_bytes);
}

TEST_P(Decimal64BuilderTest, WithNulls) {
  int precision = GetParam();
  std::vector<Decimal64> draw = {
      Decimal64(-1), Decimal64(2), Decimal64(-1), Decimal64(4), Decimal64(-1)};
  std::vector<uint8_t> valid_bytes = {true, true, false, true, false};
  this->test(precision, draw, valid_bytes);
}

TEST_P(Decimal128BuilderTest, WithNulls) {
  int precision = GetParam();
  std::vector<Decimal128> draw = {Decimal128(1), Decimal128(2), Decimal128(-1),
      Decimal128(4), Decimal128(-1), Decimal128(1), Decimal128(2),
      Decimal128("230342903942.234234"), Decimal128("-23049302932.235234")};
  std::vector<uint8_t> valid_bytes = {
      true, true, false, true, false, true, true, true, true};
  std::vector<uint8_t> sign_bitmap = {
      false, false, false, false, false, false, false, false, true};
  this->test(precision, draw, valid_bytes, sign_bitmap);
}

INSTANTIATE_TEST_CASE_P(Decimal32BuilderTest, Decimal32BuilderTest,
    ::testing::Range(
        DecimalPrecision<int32_t>::minimum, DecimalPrecision<int32_t>::maximum));
INSTANTIATE_TEST_CASE_P(Decimal64BuilderTest, Decimal64BuilderTest,
    ::testing::Range(
        DecimalPrecision<int64_t>::minimum, DecimalPrecision<int64_t>::maximum));
INSTANTIATE_TEST_CASE_P(Decimal128BuilderTest, Decimal128BuilderTest,
    ::testing::Range(
        DecimalPrecision<int128_t>::minimum, DecimalPrecision<int128_t>::maximum));

}  // namespace arrow
