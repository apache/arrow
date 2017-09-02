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

#include <cstddef>
#include <cstdint>
#include <memory>
#include <vector>

#include <gtest/gtest.h>

#include "arrow/array.h"
#include "arrow/builder.h"
#include "arrow/memory_pool.h"
#include "arrow/status.h"
#include "arrow/test-util.h"
#include "arrow/type.h"
#include "arrow/util/bit-util.h"
#include "arrow/util/decimal.h"

using std::size_t;

namespace arrow {

class Buffer;

namespace decimal {

template <typename T>
class DecimalTestBase {
 public:
  DecimalTestBase() : pool_(default_memory_pool()) {}

  virtual std::vector<uint8_t> MakeData(const std::vector<T>& input,
                                        size_t byte_width) const = 0;

  void InitBuilder(const std::shared_ptr<DecimalType>& type, const std::vector<T>& draw,
                   const std::vector<uint8_t>& valid_bytes, int byte_width,
                   std::shared_ptr<DecimalBuilder>* builder, size_t* null_count) const {
    *builder = std::make_shared<DecimalBuilder>(type, pool_);

    size_t size = draw.size();
    ASSERT_OK((*builder)->Reserve(size));

    for (size_t i = 0; i < size; ++i) {
      if (valid_bytes[i]) {
        ASSERT_OK((*builder)->Append(draw[i]));
      } else {
        ASSERT_OK((*builder)->AppendNull());
        ++*null_count;
      }
    }
  }

  void TestCreate(int precision, const std::vector<T>& draw,
                  const std::vector<uint8_t>& valid_bytes, int64_t offset) const {
    auto type = std::make_shared<DecimalType>(precision, 4);

    std::shared_ptr<DecimalBuilder> builder;

    size_t null_count = 0;

    const size_t size = draw.size();
    const int byte_width = type->byte_width();

    InitBuilder(type, draw, valid_bytes, byte_width, &builder, &null_count);

    auto raw_bytes = MakeData(draw, static_cast<size_t>(byte_width));
    auto expected_data = std::make_shared<Buffer>(raw_bytes.data(), size * byte_width);
    std::shared_ptr<Buffer> expected_null_bitmap;
    ASSERT_OK(BitUtil::BytesToBits(valid_bytes, &expected_null_bitmap));

    int64_t expected_null_count = test::null_count(valid_bytes);
    auto expected = std::make_shared<DecimalArray>(
        type, size, expected_data, expected_null_bitmap, expected_null_count, 0);

    std::shared_ptr<Array> out;
    ASSERT_OK(builder->Finish(&out));
    ASSERT_TRUE(out->Slice(offset)->Equals(
        *expected->Slice(offset, expected->length() - offset)));
  }

 private:
  MemoryPool* pool_;
};

template <typename T>
class DecimalTest : public DecimalTestBase<T> {
 public:
  std::vector<uint8_t> MakeData(const std::vector<T>& input,
                                size_t byte_width) const override {
    std::vector<uint8_t> result(input.size() * byte_width);
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
  std::vector<uint8_t> MakeData(const std::vector<Decimal128>& input,
                                size_t byte_width) const override {
    std::vector<uint8_t> result;
    result.reserve(input.size() * byte_width);
    constexpr static const size_t bytes_per_element = 16;
    for (size_t i = 0; i < input.size(); ++i) {
      uint8_t stack_bytes[bytes_per_element] = {0};
      uint8_t* bytes = stack_bytes;
      ToBytes(input[i], &bytes);

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
  std::vector<Decimal32> draw = {Decimal32(1), Decimal32(2), Decimal32(2389),
                                 Decimal32(4), Decimal32(-12348)};
  std::vector<uint8_t> valid_bytes = {true, true, true, true, true};
  this->TestCreate(precision, draw, valid_bytes, 0);
  this->TestCreate(precision, draw, valid_bytes, 2);
}

TEST_P(Decimal64BuilderTest, NoNulls) {
  int precision = GetParam();
  std::vector<Decimal64> draw = {Decimal64(1), Decimal64(2), Decimal64(2389),
                                 Decimal64(4), Decimal64(-12348)};
  std::vector<uint8_t> valid_bytes = {true, true, true, true, true};
  this->TestCreate(precision, draw, valid_bytes, 0);
  this->TestCreate(precision, draw, valid_bytes, 2);
}

TEST_P(Decimal128BuilderTest, NoNulls) {
  int precision = GetParam();
  std::vector<Decimal128> draw = {Decimal128(1), Decimal128(-2), Decimal128(2389),
                                  Decimal128(4), Decimal128(-12348)};
  std::vector<uint8_t> valid_bytes = {true, true, true, true, true};
  this->TestCreate(precision, draw, valid_bytes, 0);
  this->TestCreate(precision, draw, valid_bytes, 2);
}

TEST_P(Decimal32BuilderTest, WithNulls) {
  int precision = GetParam();
  std::vector<Decimal32> draw = {Decimal32(1), Decimal32(2), Decimal32(-1), Decimal32(4),
                                 Decimal32(-1)};
  std::vector<uint8_t> valid_bytes = {true, true, false, true, false};
  this->TestCreate(precision, draw, valid_bytes, 0);
  this->TestCreate(precision, draw, valid_bytes, 2);
}

TEST_P(Decimal64BuilderTest, WithNulls) {
  int precision = GetParam();
  std::vector<Decimal64> draw = {Decimal64(-1), Decimal64(2), Decimal64(-1), Decimal64(4),
                                 Decimal64(-1)};
  std::vector<uint8_t> valid_bytes = {true, true, false, true, false};
  this->TestCreate(precision, draw, valid_bytes, 0);
  this->TestCreate(precision, draw, valid_bytes, 2);
}

TEST_P(Decimal128BuilderTest, WithNulls) {
  int precision = GetParam();
  std::vector<Decimal128> draw = {Decimal128(1),
                                  Decimal128(2),
                                  Decimal128(-1),
                                  Decimal128(4),
                                  Decimal128(-1),
                                  Decimal128(1),
                                  Decimal128(2),
                                  Decimal128("230342903942.234234"),
                                  Decimal128("-23049302932.235234")};
  std::vector<uint8_t> valid_bytes = {true, true, false, true, false,
                                      true, true, true,  true};
  this->TestCreate(precision, draw, valid_bytes, 0);
  this->TestCreate(precision, draw, valid_bytes, 2);
}

INSTANTIATE_TEST_CASE_P(Decimal32BuilderTest, Decimal32BuilderTest,
                        ::testing::Range(DecimalPrecision<int32_t>::minimum,
                                         DecimalPrecision<int32_t>::maximum));
INSTANTIATE_TEST_CASE_P(Decimal64BuilderTest, Decimal64BuilderTest,
                        ::testing::Range(DecimalPrecision<int64_t>::minimum,
                                         DecimalPrecision<int64_t>::maximum));
INSTANTIATE_TEST_CASE_P(Decimal128BuilderTest, Decimal128BuilderTest,
                        ::testing::Range(DecimalPrecision<Int128>::minimum,
                                         DecimalPrecision<Int128>::maximum));

}  // namespace decimal
}  // namespace arrow
