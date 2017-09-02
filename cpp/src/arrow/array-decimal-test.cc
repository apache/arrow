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
#include "arrow/test-util.h"
#include "arrow/util/decimal.h"
#include "arrow/util/int128.h"

using std::size_t;

namespace arrow {

class Buffer;

namespace decimal {

using DecimalVector = std::vector<Int128>;

class DecimalTest : public ::testing::TestWithParam<int> {
 public:
  DecimalTest() {}

  template <size_t BYTE_WIDTH = 16>
  void MakeData(const DecimalVector& input, std::vector<uint8_t>* out) const {
    out->reserve(input.size() * BYTE_WIDTH);

    std::array<uint8_t, BYTE_WIDTH> bytes{{0}};

    for (const auto& value : input) {
      ASSERT_OK(value.ToBytes(&bytes));
      out->insert(out->end(), bytes.cbegin(), bytes.cend());
    }
  }

  template <size_t BYTE_WIDTH = 16>
  void TestCreate(int precision, const DecimalVector& draw,
                  const std::vector<uint8_t>& valid_bytes, int64_t offset) const {
    auto type = std::make_shared<DecimalType>(precision, 4);

    auto builder = std::make_shared<DecimalBuilder>(type);

    size_t null_count = 0;

    const size_t size = draw.size();

    ASSERT_OK(builder->Reserve(size));

    for (size_t i = 0; i < size; ++i) {
      if (valid_bytes[i]) {
        ASSERT_OK(builder->Append(draw[i]));
      } else {
        ASSERT_OK(builder->AppendNull());
        ++null_count;
      }
    }

    std::shared_ptr<Array> out;
    ASSERT_OK(builder->Finish(&out));

    std::vector<uint8_t> raw_bytes;

    raw_bytes.reserve(size * BYTE_WIDTH);
    MakeData<BYTE_WIDTH>(draw, &raw_bytes);

    auto expected_data = std::make_shared<Buffer>(raw_bytes.data(), BYTE_WIDTH);
    std::shared_ptr<Buffer> expected_null_bitmap;
    ASSERT_OK(BitUtil::BytesToBits(valid_bytes, &expected_null_bitmap));

    int64_t expected_null_count = test::null_count(valid_bytes);
    auto expected = std::make_shared<DecimalArray>(
        type, size, expected_data, expected_null_bitmap, expected_null_count);

    std::shared_ptr<Array> lhs = out->Slice(offset);
    std::shared_ptr<Array> rhs = expected->Slice(offset);
    bool result = lhs->Equals(rhs);
    ASSERT_TRUE(result);
  }
};

TEST_P(DecimalTest, NoNulls) {
  int precision = GetParam();
  std::vector<Int128> draw = {Int128(1), Int128(-2), Int128(2389), Int128(4),
                              Int128(-12348)};
  std::vector<uint8_t> valid_bytes = {true, true, true, true, true};
  this->TestCreate(precision, draw, valid_bytes, 0);
  this->TestCreate(precision, draw, valid_bytes, 2);
}

TEST_P(DecimalTest, WithNulls) {
  int precision = GetParam();
  std::vector<Int128> draw = {Int128(1),  Int128(2), Int128(-1), Int128(4),
                              Int128(-1), Int128(1), Int128(2)};
  Int128 big;
  ASSERT_OK(FromString("230342903942.234234", &big));
  draw.push_back(big);

  Int128 big_negative;
  ASSERT_OK(FromString("-23049302932.235234", &big_negative));
  draw.push_back(big_negative);

  std::vector<uint8_t> valid_bytes = {true, true, false, true, false,
                                      true, true, true,  true};
  this->TestCreate(precision, draw, valid_bytes, 0);
  this->TestCreate(precision, draw, valid_bytes, 2);
}

INSTANTIATE_TEST_CASE_P(DecimalTest, DecimalTest, ::testing::Range(1, 38));

}  // namespace decimal
}  // namespace arrow
