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

#include <gtest/gtest.h>

#include "arrow/chunked_array.h"
#include "arrow/compute/api_vector.h"
#include "arrow/compute/kernels/test_util.h"
#include "arrow/testing/generator.h"
#include "arrow/testing/gtest_util.h"

namespace arrow::compute {

// ----------------------------------------------------------------------
// ReverseIndices tests

namespace {

Result<Datum> ReverseIndices(const Datum& indices, int64_t output_length,
                             std::shared_ptr<DataType> output_type) {
  ReverseIndicesOptions options{output_length, std::move(output_type)};
  return ReverseIndices(indices, options);
}

}  // namespace

TEST(ReverseIndices, InvalidOutputType) {
  {
    ARROW_SCOPED_TRACE("Output type float");
    auto indices = ArrayFromJSON(int32(), "[]");
    ASSERT_RAISES_WITH_MESSAGE(
        Invalid, "Invalid: Output type of reverse_indices must be integer, got float",
        ReverseIndices(indices, 0, float32()));
  }
  {
    ARROW_SCOPED_TRACE("Output type string");
    auto indices = ArrayFromJSON(int32(), "[]");
    ASSERT_RAISES_WITH_MESSAGE(
        Invalid, "Invalid: Output type of reverse_indices must be integer, got string",
        ReverseIndices(indices, 0, utf8()));
  }
}

namespace {

static const std::vector<std::shared_ptr<DataType>> kIntegerTypes = {
    int8(), uint8(), int16(), uint16(), int32(), uint32(), int64(), uint64()};

}  // namespace

TEST(ReverseIndices, DefaultOptions) {
  {
    ARROW_SCOPED_TRACE("Default options values");
    ReverseIndicesOptions options;
    ASSERT_EQ(options.output_length, -1);
    ASSERT_EQ(options.output_type, nullptr);
  }
  {
    ARROW_SCOPED_TRACE("Default options semantics");
    for (const auto& input_type : kIntegerTypes) {
      ARROW_SCOPED_TRACE("Input type: " + input_type->ToString());
      auto indices = ArrayFromJSON(input_type, "[0]");
      ASSERT_OK_AND_ASSIGN(Datum result, ReverseIndices(indices));
      AssertDatumsEqual(result, indices);
    }
  }
}

template <typename ArrowType>
class TestReverseIndicesSmallOutputType : public ::testing::Test {
 protected:
  using CType = typename TypeTraits<ArrowType>::CType;

  std::shared_ptr<DataType> type_singleton() {
    return TypeTraits<ArrowType>::type_singleton();
  }

  void JustEnoughOutputType() {
    auto output_type = type_singleton();
    ReverseIndicesOptions options{1, output_type};
    int64_t input_length = static_cast<int64_t>(std::numeric_limits<CType>::max());
    auto expected = ConstantArrayGenerator::Numeric<ArrowType>(
        1, static_cast<CType>(input_length - 1));
    for (const auto& input_type : kIntegerTypes) {
      ARROW_SCOPED_TRACE("Input type: " + input_type->ToString());
      auto indices = ConstantArrayGenerator::Zeroes(input_length, input_type);
      ASSERT_OK_AND_ASSIGN(Datum result, ReverseIndices(indices, options));
      AssertDatumsEqual(expected, result);
    }
  }

  void InsufficientOutputType() {
    auto output_type = type_singleton();
    int64_t input_length = static_cast<int64_t>(std::numeric_limits<CType>::max()) + 1;
    for (const auto& input_type : kIntegerTypes) {
      ARROW_SCOPED_TRACE("Input type: " + input_type->ToString());
      auto indices = ConstantArrayGenerator::Zeroes(input_length, int64());
      ReverseIndicesOptions options{1, output_type};
      ASSERT_RAISES_WITH_MESSAGE(
          Invalid,
          "Invalid: Output type " + output_type->ToString() +
              " of reverse_indices is insufficient to store indices of length " +
              std::to_string(input_length),
          ReverseIndices(indices, options));
    }
  }
};

using SmallOutputTypes = ::testing::Types<UInt8Type, UInt16Type, Int8Type, Int16Type>;
TYPED_TEST_SUITE(TestReverseIndicesSmallOutputType, SmallOutputTypes);

TYPED_TEST(TestReverseIndicesSmallOutputType, JustEnoughOutputType) {
  this->JustEnoughOutputType();
}

TYPED_TEST(TestReverseIndicesSmallOutputType, InsufficientOutputType) {
  this->InsufficientOutputType();
}

namespace {

template <typename InputString, typename InputShapeFunc>
void TestReverseIndices(const InputString& indices_str, int64_t output_length,
                        const std::string& expected_str,
                        InputShapeFunc&& input_shape_func, bool validity_must_be_null) {
  for (const auto& input_type : kIntegerTypes) {
    auto indices = input_shape_func(input_type, indices_str);
    ARROW_SCOPED_TRACE("Input type: " + input_type->ToString());
    for (const auto& output_type : kIntegerTypes) {
      ARROW_SCOPED_TRACE("Output type: " + output_type->ToString());
      auto expected = ArrayFromJSON(output_type, expected_str);
      ASSERT_OK_AND_ASSIGN(Datum result,
                           ReverseIndices(indices, output_length, output_type));
      AssertDatumsEqual(expected, result);
      if (validity_must_be_null) {
        ASSERT_FALSE(result.array()->HasValidityBitmap());
      }
    }
  }
}

void TestReverseIndices(const std::string& indices_str,
                        const std::vector<std::string>& indices_chunked_str,
                        int64_t output_length, const std::string& expected_str,
                        bool validity_must_be_null = false) {
  {
    ARROW_SCOPED_TRACE("Array");
    TestReverseIndices(indices_str, output_length, expected_str, ArrayFromJSON,
                       validity_must_be_null);
  }
  {
    ARROW_SCOPED_TRACE("Chunked");
    TestReverseIndices(indices_chunked_str, output_length, expected_str,
                       ChunkedArrayFromJSON, validity_must_be_null);
  }
}

}  // namespace

TEST(ReverseIndices, Basic) {
  {
    ARROW_SCOPED_TRACE("Basic");
    auto indices = "[9, 7, 5, 3, 1, 0, 2, 4, 6, 8]";
    std::vector<std::string> indices_chunked{
        "[]", "[9, 7, 5, 3, 1]", "[0]", "[2, 4, 6]", "[8]", "[]"};
    int64_t output_length = 10;
    auto expected = "[5, 4, 6, 3, 7, 2, 8, 1, 9, 0]";
    TestReverseIndices(indices, indices_chunked, output_length, expected,
                       /*validity_must_be_null=*/true);
  }
  {
    ARROW_SCOPED_TRACE("Empty output");
    auto indices = "[1, 2]";
    std::vector<std::string> indices_chunked{"[]", "[1]", "[]", "[]", "[2]", "[]"};
    int64_t output_length = 0;
    auto expected = "[]";
    TestReverseIndices(indices, indices_chunked, output_length, expected,
                       /*validity_must_be_null=*/true);
  }
  {
    ARROW_SCOPED_TRACE("Output less than input");
    auto indices = "[1, 0]";
    std::vector<std::string> indices_chunked{"[]", "[]", "[]", "[1, 0]"};
    int64_t output_length = 1;
    auto expected = "[1]";
    TestReverseIndices(indices, indices_chunked, output_length, expected,
                       /*validity_must_be_null=*/true);
  }
  {
    ARROW_SCOPED_TRACE("Output greater than input");
    auto indices = "[1, 2]";
    std::vector<std::string> indices_chunked{"[]", "[1]", "[]", "[2]"};
    int64_t output_length = 7;
    auto expected = "[null, 0, 1, null, null, null, null]";
    TestReverseIndices(indices, indices_chunked, output_length, expected);
  }
  {
    ARROW_SCOPED_TRACE("Input all null");
    auto indices = "[null, null]";
    std::vector<std::string> indices_chunked{"[]", "[null]", "[]", "[null]"};
    int64_t output_length = 1;
    auto expected = "[null]";
    TestReverseIndices(indices, indices_chunked, output_length, expected);
  }
  {
    ARROW_SCOPED_TRACE("Output all null");
    auto indices = "[1, 2]";
    std::vector<std::string> indices_chunked{"[]", "[1]", "[]", "[2]"};
    int64_t output_length = 1;
    auto expected = "[null]";
    TestReverseIndices(indices, indices_chunked, output_length, expected);
  }
  {
    ARROW_SCOPED_TRACE("Empty input output null");
    auto indices = "[]";
    std::vector<std::string> indices_chunked{"[]", "[]", "[]", "[]"};
    int64_t output_length = 7;
    auto expected = "[null, null, null, null, null, null, null]";
    TestReverseIndices(indices, indices_chunked, output_length, expected);
  }
  {
    ARROW_SCOPED_TRACE("Input duplicated indices");
    auto indices = "[1, 2, 3, 1, 2, 3, 1, 2, 3]";
    std::vector<std::string> indices_chunked{"[]", "[1, 2]", "[3, 1, 2, 3, 1]",
                                             "[]", "[2]",    "[3]"};
    int64_t output_length = 5;
    auto expected = "[null, 6, 7, 8, null]";
    TestReverseIndices(indices, indices_chunked, output_length, expected);
  }
}

// ----------------------------------------------------------------------
// Permute tests

TEST(Permute, Basic) {
  {
    auto values = ArrayFromJSON(int64(), "[10, 11, 12, 13, 14, 15, 16, 17, 18, 19]");
    auto indices = ArrayFromJSON(int64(), "[9, 8, 7, 6, 5, 4, 3, 2, 1, 0]");
    auto expected = ArrayFromJSON(int64(), "[19, 18, 17, 16, 15, 14, 13, 12, 11, 10]");
    PermuteOptions options{10};
    ASSERT_OK_AND_ASSIGN(Datum result,
                         CallFunction("permute", {values, indices}, &options));
    AssertDatumsEqual(expected, result);
  }
  {
    auto values = ArrayFromJSON(int64(), "[0, 0, 0, 1, 1, 1]");
    auto indices = ArrayFromJSON(int64(), "[0, 3, 6, 1, 4, 7]");
    auto expected = ArrayFromJSON(int64(), "[0, 1, null, 0, 1, null, 0, 1, null]");
    PermuteOptions options{9};
    ASSERT_OK_AND_ASSIGN(Datum result,
                         CallFunction("permute", {values, indices}, &options));
    AssertDatumsEqual(expected, result);
  }
}

};  // namespace arrow::compute
