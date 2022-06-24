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

#include "arrow/array.h"
#include "arrow/builder.h"
#include "arrow/compute/api_vector.h"
#include "arrow/testing/gtest_util.h"

namespace arrow {
namespace compute {

struct RLETestData {
  static RLETestData JSON(std::shared_ptr<DataType> data_type, std::string input_json,
                          std::string expected_values_json,
                          std::vector<int64_t> expected_run_lengths,
                          int64_t input_offset = 0) {
    auto input_array = ArrayFromJSON(data_type, input_json);
    return {.input = input_array->Slice(input_offset),
            .expected_values = ArrayFromJSON(data_type, expected_values_json),
            .expected_run_lengths = std::move(expected_run_lengths)};
  }

  template <typename ArrowType>
  static RLETestData TypeMinMaxNull() {
    using CType = typename ArrowType::c_type;
    RLETestData result;
    NumericBuilder<ArrowType> builder;
    ARROW_EXPECT_OK(builder.Append(std::numeric_limits<CType>::min()));
    ARROW_EXPECT_OK(builder.AppendNull());
    ARROW_EXPECT_OK(builder.Append(std::numeric_limits<CType>::max()));
    result.input = *builder.Finish();
    result.expected_values = result.input;
    result.expected_run_lengths = {1, 2, 3};
    return result;
  }

  std::shared_ptr<Array> input;
  std::shared_ptr<Array> expected_values;
  std::vector<int64_t> expected_run_lengths;
};

class TestRunLengthEncode : public ::testing::TestWithParam<RLETestData> {};

TEST_P(TestRunLengthEncode, EncodeDecodeArray) {
  auto data = GetParam();

  ASSERT_OK_AND_ASSIGN(Datum encoded_datum, RunLengthEncode(data.input));

  auto encoded = encoded_datum.array();
  const int64_t* run_lengths_buffer = encoded->GetValues<int64_t>(0);
  ASSERT_EQ(std::vector<int64_t>(run_lengths_buffer,
                                 run_lengths_buffer + encoded->child_data[0]->length),
            data.expected_run_lengths);
  auto values_array = MakeArray(encoded->child_data[0]);
  ASSERT_OK(values_array->ValidateFull());
  ASSERT_TRUE(values_array->Equals(data.expected_values));
  ASSERT_EQ(encoded->buffers[0]->size(),
            data.expected_run_lengths.size() * sizeof(uint64_t));
  ASSERT_EQ(encoded->length, data.input->length());
  ASSERT_EQ(*encoded->type, RunLengthEncodedType(data.input->type()));
  ASSERT_EQ(encoded->null_count, data.input->null_count());

  ASSERT_OK_AND_ASSIGN(Datum decoded_datum, RunLengthDecode(encoded));
  auto decoded = decoded_datum.make_array();
  ASSERT_OK(decoded->ValidateFull());
  ASSERT_TRUE(decoded->Equals(data.input));
}

// Encoding an input with an offset results in a completely new encoded array without an
// offset. This means The EncodeDecodeArray test will never actually decode an array
// with an offset, even though we have inputs with offsets. This test slices one element
// off the encoded array and decodes that.
TEST_P(TestRunLengthEncode, DecodeWithOffset) {
  auto data = GetParam();

  ASSERT_OK_AND_ASSIGN(Datum encoded_datum, RunLengthEncode(data.input));

  auto encoded_array = encoded_datum.make_array();
  ASSERT_OK_AND_ASSIGN(Datum decoded_datum, RunLengthDecode(encoded_array->Slice(1)));
  auto decoded_array = decoded_datum.make_array();
  ASSERT_OK(encoded_array->ValidateFull());
  ASSERT_TRUE(encoded_array->Equals(data.input->Slice(1)));
}

// TODO: test offset in child array

INSTANTIATE_TEST_SUITE_P(
    EncodeArrayTests, TestRunLengthEncode,
    ::testing::Values(
        RLETestData::JSON(int32(), "[1, 1, 0, -5, -5, -5, 255, 255]", "[1, 0, -5, 255]",
                          {2, 3, 6, 8}),
        RLETestData::JSON(uint32(), "[null, 1, 1, null, null, 5]", "[null, 1, null, 5]",
                          {1, 3, 5, 6}),
        RLETestData::JSON(boolean(), "[true, true, true, false, false]", "[true, false]",
                          {3, 5}),
        RLETestData::JSON(boolean(), "[true, true, true, false, null, null, false]",
                          "[true, false, null, false]", {3, 4, 6, 7}),
        RLETestData::JSON(int32(), "[1, 1, 0, -5, -5, -5, 255, 255]", "[-5, 255]", {3, 5},
                          3),
        RLETestData::JSON(uint32(), "[4, 5, 5, null, null, 5]", "[5, null, 5]", {1, 3, 4},
                          2),
        RLETestData::JSON(boolean(), "[true, true, false, false, true]", "[false, true]",
                          {2, 3}, 2),
        RLETestData::JSON(boolean(), "[true, true, true, false, null, null, false]",
                          "[null, false]", {1, 2}, 5),
        RLETestData::TypeMinMaxNull<Int8Type>(), RLETestData::TypeMinMaxNull<UInt8Type>(),
        RLETestData::TypeMinMaxNull<Int16Type>(),
        RLETestData::TypeMinMaxNull<UInt16Type>(),
        RLETestData::TypeMinMaxNull<Int32Type>(),
        RLETestData::TypeMinMaxNull<UInt32Type>(),
        RLETestData::TypeMinMaxNull<Int64Type>(),
        RLETestData::TypeMinMaxNull<UInt64Type>(),
        RLETestData::TypeMinMaxNull<FloatType>(),
        RLETestData::TypeMinMaxNull<DoubleType>()));

}  // namespace compute
}  // namespace arrow
