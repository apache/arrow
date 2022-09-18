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
#include "arrow/util/rle_util.h"

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
            .expected_run_lengths = std::move(expected_run_lengths),
            .string = input_json};
  }

  template <typename ArrowType>
  static RLETestData TypeMinMaxNull() {
    using CType = typename ArrowType::c_type;
    RLETestData result;
    NumericBuilder<ArrowType> builder;
    ARROW_EXPECT_OK(builder.Append(std::numeric_limits<CType>::min()));
    ARROW_EXPECT_OK(builder.AppendNull());
    ARROW_EXPECT_OK(builder.Append(std::numeric_limits<CType>::max()));
    result.input = builder.Finish().ValueOrDie();
    result.expected_values = result.input;
    result.expected_run_lengths = {1, 2, 3};
    result.string = "Type min, max, & null values";
    return result;
  }

  std::shared_ptr<Array> input;
  std::shared_ptr<Array> expected_values;
  std::vector<int64_t> expected_run_lengths;
  // only used for gtest output
  std::string string;
};

std::ostream& operator<<(std::ostream& stream, const RLETestData& test_data) {
  return stream << "RLETestData(" << *test_data.input->type() << ", " + test_data.string
                << ")";
}

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

  auto encoded = encoded_datum.array();
  ASSERT_OK_AND_ASSIGN(Datum decoded_datum,
                       RunLengthDecode(encoded->Slice(1, encoded->length - 1)));
  auto decoded_array = decoded_datum.make_array();
  ASSERT_OK(decoded_array->ValidateFull());
  ASSERT_TRUE(decoded_array->Equals(data.input->Slice(1)));
}

// This test creates an run-length encoded array with an offset in the child array, which
// removes the first run in the test data.
TEST_P(TestRunLengthEncode, DecodeWithOffsetInChildArray) {
  auto data = GetParam();

  const int64_t first_run_length = data.expected_run_lengths[0];
  auto parent = ArrayData::Make(run_length_encoded(data.input->type()),
                                data.input->length() - first_run_length);
  parent->child_data.push_back(data.expected_values->Slice(1)->data());
  ASSERT_OK_AND_ASSIGN(
      auto run_length_buffer,
      AllocateBuffer((data.expected_run_lengths.size() - 1) * sizeof(int64_t)));
  int64_t* run_length_buffer_data =
      reinterpret_cast<int64_t*>(run_length_buffer->mutable_data());
  for (size_t index = 0; index < data.expected_run_lengths.size() - 1; index++) {
    run_length_buffer_data[index] =
        data.expected_run_lengths[index + 1] - first_run_length;
  }
  parent->buffers.push_back(std::move(run_length_buffer));

  ASSERT_OK_AND_ASSIGN(Datum decoded_datum, RunLengthDecode(parent));
  auto decoded_array = decoded_datum.make_array();
  ASSERT_OK(decoded_array->ValidateFull());
  ASSERT_TRUE(decoded_array->Equals(data.input->Slice(first_run_length)));
}

INSTANTIATE_TEST_SUITE_P(
    EncodeArrayTests, TestRunLengthEncode,
    ::testing::Values(
        RLETestData::JSON(int32(), "[1, 1, 0, -5, -5, -5, 255, 255]", "[1, 0, -5, 255]",
                          {2, 3, 6, 8}),
        RLETestData::JSON(uint32(), "[null, 1, 1, null, null, 5]", "[null, 1, null, 5]",
                          {1, 3, 5, 6}),
        RLETestData::JSON(boolean(), "[true, true, true, false, false]", "[true, false]",
                          {3, 5}),
        RLETestData::JSON(boolean(),
                          "[true, false, true, false, true, false, true, false, true]",
                          "[true, false, true, false, true, false, true, false, true]",
                          {1, 2, 3, 4, 5, 6, 7, 8, 9}),
        RLETestData::JSON(boolean(),
                          "[true, true, true, false, null, null, false, null, null]",
                          "[true, false, null, false, null]", {3, 4, 6, 7, 9}),
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

class TestRLEFilter
    : public ::testing::TestWithParam<std::tuple<RLETestData, std::string, int>> {};

// TODO: add this functionality in the existing selection tests once we have an RLE array
// class
TEST_P(TestRLEFilter, FilterArray) {
  RLETestData data;
  std::string filter_json;
  int offset_type;
  std::tie(data, filter_json, offset_type) = GetParam();
  auto filter = ArrayFromJSON(boolean(), filter_json);
  auto values = data.input;

  ASSERT_OK_AND_ASSIGN(Datum encoded_filter_datum, RunLengthEncode(filter));
  ASSERT_OK_AND_ASSIGN(Datum encoded_values_datum, RunLengthEncode(data.input));

  std::shared_ptr<ArrayData> encoded_filter = encoded_filter_datum.array();
  std::shared_ptr<ArrayData> encoded_values = encoded_values_datum.array();

  if (offset_type == 1) {
    encoded_filter = encoded_filter->Slice(1, encoded_filter->length - 1);
    encoded_values = encoded_values->Slice(1, encoded_values->length - 1);
    filter = filter->Slice(1);
    values = values->Slice(1);
  } else if (offset_type == 2) {
    std::cout << *MakeArray(encoded_values->child_data[0]) << std::endl;
    rle_util::AddArtificialOffsetInChildArray(encoded_values.get(), 42);
    rle_util::AddArtificialOffsetInChildArray(encoded_filter.get(), 1337);

    std::cout << *MakeArray(encoded_values->child_data[0]) << std::endl;
  }

  ASSERT_OK_AND_ASSIGN(auto result, CallFunction("filter", {Datum(values), Datum(filter)},
                                                 NULLPTR, NULLPTR));
  ASSERT_OK_AND_ASSIGN(
      auto encoded_result,
      CallFunction("filter", {Datum(encoded_values), Datum(encoded_filter)}, NULLPTR,
                   NULLPTR));
  ASSERT_OK_AND_ASSIGN(Datum decoded_result, RunLengthDecode(encoded_result));

  auto array_filtered_plain = result.make_array();
  auto array_filtered_encoded = decoded_result.make_array();

  std::cout << "array_filtered_plain: " << *array_filtered_plain << std::endl;
  std::cout << "array_filtered_encoded: " << *array_filtered_encoded << std::endl;

  ASSERT_TRUE(array_filtered_encoded->Equals(array_filtered_plain));
}

INSTANTIATE_TEST_SUITE_P(
    EncodeArrayTests, TestRLEFilter,
    ::testing::Combine(
        ::testing::Values(
            RLETestData::JSON(int32(), "[1, 1, 0, -5, -5, -5, 255, 255, 255]",
                              "[1, 0, -5, 255]", {2, 3, 6, 8}),
            RLETestData::JSON(uint32(), "[null, 1, 1, null, null, 5, 5, 9, 9]",
                              "[null, 1, null, 5]", {1, 3, 5, 6}),
            RLETestData::JSON(
                boolean(), "[true, true, true, false, false, false, false, false, true]",
                "[true, false]", {3, 5}),
            RLETestData::JSON(boolean(),
                              "[true, true, true, false, null, null, false, null, null]",
                              "[true, false, null, false]", {3, 4, 6, 7}),
            RLETestData::JSON(int32(),
                              "[1, 1, 0, -5, -5, -5, 255, 255, 255, 255, 255, 4]",
                              "[1, 0, -5, 255]", {2, 3, 6, 8}, 3),
            RLETestData::JSON(uint32(),
                              "[null, 1, 1, null, null, 5, 5, 9, 9, null, null]",
                              "[null, 1, null, 5]", {1, 3, 5, 6}, 2),
            RLETestData::JSON(
                boolean(),
                "[true, true, true, true, true, false, false, false, false, false, true]",
                "[true, false]", {3, 5}, 2),
            RLETestData::JSON(
                boolean(),
                "[false, true, true, true, false, null, null, false, null, null]",
                "[true, false, null, false]", {3, 4, 6, 7}, 1)),
        ::testing::Values(
            "[true, true, true, true, true, true, true, true, true]",
            //"[false, false, false, false, false, false, false, false, false]",
            "[true, false, true, false, true, false, true, false, true]",
            "[true, true, false, false, false, false, false, true, true]"),
        ::testing::Values(0, 1, 2)));

}  // namespace compute
}  // namespace arrow
