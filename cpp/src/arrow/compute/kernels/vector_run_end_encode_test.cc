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
#include "arrow/util/logging.h"
#include "arrow/util/ree_util.h"

namespace arrow {
namespace compute {

namespace {

struct REETestData {
  static REETestData JSON(std::shared_ptr<DataType> data_type, std::string input_json,
                          std::string expected_values_json,
                          std::string expected_run_ends_json, int64_t input_offset = 0) {
    auto input_array = ArrayFromJSON(data_type, input_json);
    return {.input = input_array->Slice(input_offset),
            .expected_values = ArrayFromJSON(data_type, expected_values_json),
            .expected_run_ends_json = std::move(expected_run_ends_json),
            .string = input_json};
  }

  template <typename ArrowType>
  static REETestData TypeMinMaxNull() {
    using CType = typename ArrowType::c_type;
    REETestData result;
    NumericBuilder<ArrowType> builder;
    ARROW_EXPECT_OK(builder.Append(std::numeric_limits<CType>::min()));
    ARROW_EXPECT_OK(builder.AppendNull());
    ARROW_EXPECT_OK(builder.Append(std::numeric_limits<CType>::max()));
    result.input = builder.Finish().ValueOrDie();
    result.expected_values = result.input;
    result.expected_run_ends_json = "[1, 2, 3]";
    result.string = "Type min, max, & null values";
    return result;
  }

  std::shared_ptr<Array> input;
  std::shared_ptr<Array> expected_values;
  std::string expected_run_ends_json;
  // only used for gtest output
  std::string string;
};

}  // namespace

class TestRunEndEncode : public ::testing::TestWithParam<
                             std::tuple<REETestData, std::shared_ptr<DataType>>> {
 public:
  void AddArtificialOffsetInChildArray(ArrayData* array, int64_t offset) {
    auto& child = array->child_data[1];
    auto builder = MakeBuilder(child->type).ValueOrDie();
    ARROW_CHECK_OK(builder->AppendNulls(offset));
    ARROW_CHECK_OK(builder->AppendArraySlice(ArraySpan(*child), 0, child->length));
    array->child_data[1] = builder->Finish().ValueOrDie()->Slice(offset)->data();
  }
};

TEST_P(TestRunEndEncode, EncodeDecodeArray) {
  auto [data, run_ends_type] = GetParam();

  ASSERT_OK_AND_ASSIGN(Datum encoded_datum,
                       RunEndEncode(data.input, RunEndEncodeOptions(run_ends_type)));

  auto encoded = encoded_datum.array();
  auto run_ends_array = MakeArray(encoded->child_data[0]);
  auto values_array = MakeArray(encoded->child_data[1]);
  ASSERT_OK(MakeArray(encoded)->ValidateFull());
  ASSERT_ARRAYS_EQUAL(*ArrayFromJSON(run_ends_type, data.expected_run_ends_json),
                      *run_ends_array);
  ASSERT_ARRAYS_EQUAL(*values_array, *data.expected_values);
  ASSERT_EQ(encoded->buffers.size(), 1);
  ASSERT_EQ(encoded->buffers[0], NULLPTR);
  ASSERT_EQ(encoded->child_data.size(), 2);
  ASSERT_EQ(run_ends_array->data()->buffers[0], NULLPTR);
  ASSERT_EQ(run_ends_array->length(), data.expected_values->length());
  ASSERT_EQ(run_ends_array->offset(), 0);
  ASSERT_EQ(encoded->length, data.input->length());
  ASSERT_EQ(encoded->offset, 0);
  ASSERT_EQ(*encoded->type, RunEndEncodedType(run_ends_type, data.input->type()));
  ASSERT_EQ(encoded->null_count, 0);

  ASSERT_OK_AND_ASSIGN(Datum decoded_datum, RunEndDecode(encoded));
  auto decoded = decoded_datum.make_array();
  ASSERT_OK(decoded->ValidateFull());
  ASSERT_ARRAYS_EQUAL(*decoded, *data.input);
}

// Encoding an input with an offset results in a completely new encoded array without an
// offset. This means The EncodeDecodeArray test will never actually decode an array
// with an offset, even though we have inputs with offsets. This test slices one element
// off the encoded array and decodes that.
TEST_P(TestRunEndEncode, DecodeWithOffset) {
  auto [data, run_ends_type] = GetParam();
  if (data.input->length() == 0) {
    // this test slices off one run, so it makes no sense on a 0-length input.
    // make sure to run it on an input with only one run to test the case where a 0-length
    // slice is created.
    return;
  }

  ASSERT_OK_AND_ASSIGN(Datum encoded_datum,
                       RunEndEncode(data.input, RunEndEncodeOptions(run_ends_type)));

  auto encoded = encoded_datum.array();
  ASSERT_OK_AND_ASSIGN(Datum datum_without_first,
                       RunEndDecode(encoded->Slice(1, encoded->length - 1)));
  ASSERT_OK_AND_ASSIGN(Datum datum_without_last,
                       RunEndDecode(encoded->Slice(0, encoded->length - 1)));
  auto array_without_first = datum_without_first.make_array();
  auto array_without_last = datum_without_last.make_array();
  ASSERT_OK(array_without_first->ValidateFull());
  ASSERT_OK(array_without_last->ValidateFull());
  ASSERT_ARRAYS_EQUAL(*array_without_first, *data.input->Slice(1));
  ASSERT_ARRAYS_EQUAL(*array_without_last,
                      *data.input->Slice(0, data.input->length() - 1));
}

// This test creates an run-end encoded array with an offset in the child array, which
// removes the first run in the test data.
TEST_P(TestRunEndEncode, DecodeWithOffsetInChildArray) {
  auto [data, run_ends_type] = GetParam();

  ASSERT_OK_AND_ASSIGN(Datum encoded_datum,
                       RunEndEncode(data.input, RunEndEncodeOptions(run_ends_type)));

  auto encoded = encoded_datum.array();
  this->AddArtificialOffsetInChildArray(encoded.get(), 100);
  ASSERT_OK_AND_ASSIGN(Datum datum_without_first, RunEndDecode(encoded));
  auto array_without_first = datum_without_first.make_array();
  ASSERT_OK(array_without_first->ValidateFull());
  ASSERT_ARRAYS_EQUAL(*array_without_first, *data.input);
}

INSTANTIATE_TEST_SUITE_P(
    EncodeArrayTests, TestRunEndEncode,
    ::testing::Combine(
        ::testing::Values(
            REETestData::JSON(int32(), "[1, 1, 0, -5, -5, -5, 255, 255]",
                              "[1, 0, -5, 255]", "[2, 3, 6, 8]"),
            REETestData::JSON(uint32(), "[null, 1, 1, null, null, 5]",
                              "[null, 1, null, 5]", "[1, 3, 5, 6]"),
            REETestData::JSON(boolean(), "[true, true, true, false, false]",
                              "[true, false]", "[3, 5]"),
            REETestData::JSON(
                boolean(), "[true, false, true, false, true, false, true, false, true]",
                "[true, false, true, false, true, false, true, false, true]",
                "[1, 2, 3, 4, 5, 6, 7, 8, 9]"),
            REETestData::JSON(uint32(), "[1]", "[1]", "[1]"),
            REETestData::JSON(boolean(),
                              "[true, true, true, false, null, null, false, null, null]",
                              "[true, false, null, false, null]", "[3, 4, 6, 7, 9]"),
            REETestData::JSON(int32(), "[1, 1, 0, -5, -5, -5, 255, 255]", "[-5, 255]",
                              "[3, 5]", 3),
            REETestData::JSON(uint32(), "[4, 5, 5, null, null, 5]", "[5, null, 5]",
                              "[1, 3, 4]", 2),
            REETestData::JSON(boolean(), "[true, true, false, false, true]",
                              "[false, true]", "[2, 3]", 2),
            REETestData::JSON(boolean(), "[true, true, true, false, null, null, false]",
                              "[null, false]", "[1, 2]", 5),
            REETestData::JSON(float64(), "[]", "[]", "[]"),
            REETestData::JSON(boolean(), "[]", "[]", "[]"),

            REETestData::TypeMinMaxNull<Int8Type>(),
            REETestData::TypeMinMaxNull<UInt8Type>(),
            REETestData::TypeMinMaxNull<Int16Type>(),
            REETestData::TypeMinMaxNull<UInt16Type>(),
            REETestData::TypeMinMaxNull<Int32Type>(),
            REETestData::TypeMinMaxNull<UInt32Type>(),
            REETestData::TypeMinMaxNull<Int64Type>(),
            REETestData::TypeMinMaxNull<UInt64Type>(),
            REETestData::TypeMinMaxNull<FloatType>(),
            REETestData::TypeMinMaxNull<DoubleType>()),
        ::testing::Values(int16(), int32(), int64())));

}  // namespace compute
}  // namespace arrow
