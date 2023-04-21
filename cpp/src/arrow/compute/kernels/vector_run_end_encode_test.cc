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
#include "arrow/array/validate.h"
#include "arrow/builder.h"
#include "arrow/compute/api_vector.h"
#include "arrow/testing/gtest_util.h"
#include "arrow/type_fwd.h"
#include "arrow/util/logging.h"
#include "arrow/util/ree_util.h"

namespace arrow {
namespace compute {

namespace {

struct REETestData {
 private:
  static REETestData SingleArray(std::shared_ptr<Array> input,
                                 std::shared_ptr<Array> expected_values,
                                 std::string expected_run_ends_json,
                                 std::string description) {
    REETestData result;
    result.input = std::make_shared<ChunkedArray>(std::move(input));
    result.expected_values =
        std::vector<std::shared_ptr<Array>>{std::move(expected_values)};
    result.expected_run_ends_json =
        std::vector<std::string>{std::move(expected_run_ends_json)};
    result.description = std::move(description);
    return result;
  }

 public:
  static REETestData JSON(std::shared_ptr<DataType> data_type, std::string input_json,
                          std::string expected_values_json,
                          std::string expected_run_ends_json, int64_t input_offset = 0) {
    auto input_array = ArrayFromJSON(data_type, input_json);
    return SingleArray(input_offset ? input_array->Slice(input_offset) : input_array,
                       ArrayFromJSON(data_type, expected_values_json),
                       std::move(expected_run_ends_json), std::move(input_json));
  }

  static REETestData NullArray(int64_t input_length, int64_t input_offset = 0) {
    auto input_array = std::make_shared<arrow::NullArray>(input_length);
    REETestData result;
    auto input_slice = input_offset ? input_array->Slice(input_offset) : input_array;
    const int64_t input_slice_length = input_slice->length();
    return SingleArray(
        input_slice, std::make_shared<arrow::NullArray>(input_slice_length > 0 ? 1 : 0),
        input_slice_length > 0 ? "[" + std::to_string(input_slice_length) + "]" : "[]",
        "[null * " + std::to_string(input_slice_length) + "]");
  }

  static REETestData JSONChunked(std::shared_ptr<DataType> data_type,
                                 std::vector<std::string> inputs_json,
                                 std::vector<std::string> expected_values_json,
                                 std::vector<std::string> expected_run_ends_json,
                                 int64_t input_offset = 0) {
    std::vector<std::shared_ptr<Array>> inputs;
    inputs.reserve(inputs_json.size());
    for (const auto& input_json : inputs_json) {
      inputs.push_back(ArrayFromJSON(data_type, input_json));
    }
    auto chunked_input = std::make_shared<ChunkedArray>(std::move(inputs));

    std::vector<std::shared_ptr<Array>> expected_values;
    expected_values.reserve(expected_values_json.size());
    for (const auto& expected_values_json : expected_values_json) {
      expected_values.push_back(ArrayFromJSON(data_type, expected_values_json));
    }

    std::string desc;
    for (const auto& input_json : inputs_json) {
      if (!desc.empty()) {
        desc += ", ";
      }
      desc += input_json;
    }
    desc += " (Chunks)";

    REETestData result;
    result.input = input_offset ? chunked_input->Slice(input_offset) : chunked_input;
    result.expected_values = std::move(expected_values);
    result.expected_run_ends_json = std::move(expected_run_ends_json);
    result.chunked = true;
    result.description = std::move(desc);
    return result;
  }

  template <typename ArrowType>
  static REETestData TypeMinMaxNull() {
    using CType = typename ArrowType::c_type;
    REETestData result;
    NumericBuilder<ArrowType> builder;
    ARROW_EXPECT_OK(builder.Append(std::numeric_limits<CType>::min()));
    ARROW_EXPECT_OK(builder.AppendNull());
    ARROW_EXPECT_OK(builder.Append(std::numeric_limits<CType>::max()));
    auto input = builder.Finish().ValueOrDie();
    return SingleArray(input, input, "[1, 2, 3]", "Type min, max, & null values");
  }

  Datum InputDatum() const {
    DCHECK(chunked || input->num_chunks() == 1);
    return chunked ? Datum{input} : Datum{input->chunk(0)};
  }

  std::shared_ptr<ChunkedArray> input;
  std::vector<std::shared_ptr<Array>> expected_values;
  std::vector<std::string> expected_run_ends_json;
  // Pass the input directly to functions instead of the first and only chunk
  bool chunked = false;

  // only used for gtest output
  std::string description;
};

// For valgrind
std::ostream& operator<<(std::ostream& out, const REETestData& test_data) {
  out << "REETestData{description = " << test_data.description
      << ", input = " << test_data.input->ToString() << ", expected_values = ";

  for (const auto& expected_value : test_data.expected_values) {
    out << expected_value->ToString() << ", ";
  }

  out << ", chunked = " << test_data.chunked << "}";
  return out;
}

}  // namespace

class TestRunEndEncodeDecode : public ::testing::TestWithParam<
                                   std::tuple<REETestData, std::shared_ptr<DataType>>> {
 public:
  void AddArtificialOffsetInChildArray(ArrayData* array, int64_t offset) {
    auto& child = array->child_data[1];
    auto builder = MakeBuilder(child->type).ValueOrDie();
    ASSERT_OK(builder->AppendNulls(offset));
    ASSERT_OK(builder->AppendArraySlice(ArraySpan(*child), 0, child->length));
    array->child_data[1] = builder->Finish().ValueOrDie()->Slice(offset)->data();
    ASSERT_OK(arrow::internal::ValidateArrayFull(*array));
  }

  std::shared_ptr<ChunkedArray> AsChunkedArray(const Datum& datum) {
    if (datum.is_array()) {
      return std::make_shared<ChunkedArray>(datum.make_array());
    }
    DCHECK(datum.is_chunked_array());
    return datum.chunked_array();
  }
};

TEST_P(TestRunEndEncodeDecode, EncodeDecodeArray) {
  auto [data, run_end_type] = GetParam();

  ASSERT_OK_AND_ASSIGN(
      Datum encoded_datum,
      RunEndEncode(data.InputDatum(), RunEndEncodeOptions{run_end_type}));

  auto encoded = AsChunkedArray(encoded_datum);
  ASSERT_OK(encoded->ValidateFull());
  ASSERT_EQ(data.input->length(), encoded->length());

  for (int i = 0; i < encoded->num_chunks(); i++) {
    auto& chunk = encoded->chunk(i);
    auto run_ends_array = MakeArray(chunk->data()->child_data[0]);
    auto values_array = MakeArray(chunk->data()->child_data[1]);
    ASSERT_OK(chunk->ValidateFull());
    ASSERT_ARRAYS_EQUAL(*ArrayFromJSON(run_end_type, data.expected_run_ends_json[i]),
                        *run_ends_array);
    ASSERT_ARRAYS_EQUAL(*values_array, *data.expected_values[i]);
    ASSERT_EQ(chunk->data()->buffers.size(), 1);
    ASSERT_EQ(chunk->data()->buffers[0], NULLPTR);
    ASSERT_EQ(chunk->data()->child_data.size(), 2);
    ASSERT_EQ(run_ends_array->data()->buffers[0], NULLPTR);
    ASSERT_EQ(run_ends_array->length(), data.expected_values[i]->length());
    ASSERT_EQ(run_ends_array->offset(), 0);
    ASSERT_EQ(chunk->data()->length, data.input->chunk(i)->length());
    ASSERT_EQ(chunk->data()->offset, 0);
    ASSERT_EQ(*chunk->data()->type, RunEndEncodedType(run_end_type, data.input->type()));
    ASSERT_EQ(chunk->data()->null_count, 0);
  }

  ASSERT_OK_AND_ASSIGN(Datum decoded_datum, data.chunked
                                                ? RunEndDecode(encoded)
                                                : RunEndDecode(encoded->chunk(0)));
  auto decoded = AsChunkedArray(decoded_datum);
  ASSERT_OK(decoded->ValidateFull());
  for (int i = 0; i < decoded->num_chunks(); i++) {
    ASSERT_ARRAYS_EQUAL(*decoded->chunk(i), *data.input->chunk(i));
  }
}

// Encoding an input with an offset results in a completely new encoded array without an
// offset. This means the EncodeDecodeArray test will never actually decode an array
// with an offset, even though we have inputs with offsets. This test slices one element
// off the encoded array and decodes that.
TEST_P(TestRunEndEncodeDecode, DecodeWithOffset) {
  auto [data, run_end_type] = GetParam();
  if (data.input->length() == 0) {
    return;
  }

  ASSERT_OK_AND_ASSIGN(
      Datum encoded_datum,
      RunEndEncode(data.InputDatum(), RunEndEncodeOptions{run_end_type}));

  auto encoded = AsChunkedArray(encoded_datum);
  ASSERT_EQ(encoded->num_chunks(), data.input->num_chunks());
  ASSERT_GT(encoded->num_chunks(), 0);

  ASSERT_OK(encoded->ValidateFull());

  ASSERT_OK_AND_ASSIGN(Datum datum_without_first,
                       data.chunked
                           ? RunEndDecode(encoded->Slice(1, encoded->length() - 1))
                           : RunEndDecode(encoded->chunk(0)->Slice(
                                 1, encoded->chunk(0)->data()->length - 1)));
  ASSERT_OK_AND_ASSIGN(Datum datum_without_last,
                       data.chunked
                           ? RunEndDecode(encoded->Slice(0, encoded->length() - 1))
                           : RunEndDecode(encoded->chunk(0)->Slice(
                                 0, encoded->chunk(0)->data()->length - 1)));
  auto array_without_first = AsChunkedArray(datum_without_first);
  auto array_without_last = AsChunkedArray(datum_without_last);
  for (int i = 0; i < encoded->num_chunks(); i++) {
    ASSERT_OK(array_without_first->ValidateFull());
    ASSERT_OK(array_without_last->ValidateFull());
    const auto expected_without_first =
        (i == 0) ? data.input->chunk(i)->Slice(1) : data.input->chunk(i);
    const auto expected_without_last =
        (i == encoded->num_chunks() - 1)
            ? data.input->chunk(i)->Slice(0, data.input->chunk(i)->length() - 1)
            : data.input->chunk(i);
    ASSERT_ARRAYS_EQUAL(*array_without_first->chunk(i), *expected_without_first);
    ASSERT_ARRAYS_EQUAL(*array_without_last->chunk(i), *expected_without_last);
  }
}

// This test creates an run-end encoded array with an offset in the child array, which
// removes the first run in the test data. It's no-op for chunked input.
TEST_P(TestRunEndEncodeDecode, DecodeWithOffsetInChildArray) {
  auto [data, run_end_type] = GetParam();
  if (data.chunked) {
    return;
  }

  ASSERT_EQ(data.input->num_chunks(), 1);

  ASSERT_OK_AND_ASSIGN(
      Datum encoded_datum,
      RunEndEncode(data.InputDatum(), RunEndEncodeOptions{run_end_type}));
  auto encoded = encoded_datum.array();

  ASSERT_NO_FATAL_FAILURE(this->AddArtificialOffsetInChildArray(encoded.get(), 100));
  ASSERT_OK_AND_ASSIGN(Datum datum_without_first, RunEndDecode(encoded));
  auto array_without_first = datum_without_first.make_array();
  ASSERT_OK(array_without_first->ValidateFull());
  ASSERT_ARRAYS_EQUAL(*array_without_first, *data.input->chunk(0));
}

std::vector<REETestData> GenerateTestData() {
  std::vector<REETestData> test_data = {
      REETestData::JSON(int32(), "[1, 1, 0, -5, -5, -5, 255, 255]", "[1, 0, -5, 255]",
                        "[2, 3, 6, 8]"),
      REETestData::JSON(uint32(), "[null, 1, 1, null, null, 5]", "[null, 1, null, 5]",
                        "[1, 3, 5, 6]"),
      REETestData::JSON(boolean(), "[true, true, true, false, false]", "[true, false]",
                        "[3, 5]"),
      REETestData::JSON(boolean(),
                        "[true, false, true, false, true, false, true, false, true]",
                        "[true, false, true, false, true, false, true, false, true]",
                        "[1, 2, 3, 4, 5, 6, 7, 8, 9]"),
      REETestData::JSON(uint32(), "[1]", "[1]", "[1]"),

      REETestData::JSON(boolean(),
                        "[true, true, true, false, null, null, false, null, null]",
                        "[true, false, null, false, null]", "[3, 4, 6, 7, 9]"),
      REETestData::JSONChunked(
          boolean(), {"[true, true]", "[true, false, null, null, false]", "[null, null]"},
          {"[true]", "[true, false, null, false]", "[null]"},
          {"[2]", "[1, 2, 4, 5]", "[2]"}),
      REETestData::JSON(int32(), "[1, 1, 0, -5, -5, -5, 255, 255]", "[-5, 255]", "[3, 5]",
                        3),
      REETestData::JSONChunked(int32(), {"[1, 1, 0, -5, -5]", "[-5, 255, 255]"},
                               {"[-5]", "[-5, 255]"}, {"[2]", "[1, 3]"}, 3),
      REETestData::JSON(uint32(), "[4, 5, 5, null, null, 5]", "[5, null, 5]", "[1, 3, 4]",
                        2),
      REETestData::JSONChunked(uint32(), {"[4, 5, 5, null, null, 5]"}, {"[5, null, 5]"},
                               {"[1, 3, 4]"}, 2),
      REETestData::JSON(boolean(), "[true, true, false, false, true]", "[false, true]",
                        "[2, 3]", 2),
      REETestData::JSONChunked(boolean(), {"[true]", "[true, false, false, true]"},
                               {"[false, true]"}, {"[2, 3]"}, 2),
      REETestData::JSON(boolean(), "[true, true, true, false, null, null, false]",
                        "[null, false]", "[1, 2]", 5),

      REETestData::JSON(float64(), "[]", "[]", "[]"),
      REETestData::JSONChunked(float64(), {"[]"}, {"[]"}, {"[]"}),
      REETestData::JSON(boolean(), "[]", "[]", "[]"),
      REETestData::JSONChunked(boolean(), {"[]"}, {"[]"}, {"[]"}),

      REETestData::NullArray(4),
      REETestData::NullArray(std::numeric_limits<int16_t>::max()),
      REETestData::NullArray(std::numeric_limits<int16_t>::max(), 1000),

      REETestData::TypeMinMaxNull<Int8Type>(),
      REETestData::TypeMinMaxNull<UInt8Type>(),
      REETestData::TypeMinMaxNull<Int16Type>(),
      REETestData::TypeMinMaxNull<UInt16Type>(),
      REETestData::TypeMinMaxNull<Int32Type>(),
      REETestData::TypeMinMaxNull<UInt32Type>(),
      REETestData::TypeMinMaxNull<Int64Type>(),
      REETestData::TypeMinMaxNull<UInt64Type>(),
      REETestData::TypeMinMaxNull<FloatType>(),
      REETestData::TypeMinMaxNull<DoubleType>(),
      // A few temporal types
      REETestData::JSON(date32(),
                        "[86400, 86400, 0, 432000, 432000, 432000, 22075200, 22075200]",
                        "[86400, 0, 432000, 22075200]", "[2, 3, 6, 8]"),
      REETestData::JSON(date64(),
                        "[86400000, 86400000, 0, 432000000, 432000000, 432000000, "
                        "22032000000, 22032000000]",
                        "[86400000, 0, 432000000, 22032000000]", "[2, 3, 6, 8]"),
      REETestData::JSON(time32(TimeUnit::SECOND), "[1, 1, 0, 5, 5, 5, 255, 255]",
                        "[1, 0, 5, 255]", "[2, 3, 6, 8]"),
      REETestData::JSON(time64(TimeUnit::MICRO), "[1, 1, 0, 5, 5, 5, 255, 255]",
                        "[1, 0, 5, 255]", "[2, 3, 6, 8]"),
      // Decimal and fixed size binary types
      REETestData::JSON(decimal128(4, 1),
                        R"(["1.0", "1.0", "0.0", "5.2", "5.2", "5.2", "255.0", "255.0"])",
                        R"(["1.0", "0.0", "5.2", "255.0"])", "[2, 3, 6, 8]"),
      REETestData::JSON(decimal256(4, 1),
                        R"(["1.0", "1.0", "0.0", "5.2", "5.2", "5.2", "255.0", "255.0"])",
                        R"(["1.0", "0.0", "5.2", "255.0"])", "[2, 3, 6, 8]"),
      REETestData::JSON(fixed_size_binary(3),
                        R"(["abc", "abc", "abc", "def", "def", "def", "ghi", "ghi"])",
                        R"(["abc", "def", "ghi"])", "[3, 6, 8]"),
      REETestData::JSON(
          fixed_size_binary(3),
          R"([null, "abc", "abc", "abc", "def", "def", "def", "ghi", "ghi", null, null])",
          R"([null, "abc", "def", "ghi", null])", "[1, 4, 7, 9, 11]"),
  };
  for (auto& binary_type : {binary(), large_binary(), utf8(), large_utf8()}) {
    test_data.push_back(REETestData::JSON(
        binary_type, R"(["abc", "abc", "", "", "de", "de", "de", "ghijkl", "ghijkl"])",
        R"(["abc", "", "de", "ghijkl"])", "[2, 4, 7, 9]"));
    test_data.push_back(REETestData::JSON(
        binary_type,
        R"(["abc", "abc", "", "", "de", "de", "de", null, null, "ghijkl", "ghijkl"])",
        R"(["abc", "", "de", null, "ghijkl"])", "[2, 4, 7, 9, 11]"));
  }
  return test_data;
}

INSTANTIATE_TEST_SUITE_P(EncodeArrayTests, TestRunEndEncodeDecode,
                         ::testing::Combine(::testing::ValuesIn(GenerateTestData()),
                                            ::testing::Values(int16(), int32(),
                                                              int64())));

}  // namespace compute
}  // namespace arrow
