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
#include "arrow/datum.h"
#include "arrow/testing/gtest_util.h"
#include "arrow/type_fwd.h"
#include "arrow/util/logging_internal.h"
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
                                 int64_t input_offset = 0,
                                 bool force_validity_bitmap = false) {
    std::vector<std::shared_ptr<Array>> inputs;
    inputs.reserve(inputs_json.size());
    for (const auto& input_json : inputs_json) {
      auto chunk = ArrayFromJSON(data_type, input_json);
      auto& data = chunk->data();
      if (force_validity_bitmap && !data->HasValidityBitmap()) {
        EXPECT_OK_AND_ASSIGN(auto validity, AllocateBitmap(data->length));
        memset(validity->mutable_data(), 0xFF, validity->size());
        data->buffers[0] = std::move(validity);
      }
      inputs.push_back(std::move(chunk));
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

  void TestEncodeDecodeArray(REETestData& data,
                             const std::shared_ptr<DataType>& run_end_type) {
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
      ASSERT_EQ(*chunk->data()->type,
                RunEndEncodedType(run_end_type, data.input->type()));
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
};

TEST_P(TestRunEndEncodeDecode, EncodeDecodeArray) {
  auto [data, run_end_type] = GetParam();
  TestEncodeDecodeArray(data, run_end_type);
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

// GH-36708
TEST_P(TestRunEndEncodeDecode, InputWithValidityAndNoNulls) {
  auto data =
      REETestData::JSONChunked(int32(),
                               /*inputs=*/{"[1, 1, 2, 2, 2, 3]", "[4, 5, 5, 5, 6, 6]"},
                               /*expected_values=*/{"[1, 2, 3]", "[4, 5, 6]"},
                               /*expected_run_ends=*/{"[2, 5, 6]", "[1, 4, 6]"},
                               /*input_offset=*/0, /*force_validity_bitmap=*/true);
  TestEncodeDecodeArray(data, int32());
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
      // Float types
      REETestData::JSON(float16(), "[1, 1, 0, -5, -5, -5, 255, 255]", "[1, 0, -5, 255]",
                        "[2, 3, 6, 8]"),
      REETestData::JSON(float32(), "[1, 1, 0, -5, -5, -5, 255, 255]", "[1, 0, -5, 255]",
                        "[2, 3, 6, 8]"),
      REETestData::JSON(float64(), "[1, 1, 0, -5, -5, -5, 255, 255]", "[1, 0, -5, 255]",
                        "[2, 3, 6, 8]"),
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
      REETestData::JSON(decimal32(4, 1),
                        R"(["1.0", "1.0", "0.0", "5.2", "5.2", "5.2", "255.0", "255.0"])",
                        R"(["1.0", "0.0", "5.2", "255.0"])", "[2, 3, 6, 8]"),
      REETestData::JSON(decimal64(4, 1),
                        R"(["1.0", "1.0", "0.0", "5.2", "5.2", "5.2", "255.0", "255.0"])",
                        R"(["1.0", "0.0", "5.2", "255.0"])", "[2, 3, 6, 8]"),
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

void AssertNestedRunEndEncodeDecode(const std::shared_ptr<Array>& input,
                                    const std::string& expected_run_ends_json,
                                    const std::shared_ptr<Array>& expected_values) {
  for (const auto& run_end_type : {int16(), int32(), int64()}) {
    ARROW_SCOPED_TRACE("run end type = ", *run_end_type);
    ASSERT_OK_AND_ASSIGN(Datum encoded_datum,
                         RunEndEncode(input, RunEndEncodeOptions{run_end_type}));
    auto encoded =
        std::dynamic_pointer_cast<RunEndEncodedArray>(encoded_datum.make_array());

    ASSERT_NE(encoded, NULLPTR);
    ASSERT_OK(encoded->ValidateFull());
    ASSERT_EQ(encoded->length(), input->length());
    ASSERT_EQ(*encoded->type(), *run_end_encoded(run_end_type, input->type()));
    ASSERT_ARRAYS_EQUAL(*encoded->run_ends(),
                        *ArrayFromJSON(run_end_type, expected_run_ends_json));
    ASSERT_ARRAYS_EQUAL(*encoded->values(), *expected_values);

    ASSERT_OK_AND_ASSIGN(Datum decoded_datum, RunEndDecode(encoded));
    auto decoded = decoded_datum.make_array();
    ASSERT_OK(decoded->ValidateFull());
    ASSERT_ARRAYS_EQUAL(*decoded, *input);

    if (input->length() > 0) {
      ASSERT_OK_AND_ASSIGN(Datum decoded_slice_datum, RunEndDecode(encoded->Slice(1)));
      auto decoded_slice = decoded_slice_datum.make_array();
      ASSERT_OK(decoded_slice->ValidateFull());
      ASSERT_ARRAYS_EQUAL(*decoded_slice, *input->Slice(1));

      ASSERT_OK_AND_ASSIGN(Datum decoded_prefix_datum,
                           RunEndDecode(encoded->Slice(0, encoded->length() - 1)));
      auto decoded_prefix = decoded_prefix_datum.make_array();
      ASSERT_OK(decoded_prefix->ValidateFull());
      ASSERT_ARRAYS_EQUAL(*decoded_prefix, *input->Slice(0, input->length() - 1));
    }
  }
}

TEST(TestRunEndEncodeDecodeNested, VariableSizeList) {
  auto value_type = list(int32());
  auto input = ArrayFromJSON(value_type, R"([
      [9], [1, 2], [1, 2], [], [], null, null, [null], [null], [3], [3], [4], [9]
  ])");
  input = input->Slice(1, 11);
  auto expected_values =
      ArrayFromJSON(value_type, R"([[1, 2], [], null, [null], [3], [4]])");
  AssertNestedRunEndEncodeDecode(input, "[2, 4, 6, 8, 10, 11]", expected_values);

  AssertNestedRunEndEncodeDecode(ArrayFromJSON(value_type, "[]"), "[]",
                                 ArrayFromJSON(value_type, "[]"));

  auto signed_zeros = ArrayFromJSON(list(float64()), "[[0.0], [-0.0]]");
  AssertNestedRunEndEncodeDecode(signed_zeros, "[1, 2]", signed_zeros);

  auto null_lists = ArrayFromJSON(list(null()), "[[null], [null], [], [null]]");
  auto expected_null_lists = ArrayFromJSON(list(null()), "[[null], [], [null]]");
  AssertNestedRunEndEncodeDecode(null_lists, "[2, 3, 4]", expected_null_lists);

  auto nested_value_type = list(list(int32()));
  auto nested_lists =
      ArrayFromJSON(nested_value_type, "[[[1], [2]], [[1], [2]], [], null, [[3, null]]]");
  auto expected_nested_lists =
      ArrayFromJSON(nested_value_type, "[[[1], [2]], [], null, [[3, null]]]");
  AssertNestedRunEndEncodeDecode(nested_lists, "[2, 3, 4, 5]", expected_nested_lists);
}

TEST(TestRunEndEncodeDecodeNested, FixedSizeList) {
  auto value_type = fixed_size_list(int32(), 2);
  auto input = ArrayFromJSON(value_type, R"([
      [9, 9], [1, 2], [1, 2], [null, 2], [null, 2], null, null,
      [3, 4], [3, 4], [5, 6], [9, 9]
  ])");
  input = input->Slice(1, 9);
  auto expected_values =
      ArrayFromJSON(value_type, "[[1, 2], [null, 2], null, [3, 4], [5, 6]]");
  AssertNestedRunEndEncodeDecode(input, "[2, 4, 6, 8, 9]", expected_values);

  AssertNestedRunEndEncodeDecode(ArrayFromJSON(value_type, "[]"), "[]",
                                 ArrayFromJSON(value_type, "[]"));
}

TEST(TestRunEndEncodeDecodeNested, PreservesDictionaryIndexType) {
  auto dictionary_type = dictionary(uint8(), utf8());
  auto dictionary_values = ArrayFromJSON(utf8(), R"(["a", "b"])");
  ASSERT_OK_AND_ASSIGN(
      auto input_values,
      DictionaryArray::FromArrays(dictionary_type, ArrayFromJSON(uint8(), "[0, 0, 1, 0]"),
                                  dictionary_values));
  ASSERT_OK_AND_ASSIGN(
      auto input,
      ListArray::FromArrays(*ArrayFromJSON(int32(), "[0, 1, 2, 3, 4]"), *input_values));

  ASSERT_OK_AND_ASSIGN(
      auto expected_dictionary_values,
      DictionaryArray::FromArrays(dictionary_type, ArrayFromJSON(uint8(), "[0, 1, 0]"),
                                  dictionary_values));
  ASSERT_OK_AND_ASSIGN(auto expected_values,
                       ListArray::FromArrays(*ArrayFromJSON(int32(), "[0, 1, 2, 3]"),
                                             *expected_dictionary_values));
  AssertNestedRunEndEncodeDecode(input, "[2, 3, 4]", expected_values);
}

TEST(TestRunEndEncodeDecodeNested, DecodeWithOffsetInValuesArray) {
  auto value_type = list(int32());
  auto values = ArrayFromJSON(value_type, "[[9], [1], [2]]")->Slice(1);
  auto expected = ArrayFromJSON(value_type, "[[1], [1], [2], [2], [2]]");

  for (const auto& run_end_type : {int16(), int32(), int64()}) {
    ARROW_SCOPED_TRACE("run end type = ", *run_end_type);
    auto run_ends = ArrayFromJSON(run_end_type, "[1, 2, 5]")->Slice(1);
    ASSERT_OK_AND_ASSIGN(auto encoded, RunEndEncodedArray::Make(5, run_ends, values));
    ASSERT_OK(encoded->ValidateFull());

    ASSERT_OK_AND_ASSIGN(Datum decoded_datum, RunEndDecode(encoded));
    auto decoded = decoded_datum.make_array();
    ASSERT_OK(decoded->ValidateFull());
    ASSERT_ARRAYS_EQUAL(*decoded, *expected);

    ASSERT_OK_AND_ASSIGN(Datum decoded_slice_datum, RunEndDecode(encoded->Slice(1, 3)));
    auto decoded_slice = decoded_slice_datum.make_array();
    ASSERT_OK(decoded_slice->ValidateFull());
    ASSERT_ARRAYS_EQUAL(*decoded_slice, *expected->Slice(1, 3));
  }
}

TEST(TestRunEndEncodeDecodeNested, Struct) {
  auto value_type = struct_({field("age", int32()), field("name", utf8())});
  auto input = ArrayFromJSON(value_type, R"([
      {"age": 99, "name": "skip"},
      {"age": 20, "name": "a"},
      {"age": 20, "name": "a"},
      {"age": 20, "name": "b"},
      null,
      null,
      {"age": null, "name": null},
      {"age": null, "name": null},
      {"age": 99, "name": "skip"}
  ])");
  input = input->Slice(1, 7);
  auto expected_values = ArrayFromJSON(value_type, R"([
      {"age": 20, "name": "a"},
      {"age": 20, "name": "b"},
      null,
      {"age": null, "name": null}
  ])");
  AssertNestedRunEndEncodeDecode(input, "[2, 3, 5, 7]", expected_values);

  AssertNestedRunEndEncodeDecode(ArrayFromJSON(value_type, "[]"), "[]",
                                 ArrayFromJSON(value_type, "[]"));
}

}  // namespace compute
}  // namespace arrow
