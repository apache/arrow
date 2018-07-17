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

#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <memory>
#include <sstream>
#include <vector>

#include "gtest/gtest.h"

#include "arrow/array.h"
#include "arrow/builder.h"
#include "arrow/pretty_print.h"
#include "arrow/test-util.h"
#include "arrow/type.h"
#include "arrow/type_traits.h"
#include "arrow/util/decimal.h"

namespace arrow {

class TestPrettyPrint : public ::testing::Test {
 public:
  void SetUp() {}

  void Print(const Array& array) {}

 private:
  std::ostringstream sink_;
};

template <typename T>
void CheckStream(const T& obj, const PrettyPrintOptions& options, const char* expected) {
  std::ostringstream sink;
  ASSERT_OK(PrettyPrint(obj, options, &sink));
  std::string result = sink.str();
  ASSERT_EQ(std::string(expected, strlen(expected)), result);
}

void CheckArray(const Array& arr, const PrettyPrintOptions& options, const char* expected,
                bool check_operator = true) {
  CheckStream(arr, options, expected);

  if (options.indent == 0 && check_operator) {
    std::stringstream ss;
    ss << arr;
    std::string result = std::string(expected, strlen(expected));
    ASSERT_EQ(result, ss.str());
  }
}

template <typename T>
void Check(const T& obj, const PrettyPrintOptions& options, const char* expected) {
  std::string result;
  ASSERT_OK(PrettyPrint(obj, options, &result));
  ASSERT_EQ(std::string(expected, strlen(expected)), result);
}

template <typename TYPE, typename C_TYPE>
void CheckPrimitive(const PrettyPrintOptions& options, const std::vector<bool>& is_valid,
                    const std::vector<C_TYPE>& values, const char* expected,
                    bool check_operator = true) {
  std::shared_ptr<Array> array;
  ArrayFromVector<TYPE, C_TYPE>(is_valid, values, &array);
  CheckArray(*array, options, expected, check_operator);
}

TEST_F(TestPrettyPrint, PrimitiveType) {
  std::vector<bool> is_valid = {true, true, false, true, false};

  std::vector<int32_t> values = {0, 1, 2, 3, 4};
  static const char* expected = "[\n  0,\n  1,\n  null,\n  3,\n  null\n]";
  CheckPrimitive<Int32Type, int32_t>({0, 10}, is_valid, values, expected);

  static const char* expected_na = "[\n  0,\n  1,\n  NA,\n  3,\n  NA\n]";
  CheckPrimitive<Int32Type, int32_t>({0, 10, 2, "NA"}, is_valid, values, expected_na,
                                     false);

  static const char* ex_in2 = "  [\n    0,\n    1,\n    null,\n    3,\n    null\n  ]";
  CheckPrimitive<Int32Type, int32_t>({2, 10}, is_valid, values, ex_in2);
  static const char* ex_in2_w2 = "  [\n    0,\n    1,\n    ...\n    3,\n    null\n  ]";
  CheckPrimitive<Int32Type, int32_t>({2, 2}, is_valid, values, ex_in2_w2);

  std::vector<double> values2 = {0., 1., 2., 3., 4.};
  static const char* ex2 = "[\n  0,\n  1,\n  null,\n  3,\n  null\n]";
  CheckPrimitive<DoubleType, double>({0, 10}, is_valid, values2, ex2);
  static const char* ex2_in2 = "  [\n    0,\n    1,\n    null,\n    3,\n    null\n  ]";
  CheckPrimitive<DoubleType, double>({2, 10}, is_valid, values2, ex2_in2);

  std::vector<std::string> values3 = {"foo", "bar", "", "baz", ""};
  static const char* ex3 = "[\n  \"foo\",\n  \"bar\",\n  null,\n  \"baz\",\n  null\n]";
  CheckPrimitive<StringType, std::string>({0, 10}, is_valid, values3, ex3);
  static const char* ex3_in2 =
      "  [\n    \"foo\",\n    \"bar\",\n    null,\n    \"baz\",\n    null\n  ]";
  CheckPrimitive<StringType, std::string>({2, 10}, is_valid, values3, ex3_in2);
}

TEST_F(TestPrettyPrint, BinaryType) {
  std::vector<bool> is_valid = {true, true, false, true, false};
  std::vector<std::string> values = {"foo", "bar", "", "baz", ""};
  static const char* ex = "[\n  666F6F,\n  626172,\n  null,\n  62617A,\n  null\n]";
  CheckPrimitive<BinaryType, std::string>({0}, is_valid, values, ex);
  static const char* ex_in2 =
      "  [\n    666F6F,\n    626172,\n    null,\n    62617A,\n    null\n  ]";
  CheckPrimitive<BinaryType, std::string>({2}, is_valid, values, ex_in2);
}

TEST_F(TestPrettyPrint, ListType) {
  Int64Builder* int_builder = new Int64Builder();
  ListBuilder list_builder(default_memory_pool(),
                           std::unique_ptr<ArrayBuilder>(int_builder));

  ASSERT_OK(list_builder.Append());
  ASSERT_OK(int_builder->AppendNull());
  ASSERT_OK(list_builder.Append());
  ASSERT_OK(list_builder.Append(false));
  ASSERT_OK(list_builder.Append());
  ASSERT_OK(int_builder->Append(4));
  ASSERT_OK(int_builder->Append(6));
  ASSERT_OK(int_builder->Append(7));
  ASSERT_OK(list_builder.Append());
  ASSERT_OK(int_builder->Append(2));
  ASSERT_OK(int_builder->Append(3));

  std::shared_ptr<Array> array;
  ASSERT_OK(list_builder.Finish(&array));
  static const char* ex =
      "[\n  [\n    null\n  ],\n  [],\n  null,\n  [\n    4,\n    6,\n    7\n  ],\n  [\n   "
      " "
      "2,\n    3\n  ]\n]";
  CheckArray(*array, {0, 10}, ex);
  static const char* ex_2 =
      "  [\n    [\n      null\n    ],\n    [],\n    null,\n    [\n      4,\n      6,\n   "
      "   "
      "7\n    ],\n    [\n      2,\n      3\n    ]\n  ]";
  CheckArray(*array, {2, 10}, ex_2);
  static const char* ex_3 = "[\n  [\n    null\n  ],\n  ...\n  [\n    2,\n    3\n  ]\n]";
  CheckStream(*array, {0, 1}, ex_3);
}

TEST_F(TestPrettyPrint, FixedSizeBinaryType) {
  std::vector<bool> is_valid = {true, true, false, true, false};
  std::vector<std::string> values = {"foo", "bar", "baz"};

  std::shared_ptr<Array> array;
  auto type = fixed_size_binary(3);
  FixedSizeBinaryBuilder builder(type);

  ASSERT_OK(builder.Append(values[0]));
  ASSERT_OK(builder.Append(values[1]));
  ASSERT_OK(builder.Append(values[2]));
  ASSERT_OK(builder.Finish(&array));

  static const char* ex = "[\n  666F6F,\n  626172,\n  62617A\n]";
  CheckArray(*array, {0, 10}, ex);
  static const char* ex_2 = "  [\n    666F6F,\n    ...\n    62617A\n  ]";
  CheckArray(*array, {2, 1}, ex_2);
}

TEST_F(TestPrettyPrint, Decimal128Type) {
  int32_t p = 19;
  int32_t s = 4;

  auto type = decimal(p, s);

  Decimal128Builder builder(type);
  Decimal128 val;

  ASSERT_OK(Decimal128::FromString("123.4567", &val));
  ASSERT_OK(builder.Append(val));

  ASSERT_OK(Decimal128::FromString("456.7891", &val));
  ASSERT_OK(builder.Append(val));
  ASSERT_OK(builder.AppendNull());

  std::shared_ptr<Array> array;
  ASSERT_OK(builder.Finish(&array));

  static const char* ex = "[\n  123.4567,\n  456.7891,\n  null\n]";
  CheckArray(*array, {0}, ex);
}

TEST_F(TestPrettyPrint, DictionaryType) {
  std::vector<bool> is_valid = {true, true, false, true, true, true};

  std::shared_ptr<Array> dict;
  std::vector<std::string> dict_values = {"foo", "bar", "baz"};
  ArrayFromVector<StringType, std::string>(dict_values, &dict);
  std::shared_ptr<DataType> dict_type = dictionary(int16(), dict);

  std::shared_ptr<Array> indices;
  std::vector<int16_t> indices_values = {1, 2, -1, 0, 2, 0};
  ArrayFromVector<Int16Type, int16_t>(is_valid, indices_values, &indices);
  auto arr = std::make_shared<DictionaryArray>(dict_type, indices);

  static const char* expected = R"expected(
-- dictionary:
  [
    "foo",
    "bar",
    "baz"
  ]
-- indices:
  [
    1,
    2,
    null,
    0,
    2,
    0
  ])expected";

  CheckArray(*arr, {0}, expected);
}

TEST_F(TestPrettyPrint, ChunkedArrayPrimitiveType) {
  std::vector<bool> is_valid = {true, true, false, true, false};
  std::vector<int32_t> values = {0, 1, 2, 3, 4};
  std::shared_ptr<Array> array;
  ArrayFromVector<Int32Type, int32_t>(is_valid, values, &array);
  ChunkedArray chunked_array({array});

  static const char* expected = R"expected([
  [
    0,
    1,
    null,
    3,
    null
  ]
])expected";
  CheckStream(chunked_array, {0}, expected);

  ChunkedArray chunked_array_2({array, array});

  static const char* expected_2 = R"expected([
  [
    0,
    1,
    null,
    3,
    null
  ],
  [
    0,
    1,
    null,
    3,
    null
  ]
])expected";

  CheckStream(chunked_array_2, {0}, expected_2);
}

TEST_F(TestPrettyPrint, SchemaWithDictionary) {
  std::vector<bool> is_valid = {true, true, false, true, true, true};

  std::shared_ptr<Array> dict;
  std::vector<std::string> dict_values = {"foo", "bar", "baz"};
  ArrayFromVector<StringType, std::string>(dict_values, &dict);

  auto simple = field("one", int32());
  auto simple_dict = field("two", dictionary(int16(), dict));
  auto list_of_dict = field("three", list(simple_dict));

  auto struct_with_dict = field("four", struct_({simple, simple_dict}));

  auto sch = schema({simple, simple_dict, list_of_dict, struct_with_dict});

  static const char* expected = R"expected(one: int32
two: dictionary<values=string, indices=int16, ordered=0>
  dictionary:
    [
      "foo",
      "bar",
      "baz"
    ]
three: list<two: dictionary<values=string, indices=int16, ordered=0>>
  child 0, two: dictionary<values=string, indices=int16, ordered=0>
    dictionary:
      [
        "foo",
        "bar",
        "baz"
      ]
four: struct<one: int32, two: dictionary<values=string, indices=int16, ordered=0>>
  child 0, one: int32
  child 1, two: dictionary<values=string, indices=int16, ordered=0>
    dictionary:
      [
        "foo",
        "bar",
        "baz"
      ])expected";

  PrettyPrintOptions options{0};

  Check(*sch, options, expected);
}

}  // namespace arrow
