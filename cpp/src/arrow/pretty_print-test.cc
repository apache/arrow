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
#include <cstring>
#include <memory>
#include <sstream>
#include <string>
#include <vector>

#include <gtest/gtest.h>

#include "arrow/array.h"
#include "arrow/builder.h"
#include "arrow/pretty_print.h"
#include "arrow/table.h"
#include "arrow/testing/gtest_util.h"
#include "arrow/type.h"

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
void CheckPrimitive(const std::shared_ptr<DataType>& type,
                    const PrettyPrintOptions& options, const std::vector<bool>& is_valid,
                    const std::vector<C_TYPE>& values, const char* expected,
                    bool check_operator = true) {
  std::shared_ptr<Array> array;
  ArrayFromVector<TYPE, C_TYPE>(type, is_valid, values, &array);
  CheckArray(*array, options, expected, check_operator);
}

template <typename TYPE, typename C_TYPE>
void CheckPrimitive(const PrettyPrintOptions& options, const std::vector<bool>& is_valid,
                    const std::vector<C_TYPE>& values, const char* expected,
                    bool check_operator = true) {
  CheckPrimitive<TYPE, C_TYPE>(TypeTraits<TYPE>::type_singleton(), options, is_valid,
                               values, expected, check_operator);
}

TEST_F(TestPrettyPrint, PrimitiveType) {
  std::vector<bool> is_valid = {true, true, false, true, false};

  std::vector<int32_t> values = {0, 1, 2, 3, 4};
  static const char* expected = R"expected([
  0,
  1,
  null,
  3,
  null
])expected";
  CheckPrimitive<Int32Type, int32_t>({0, 10}, is_valid, values, expected);

  static const char* expected_na = R"expected([
  0,
  1,
  NA,
  3,
  NA
])expected";
  CheckPrimitive<Int32Type, int32_t>({0, 10, 2, "NA"}, is_valid, values, expected_na,
                                     false);

  static const char* ex_in2 = R"expected(  [
    0,
    1,
    null,
    3,
    null
  ])expected";
  CheckPrimitive<Int32Type, int32_t>({2, 10}, is_valid, values, ex_in2);
  static const char* ex_in2_w2 = R"expected(  [
    0,
    1,
    ...
    3,
    null
  ])expected";
  CheckPrimitive<Int32Type, int32_t>({2, 2}, is_valid, values, ex_in2_w2);

  std::vector<double> values2 = {0., 1., 2., 3., 4.};
  static const char* ex2 = R"expected([
  0,
  1,
  null,
  3,
  null
])expected";
  CheckPrimitive<DoubleType, double>({0, 10}, is_valid, values2, ex2);
  static const char* ex2_in2 = R"expected(  [
    0,
    1,
    null,
    3,
    null
  ])expected";
  CheckPrimitive<DoubleType, double>({2, 10}, is_valid, values2, ex2_in2);

  std::vector<std::string> values3 = {"foo", "bar", "", "baz", ""};
  static const char* ex3 = R"expected([
  "foo",
  "bar",
  null,
  "baz",
  null
])expected";
  CheckPrimitive<StringType, std::string>({0, 10}, is_valid, values3, ex3);
  static const char* ex3_in2 = R"expected(  [
    "foo",
    "bar",
    null,
    "baz",
    null
  ])expected";
  CheckPrimitive<StringType, std::string>({2, 10}, is_valid, values3, ex3_in2);
}

TEST_F(TestPrettyPrint, Int8) {
  static const char* expected = R"expected([
  0,
  127,
  -128
])expected";
  CheckPrimitive<Int8Type, int8_t>({0, 10}, {true, true, true}, {0, 127, -128}, expected);
}

TEST_F(TestPrettyPrint, UInt8) {
  static const char* expected = R"expected([
  0,
  255
])expected";
  CheckPrimitive<UInt8Type, uint8_t>({0, 10}, {true, true}, {0, 255}, expected);
}

TEST_F(TestPrettyPrint, Int64) {
  static const char* expected = R"expected([
  0,
  9223372036854775807,
  -9223372036854775808
])expected";
  CheckPrimitive<Int64Type, int64_t>(
      {0, 10}, {true, true, true}, {0, 9223372036854775807LL, -9223372036854775807LL - 1},
      expected);
}

TEST_F(TestPrettyPrint, UInt64) {
  static const char* expected = R"expected([
  0,
  9223372036854775803,
  18446744073709551615
])expected";
  CheckPrimitive<UInt64Type, uint64_t>(
      {0, 10}, {true, true, true}, {0, 9223372036854775803ULL, 18446744073709551615ULL},
      expected);
}

TEST_F(TestPrettyPrint, DateTimeTypes) {
  std::vector<bool> is_valid = {true, true, false, true, false};

  {
    std::vector<int32_t> values = {0, 1, 2, 31, 4};
    static const char* expected = R"expected([
  1970-01-01,
  1970-01-02,
  null,
  1970-02-01,
  null
])expected";
    CheckPrimitive<Date32Type, int32_t>({0, 10}, is_valid, values, expected);
  }

  {
    constexpr int64_t ms_per_day = 24 * 60 * 60 * 1000;
    std::vector<int64_t> values = {0 * ms_per_day, 1 * ms_per_day, 2 * ms_per_day,
                                   31 * ms_per_day, 4 * ms_per_day};
    static const char* expected = R"expected([
  1970-01-01,
  1970-01-02,
  null,
  1970-02-01,
  null
])expected";
    CheckPrimitive<Date64Type, int64_t>({0, 10}, is_valid, values, expected);
  }

  {
    std::vector<int64_t> values = {
        0, 1, 2, 678 + 1000000 * (5 + 60 * (4 + 60 * (3 + 24 * int64_t(1)))), 4};
    static const char* expected = R"expected([
  1970-01-01 00:00:00.000000,
  1970-01-01 00:00:00.000001,
  null,
  1970-01-02 03:04:05.000678,
  null
])expected";
    CheckPrimitive<TimestampType, int64_t>(timestamp(TimeUnit::MICRO, "Transylvania"),
                                           {0, 10}, is_valid, values, expected);
  }

  {
    std::vector<int32_t> values = {1, 62, 2, 3 + 60 * (2 + 60 * 1), 4};
    static const char* expected = R"expected([
  00:00:01,
  00:01:02,
  null,
  01:02:03,
  null
])expected";
    CheckPrimitive<Time32Type, int32_t>(time32(TimeUnit::SECOND), {0, 10}, is_valid,
                                        values, expected);
  }

  {
    std::vector<int64_t> values = {
        0, 1, 2, 678 + int64_t(1000000000) * (5 + 60 * (4 + 60 * 3)), 4};
    static const char* expected = R"expected([
  00:00:00.000000000,
  00:00:00.000000001,
  null,
  03:04:05.000000678,
  null
])expected";
    CheckPrimitive<Time64Type, int64_t>(time64(TimeUnit::NANO), {0, 10}, is_valid, values,
                                        expected);
  }
}

TEST_F(TestPrettyPrint, StructTypeBasic) {
  auto simple_1 = field("one", int32());
  auto simple_2 = field("two", int32());
  auto simple_struct = struct_({simple_1, simple_2});

  auto array = ArrayFromJSON(simple_struct, "[[11, 22]]");

  static const char* ex = R"expected(-- is_valid: all not null
-- child 0 type: int32
  [
    11
  ]
-- child 1 type: int32
  [
    22
  ])expected";
  CheckStream(*array, {0, 10}, ex);

  static const char* ex_2 = R"expected(  -- is_valid: all not null
  -- child 0 type: int32
    [
      11
    ]
  -- child 1 type: int32
    [
      22
    ])expected";
  CheckStream(*array, {2, 10}, ex_2);
}

TEST_F(TestPrettyPrint, StructTypeAdvanced) {
  auto simple_1 = field("one", int32());
  auto simple_2 = field("two", int32());
  auto simple_struct = struct_({simple_1, simple_2});

  auto array = ArrayFromJSON(simple_struct, "[[11, 22], null, [null, 33]]");

  static const char* ex = R"expected(-- is_valid:
  [
    true,
    false,
    true
  ]
-- child 0 type: int32
  [
    11,
    null,
    null
  ]
-- child 1 type: int32
  [
    22,
    null,
    33
  ])expected";
  CheckStream(*array, {0, 10}, ex);
}

TEST_F(TestPrettyPrint, BinaryType) {
  std::vector<bool> is_valid = {true, true, false, true, true, true};
  std::vector<std::string> values = {"foo", "bar", "", "baz", "", "\xff"};
  static const char* ex = "[\n  666F6F,\n  626172,\n  null,\n  62617A,\n  ,\n  FF\n]";
  CheckPrimitive<BinaryType, std::string>({0}, is_valid, values, ex);
  static const char* ex_in2 =
      "  [\n    666F6F,\n    626172,\n    null,\n    62617A,\n    ,\n    FF\n  ]";
  CheckPrimitive<BinaryType, std::string>({2}, is_valid, values, ex_in2);
}

TEST_F(TestPrettyPrint, ListType) {
  auto list_type = list(int64());
  auto array = ArrayFromJSON(list_type, "[[null], [], null, [4, 6, 7], [2, 3]]");

  static const char* ex = R"expected([
  [
    null
  ],
  [],
  null,
  [
    4,
    6,
    7
  ],
  [
    2,
    3
  ]
])expected";
  CheckArray(*array, {0, 10}, ex);
  static const char* ex_2 = R"expected(  [
    [
      null
    ],
    [],
    null,
    [
      4,
      6,
      7
    ],
    [
      2,
      3
    ]
  ])expected";
  CheckArray(*array, {2, 10}, ex_2);
  static const char* ex_3 = R"expected([
  [
    null
  ],
  ...
  [
    2,
    3
  ]
])expected";
  CheckStream(*array, {0, 1}, ex_3);
}

TEST_F(TestPrettyPrint, MapType) {
  auto map_type = map(utf8(), int64());
  auto array = ArrayFromJSON(map_type, R"([
    [["joe", 0], ["mark", null]],
    null,
    [["cap", 8]],
    []
  ])");

  static const char* ex = R"expected([
  keys:
  [
    "joe",
    "mark"
  ]
  values:
  [
    0,
    null
  ],
  null,
  keys:
  [
    "cap"
  ]
  values:
  [
    8
  ],
  keys:
  []
  values:
  []
])expected";
  CheckArray(*array, {0, 10}, ex);
}

TEST_F(TestPrettyPrint, FixedSizeListType) {
  auto list_type = fixed_size_list(int32(), 3);
  auto array = ArrayFromJSON(list_type,
                             "[[null, 0, 1], [2, 3, null], null, [4, 6, 7], [8, 9, 5]]");

  CheckArray(*array, {0, 10}, R"expected([
  [
    null,
    0,
    1
  ],
  [
    2,
    3,
    null
  ],
  null,
  [
    4,
    6,
    7
  ],
  [
    8,
    9,
    5
  ]
])expected");
  CheckStream(*array, {0, 1}, R"expected([
  [
    null,
    ...
    1
  ],
  ...
  [
    8,
    ...
    5
  ]
])expected");
}

TEST_F(TestPrettyPrint, FixedSizeBinaryType) {
  std::vector<bool> is_valid = {true, true, false, true, false};

  auto type = fixed_size_binary(3);
  auto array = ArrayFromJSON(type, "[\"foo\", \"bar\", null, \"baz\"]");

  static const char* ex = "[\n  666F6F,\n  626172,\n  null,\n  62617A\n]";
  CheckArray(*array, {0, 10}, ex);
  static const char* ex_2 = "  [\n    666F6F,\n    ...\n    62617A\n  ]";
  CheckArray(*array, {2, 1}, ex_2);
}

TEST_F(TestPrettyPrint, Decimal128Type) {
  int32_t p = 19;
  int32_t s = 4;

  auto type = decimal(p, s);
  auto array = ArrayFromJSON(type, "[\"123.4567\", \"456.7891\", null]");

  static const char* ex = "[\n  123.4567,\n  456.7891,\n  null\n]";
  CheckArray(*array, {0}, ex);
}

TEST_F(TestPrettyPrint, DictionaryType) {
  std::vector<bool> is_valid = {true, true, false, true, true, true};

  std::shared_ptr<Array> dict;
  std::vector<std::string> dict_values = {"foo", "bar", "baz"};
  ArrayFromVector<StringType, std::string>(dict_values, &dict);
  std::shared_ptr<DataType> dict_type = dictionary(int16(), utf8());

  std::shared_ptr<Array> indices;
  std::vector<int16_t> indices_values = {1, 2, -1, 0, 2, 0};
  ArrayFromVector<Int16Type, int16_t>(is_valid, indices_values, &indices);
  auto arr = std::make_shared<DictionaryArray>(dict_type, indices, dict);

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
  auto array = ArrayFromJSON(int32(), "[0, 1, null, 3, null]");
  ChunkedArray chunked_array(array);

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

TEST_F(TestPrettyPrint, ColumnPrimitiveType) {
  std::shared_ptr<Field> int_field = field("column", int32());
  auto array = ArrayFromJSON(int_field->type(), "[0, 1, null, 3, null]");
  Column column(int_field, ArrayVector({array}));

  static const char* expected = R"expected(column: int32
[
  [
    0,
    1,
    null,
    3,
    null
  ]
])expected";
  CheckStream(column, {0}, expected);

  Column column_2(int_field, {array, array});

  static const char* expected_2 = R"expected(column: int32
[
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

  CheckStream(column_2, {0}, expected_2);
}

TEST_F(TestPrettyPrint, TablePrimitive) {
  std::shared_ptr<Field> int_field = field("column", int32());
  auto array = ArrayFromJSON(int_field->type(), "[0, 1, null, 3, null]");
  std::shared_ptr<Column> column =
      std::make_shared<Column>(int_field, ArrayVector({array}));
  std::shared_ptr<Schema> table_schema = schema({int_field});
  std::shared_ptr<Table> table = Table::Make(table_schema, {column});

  static const char* expected = R"expected(column: int32
----
column:
  [
    [
      0,
      1,
      null,
      3,
      null
    ]
  ]
)expected";
  CheckStream(*table, {0}, expected);
}

TEST_F(TestPrettyPrint, SchemaWithDictionary) {
  std::vector<bool> is_valid = {true, true, false, true, true, true};

  std::shared_ptr<Array> dict;
  std::vector<std::string> dict_values = {"foo", "bar", "baz"};
  ArrayFromVector<StringType, std::string>(dict_values, &dict);

  auto simple = field("one", int32());
  auto simple_dict = field("two", dictionary(int16(), utf8()));
  auto list_of_dict = field("three", list(simple_dict));
  auto struct_with_dict = field("four", struct_({simple, simple_dict}));

  auto sch = schema({simple, simple_dict, list_of_dict, struct_with_dict});

  static const char* expected = R"expected(one: int32
two: dictionary<values=string, indices=int16, ordered=0>
three: list<two: dictionary<values=string, indices=int16, ordered=0>>
  child 0, two: dictionary<values=string, indices=int16, ordered=0>
four: struct<one: int32, two: dictionary<values=string, indices=int16, ordered=0>>
  child 0, one: int32
  child 1, two: dictionary<values=string, indices=int16, ordered=0>)expected";

  PrettyPrintOptions options{0};

  Check(*sch, options, expected);
}

}  // namespace arrow
