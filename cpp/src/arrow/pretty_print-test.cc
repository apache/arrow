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

namespace arrow {

class TestPrettyPrint : public ::testing::Test {
 public:
  void SetUp() {}

  void Print(const Array& array) {}

 private:
  std::ostringstream sink_;
};

void CheckArray(const Array& arr, int indent, const char* expected) {
  std::ostringstream sink;
  ASSERT_OK(PrettyPrint(arr, indent, &sink));
  std::string result = sink.str();
  ASSERT_EQ(std::string(expected, strlen(expected)), result);

  std::stringstream ss;
  ss << arr;
  ASSERT_EQ(result, ss.str());
}

template <typename T>
void Check(const T& obj, const PrettyPrintOptions& options, const char* expected) {
  std::string result;
  ASSERT_OK(PrettyPrint(obj, options, &result));
  ASSERT_EQ(std::string(expected, strlen(expected)), result);
}

template <typename TYPE, typename C_TYPE>
void CheckPrimitive(int indent, const std::vector<bool>& is_valid,
                    const std::vector<C_TYPE>& values, const char* expected) {
  std::shared_ptr<Array> array;
  ArrayFromVector<TYPE, C_TYPE>(is_valid, values, &array);
  CheckArray(*array, indent, expected);
}

TEST_F(TestPrettyPrint, PrimitiveType) {
  std::vector<bool> is_valid = {true, true, false, true, false};

  std::vector<int32_t> values = {0, 1, 2, 3, 4};
  static const char* expected = R"expected([0, 1, null, 3, null])expected";
  CheckPrimitive<Int32Type, int32_t>(0, is_valid, values, expected);

  std::vector<std::string> values2 = {"foo", "bar", "", "baz", ""};
  static const char* ex2 = R"expected(["foo", "bar", null, "baz", null])expected";
  CheckPrimitive<StringType, std::string>(0, is_valid, values2, ex2);
}

TEST_F(TestPrettyPrint, BinaryType) {
  std::vector<bool> is_valid = {true, true, false, true, false};
  std::vector<std::string> values = {"foo", "bar", "", "baz", ""};
  static const char* ex = R"expected([666F6F, 626172, null, 62617A, null])expected";
  CheckPrimitive<BinaryType, std::string>(0, is_valid, values, ex);
}

TEST_F(TestPrettyPrint, FixedSizeBinaryType) {
  std::vector<bool> is_valid = {true, true, false, true, false};
  std::vector<std::string> values = {"foo", "bar", "baz"};
  static const char* ex = R"expected([666F6F, 626172, 62617A])expected";

  std::shared_ptr<Array> array;
  auto type = fixed_size_binary(3);
  FixedSizeBinaryBuilder builder(type);

  ASSERT_OK(builder.Append(values[0]));
  ASSERT_OK(builder.Append(values[1]));
  ASSERT_OK(builder.Append(values[2]));
  ASSERT_OK(builder.Finish(&array));

  CheckArray(*array, 0, ex);
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
-- is_valid: [true, true, false, true, true, true]
-- dictionary: ["foo", "bar", "baz"]
-- indices: [1, 2, null, 0, 2, 0])expected";

  CheckArray(*arr, 0, expected);
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
  dictionary: ["foo", "bar", "baz"]
three: list<two: dictionary<values=string, indices=int16, ordered=0>>
  child 0, two: dictionary<values=string, indices=int16, ordered=0>
      dictionary: ["foo", "bar", "baz"]
four: struct<one: int32, two: dictionary<values=string, indices=int16, ordered=0>>
  child 0, one: int32
  child 1, two: dictionary<values=string, indices=int16, ordered=0>
      dictionary: ["foo", "bar", "baz"])expected";

  PrettyPrintOptions options{0};

  Check(*sch, options, expected);
}

}  // namespace arrow
