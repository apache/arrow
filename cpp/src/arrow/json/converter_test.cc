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

#include "arrow/json/converter.h"

#include <gtest/gtest.h>

#include <string>

#include "arrow/json/options.h"
#include "arrow/json/test_common.h"

namespace arrow {
namespace json {

Result<std::shared_ptr<Array>> Convert(std::shared_ptr<DataType> type,
                                       std::shared_ptr<Array> unconverted) {
  std::shared_ptr<Array> converted;
  // convert the array
  std::shared_ptr<Converter> converter;
  RETURN_NOT_OK(MakeConverter(type, default_memory_pool(), &converter));
  RETURN_NOT_OK(converter->Convert(unconverted, &converted));
  RETURN_NOT_OK(converted->ValidateFull());
  return converted;
}

// bool, null are trivial pass throughs

TEST(ConverterTest, Integers) {
  for (auto int_type : {int8(), int16(), int32(), int64()}) {
    ParseOptions options;
    options.explicit_schema = schema({field("", int_type)});

    std::string json_source = R"(
    {"" : -0}
    {"" : null}
    {"" : -1}
    {"" : 32}
    {"" : -45}
    {"" : 12}
    {"" : -64}
    {"" : 124}
  )";

    std::shared_ptr<StructArray> parse_array;
    ASSERT_OK(ParseFromString(options, json_source, &parse_array));

    // call to convert
    ASSERT_OK_AND_ASSIGN(auto converted,
                         Convert(int_type, parse_array->GetFieldByName("")));

    // assert equality
    auto expected = ArrayFromJSON(int_type, R"([
          -0, null, -1, 32, -45, 12, -64, 124])");

    AssertArraysEqual(*expected, *converted);
  }
}

TEST(ConverterTest, UnsignedIntegers) {
  for (auto uint_type : {uint8(), uint16(), uint32(), uint64()}) {
    ParseOptions options;
    options.explicit_schema = schema({field("", uint_type)});

    std::string json_source = R"(
    {"" : 0}
    {"" : null}
    {"" : 1}
    {"" : 32}
    {"" : 45}
    {"" : 12}
    {"" : 64}
    {"" : 124}
  )";

    std::shared_ptr<StructArray> parse_array;
    ASSERT_OK(ParseFromString(options, json_source, &parse_array));

    // call to convert
    ASSERT_OK_AND_ASSIGN(auto converted,
                         Convert(uint_type, parse_array->GetFieldByName("")));

    // assert equality
    auto expected = ArrayFromJSON(uint_type, R"([
          0, null, 1, 32, 45, 12, 64, 124])");

    AssertArraysEqual(*expected, *converted);
  }
}

TEST(ConverterTest, Floats) {
  for (auto float_type : {float32(), float64()}) {
    ParseOptions options;
    options.explicit_schema = schema({field("", float_type)});

    std::string json_source = R"(
    {"" : 0}
    {"" : -0.0}
    {"" : null}
    {"" : 32.0}
    {"" : 1e5}
  )";

    std::shared_ptr<StructArray> parse_array;
    ASSERT_OK(ParseFromString(options, json_source, &parse_array));

    // call to convert
    ASSERT_OK_AND_ASSIGN(auto converted,
                         Convert(float_type, parse_array->GetFieldByName("")));

    // assert equality
    auto expected = ArrayFromJSON(float_type, R"([
          0, -0.0, null, 32.0, 1e5])");

    AssertArraysEqual(*expected, *converted);
  }
}

TEST(ConverterTest, StringAndLargeString) {
  for (auto string_type : {utf8(), large_utf8()}) {
    ParseOptions options;
    options.explicit_schema = schema({field("", string_type)});

    std::string json_source = R"(
    {"" : "a"}
    {"" : "b c"}
    {"" : null}
    {"" : "d e f"}
    {"" : "g"}
  )";

    std::shared_ptr<StructArray> parse_array;
    ASSERT_OK(ParseFromString(options, json_source, &parse_array));

    // call to convert
    ASSERT_OK_AND_ASSIGN(auto converted,
                         Convert(string_type, parse_array->GetFieldByName("")));

    // assert equality
    auto expected = ArrayFromJSON(string_type, R"([
          "a", "b c", null, "d e f", "g"])");

    AssertArraysEqual(*expected, *converted);
  }
}

TEST(ConverterTest, Timestamp) {
  auto timestamp_type = timestamp(TimeUnit::SECOND);

  ParseOptions options;
  options.explicit_schema = schema({field("", timestamp_type)});

  std::string json_source = R"(
    {"" : null}
    {"" : "1970-01-01"}
    {"" : "2018-11-13 17:11:10"}
  )";

  std::shared_ptr<StructArray> parse_array;
  ASSERT_OK(ParseFromString(options, json_source, &parse_array));

  // call to convert
  ASSERT_OK_AND_ASSIGN(auto converted,
                       Convert(timestamp_type, parse_array->GetFieldByName("")));

  // assert equality
  auto expected = ArrayFromJSON(timestamp_type, R"([
          null, "1970-01-01", "2018-11-13 17:11:10"])");

  AssertArraysEqual(*expected, *converted);
}

TEST(ConverterTest, Decimal128And256) {
  for (auto decimal_type : {decimal128(38, 10), decimal256(38, 10)}) {
    ParseOptions options;
    options.explicit_schema = schema({field("", decimal_type)});

    std::string json_source = R"(
    {"" : "02.0000000000"}
    {"" : "30.0000000000"}
  )";

    std::shared_ptr<StructArray> parse_array;
    ASSERT_OK(ParseFromString(options, json_source, &parse_array));

    // call to convert
    ASSERT_OK_AND_ASSIGN(auto converted,
                         Convert(decimal_type, parse_array->GetFieldByName("")));

    // assert equality
    auto expected = ArrayFromJSON(decimal_type, R"([
          "02.0000000000",
          "30.0000000000"])");

    AssertArraysEqual(*expected, *converted);
  }
}

}  // namespace json
}  // namespace arrow
