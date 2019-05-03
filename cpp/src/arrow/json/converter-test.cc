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

#include <string>

#include <gtest/gtest.h>

#include "arrow/json/options.h"
#include "arrow/json/test-common.h"

namespace arrow {
namespace json {

using util::string_view;

void AssertConvert(const std::shared_ptr<DataType>& expected_type,
                   const std::string& expected_json,
                   const std::string& unconverted_json) {
  // make an unconverted array
  auto scalar_values = ArrayFromJSON(utf8(), unconverted_json);
  Int32Builder indices_builder;
  ASSERT_OK(indices_builder.Resize(scalar_values->length()));
  for (int i = 0; i < scalar_values->length(); ++i) {
    if (scalar_values->IsNull(i)) {
      indices_builder.UnsafeAppendNull();
    } else {
      indices_builder.UnsafeAppend(i);
    }
  }
  std::shared_ptr<Array> indices, unconverted, converted;
  ASSERT_OK(indices_builder.Finish(&indices));
  ASSERT_OK(DictionaryArray::FromArrays(dictionary(int32(), scalar_values), indices,
                                        &unconverted));

  // convert the array
  std::shared_ptr<Converter> converter;
  ASSERT_OK(MakeConverter(expected_type, default_memory_pool(), &converter));
  ASSERT_OK(converter->Convert(unconverted, &converted));

  // assert equality
  auto expected = ArrayFromJSON(expected_type, expected_json);
  AssertArraysEqual(*expected, *converted);
}

// bool, null are trivial pass throughs

TEST(ConverterTest, Integers) {
  for (auto expected_type : {uint8(), uint16(), uint32(), uint64()}) {
    AssertConvert(expected_type, "[0, null, 1, 32, 45, 12, 64, 124]",
                  R"(["0", null, "1", "32", "45", "12", "64", "124"])");
  }
  for (auto expected_type : {int8(), int16(), int32(), int64()}) {
    AssertConvert(expected_type, "[0, null, -1, 32, -45, 12, -64, 124]",
                  R"(["-0", null, "-1", "32", "-45", "12", "-64", "124"])");
  }
}

TEST(ConverterTest, Floats) {
  for (auto expected_type : {float32(), float64()}) {
    AssertConvert(expected_type, "[0, -0.0, null, 32.0, 1e5]",
                  R"(["0", "-0.0", null, "32.0", "1e5"])");
  }
}

TEST(ConverterTest, String) {
  std::string src = R"(["a", "b c", null, "d e f", "g"])";
  AssertConvert(utf8(), src, src);
}

TEST(ConverterTest, Timestamp) {
  std::string src = R"([null, "1970-01-01", "2018-11-13 17:11:10"])";
  AssertConvert(timestamp(TimeUnit::SECOND), src, src);
}

}  // namespace json
}  // namespace arrow
