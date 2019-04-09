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

static std::string scalars_only_src() {
  return R"(
    { "hello": 3.5, "world": false, "yo": "thing" }
    { "hello": 3.2, "world": null }
    { "hello": 3.4, "world": null, "yo": "\u5fcd" }
    { "hello": 0.0, "world": true, "yo": null }
  )";
}

static std::string nested_src() {
  return R"(
    { "hello": 3.5, "world": false, "yo": "thing", "arr": [1, 2, 3], "nuf": {} }
    { "hello": 3.2, "world": null, "arr": [2], "nuf": null }
    { "hello": 3.4, "world": null, "yo": "\u5fcd", "arr": [], "nuf": { "ps": 78 } }
    { "hello": 0.0, "world": true, "yo": null, "arr": null, "nuf": { "ps": 90 } }
  )";
}

void AssertParseAndConvert(ParseOptions options, string_view src_str,
                           const std::vector<std::shared_ptr<Field>>& fields,
                           const std::vector<std::string>& columns_json) {
  std::shared_ptr<Array> parsed;
  ASSERT_OK(ParseFromString(options, src_str, &parsed));
  auto struct_array = static_cast<StructArray*>(parsed.get());
  for (size_t i = 0; i < fields.size(); ++i) {
    auto column_expected = ArrayFromJSON(fields[i]->type(), columns_json[i]);
    std::shared_ptr<Array> column;
    ASSERT_OK(Convert(default_memory_pool(), fields[i]->type(),
                      struct_array->GetFieldByName(fields[i]->name()), &column));
    AssertArraysEqual(*column_expected, *column);
  }
}

TEST(ConversionTest, Basics) {
  auto options = ParseOptions::Defaults();
  options.unexpected_field_behavior = UnexpectedFieldBehavior::InferType;
  AssertParseAndConvert(
      options, scalars_only_src(),
      {field("hello", float64()), field("world", boolean()), field("yo", utf8())},
      {"[3.5, 3.2, 3.4, 0.0]", "[false, null, null, true]",
       "[\"thing\", null, \"\xe5\xbf\x8d\", null]"});
}

TEST(ConversionTest, Nested) {
  auto options = ParseOptions::Defaults();
  options.unexpected_field_behavior = UnexpectedFieldBehavior::InferType;
  AssertParseAndConvert(
      options, nested_src(),
      {field("yo", utf8()), field("arr", list(int64())),
       field("nuf", struct_({field("ps", int64())}))},
      {"[\"thing\", null, \"\xe5\xbf\x8d\", null]", R"([[1, 2, 3], [2], [], null])",
       R"([{"ps":null}, null, {"ps":78}, {"ps":90}])"});
}

TEST(ConversionTest, PartialSchema) {
  auto options = ParseOptions::Defaults();
  options.unexpected_field_behavior = UnexpectedFieldBehavior::InferType;
  options.explicit_schema = schema({field("nuf", struct_({field("absent", date32())})),
                                    field("arr", list(float32()))});
  AssertParseAndConvert(
      options, nested_src(),
      {field("yo", utf8()), field("arr", list(float32())),
       field("nuf", struct_({field("absent", date32()), field("ps", int64())}))},
      {"[\"thing\", null, \"\xe5\xbf\x8d\", null]", R"([[1, 2, 3], [2], [], null])",
       R"([{"absent":null,"ps":null}, null,)"
       R"( {"absent":null,"ps":78}, {"absent":null,"ps":90}])"});
}

}  // namespace json
}  // namespace arrow
