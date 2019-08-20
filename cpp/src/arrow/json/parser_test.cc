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

#include <string>
#include <utility>
#include <vector>

#include <gtest/gtest.h>

#include "arrow/json/options.h"
#include "arrow/json/parser.h"
#include "arrow/json/test_common.h"
#include "arrow/status.h"
#include "arrow/testing/gtest_util.h"
#include "arrow/util/string_view.h"

namespace arrow {
namespace json {

using util::string_view;

static std::string scalars_only_src() {
  return R"(
    { "hello": 3.5, "world": false, "yo": "thing" }
    { "hello": 3.25, "world": null }
    { "hello": 3.125, "world": null, "yo": "\u5fcd" }
    { "hello": 0.0, "world": true, "yo": null }
  )";
}

static std::string nested_src() {
  return R"(
    { "hello": 3.5, "world": false, "yo": "thing", "arr": [1, 2, 3], "nuf": {} }
    { "hello": 3.25, "world": null, "arr": [2], "nuf": null }
    { "hello": 3.125, "world": null, "yo": "\u5fcd", "arr": [], "nuf": { "ps": 78 } }
    { "hello": 0.0, "world": true, "yo": null, "arr": null, "nuf": { "ps": 90 } }
  )";
}

void AssertUnconvertedStructArraysEqual(const StructArray& expected,
                                        const StructArray& actual);

void AssertUnconvertedArraysEqual(const Array& expected, const Array& actual) {
  switch (actual.type_id()) {
    case Type::BOOL:
    case Type::NA:
      return AssertArraysEqual(expected, actual);
    case Type::DICTIONARY: {
      ASSERT_EQ(expected.type_id(), Type::STRING);
      std::shared_ptr<Array> actual_decoded;
      ASSERT_OK(DecodeStringDictionary(static_cast<const DictionaryArray&>(actual),
                                       &actual_decoded));
      return AssertArraysEqual(expected, *actual_decoded);
    }
    case Type::LIST: {
      ASSERT_EQ(expected.type_id(), Type::LIST);
      AssertBufferEqual(*expected.null_bitmap(), *actual.null_bitmap());
      const auto& expected_offsets = expected.data()->buffers[1];
      const auto& actual_offsets = actual.data()->buffers[1];
      AssertBufferEqual(*expected_offsets, *actual_offsets);
      auto expected_values = static_cast<const ListArray&>(expected).values();
      auto actual_values = static_cast<const ListArray&>(actual).values();
      return AssertUnconvertedArraysEqual(*expected_values, *actual_values);
    }
    case Type::STRUCT:
      ASSERT_EQ(expected.type_id(), Type::STRUCT);
      return AssertUnconvertedStructArraysEqual(static_cast<const StructArray&>(expected),
                                                static_cast<const StructArray&>(actual));
    default:
      FAIL();
  }
}

void AssertUnconvertedStructArraysEqual(const StructArray& expected,
                                        const StructArray& actual) {
  ASSERT_EQ(expected.num_fields(), actual.num_fields());
  for (int i = 0; i < expected.num_fields(); ++i) {
    auto expected_name = expected.type()->child(i)->name();
    auto actual_name = actual.type()->child(i)->name();
    ASSERT_EQ(expected_name, actual_name);
    AssertUnconvertedArraysEqual(*expected.field(i), *actual.field(i));
  }
}

void AssertParseColumns(ParseOptions options, string_view src_str,
                        const std::vector<std::shared_ptr<Field>>& fields,
                        const std::vector<std::string>& columns_json) {
  std::shared_ptr<Array> parsed;
  ASSERT_OK(ParseFromString(options, src_str, &parsed));
  auto struct_array = std::static_pointer_cast<StructArray>(parsed);
  for (size_t i = 0; i < fields.size(); ++i) {
    auto column_expected = ArrayFromJSON(fields[i]->type(), columns_json[i]);
    auto column = struct_array->GetFieldByName(fields[i]->name());
    AssertUnconvertedArraysEqual(*column_expected, *column);
  }
}

// TODO(bkietz) parameterize (at least some of) these tests over UnexpectedFieldBehavior

TEST(BlockParserWithSchema, Basics) {
  auto options = ParseOptions::Defaults();
  options.explicit_schema =
      schema({field("hello", float64()), field("world", boolean()), field("yo", utf8())});
  options.unexpected_field_behavior = UnexpectedFieldBehavior::Ignore;
  AssertParseColumns(
      options, scalars_only_src(),
      {field("hello", utf8()), field("world", boolean()), field("yo", utf8())},
      {"[\"3.5\", \"3.25\", \"3.125\", \"0.0\"]", "[false, null, null, true]",
       "[\"thing\", null, \"\xe5\xbf\x8d\", null]"});
}

TEST(BlockParserWithSchema, Empty) {
  auto options = ParseOptions::Defaults();
  options.explicit_schema =
      schema({field("hello", float64()), field("world", boolean()), field("yo", utf8())});
  options.unexpected_field_behavior = UnexpectedFieldBehavior::Ignore;
  AssertParseColumns(
      options, "",
      {field("hello", utf8()), field("world", boolean()), field("yo", utf8())},
      {"[]", "[]", "[]"});
}

TEST(BlockParserWithSchema, SkipFieldsOutsideSchema) {
  auto options = ParseOptions::Defaults();
  options.explicit_schema = schema({field("hello", float64()), field("yo", utf8())});
  options.unexpected_field_behavior = UnexpectedFieldBehavior::Ignore;
  AssertParseColumns(options, scalars_only_src(),
                     {field("hello", utf8()), field("yo", utf8())},
                     {"[\"3.5\", \"3.25\", \"3.125\", \"0.0\"]",
                      "[\"thing\", null, \"\xe5\xbf\x8d\", null]"});
}

TEST(BlockParserWithSchema, FailOnInconvertible) {
  auto options = ParseOptions::Defaults();
  options.explicit_schema = schema({field("a", int32())});
  options.unexpected_field_behavior = UnexpectedFieldBehavior::Ignore;
  std::shared_ptr<Array> parsed;
  ASSERT_RAISES(Invalid, ParseFromString(options, "{\"a\":0}\n{\"a\":true}", &parsed));
}

TEST(BlockParserWithSchema, Nested) {
  auto options = ParseOptions::Defaults();
  options.explicit_schema = schema({field("yo", utf8()), field("arr", list(int32())),
                                    field("nuf", struct_({field("ps", int32())}))});
  options.unexpected_field_behavior = UnexpectedFieldBehavior::Ignore;
  AssertParseColumns(options, nested_src(),
                     {field("yo", utf8()), field("arr", list(utf8())),
                      field("nuf", struct_({field("ps", utf8())}))},
                     {"[\"thing\", null, \"\xe5\xbf\x8d\", null]",
                      R"([["1", "2", "3"], ["2"], [], null])",
                      R"([{"ps":null}, null, {"ps":"78"}, {"ps":"90"}])"});
}

TEST(BlockParserWithSchema, FailOnIncompleteJson) {
  auto options = ParseOptions::Defaults();
  options.explicit_schema = schema({field("a", int32())});
  options.unexpected_field_behavior = UnexpectedFieldBehavior::Ignore;
  std::shared_ptr<Array> parsed;
  ASSERT_RAISES(Invalid, ParseFromString(options, "{\"a\":0, \"b\"", &parsed));
}

TEST(BlockParser, Basics) {
  auto options = ParseOptions::Defaults();
  options.unexpected_field_behavior = UnexpectedFieldBehavior::InferType;
  AssertParseColumns(
      options, scalars_only_src(),
      {field("hello", utf8()), field("world", boolean()), field("yo", utf8())},
      {"[\"3.5\", \"3.25\", \"3.125\", \"0.0\"]", "[false, null, null, true]",
       "[\"thing\", null, \"\xe5\xbf\x8d\", null]"});
}

TEST(BlockParser, Nested) {
  auto options = ParseOptions::Defaults();
  options.unexpected_field_behavior = UnexpectedFieldBehavior::InferType;
  AssertParseColumns(options, nested_src(),
                     {field("yo", utf8()), field("arr", list(utf8())),
                      field("nuf", struct_({field("ps", utf8())}))},
                     {"[\"thing\", null, \"\xe5\xbf\x8d\", null]",
                      R"([["1", "2", "3"], ["2"], [], null])",
                      R"([{"ps":null}, null, {"ps":"78"}, {"ps":"90"}])"});
}

}  // namespace json
}  // namespace arrow
