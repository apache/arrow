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

#include "arrow/json/parser.h"

#include <gmock/gmock-matchers.h>
#include <gtest/gtest.h>

#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "arrow/json/options.h"
#include "arrow/json/test_common.h"
#include "arrow/status.h"
#include "arrow/testing/gtest_util.h"
#include "arrow/type_fwd.h"
#include "arrow/util/checked_cast.h"

namespace arrow {

using internal::checked_cast;

namespace json {

using std::string_view;

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
      ASSERT_OK(DecodeStringDictionary(checked_cast<const DictionaryArray&>(actual),
                                       &actual_decoded));
      return AssertArraysEqual(expected, *actual_decoded);
    }
    case Type::LIST: {
      ASSERT_EQ(expected.type_id(), Type::LIST);
      ASSERT_EQ(expected.null_count(), actual.null_count());
      if (expected.null_count() != 0) {
        AssertBufferEqual(*expected.null_bitmap(), *actual.null_bitmap());
      }
      const auto& expected_offsets = expected.data()->buffers[1];
      const auto& actual_offsets = actual.data()->buffers[1];
      AssertBufferEqual(*expected_offsets, *actual_offsets);
      auto expected_values = checked_cast<const ListArray&>(expected).values();
      auto actual_values = checked_cast<const ListArray&>(actual).values();
      return AssertUnconvertedArraysEqual(*expected_values, *actual_values);
    }
    case Type::STRUCT:
      ASSERT_EQ(expected.type_id(), Type::STRUCT);
      return AssertUnconvertedStructArraysEqual(
          checked_cast<const StructArray&>(expected),
          checked_cast<const StructArray&>(actual));
    default:
      FAIL();
  }
}

void AssertUnconvertedStructArraysEqual(const StructArray& expected,
                                        const StructArray& actual) {
  ASSERT_EQ(expected.num_fields(), actual.num_fields());
  for (int i = 0; i < expected.num_fields(); ++i) {
    auto expected_name = expected.type()->field(i)->name();
    auto actual_name = actual.type()->field(i)->name();
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

TEST(BlockParserWithSchema, UnquotedDecimal) {
  auto options = ParseOptions::Defaults();
  options.explicit_schema =
      schema({field("price", decimal128(9, 2)), field("cost", decimal128(9, 3))});
  AssertParseColumns(options, unquoted_decimal_src(),
                     {field("price", utf8()), field("cost", utf8())},
                     {R"(["30.04", "1.23"])", R"(["30.001", "1.229"])"});
}

TEST(BlockParserWithSchema, MixedDecimal) {
  auto options = ParseOptions::Defaults();
  options.explicit_schema =
      schema({field("price", decimal128(9, 2)), field("cost", decimal128(9, 3))});
  AssertParseColumns(options, mixed_decimal_src(),
                     {field("price", utf8()), field("cost", utf8())},
                     {R"(["30.04", "1.23"])", R"(["30.001", "1.229"])"});
}

class BlockParserTypeError : public ::testing::TestWithParam<UnexpectedFieldBehavior> {
 public:
  ParseOptions Options(std::shared_ptr<Schema> explicit_schema) {
    auto options = ParseOptions::Defaults();
    options.explicit_schema = std::move(explicit_schema);
    options.unexpected_field_behavior = GetParam();
    return options;
  }
};

TEST_P(BlockParserTypeError, FailOnInconvertible) {
  auto options = Options(schema({field("a", int32())}));
  std::shared_ptr<Array> parsed;
  Status error = ParseFromString(options, "{\"a\":0}\n{\"a\":true}", &parsed);
  ASSERT_RAISES(Invalid, error);
  EXPECT_THAT(
      error.message(),
      testing::StartsWith(
          "JSON parse error: Column(/a) changed from number to boolean in row 1"));
}

TEST_P(BlockParserTypeError, FailOnNestedInconvertible) {
  auto options = Options(schema({field("a", list(struct_({field("b", int32())})))}));
  std::shared_ptr<Array> parsed;
  Status error =
      ParseFromString(options, "{\"a\":[{\"b\":0}]}\n{\"a\":[{\"b\":true}]}", &parsed);
  ASSERT_RAISES(Invalid, error);
  EXPECT_THAT(
      error.message(),
      testing::StartsWith(
          "JSON parse error: Column(/a/[]/b) changed from number to boolean in row 1"));
}

TEST_P(BlockParserTypeError, FailOnDuplicateKeys) {
  std::shared_ptr<Array> parsed;
  Status error = ParseFromString(Options(schema({field("a", int32())})),
                                 "{\"a\":0, \"a\":1}\n", &parsed);
  ASSERT_RAISES(Invalid, error);
  EXPECT_THAT(
      error.message(),
      testing::StartsWith("JSON parse error: Column(/a) was specified twice in row 0"));
}

TEST_P(BlockParserTypeError, FailOnDuplicateKeysNoSchema) {
  std::shared_ptr<Array> parsed;
  Status error =
      ParseFromString(ParseOptions::Defaults(), "{\"a\":0, \"a\":1}\n", &parsed);

  ASSERT_RAISES(Invalid, error);
  EXPECT_THAT(
      error.message(),
      testing::StartsWith("JSON parse error: Column(/a) was specified twice in row 0"));
}

INSTANTIATE_TEST_SUITE_P(BlockParserTypeError, BlockParserTypeError,
                         ::testing::Values(UnexpectedFieldBehavior::Ignore,
                                           UnexpectedFieldBehavior::Error,
                                           UnexpectedFieldBehavior::InferType));

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
                      R"([{"ps":null}, {}, {"ps":"78"}, {"ps":"90"}])"});
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
                      R"([{"ps":null}, {}, {"ps":"78"}, {"ps":"90"}])"});
}

TEST(BlockParser, Null) {
  auto options = ParseOptions::Defaults();
  options.unexpected_field_behavior = UnexpectedFieldBehavior::InferType;
  AssertParseColumns(
      options, null_src(),
      {field("plain", null()), field("list1", list(null())), field("list2", list(null())),
       field("struct", struct_({field("plain", null())}))},
      {"[null, null]", "[[], []]", "[[], [null]]",
       R"([{"plain": null}, {"plain": null}])"});
}

TEST(BlockParser, InferNewFields) {
  std::string src = R"(
    {}
    {"a": true}
    {"a": false, "b": true}
  )";
  auto options = ParseOptions::Defaults();
  options.unexpected_field_behavior = UnexpectedFieldBehavior::InferType;
  for (const auto& s : {schema({field("a", boolean()), field("b", boolean())}),
                        std::shared_ptr<Schema>(nullptr)}) {
    options.explicit_schema = s;
    AssertParseColumns(options, src, {field("a", boolean()), field("b", boolean())},
                       {"[null, true, false]", "[null, null, true]"});
  }
}

TEST(BlockParser, InferNewFieldsInMiddle) {
  std::string src = R"(
    {"a": true, "b": false}
    {"a": false, "c": "foo", "b": true}
    {"b": false}
  )";
  auto options = ParseOptions::Defaults();
  options.unexpected_field_behavior = UnexpectedFieldBehavior::InferType;
  for (const auto& s : {schema({field("a", boolean()), field("b", boolean())}),
                        std::shared_ptr<Schema>(nullptr)}) {
    options.explicit_schema = s;
    AssertParseColumns(
        options, src, {field("a", boolean()), field("b", boolean()), field("c", utf8())},
        {"[true, false, null]", "[false, true, false]", "[null, \"foo\", null]"});
  }
}

TEST(BlockParser, FailOnInvalidEOF) {
  std::shared_ptr<Array> parsed;
  auto status = ParseFromString(ParseOptions::Defaults(), "}", &parsed);
  ASSERT_RAISES(Invalid, status);
  EXPECT_THAT(status.message(),
              ::testing::StartsWith("JSON parse error: The document is empty"));
}

TEST(BlockParser, AdHoc) {
  auto options = ParseOptions::Defaults();
  options.unexpected_field_behavior = UnexpectedFieldBehavior::InferType;
  AssertParseColumns(
      options, R"({"a": [1], "b": {"c": true, "d": "1991-02-03"}}
{"a": [], "b": {"c": false, "d": "2019-04-01"}}
)",
      {field("a", list(utf8())),
       field("b", struct_({field("c", boolean()), field("d", utf8())}))},
      {R"([["1"], []])",
       R"([{"c":true, "d": "1991-02-03"}, {"c":false, "d":"2019-04-01"}])"});
}

}  // namespace json
}  // namespace arrow
