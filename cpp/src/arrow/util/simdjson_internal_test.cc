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

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <string>
#include <unordered_map>

#include "arrow/testing/gtest_util.h"
#include "arrow/util/simdjson_internal.h"

namespace sj = simdjson::ondemand;

namespace arrow::internal {

TEST(JsonWriter, SimpleObject) {
  JsonWriter writer;

  writer.StartObject();
  writer.Key("a");
  writer.Int(42);
  writer.Key("b");
  writer.String("hello");
  writer.EndObject();

  ASSERT_OK_AND_ASSIGN(std::string_view json, writer.GetString());

  EXPECT_EQ(json, R"({"a":42,"b":"hello"})");
}

TEST(JsonWriter, Array) {
  JsonWriter writer;

  writer.StartArray();
  writer.Int(1);
  writer.Int(2);
  writer.Int(3);
  writer.EndArray();

  ASSERT_OK_AND_ASSIGN(std::string_view json, writer.GetString());

  EXPECT_EQ(json, "[1,2,3]");
}

TEST(JsonWriter, NestedObject) {
  JsonWriter writer;

  writer.StartObject();

  writer.Key("child");
  writer.StartObject();
  writer.Key("x");
  writer.Bool(true);
  writer.EndObject();

  writer.EndObject();

  ASSERT_OK_AND_ASSIGN(std::string_view json, writer.GetString());

  EXPECT_EQ(json, R"({"child":{"x":true}})");
}

TEST(JsonWriter, NullValue) {
  JsonWriter writer;

  writer.StartObject();
  writer.Key("value");
  writer.Null();
  writer.EndObject();

  ASSERT_OK_AND_ASSIGN(std::string_view json, writer.GetString());

  EXPECT_EQ(json, R"({"value":null})");
}

TEST(JsonWriter, DoubleValue) {
  JsonWriter writer;

  writer.StartObject();
  writer.Key("pi");
  writer.Double(3.14);
  writer.EndObject();

  ASSERT_OK_AND_ASSIGN(std::string_view json, writer.GetString());

  EXPECT_EQ(json, R"({"pi":3.14})");
}

TEST(JsonWriter, UnsignedValues) {
  JsonWriter writer;

  writer.StartObject();
  writer.Key("u32");
  writer.Uint(42);
  writer.Key("u64");
  writer.Uint64(1234567890123ULL);
  writer.EndObject();

  ASSERT_OK_AND_ASSIGN(std::string_view json, writer.GetString());

  EXPECT_EQ(json, R"({"u32":42,"u64":1234567890123})");
}

TEST(JsonWriter, Int64Value) {
  JsonWriter writer;

  writer.StartObject();
  writer.Key("i64");
  writer.Int64(-1234567890123LL);
  writer.EndObject();

  ASSERT_OK_AND_ASSIGN(std::string_view json, writer.GetString());

  EXPECT_EQ(json, R"({"i64":-1234567890123})");
}

TEST(JsonWriter, Clear) {
  JsonWriter writer;

  writer.StartObject();
  writer.Key("a");
  writer.Int(1);
  writer.EndObject();

  writer.Clear();

  writer.StartArray();
  writer.Int(5);
  writer.EndArray();

  ASSERT_OK_AND_ASSIGN(std::string_view json, writer.GetString());

  EXPECT_EQ(json, "[5]");
}

TEST(JsonWriter, RawValue) {
  JsonWriter writer;

  writer.StartObject();
  writer.Key("number");
  writer.RawValue("123.456");
  writer.EndObject();

  ASSERT_OK_AND_ASSIGN(std::string_view json, writer.GetString());

  EXPECT_EQ(json, R"({"number":123.456})");
}

TEST(JsonWriter, StringWithExplicitLength) {
  JsonWriter writer;

  const char value[] = {'a', 'b', 'c', 'd', 'e'};

  writer.StartObject();
  writer.Key("value");
  writer.String(std::string_view(value, 3));
  writer.EndObject();

  ASSERT_OK_AND_ASSIGN(std::string_view json, writer.GetString());

  EXPECT_EQ(json, R"({"value":"abc"})");
}

TEST(JsonWriter, WriteValueSimpleObject) {
  sj::parser parser;
  std::string json_str = R"({"a":42,"b":"hello"})";
  simdjson::padded_string json(json_str);

  sj::document doc;
  ASSERT_EQ(parser.iterate(json).get(doc), simdjson::SUCCESS);

  sj::value value;
  ASSERT_EQ(doc.get_value().get(value), simdjson::SUCCESS);

  JsonWriter writer;
  ASSERT_OK(writer.WriteValue(value));

  ASSERT_OK_AND_ASSIGN(std::string_view out, writer.GetString());
  EXPECT_EQ(out, R"({"a":42,"b":"hello"})");
}

TEST(JsonWriter, WriteValueNestedObject) {
  sj::parser parser;
  std::string json_str = R"({"child":{"x":true}})";
  simdjson::padded_string json(json_str);

  sj::document doc;
  ASSERT_EQ(parser.iterate(json).get(doc), simdjson::SUCCESS);

  sj::value value;
  ASSERT_EQ(doc.get_value().get(value), simdjson::SUCCESS);

  JsonWriter writer;
  ASSERT_OK(writer.WriteValue(value));

  ASSERT_OK_AND_ASSIGN(std::string_view out, writer.GetString());
  EXPECT_EQ(out, R"({"child":{"x":true}})");
}

TEST(JsonWriter, WriteValueObjectWithArray) {
  sj::parser parser;
  std::string json_str = R"({"values":[1,2,3]})";
  simdjson::padded_string json(json_str);

  sj::document doc;
  ASSERT_EQ(parser.iterate(json).get(doc), simdjson::SUCCESS);

  sj::value value;
  ASSERT_EQ(doc.get_value().get(value), simdjson::SUCCESS);

  JsonWriter writer;
  ASSERT_OK(writer.WriteValue(value));

  ASSERT_OK_AND_ASSIGN(std::string_view out, writer.GetString());
  EXPECT_EQ(out, R"({"values":[1,2,3]})");
}

TEST(JsonWriter, WriteValueComplexObject) {
  sj::parser parser;
  std::string json_str =
      R"({"name":"arrow","version":1,"enabled":true,"values":[1,2.5,null,{"nested":[false,{"x":10}]}]})";
  simdjson::padded_string json(json_str);

  sj::document doc;
  ASSERT_EQ(parser.iterate(json).get(doc), simdjson::SUCCESS);

  sj::value value;
  ASSERT_EQ(doc.get_value().get(value), simdjson::SUCCESS);

  JsonWriter writer;
  ASSERT_OK(writer.WriteValue(value));

  ASSERT_OK_AND_ASSIGN(std::string_view out, writer.GetString());
  EXPECT_EQ(
      out,
      R"({"name":"arrow","version":1,"enabled":true,"values":[1,2.5,null,{"nested":[false,{"x":10}]}]})");
}

TEST(JsonWriter, WriteValueEmptyObject) {
  sj::parser parser;
  std::string json_str = "{}";
  simdjson::padded_string json(json_str);

  sj::document doc;
  ASSERT_EQ(parser.iterate(json).get(doc), simdjson::SUCCESS);

  sj::value value;
  ASSERT_EQ(doc.get_value().get(value), simdjson::SUCCESS);

  JsonWriter writer;
  ASSERT_OK(writer.WriteValue(value));

  ASSERT_OK_AND_ASSIGN(std::string_view out, writer.GetString());
  EXPECT_EQ(out, "{}");
}

TEST(JsonWriter, WriteValueAllNumberTypes) {
  sj::parser parser;
  std::string json_str = R"({
    "signed":-42,
    "unsigned":18446744073709551615,
    "double":2.5,
    "big":184467440737095516161234567890
  })";
  simdjson::padded_string json(json_str);

  sj::document doc;
  ASSERT_EQ(parser.iterate(json).get(doc), simdjson::SUCCESS);

  sj::value value;
  ASSERT_EQ(doc.get_value().get(value), simdjson::SUCCESS);

  JsonWriter writer;
  ASSERT_OK(writer.WriteValue(value));

  ASSERT_OK_AND_ASSIGN(std::string_view out, writer.GetString());
  EXPECT_EQ(
      out,
      R"({"signed":-42,"unsigned":18446744073709551615,"double":2.5,"big":184467440737095516161234567890})");
}

TEST(JsonWriter, IntField) {
  JsonWriter writer;

  writer.StartObject();
  writer.IntField("a", 42);
  writer.EndObject();

  ASSERT_OK_AND_ASSIGN(std::string_view json, writer.GetString());

  EXPECT_EQ(json, R"({"a":42})");
}

TEST(JsonWriter, GetPrettyString) {
  JsonWriter writer;

  writer.StartObject();
  writer.Key("a");
  writer.Int(42);
  writer.Key("b");
  writer.String("hello");
  writer.EndObject();

  ASSERT_OK_AND_ASSIGN(std::string pretty, writer.GetPrettyString());

  // Pretty output should differ from the compact form (padded spacing, at minimum),
  // even though a small object like this may still be rendered on one line.
  EXPECT_NE(pretty, R"({"a":42,"b":"hello"})");

  // But it should still parse back to the same values.
  sj::parser parser;
  simdjson::padded_string padded(pretty);
  sj::document doc;
  ASSERT_EQ(parser.iterate(padded).get(doc), simdjson::SUCCESS);

  int64_t a_value;
  ASSERT_EQ(doc["a"].get(a_value), simdjson::SUCCESS);
  EXPECT_EQ(a_value, 42);

  std::string_view b_value;
  ASSERT_EQ(doc["b"].get(b_value), simdjson::SUCCESS);
  EXPECT_EQ(b_value, "hello");
}

TEST(ObjectParser, GetString) {
  ObjectParser parser;

  ASSERT_OK(parser.Parse(R"({"name":"arrow"})"));

  ASSERT_OK_AND_ASSIGN(auto value, parser.GetString("name"));
  EXPECT_EQ(value, "arrow");
}

TEST(ObjectParser, GetBool) {
  ObjectParser parser;

  ASSERT_OK(parser.Parse(R"({"enabled":true})"));

  ASSERT_OK_AND_ASSIGN(auto value, parser.GetBool("enabled"));
  EXPECT_TRUE(value);
}

TEST(ObjectParser, InvalidJson) {
  ObjectParser parser;

  EXPECT_RAISES_WITH_MESSAGE_THAT(Invalid, ::testing::HasSubstr("JSON parse error"),
                                  parser.Parse(R"({"name":)"));
}

TEST(ObjectParser, GetStringMap) {
  ObjectParser parser;

  ASSERT_OK(parser.Parse(R"({
    "k1": "v1",
    "k2": "v2"
  })"));

  ASSERT_OK_AND_ASSIGN(auto map, parser.GetStringMap());

  ASSERT_EQ(map.size(), 2U);
  EXPECT_EQ(map["k1"], "v1");
  EXPECT_EQ(map["k2"], "v2");
}

TEST(ObjectParser, MissingKey) {
  ObjectParser parser;

  ASSERT_OK(parser.Parse(R"({
    "name": "arrow"
  })"));

  ASSERT_RAISES(KeyError, parser.GetString("missing"));
  ASSERT_RAISES(KeyError, parser.GetBool("missing"));
}

TEST(ObjectParser, WrongType) {
  ObjectParser parser;

  ASSERT_OK(parser.Parse(R"({
    "flag": true,
    "name": "arrow"
  })"));

  ASSERT_RAISES(TypeError, parser.GetString("flag"));
  ASSERT_RAISES(TypeError, parser.GetBool("name"));
}

TEST(ObjectParser, NonObjectRoot) {
  ObjectParser parser;

  ASSERT_RAISES(TypeError, parser.Parse(R"(["a", "b"])"));
}

TEST(ObjectParser, EmptyObject) {
  ObjectParser parser;

  ASSERT_OK(parser.Parse(R"({})"));

  ASSERT_OK_AND_ASSIGN(auto map, parser.GetStringMap());

  EXPECT_TRUE(map.empty());
}

}  // namespace arrow::internal
