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

#include "arrow/json/json_writer_internal.h"

namespace arrow::json {

TEST(JsonWriter, SimpleObject) {
  JsonWriter writer;

  writer.StartObject();
  writer.Key("a");
  writer.Int(42);
  writer.Key("b");
  writer.String("hello");
  writer.EndObject();

  EXPECT_EQ(writer.GetString(), R"({"a":42,"b":"hello"})");
}

TEST(JsonWriter, Array) {
  JsonWriter writer;

  writer.StartArray();
  writer.Int(1);
  writer.Int(2);
  writer.Int(3);
  writer.EndArray();

  EXPECT_EQ(writer.GetString(), "[1,2,3]");
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

  EXPECT_EQ(writer.GetString(), R"({"child":{"x":true}})");
}

TEST(JsonWriter, NullValue) {
  JsonWriter writer;

  writer.StartObject();
  writer.Key("value");
  writer.Null();
  writer.EndObject();

  EXPECT_EQ(writer.GetString(), R"({"value":null})");
}

TEST(JsonWriter, DoubleValue) {
  JsonWriter writer;

  writer.StartObject();
  writer.Key("pi");
  writer.Double(3.14);
  writer.EndObject();

  EXPECT_EQ(writer.GetString(), R"({"pi":3.14})");
}

TEST(JsonWriter, UnsignedValues) {
  JsonWriter writer;

  writer.StartObject();
  writer.Key("u32");
  writer.Uint(42);
  writer.Key("u64");
  writer.Uint64(1234567890123ULL);
  writer.EndObject();

  EXPECT_EQ(writer.GetString(), R"({"u32":42,"u64":1234567890123})");
}

TEST(JsonWriter, Int64Value) {
  JsonWriter writer;

  writer.StartObject();
  writer.Key("i64");
  writer.Int64(-1234567890123LL);
  writer.EndObject();

  EXPECT_EQ(writer.GetString(), R"({"i64":-1234567890123})");
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

  EXPECT_EQ(writer.GetString(), "[5]");
}

TEST(JsonWriter, RawValue) {
  JsonWriter writer;

  writer.StartObject();
  writer.Key("number");
  writer.RawValue("123.456");
  writer.EndObject();

  ASSERT_EQ(writer.GetString(), R"({"number":123.456})");
}

TEST(JsonWriter, StringWithExplicitLength) {
  JsonWriter writer;

  const char value[] = {'a', 'b', 'c', 'd', 'e'};

  writer.StartObject();
  writer.Key("value");
  writer.String(std::string_view(value, 3));
  writer.EndObject();

  ASSERT_EQ(writer.GetString(), R"({"value":"abc"})");
}

}  // namespace arrow::json
