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

#include "arrow/json/object_parser.h"

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <string>
#include <unordered_map>

#include "arrow/testing/gtest_util.h"

namespace arrow {
namespace json {
namespace internal {

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

}  // namespace internal
}  // namespace json
}  // namespace arrow
