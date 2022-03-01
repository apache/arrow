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

#include "gandiva/parse_url_holder.h"
#include "gandiva/regex_util.h"

#include <memory>
#include <vector>

#include <gtest/gtest.h>

namespace gandiva {
class TestParseUrlHolder : public ::testing::Test {
public:
  FunctionNode BuildParseUrl(std::string part_to_extract) {
    auto field = std::make_shared<FieldNode>(arrow::field("parse_url", arrow::utf8()));
    auto pattern_node = std::make_shared<LiteralNode>(
        arrow::utf8(), LiteralHolder(part_to_extract), false);
    return FunctionNode("parse_url", {field, pattern_node}, arrow::utf8());
  }
};
TEST_F(TestParseUrlHolder, Test) {
  std::shared_ptr<ParseUrlHolder> parse_url_holder;
  auto node = BuildParseUrl("HOST");

  auto status = ParseUrlHolder::Make(node, &parse_url_holder);
  EXPECT_TRUE(status.ok()) << status.message();

  EXPECT_EQ(parse_url_holder->Parse("http://userinfo@github.io/path1/path2/p.php?k1=v1&k2=v2#Ref1"), "userinfo@github.io");
}
}  // namespace gandiva