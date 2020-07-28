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

#include "gandiva/rex_holder.h"

#include <gtest/gtest.h>

#include <memory>

#include "gandiva/like_holder.h"
#include "gandiva/regex_util.h"
#include "rapidjson/document.h"

namespace gandiva {

class TestRexHolder : public ::testing::Test {
 public:
  FunctionNode BuildRex(std::string pattern) {
    auto field = std::make_shared<FieldNode>(arrow::field("in", arrow::utf8()));
    auto pattern_node =
        std::make_shared<LiteralNode>(arrow::utf8(), LiteralHolder(pattern), false);
    return FunctionNode("rex", {field, pattern_node},
                        arrow::map(arrow::utf8(), arrow::utf8()));
  }
};

TEST_F(TestRexHolder, TestTest) {
  std::shared_ptr<RexHolder> rex_holder;
  string pattern =
      ".+ (?P<url>/[0-9a-zA-Z\\./\\-\\_\\~]*) +(?P<version>HTTP/[0-9]+\\.[0-9]+).+ "
      "[0-9]{3} +(?P<bytes>[0-9\\-]+) .+\\((?P<os>Macintosh|Windows "
      "NT|iPhone|compatible).+";
  auto status = RexHolder::Make(pattern, &rex_holder);
  EXPECT_EQ(status.ok(), true) << status.message();

  auto& rex = *rex_holder;
  rapidjson::Document exp_result;
  auto expected_json_string =
      R"j({"os":"Windows NT","bytes":"97106","version":"HTTP/1.1","url":"/apache-log/access.log"})j";
  exp_result.Parse(expected_json_string);
  auto act_result =
      rex("107.173.176.148 - - [13/Dec/2015:05:11:56 +0100] \"GET /apache-log/access.log "
          "HTTP/1.1\" 200 97106 \"http://www.almhuette-raith.at/\" \"Mozilla/5.0 "
          "(Windows NT "
          "6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Iron/29.0.1600.1 "
          "Chrome/29.0.1600.1 Safari/537.36\" \"-\"");
  EXPECT_EQ(act_result, exp_result);
  auto node = BuildRex(pattern);
  EXPECT_EQ(node.descriptor()->name(), "rex");
}

}  // namespace gandiva
