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

#include <iosfwd>
#include <string>
#include <vector>

#include "parquet/schema/printer.h"
#include "parquet/schema/types.h"
#include "parquet/types.h"

using std::string;
using std::vector;

namespace parquet {

namespace schema {

static std::string Print(const NodePtr& node) {
  std::stringstream ss;
  PrintSchema(node.get(), ss);
  return ss.str();
}

TEST(TestSchemaPrinter, Examples) {
  // Test schema 1
  NodeVector fields;
  fields.push_back(Int32("a", Repetition::REQUIRED));

  // 3-level list encoding
  NodePtr item1 = Int64("item1");
  NodePtr item2 = Boolean("item2", Repetition::REQUIRED);
  NodePtr list(GroupNode::Make("b", Repetition::REPEATED, {item1, item2},
          LogicalType::LIST));
  NodePtr bag(GroupNode::Make("bag", Repetition::OPTIONAL, {list}));
  fields.push_back(bag);

  NodePtr schema = GroupNode::Make("schema", Repetition::REPEATED, fields);

  std::string result = Print(schema);
  std::string expected = R"(repeated group schema {
  required int32 a
  optional group bag {
    repeated group b {
      optional int64 item1
      required boolean item2
    }
  }
}
)";
  ASSERT_EQ(expected, result);
}

} // namespace schema

} // namespace parquet
