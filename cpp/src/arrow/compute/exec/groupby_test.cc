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
#include "arrow/compute/exec/groupby.h"

#include <gmock/gmock-matchers.h>
#include <gtest/gtest.h>

#include <memory>

#include "arrow/testing/gtest_util.h"

namespace arrow {

namespace compute {

TEST(GroupByConvenienceFunc, Basic) {
  std::shared_ptr<Schema> in_schema =
      schema({field("key1", utf8()), field("key2", int32()), field("value", int32())});
  std::shared_ptr<Table> in_table = TableFromJSON(in_schema, {R"([
    ["x", 1, 1],
    ["y", 1, 2],
    ["y", 2, 3],
    ["z", 2, 4],
    ["z", 2, 5]
  ])"});

  // One key, two aggregates, same values array
  std::shared_ptr<Table> expected =
      TableFromJSON(schema({field("value_sum", int64()), field("value_count", int64()),
                            field("key1", utf8())}),
                    {R"([
        [1, 1, "x"],
        [5, 2, "y"],
        [9, 2, "z"]
    ])"});
  ASSERT_OK_AND_ASSIGN(std::shared_ptr<Table> actual,
                       TableGroupBy(in_table,
                                    {{"hash_sum", {"value"}, "value_sum"},
                                     {"hash_count", {"value"}, "value_count"}},
                                    {"key1"}));
  AssertTablesEqual(*expected, *actual);

  // Two keys, one aggregate
  expected = TableFromJSON(schema({field("value_sum", int64()), field("key1", utf8()),
                                   field("key2", int32())}),
                           {
                               R"([
        [1, "x", 1],
        [2, "y", 1],
        [3, "y", 2],
        [9, "z", 2]
      ])"});

  ASSERT_OK_AND_ASSIGN(actual,
                       TableGroupBy(in_table, {{"hash_sum", {"value"}, "value_sum"}},
                                    {{"key1"}, {"key2"}}));
  AssertTablesEqual(*expected, *actual);

  // No keys (whole table aggregate)
  expected =
      TableFromJSON(schema({field("sum", int64()), field("count", int64())}), {
                                                                                  R"([
      [15, 5]
    ])"});
  ASSERT_OK_AND_ASSIGN(actual, TableGroupBy(in_table,
                                            {{"sum", {"value"}, "value_sum"},
                                             {"count", {"value"}, "value_count"}},
                                            {}));

  // No aggregates (used to get distinct key values)
  expected =
      TableFromJSON(schema({field("key_0", utf8()), field("key_1", int32())}), {
                                                                                   R"([
      ["x", 1],
      ["y", 1],
      ["y", 2],
      ["z", 2]
    ])"});
  ASSERT_OK_AND_ASSIGN(actual, TableGroupBy(in_table, {}, {{"key1"}, {"key2"}}));
  AssertTablesEqual(*expected, *actual);
}

TEST(GroupByConvenienceFunc, Invalid) {
  std::shared_ptr<Schema> in_schema =
      schema({field("key1", utf8()), field("key2", int32()), field("value", int32())});
  std::shared_ptr<Table> in_table = TableFromJSON(in_schema, {R"([
    ["x", 1, 1],
    ["y", 1, 2],
    ["y", 2, 3],
    ["z", 2, 4],
    ["z", 2, 5]
  ])"});

  // Scalar/hash mismatch
  EXPECT_RAISES_WITH_MESSAGE_THAT(
      Invalid, ::testing::HasSubstr("a hash aggregate function was expected"),
      TableGroupBy(in_table, {{"count", {"value"}, "value_count"}}, {"key1"}));
  EXPECT_RAISES_WITH_MESSAGE_THAT(
      Invalid, ::testing::HasSubstr("a scalar aggregate function was expected"),
      TableGroupBy(in_table, {{"hash_count", {"value"}, "value_count"}}, {}));
  // Not an aggregate function
  EXPECT_RAISES_WITH_MESSAGE_THAT(
      Invalid, ::testing::HasSubstr("is not an aggregate function"),
      TableGroupBy(in_table, {{"add", {"value"}, "value_add"}}, {"key1"}));
  EXPECT_RAISES_WITH_MESSAGE_THAT(
      Invalid, ::testing::HasSubstr("is not an aggregate function"),
      TableGroupBy(in_table, {{"add", {"value"}, "value_add"}}, {}));
}

}  // namespace compute
}  // namespace arrow
