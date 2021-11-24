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

#include "arrow/compute/exec/hash_join.h"
#include "arrow/compute/exec/schema_util.h"
#include "arrow/testing/gtest_util.h"
#include "arrow/testing/matchers.h"

using testing::Eq;

namespace arrow {
namespace compute {

const char* kLeftPrefix = "left.";
const char* kRightPrefix = "right.";

TEST(FieldMap, Trivial) {
  HashJoinSchema schema_mgr;

  auto left = schema({field("i32", int32())});
  auto right = schema({field("i32", int32())});

  ASSERT_OK(schema_mgr.Init(JoinType::INNER, *left, {"i32"}, *right, {"i32"}, kLeftPrefix,
                            kRightPrefix));

  auto output = schema_mgr.MakeOutputSchema(kLeftPrefix, kRightPrefix);
  EXPECT_THAT(*output, Eq(Schema({
                           field("left.i32", int32()),
                           field("right.i32", int32()),
                       })));

  auto i =
      schema_mgr.proj_maps[0].map(HashJoinProjection::INPUT, HashJoinProjection::OUTPUT);
  EXPECT_EQ(i.get(0), 0);
}

TEST(FieldMap, TrivialDuplicates) {
  HashJoinSchema schema_mgr;

  auto left = schema({field("i32", int32())});
  auto right = schema({field("i32", int32())});

  ASSERT_OK(schema_mgr.Init(JoinType::INNER, *left, {"i32"}, *right, {"i32"}, "", ""));

  auto output = schema_mgr.MakeOutputSchema("", "");
  EXPECT_THAT(*output, Eq(Schema({
                           field("i32", int32()),
                           field("i32", int32()),
                       })));

  auto i =
      schema_mgr.proj_maps[0].map(HashJoinProjection::INPUT, HashJoinProjection::OUTPUT);
  EXPECT_EQ(i.get(0), 0);
}

TEST(FieldMap, SingleKeyField) {
  HashJoinSchema schema_mgr;

  auto left = schema({field("i32", int32()), field("str", utf8())});
  auto right = schema({field("f32", float32()), field("i32", int32())});

  ASSERT_OK(schema_mgr.Init(JoinType::INNER, *left, {"i32"}, *right, {"i32"}, kLeftPrefix,
                            kRightPrefix));

  EXPECT_EQ(schema_mgr.proj_maps[0].num_cols(HashJoinProjection::INPUT), 2);
  EXPECT_EQ(schema_mgr.proj_maps[1].num_cols(HashJoinProjection::INPUT), 2);
  EXPECT_EQ(schema_mgr.proj_maps[0].num_cols(HashJoinProjection::KEY), 1);
  EXPECT_EQ(schema_mgr.proj_maps[1].num_cols(HashJoinProjection::KEY), 1);
  EXPECT_EQ(schema_mgr.proj_maps[0].num_cols(HashJoinProjection::OUTPUT), 2);
  EXPECT_EQ(schema_mgr.proj_maps[1].num_cols(HashJoinProjection::OUTPUT), 2);

  auto output = schema_mgr.MakeOutputSchema(kLeftPrefix, kRightPrefix);
  EXPECT_THAT(*output, Eq(Schema({
                           field("left.i32", int32()),
                           field("left.str", utf8()),
                           field("right.f32", float32()),
                           field("right.i32", int32()),
                       })));

  auto i =
      schema_mgr.proj_maps[0].map(HashJoinProjection::INPUT, HashJoinProjection::OUTPUT);
  EXPECT_EQ(i.get(0), 0);
}

TEST(FieldMap, TwoKeyFields) {
  HashJoinSchema schema_mgr;

  auto left = schema({
      field("i32", int32()),
      field("str", utf8()),
      field("bool", boolean()),
  });
  auto right = schema({
      field("i32", int32()),
      field("str", utf8()),
      field("f32", float32()),
      field("f64", float64()),
  });

  ASSERT_OK(schema_mgr.Init(JoinType::INNER, *left, {"i32", "str"}, *right,
                            {"i32", "str"}, kLeftPrefix, kRightPrefix));

  auto output = schema_mgr.MakeOutputSchema(kLeftPrefix, kRightPrefix);
  EXPECT_THAT(*output, Eq(Schema({
                           field("left.i32", int32()),
                           field("left.str", utf8()),
                           field("left.bool", boolean()),

                           field("right.i32", int32()),
                           field("right.str", utf8()),
                           field("right.f32", float32()),
                           field("right.f64", float64()),
                       })));
}

}  // namespace compute
}  // namespace arrow
