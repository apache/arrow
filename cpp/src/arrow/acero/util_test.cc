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

#include "arrow/acero/hash_join_node.h"
#include "arrow/acero/schema_util.h"
#include "arrow/testing/extension_type.h"
#include "arrow/testing/gtest_util.h"
#include "arrow/testing/matchers.h"

using testing::Eq;

namespace arrow {
namespace acero {

const char* kLeftSuffix = ".left";
const char* kRightSuffix = ".right";

TEST(FieldMap, Trivial) {
  HashJoinSchema schema_mgr;

  auto left = schema({field("i32", int32())});
  auto right = schema({field("i32", int32())});

  ASSERT_OK(schema_mgr.Init(JoinType::INNER, *left, {"i32"}, *right, {"i32"},
                            literal(true), kLeftSuffix, kRightSuffix));

  auto output = schema_mgr.MakeOutputSchema(kLeftSuffix, kRightSuffix);
  EXPECT_THAT(*output, Eq(Schema({
                           field("i32.left", int32()),
                           field("i32.right", int32()),
                       })));

  auto i =
      schema_mgr.proj_maps[0].map(HashJoinProjection::INPUT, HashJoinProjection::OUTPUT);
  EXPECT_EQ(i.get(0), 0);
}

TEST(FieldMap, TrivialDuplicates) {
  HashJoinSchema schema_mgr;

  auto left = schema({field("i32", int32())});
  auto right = schema({field("i32", int32())});

  ASSERT_OK(schema_mgr.Init(JoinType::INNER, *left, {"i32"}, *right, {"i32"},
                            literal(true), "", ""));

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

  ASSERT_OK(schema_mgr.Init(JoinType::INNER, *left, {"i32"}, *right, {"i32"},
                            literal(true), kLeftSuffix, kRightSuffix));

  EXPECT_EQ(schema_mgr.proj_maps[0].num_cols(HashJoinProjection::INPUT), 2);
  EXPECT_EQ(schema_mgr.proj_maps[1].num_cols(HashJoinProjection::INPUT), 2);
  EXPECT_EQ(schema_mgr.proj_maps[0].num_cols(HashJoinProjection::KEY), 1);
  EXPECT_EQ(schema_mgr.proj_maps[1].num_cols(HashJoinProjection::KEY), 1);
  EXPECT_EQ(schema_mgr.proj_maps[0].num_cols(HashJoinProjection::OUTPUT), 2);
  EXPECT_EQ(schema_mgr.proj_maps[1].num_cols(HashJoinProjection::OUTPUT), 2);

  auto output = schema_mgr.MakeOutputSchema(kLeftSuffix, kRightSuffix);
  EXPECT_THAT(*output, Eq(Schema({
                           field("i32.left", int32()),
                           field("str", utf8()),
                           field("f32", float32()),
                           field("i32.right", int32()),
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
                            {"i32", "str"}, literal(true), kLeftSuffix, kRightSuffix));

  auto output = schema_mgr.MakeOutputSchema(kLeftSuffix, kRightSuffix);
  EXPECT_THAT(*output, Eq(Schema({
                           field("i32.left", int32()),
                           field("str.left", utf8()),
                           field("bool", boolean()),

                           field("i32.right", int32()),
                           field("str.right", utf8()),
                           field("f32", float32()),
                           field("f64", float64()),
                       })));
}

TEST(FieldMap, ExtensionTypeSwissJoin) {
  // For simpler types swiss join will be used.
  HashJoinSchema schema_mgr;

  auto left = schema({field("i32", int32()), field("ext", uuid())});
  auto right = schema({field("i32", int32())});

  ASSERT_OK(schema_mgr.Init(JoinType::INNER, *left, {"i32"}, *right, {"i32"},
                            literal(true), kLeftSuffix, kRightSuffix));

  EXPECT_EQ(schema_mgr.proj_maps[0].num_cols(HashJoinProjection::INPUT), 2);
  EXPECT_EQ(schema_mgr.proj_maps[0].num_cols(HashJoinProjection::KEY), 1);
  EXPECT_EQ(schema_mgr.proj_maps[1].num_cols(HashJoinProjection::KEY), 1);
  EXPECT_EQ(schema_mgr.proj_maps[0].num_cols(HashJoinProjection::OUTPUT), 2);

  auto output = schema_mgr.MakeOutputSchema(kLeftSuffix, kRightSuffix);
  EXPECT_THAT(*output, Eq(Schema({field("i32.left", int32()), field("ext", uuid()),
                                  field("i32.right", int32())})));

  auto i =
      schema_mgr.proj_maps[0].map(HashJoinProjection::INPUT, HashJoinProjection::OUTPUT);
  EXPECT_EQ(i.get(0), 0);
}

TEST(FieldMap, ExtensionTypeHashJoin) {
  // Swiss join doesn't support dictionaries so HashJoin will be used.
  HashJoinSchema schema_mgr;

  auto dict_type = dictionary(int64(), int8());
  auto left = schema({field("i32", int32()), field("ext", uuid())});
  auto right = schema({field("i32", int32()), field("dict_type", dict_type)});

  ASSERT_OK(schema_mgr.Init(JoinType::INNER, *left, {"i32"}, *right, {"i32"},
                            literal(true), kLeftSuffix, kRightSuffix));

  EXPECT_EQ(schema_mgr.proj_maps[0].num_cols(HashJoinProjection::INPUT), 2);
  EXPECT_EQ(schema_mgr.proj_maps[1].num_cols(HashJoinProjection::INPUT), 2);
  EXPECT_EQ(schema_mgr.proj_maps[0].num_cols(HashJoinProjection::KEY), 1);
  EXPECT_EQ(schema_mgr.proj_maps[1].num_cols(HashJoinProjection::KEY), 1);
  EXPECT_EQ(schema_mgr.proj_maps[0].num_cols(HashJoinProjection::OUTPUT), 2);
  EXPECT_EQ(schema_mgr.proj_maps[1].num_cols(HashJoinProjection::OUTPUT), 2);

  auto output = schema_mgr.MakeOutputSchema(kLeftSuffix, kRightSuffix);
  EXPECT_THAT(*output, Eq(Schema({
                           field("i32.left", int32()),
                           field("ext", uuid()),
                           field("i32.right", int32()),
                           field("dict_type", dict_type),
                       })));

  auto i =
      schema_mgr.proj_maps[0].map(HashJoinProjection::INPUT, HashJoinProjection::OUTPUT);
  EXPECT_EQ(i.get(0), 0);
}

}  // namespace acero
}  // namespace arrow
