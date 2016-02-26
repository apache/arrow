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

// Schema / column descriptor correctness tests (from flat Parquet schemas)

#include <gtest/gtest.h>
#include <cstdint>
#include <cstdlib>
#include <string>
#include <vector>

#include "parquet/exception.h"
#include "parquet/schema/descriptor.h"
#include "parquet/schema/types.h"
#include "parquet/types.h"

using std::string;
using std::vector;

namespace parquet_cpp {

namespace schema {

TEST(TestColumnDescriptor, TestAttrs) {
  NodePtr node = PrimitiveNode::Make("name", Repetition::OPTIONAL,
      Type::BYTE_ARRAY, LogicalType::UTF8);
  ColumnDescriptor descr(node, 4, 1);

  ASSERT_EQ("name", descr.name());
  ASSERT_EQ(4, descr.max_definition_level());
  ASSERT_EQ(1, descr.max_repetition_level());

  ASSERT_EQ(Type::BYTE_ARRAY, descr.physical_type());

  ASSERT_THROW(descr.type_length(), ParquetException);

  // Test FIXED_LEN_BYTE_ARRAY
  node = PrimitiveNode::MakeFLBA("name", Repetition::OPTIONAL,
      12, LogicalType::UTF8);
  descr = ColumnDescriptor(node, 4, 1);

  ASSERT_EQ(Type::FIXED_LEN_BYTE_ARRAY, descr.physical_type());
  ASSERT_EQ(12, descr.type_length());
}

class TestSchemaDescriptor : public ::testing::Test {
 public:
  void setUp() {
  }

 protected:
  SchemaDescriptor descr_;
};

TEST_F(TestSchemaDescriptor, InitNonGroup) {
  NodePtr node = PrimitiveNode::Make("field", Repetition::OPTIONAL,
      Type::INT32);

  ASSERT_THROW(descr_.Init(node), ParquetException);
}

TEST_F(TestSchemaDescriptor, BuildTree) {
  NodeVector fields;
  NodePtr schema;

  fields.push_back(Int32("a", Repetition::REQUIRED));
  fields.push_back(Int64("b", Repetition::OPTIONAL));
  fields.push_back(ByteArray("c", Repetition::REPEATED));

  // 3-level list encoding
  NodePtr item1 = Int64("item1", Repetition::REQUIRED);
  NodePtr item2 = Boolean("item2", Repetition::OPTIONAL);
  NodePtr item3 = Int32("item3", Repetition::REPEATED);
  NodePtr list(GroupNode::Make("records", Repetition::REPEATED,
          {item1, item2, item3}, LogicalType::LIST));
  NodePtr bag(GroupNode::Make("bag", Repetition::OPTIONAL, {list}));
  fields.push_back(bag);

  schema = GroupNode::Make("schema", Repetition::REPEATED, fields);

  descr_.Init(schema);

  size_t nleaves = 6;

  // 6 leaves
  ASSERT_EQ(nleaves, descr_.num_columns());

  //                             mdef mrep
  // required int32 a            0    0
  // optional int64 b            1    0
  // repeated byte_array c       1    1
  // optional group bag          1    0
  //   repeated group records    2    1
  //     required int64 item1    2    1
  //     optional boolean item1  3    1
  //     repeated int32 item3    3    2
  int16_t ex_max_def_levels[6] = {0, 1, 1, 2, 3, 3};
  int16_t ex_max_rep_levels[6] = {0, 0, 1, 1, 1, 2};

  for (size_t i = 0; i < nleaves; ++i) {
    const ColumnDescriptor* col = descr_.Column(i);
    EXPECT_EQ(ex_max_def_levels[i], col->max_definition_level()) << i;
    EXPECT_EQ(ex_max_rep_levels[i], col->max_repetition_level()) << i;
  }

  // Init clears the leaves
  descr_.Init(schema);
  ASSERT_EQ(nleaves, descr_.num_columns());
}

} // namespace schema

} // namespace parquet_cpp
