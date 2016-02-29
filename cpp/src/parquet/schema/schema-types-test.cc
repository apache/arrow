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

#include <memory>
#include <string>
#include <vector>

#include "parquet/exception.h"
#include "parquet/schema/test-util.h"
#include "parquet/schema/types.h"
#include "parquet/thrift/parquet_types.h"
#include "parquet/types.h"

using std::string;
using std::vector;

namespace parquet_cpp {

namespace schema {

// ----------------------------------------------------------------------
// Primitive node

class TestPrimitiveNode : public ::testing::Test {
 public:
  void setUp() {
    name_ = "name";
    id_ = 5;
  }

  void Convert(const parquet::SchemaElement* element) {
    node_ = PrimitiveNode::FromParquet(element, id_);
    ASSERT_TRUE(node_->is_primitive());
    prim_node_ = static_cast<const PrimitiveNode*>(node_.get());
  }

 protected:
  std::string name_;
  const PrimitiveNode* prim_node_;

  int id_;
  std::unique_ptr<Node> node_;
};

TEST_F(TestPrimitiveNode, Attrs) {
  PrimitiveNode node1("foo", Repetition::REPEATED, Type::INT32);

  PrimitiveNode node2("bar", Repetition::OPTIONAL, Type::BYTE_ARRAY,
      LogicalType::UTF8);

  ASSERT_EQ("foo", node1.name());

  ASSERT_TRUE(node1.is_primitive());
  ASSERT_FALSE(node1.is_group());

  ASSERT_EQ(Repetition::REPEATED, node1.repetition());
  ASSERT_EQ(Repetition::OPTIONAL, node2.repetition());

  ASSERT_EQ(Node::PRIMITIVE, node1.node_type());

  ASSERT_EQ(Type::INT32, node1.physical_type());
  ASSERT_EQ(Type::BYTE_ARRAY, node2.physical_type());

  // logical types
  ASSERT_EQ(LogicalType::NONE, node1.logical_type());
  ASSERT_EQ(LogicalType::UTF8, node2.logical_type());

  // repetition
  node1 = PrimitiveNode("foo", Repetition::REQUIRED, Type::INT32);
  node2 = PrimitiveNode("foo", Repetition::OPTIONAL, Type::INT32);
  PrimitiveNode node3("foo", Repetition::REPEATED, Type::INT32);

  ASSERT_TRUE(node1.is_required());

  ASSERT_TRUE(node2.is_optional());
  ASSERT_FALSE(node2.is_required());

  ASSERT_TRUE(node3.is_repeated());
  ASSERT_FALSE(node3.is_optional());
}

TEST_F(TestPrimitiveNode, FromParquet) {
  SchemaElement elt = NewPrimitive(name_, FieldRepetitionType::OPTIONAL,
      parquet::Type::INT32);
  Convert(&elt);
  ASSERT_EQ(name_, prim_node_->name());
  ASSERT_EQ(id_, prim_node_->id());
  ASSERT_EQ(Repetition::OPTIONAL, prim_node_->repetition());
  ASSERT_EQ(Type::INT32, prim_node_->physical_type());
  ASSERT_EQ(LogicalType::NONE, prim_node_->logical_type());

  // Test a logical type
  elt = NewPrimitive(name_, FieldRepetitionType::REQUIRED, parquet::Type::BYTE_ARRAY);
  elt.__set_converted_type(ConvertedType::UTF8);

  Convert(&elt);
  ASSERT_EQ(Repetition::REQUIRED, prim_node_->repetition());
  ASSERT_EQ(Type::BYTE_ARRAY, prim_node_->physical_type());
  ASSERT_EQ(LogicalType::UTF8, prim_node_->logical_type());

  // FIXED_LEN_BYTE_ARRAY
  elt = NewPrimitive(name_, FieldRepetitionType::OPTIONAL,
      parquet::Type::FIXED_LEN_BYTE_ARRAY);
  elt.__set_type_length(16);

  Convert(&elt);
  ASSERT_EQ(name_, prim_node_->name());
  ASSERT_EQ(id_, prim_node_->id());
  ASSERT_EQ(Repetition::OPTIONAL, prim_node_->repetition());
  ASSERT_EQ(Type::FIXED_LEN_BYTE_ARRAY, prim_node_->physical_type());
  ASSERT_EQ(16, prim_node_->type_length());

  // ConvertedType::Decimal
  elt = NewPrimitive(name_, FieldRepetitionType::OPTIONAL,
      parquet::Type::FIXED_LEN_BYTE_ARRAY);
  elt.__set_converted_type(ConvertedType::DECIMAL);
  elt.__set_type_length(6);
  elt.__set_scale(2);
  elt.__set_precision(12);

  Convert(&elt);
  ASSERT_EQ(Type::FIXED_LEN_BYTE_ARRAY, prim_node_->physical_type());
  ASSERT_EQ(LogicalType::DECIMAL, prim_node_->logical_type());
  ASSERT_EQ(6, prim_node_->type_length());
  ASSERT_EQ(2, prim_node_->decimal_metadata().scale);
  ASSERT_EQ(12, prim_node_->decimal_metadata().precision);
}

TEST_F(TestPrimitiveNode, Equals) {
  PrimitiveNode node1("foo", Repetition::REQUIRED, Type::INT32);
  PrimitiveNode node2("foo", Repetition::REQUIRED, Type::INT64);
  PrimitiveNode node3("bar", Repetition::REQUIRED, Type::INT32);
  PrimitiveNode node4("foo", Repetition::OPTIONAL, Type::INT32);
  PrimitiveNode node5("foo", Repetition::REQUIRED, Type::INT32);

  ASSERT_TRUE(node1.Equals(&node1));
  ASSERT_FALSE(node1.Equals(&node2));
  ASSERT_FALSE(node1.Equals(&node3));
  ASSERT_FALSE(node1.Equals(&node4));
  ASSERT_TRUE(node1.Equals(&node5));

  PrimitiveNode flba1("foo", Repetition::REQUIRED, Type::FIXED_LEN_BYTE_ARRAY,
      LogicalType::DECIMAL, 12, 4, 2);

  PrimitiveNode flba2("foo", Repetition::REQUIRED, Type::FIXED_LEN_BYTE_ARRAY,
      LogicalType::DECIMAL, 1, 4, 2);
  flba2.SetTypeLength(12);

  PrimitiveNode flba3("foo", Repetition::REQUIRED, Type::FIXED_LEN_BYTE_ARRAY,
      LogicalType::DECIMAL, 1, 4, 2);
  flba3.SetTypeLength(16);

  PrimitiveNode flba4("foo", Repetition::REQUIRED, Type::FIXED_LEN_BYTE_ARRAY,
      LogicalType::DECIMAL, 12, 4, 0);

  PrimitiveNode flba5("foo", Repetition::REQUIRED, Type::FIXED_LEN_BYTE_ARRAY,
      LogicalType::NONE, 12, 4, 0);

  ASSERT_TRUE(flba1.Equals(&flba2));
  ASSERT_FALSE(flba1.Equals(&flba3));
  ASSERT_FALSE(flba1.Equals(&flba4));
  ASSERT_FALSE(flba1.Equals(&flba5));
}

TEST_F(TestPrimitiveNode, PhysicalLogicalMapping) {
  ASSERT_NO_THROW(PrimitiveNode::Make("foo", Repetition::REQUIRED,
        Type::INT32, LogicalType::INT_32));
  ASSERT_NO_THROW(PrimitiveNode::Make("foo", Repetition::REQUIRED,
        Type::BYTE_ARRAY, LogicalType::JSON));
  ASSERT_THROW(PrimitiveNode::Make("foo", Repetition::REQUIRED,
        Type::INT32, LogicalType::JSON), ParquetException);
  ASSERT_NO_THROW(PrimitiveNode::Make("foo", Repetition::REQUIRED,
        Type::INT64, LogicalType::TIMESTAMP_MILLIS));
  ASSERT_THROW(PrimitiveNode::Make("foo", Repetition::REQUIRED,
      Type::INT32, LogicalType::INT_64), ParquetException);
  ASSERT_THROW(PrimitiveNode::Make("foo", Repetition::REQUIRED,
        Type::BYTE_ARRAY, LogicalType::INT_8), ParquetException);
  ASSERT_THROW(PrimitiveNode::Make("foo", Repetition::REQUIRED,
        Type::BYTE_ARRAY, LogicalType::INTERVAL), ParquetException);
  ASSERT_THROW(PrimitiveNode::Make("foo", Repetition::REQUIRED,
      Type::FIXED_LEN_BYTE_ARRAY, LogicalType::ENUM), ParquetException);
  ASSERT_NO_THROW(PrimitiveNode::Make("foo", Repetition::REQUIRED,
      Type::BYTE_ARRAY, LogicalType::ENUM));
  ASSERT_THROW(PrimitiveNode::Make("foo", Repetition::REQUIRED,
      Type::FIXED_LEN_BYTE_ARRAY, LogicalType::DECIMAL, 0, 2, 4), ParquetException);
  ASSERT_THROW(PrimitiveNode::Make("foo", Repetition::REQUIRED,
      Type::FLOAT, LogicalType::DECIMAL, 0, 2, 4), ParquetException);
  ASSERT_THROW(PrimitiveNode::Make("foo", Repetition::REQUIRED,
      Type::FIXED_LEN_BYTE_ARRAY, LogicalType::DECIMAL, 0, 4, 0), ParquetException);
  ASSERT_THROW(PrimitiveNode::Make("foo", Repetition::REQUIRED,
      Type::FIXED_LEN_BYTE_ARRAY, LogicalType::DECIMAL, 10, 0, 4), ParquetException);
  ASSERT_THROW(PrimitiveNode::Make("foo", Repetition::REQUIRED,
      Type::FIXED_LEN_BYTE_ARRAY, LogicalType::DECIMAL, 10, 4, -1), ParquetException);
  ASSERT_THROW(PrimitiveNode::Make("foo", Repetition::REQUIRED,
      Type::FIXED_LEN_BYTE_ARRAY, LogicalType::DECIMAL, 10, 2, 4), ParquetException);
  ASSERT_NO_THROW(PrimitiveNode::Make("foo", Repetition::REQUIRED,
      Type::FIXED_LEN_BYTE_ARRAY, LogicalType::DECIMAL, 10, 6, 4));
  ASSERT_NO_THROW(PrimitiveNode::Make("foo", Repetition::REQUIRED,
      Type::FIXED_LEN_BYTE_ARRAY, LogicalType::INTERVAL, 12));
  ASSERT_THROW(PrimitiveNode::Make("foo", Repetition::REQUIRED,
      Type::FIXED_LEN_BYTE_ARRAY, LogicalType::INTERVAL, 10), ParquetException);
}

// ----------------------------------------------------------------------
// Group node

class TestGroupNode : public ::testing::Test {
 public:
  NodeVector Fields1() {
    NodeVector fields;

    fields.push_back(Int32("one", Repetition::REQUIRED));
    fields.push_back(Int64("two"));
    fields.push_back(Double("three"));

    return fields;
  }
};

TEST_F(TestGroupNode, Attrs) {
  NodeVector fields = Fields1();

  GroupNode node1("foo", Repetition::REPEATED, fields);
  GroupNode node2("bar", Repetition::OPTIONAL, fields, LogicalType::LIST);

  ASSERT_EQ("foo", node1.name());

  ASSERT_TRUE(node1.is_group());
  ASSERT_FALSE(node1.is_primitive());

  ASSERT_EQ(fields.size(), node1.field_count());

  ASSERT_TRUE(node1.is_repeated());
  ASSERT_TRUE(node2.is_optional());

  ASSERT_EQ(Repetition::REPEATED, node1.repetition());
  ASSERT_EQ(Repetition::OPTIONAL, node2.repetition());

  ASSERT_EQ(Node::GROUP, node1.node_type());

  // logical types
  ASSERT_EQ(LogicalType::NONE, node1.logical_type());
  ASSERT_EQ(LogicalType::LIST, node2.logical_type());
}

TEST_F(TestGroupNode, Equals) {
  NodeVector f1 = Fields1();
  NodeVector f2 = Fields1();

  GroupNode group1("group", Repetition::REPEATED, f1);
  GroupNode group2("group", Repetition::REPEATED, f2);
  GroupNode group3("group2", Repetition::REPEATED, f2);

  // This is copied in the GroupNode ctor, so this is okay
  f2.push_back(Float("four", Repetition::OPTIONAL));
  GroupNode group4("group", Repetition::REPEATED, f2);
  GroupNode group5("group", Repetition::REPEATED, Fields1());

  ASSERT_TRUE(group1.Equals(&group1));
  ASSERT_TRUE(group1.Equals(&group2));
  ASSERT_FALSE(group1.Equals(&group3));

  ASSERT_FALSE(group1.Equals(&group4));
  ASSERT_FALSE(group5.Equals(&group4));
}

} // namespace schema

} // namespace parquet_cpp
