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

#include <cstdlib>
#include <cstring>
#include <functional>
#include <iosfwd>
#include <memory>
#include <string>
#include <vector>

#include "arrow/util/checked_cast.h"
#include "parquet/exception.h"
#include "parquet/schema-internal.h"
#include "parquet/schema.h"
#include "parquet/thrift.h"
#include "parquet/types.h"

using ::arrow::internal::checked_cast;

namespace parquet {

using format::ConvertedType;
using format::FieldRepetitionType;
using format::SchemaElement;

namespace schema {

static inline SchemaElement NewPrimitive(const std::string& name,
                                         FieldRepetitionType::type repetition,
                                         Type::type type, int id = 0) {
  SchemaElement result;
  result.__set_name(name);
  result.__set_repetition_type(repetition);
  result.__set_type(static_cast<format::Type::type>(type));

  return result;
}

static inline SchemaElement NewGroup(const std::string& name,
                                     FieldRepetitionType::type repetition,
                                     int num_children, int id = 0) {
  SchemaElement result;
  result.__set_name(name);
  result.__set_repetition_type(repetition);
  result.__set_num_children(num_children);

  return result;
}

// ----------------------------------------------------------------------
// ColumnPath

TEST(TestColumnPath, TestAttrs) {
  ColumnPath path(std::vector<std::string>({"toplevel", "leaf"}));

  ASSERT_EQ(path.ToDotString(), "toplevel.leaf");

  std::shared_ptr<ColumnPath> path_ptr = ColumnPath::FromDotString("toplevel.leaf");
  ASSERT_EQ(path_ptr->ToDotString(), "toplevel.leaf");

  std::shared_ptr<ColumnPath> extended = path_ptr->extend("anotherlevel");
  ASSERT_EQ(extended->ToDotString(), "toplevel.leaf.anotherlevel");
}

// ----------------------------------------------------------------------
// Primitive node

class TestPrimitiveNode : public ::testing::Test {
 public:
  void SetUp() {
    name_ = "name";
    id_ = 5;
  }

  void Convert(const format::SchemaElement* element) {
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

  PrimitiveNode node2("bar", Repetition::OPTIONAL, Type::BYTE_ARRAY, LogicalType::UTF8);

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
  PrimitiveNode node3("foo", Repetition::REPEATED, Type::INT32);
  PrimitiveNode node4("foo", Repetition::REQUIRED, Type::INT32);
  PrimitiveNode node5("foo", Repetition::OPTIONAL, Type::INT32);

  ASSERT_TRUE(node3.is_repeated());
  ASSERT_FALSE(node3.is_optional());

  ASSERT_TRUE(node4.is_required());

  ASSERT_TRUE(node5.is_optional());
  ASSERT_FALSE(node5.is_required());
}

TEST_F(TestPrimitiveNode, FromParquet) {
  SchemaElement elt = NewPrimitive(name_, FieldRepetitionType::OPTIONAL, Type::INT32, 0);
  ASSERT_NO_FATAL_FAILURE(Convert(&elt));
  ASSERT_EQ(name_, prim_node_->name());
  ASSERT_EQ(id_, prim_node_->id());
  ASSERT_EQ(Repetition::OPTIONAL, prim_node_->repetition());
  ASSERT_EQ(Type::INT32, prim_node_->physical_type());
  ASSERT_EQ(LogicalType::NONE, prim_node_->logical_type());

  // Test a logical type
  elt = NewPrimitive(name_, FieldRepetitionType::REQUIRED, Type::BYTE_ARRAY, 0);
  elt.__set_converted_type(ConvertedType::UTF8);

  ASSERT_NO_FATAL_FAILURE(Convert(&elt));
  ASSERT_EQ(Repetition::REQUIRED, prim_node_->repetition());
  ASSERT_EQ(Type::BYTE_ARRAY, prim_node_->physical_type());
  ASSERT_EQ(LogicalType::UTF8, prim_node_->logical_type());

  // FIXED_LEN_BYTE_ARRAY
  elt = NewPrimitive(name_, FieldRepetitionType::OPTIONAL, Type::FIXED_LEN_BYTE_ARRAY, 0);
  elt.__set_type_length(16);

  ASSERT_NO_FATAL_FAILURE(Convert(&elt));
  ASSERT_EQ(name_, prim_node_->name());
  ASSERT_EQ(id_, prim_node_->id());
  ASSERT_EQ(Repetition::OPTIONAL, prim_node_->repetition());
  ASSERT_EQ(Type::FIXED_LEN_BYTE_ARRAY, prim_node_->physical_type());
  ASSERT_EQ(16, prim_node_->type_length());

  // ConvertedType::Decimal
  elt = NewPrimitive(name_, FieldRepetitionType::OPTIONAL, Type::FIXED_LEN_BYTE_ARRAY, 0);
  elt.__set_converted_type(ConvertedType::DECIMAL);
  elt.__set_type_length(6);
  elt.__set_scale(2);
  elt.__set_precision(12);

  ASSERT_NO_FATAL_FAILURE(Convert(&elt));
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
  ASSERT_NO_THROW(
      PrimitiveNode::Make("foo", Repetition::REQUIRED, Type::INT32, LogicalType::INT_32));
  ASSERT_NO_THROW(PrimitiveNode::Make("foo", Repetition::REQUIRED, Type::BYTE_ARRAY,
                                      LogicalType::JSON));
  ASSERT_THROW(
      PrimitiveNode::Make("foo", Repetition::REQUIRED, Type::INT32, LogicalType::JSON),
      ParquetException);
  ASSERT_NO_THROW(PrimitiveNode::Make("foo", Repetition::REQUIRED, Type::INT64,
                                      LogicalType::TIMESTAMP_MILLIS));
  ASSERT_THROW(
      PrimitiveNode::Make("foo", Repetition::REQUIRED, Type::INT32, LogicalType::INT_64),
      ParquetException);
  ASSERT_THROW(PrimitiveNode::Make("foo", Repetition::REQUIRED, Type::BYTE_ARRAY,
                                   LogicalType::INT_8),
               ParquetException);
  ASSERT_THROW(PrimitiveNode::Make("foo", Repetition::REQUIRED, Type::BYTE_ARRAY,
                                   LogicalType::INTERVAL),
               ParquetException);
  ASSERT_THROW(PrimitiveNode::Make("foo", Repetition::REQUIRED,
                                   Type::FIXED_LEN_BYTE_ARRAY, LogicalType::ENUM),
               ParquetException);
  ASSERT_NO_THROW(PrimitiveNode::Make("foo", Repetition::REQUIRED, Type::BYTE_ARRAY,
                                      LogicalType::ENUM));
  ASSERT_THROW(
      PrimitiveNode::Make("foo", Repetition::REQUIRED, Type::FIXED_LEN_BYTE_ARRAY,
                          LogicalType::DECIMAL, 0, 2, 4),
      ParquetException);
  ASSERT_THROW(PrimitiveNode::Make("foo", Repetition::REQUIRED, Type::FLOAT,
                                   LogicalType::DECIMAL, 0, 2, 4),
               ParquetException);
  ASSERT_THROW(
      PrimitiveNode::Make("foo", Repetition::REQUIRED, Type::FIXED_LEN_BYTE_ARRAY,
                          LogicalType::DECIMAL, 0, 4, 0),
      ParquetException);
  ASSERT_THROW(
      PrimitiveNode::Make("foo", Repetition::REQUIRED, Type::FIXED_LEN_BYTE_ARRAY,
                          LogicalType::DECIMAL, 10, 0, 4),
      ParquetException);
  ASSERT_THROW(
      PrimitiveNode::Make("foo", Repetition::REQUIRED, Type::FIXED_LEN_BYTE_ARRAY,
                          LogicalType::DECIMAL, 10, 4, -1),
      ParquetException);
  ASSERT_THROW(
      PrimitiveNode::Make("foo", Repetition::REQUIRED, Type::FIXED_LEN_BYTE_ARRAY,
                          LogicalType::DECIMAL, 10, 2, 4),
      ParquetException);
  ASSERT_NO_THROW(PrimitiveNode::Make("foo", Repetition::REQUIRED,
                                      Type::FIXED_LEN_BYTE_ARRAY, LogicalType::DECIMAL,
                                      10, 6, 4));
  ASSERT_NO_THROW(PrimitiveNode::Make("foo", Repetition::REQUIRED,
                                      Type::FIXED_LEN_BYTE_ARRAY, LogicalType::INTERVAL,
                                      12));
  ASSERT_THROW(PrimitiveNode::Make("foo", Repetition::REQUIRED,
                                   Type::FIXED_LEN_BYTE_ARRAY, LogicalType::INTERVAL, 10),
               ParquetException);
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

  NodeVector Fields2() {
    // Fields with a duplicate name
    NodeVector fields;

    fields.push_back(Int32("duplicate", Repetition::REQUIRED));
    fields.push_back(Int64("unique"));
    fields.push_back(Double("duplicate"));

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

TEST_F(TestGroupNode, FieldIndex) {
  NodeVector fields = Fields1();
  GroupNode group("group", Repetition::REQUIRED, fields);
  for (size_t i = 0; i < fields.size(); i++) {
    auto field = group.field(static_cast<int>(i));
    ASSERT_EQ(i, group.FieldIndex(*field));
  }

  // Test a non field node
  auto non_field_alien = Int32("alien", Repetition::REQUIRED);   // other name
  auto non_field_familiar = Int32("one", Repetition::REPEATED);  // other node
  ASSERT_LT(group.FieldIndex(*non_field_alien), 0);
  ASSERT_LT(group.FieldIndex(*non_field_familiar), 0);
}

TEST_F(TestGroupNode, FieldIndexDuplicateName) {
  NodeVector fields = Fields2();
  GroupNode group("group", Repetition::REQUIRED, fields);
  for (size_t i = 0; i < fields.size(); i++) {
    auto field = group.field(static_cast<int>(i));
    ASSERT_EQ(i, group.FieldIndex(*field));
  }
}

// ----------------------------------------------------------------------
// Test convert group

class TestSchemaConverter : public ::testing::Test {
 public:
  void setUp() { name_ = "parquet_schema"; }

  void Convert(const parquet::format::SchemaElement* elements, int length) {
    FlatSchemaConverter converter(elements, length);
    node_ = converter.Convert();
    ASSERT_TRUE(node_->is_group());
    group_ = static_cast<const GroupNode*>(node_.get());
  }

 protected:
  std::string name_;
  const GroupNode* group_;
  std::unique_ptr<Node> node_;
};

bool check_for_parent_consistency(const GroupNode* node) {
  // Each node should have the group as parent
  for (int i = 0; i < node->field_count(); i++) {
    const NodePtr& field = node->field(i);
    if (field->parent() != node) {
      return false;
    }
    if (field->is_group()) {
      const GroupNode* group = static_cast<GroupNode*>(field.get());
      if (!check_for_parent_consistency(group)) {
        return false;
      }
    }
  }
  return true;
}

TEST_F(TestSchemaConverter, NestedExample) {
  SchemaElement elt;
  std::vector<SchemaElement> elements;
  elements.push_back(NewGroup(name_, FieldRepetitionType::REPEATED, 2, 0));

  // A primitive one
  elements.push_back(NewPrimitive("a", FieldRepetitionType::REQUIRED, Type::INT32, 1));

  // A group
  elements.push_back(NewGroup("bag", FieldRepetitionType::OPTIONAL, 1, 2));

  // 3-level list encoding, by hand
  elt = NewGroup("b", FieldRepetitionType::REPEATED, 1, 3);
  elt.__set_converted_type(ConvertedType::LIST);
  elements.push_back(elt);
  elements.push_back(NewPrimitive("item", FieldRepetitionType::OPTIONAL, Type::INT64, 4));

  ASSERT_NO_FATAL_FAILURE(Convert(&elements[0], static_cast<int>(elements.size())));

  // Construct the expected schema
  NodeVector fields;
  fields.push_back(Int32("a", Repetition::REQUIRED));

  // 3-level list encoding
  NodePtr item = Int64("item");
  NodePtr list(GroupNode::Make("b", Repetition::REPEATED, {item}, LogicalType::LIST));
  NodePtr bag(GroupNode::Make("bag", Repetition::OPTIONAL, {list}));
  fields.push_back(bag);

  NodePtr schema = GroupNode::Make(name_, Repetition::REPEATED, fields);

  ASSERT_TRUE(schema->Equals(group_));

  // Check that the parent relationship in each node is consitent
  ASSERT_EQ(group_->parent(), nullptr);
  ASSERT_TRUE(check_for_parent_consistency(group_));
}

TEST_F(TestSchemaConverter, ZeroColumns) {
  // ARROW-3843
  SchemaElement elements[1];
  elements[0] = NewGroup("schema", FieldRepetitionType::REPEATED, 0, 0);
  ASSERT_NO_THROW(Convert(elements, 1));
}

TEST_F(TestSchemaConverter, InvalidRoot) {
  // According to the Parquet specification, the first element in the
  // list<SchemaElement> is a group whose children (and their descendants)
  // contain all of the rest of the flattened schema elements. If the first
  // element is not a group, it is a malformed Parquet file.

  SchemaElement elements[2];
  elements[0] =
      NewPrimitive("not-a-group", FieldRepetitionType::REQUIRED, Type::INT32, 0);
  ASSERT_THROW(Convert(elements, 2), ParquetException);

  // While the Parquet spec indicates that the root group should have REPEATED
  // repetition type, some implementations may return REQUIRED or OPTIONAL
  // groups as the first element. These tests check that this is okay as a
  // practicality matter.
  elements[0] = NewGroup("not-repeated", FieldRepetitionType::REQUIRED, 1, 0);
  elements[1] = NewPrimitive("a", FieldRepetitionType::REQUIRED, Type::INT32, 1);
  ASSERT_NO_FATAL_FAILURE(Convert(elements, 2));

  elements[0] = NewGroup("not-repeated", FieldRepetitionType::OPTIONAL, 1, 0);
  ASSERT_NO_FATAL_FAILURE(Convert(elements, 2));
}

TEST_F(TestSchemaConverter, NotEnoughChildren) {
  // Throw a ParquetException, but don't core dump or anything
  SchemaElement elt;
  std::vector<SchemaElement> elements;
  elements.push_back(NewGroup(name_, FieldRepetitionType::REPEATED, 2, 0));
  ASSERT_THROW(Convert(&elements[0], 1), ParquetException);
}

// ----------------------------------------------------------------------
// Schema tree flatten / unflatten

class TestSchemaFlatten : public ::testing::Test {
 public:
  void setUp() { name_ = "parquet_schema"; }

  void Flatten(const GroupNode* schema) { ToParquet(schema, &elements_); }

 protected:
  std::string name_;
  std::vector<format::SchemaElement> elements_;
};

TEST_F(TestSchemaFlatten, DecimalMetadata) {
  // Checks that DecimalMetadata is only set for DecimalTypes
  NodePtr node = PrimitiveNode::Make("decimal", Repetition::REQUIRED, Type::INT64,
                                     LogicalType::DECIMAL, -1, 8, 4);
  NodePtr group =
      GroupNode::Make("group", Repetition::REPEATED, {node}, LogicalType::LIST);
  Flatten(reinterpret_cast<GroupNode*>(group.get()));
  ASSERT_EQ("decimal", elements_[1].name);
  ASSERT_TRUE(elements_[1].__isset.precision);
  ASSERT_TRUE(elements_[1].__isset.scale);

  elements_.clear();
  // ... including those created with new logical annotations
  node = PrimitiveNode::Make("decimal", Repetition::REQUIRED,
                             DecimalAnnotation::Make(10, 5), Type::INT64, -1);
  group = GroupNode::Make("group", Repetition::REPEATED, {node}, ListAnnotation::Make());
  Flatten(reinterpret_cast<GroupNode*>(group.get()));
  ASSERT_EQ("decimal", elements_[1].name);
  ASSERT_TRUE(elements_[1].__isset.precision);
  ASSERT_TRUE(elements_[1].__isset.scale);

  elements_.clear();
  // Not for integers with no logical type
  group =
      GroupNode::Make("group", Repetition::REPEATED, {Int64("int64")}, LogicalType::LIST);
  Flatten(reinterpret_cast<GroupNode*>(group.get()));
  ASSERT_EQ("int64", elements_[1].name);
  ASSERT_FALSE(elements_[0].__isset.precision);
  ASSERT_FALSE(elements_[0].__isset.scale);
}

TEST_F(TestSchemaFlatten, NestedExample) {
  SchemaElement elt;
  std::vector<SchemaElement> elements;
  elements.push_back(NewGroup(name_, FieldRepetitionType::REPEATED, 2, 0));

  // A primitive one
  elements.push_back(NewPrimitive("a", FieldRepetitionType::REQUIRED, Type::INT32, 1));

  // A group
  elements.push_back(NewGroup("bag", FieldRepetitionType::OPTIONAL, 1, 2));

  // 3-level list encoding, by hand
  elt = NewGroup("b", FieldRepetitionType::REPEATED, 1, 3);
  elt.__set_converted_type(ConvertedType::LIST);
  format::ListType ls;
  format::LogicalType lt;
  lt.__set_LIST(ls);
  elt.__set_logicalType(lt);
  elements.push_back(elt);
  elements.push_back(NewPrimitive("item", FieldRepetitionType::OPTIONAL, Type::INT64, 4));

  // Construct the schema
  NodeVector fields;
  fields.push_back(Int32("a", Repetition::REQUIRED));

  // 3-level list encoding
  NodePtr item = Int64("item");
  NodePtr list(GroupNode::Make("b", Repetition::REPEATED, {item}, LogicalType::LIST));
  NodePtr bag(GroupNode::Make("bag", Repetition::OPTIONAL, {list}));
  fields.push_back(bag);

  NodePtr schema = GroupNode::Make(name_, Repetition::REPEATED, fields);

  Flatten(static_cast<GroupNode*>(schema.get()));
  ASSERT_EQ(elements_.size(), elements.size());
  for (size_t i = 0; i < elements_.size(); i++) {
    ASSERT_EQ(elements_[i], elements[i]);
  }
}

TEST(TestColumnDescriptor, TestAttrs) {
  NodePtr node = PrimitiveNode::Make("name", Repetition::OPTIONAL, Type::BYTE_ARRAY,
                                     LogicalType::UTF8);
  ColumnDescriptor descr(node, 4, 1);

  ASSERT_EQ("name", descr.name());
  ASSERT_EQ(4, descr.max_definition_level());
  ASSERT_EQ(1, descr.max_repetition_level());

  ASSERT_EQ(Type::BYTE_ARRAY, descr.physical_type());

  ASSERT_EQ(-1, descr.type_length());
  const char* expected_descr = R"(column descriptor = {
  name: name,
  path: ,
  physical_type: BYTE_ARRAY,
  logical_type: UTF8,
  logical_annotation: String,
  max_definition_level: 4,
  max_repetition_level: 1,
})";
  ASSERT_EQ(expected_descr, descr.ToString());

  // Test FIXED_LEN_BYTE_ARRAY
  node = PrimitiveNode::Make("name", Repetition::OPTIONAL, Type::FIXED_LEN_BYTE_ARRAY,
                             LogicalType::DECIMAL, 12, 10, 4);
  descr = ColumnDescriptor(node, 4, 1);

  ASSERT_EQ(Type::FIXED_LEN_BYTE_ARRAY, descr.physical_type());
  ASSERT_EQ(12, descr.type_length());

  expected_descr = R"(column descriptor = {
  name: name,
  path: ,
  physical_type: FIXED_LEN_BYTE_ARRAY,
  logical_type: DECIMAL,
  logical_annotation: Decimal(precision=10, scale=4),
  max_definition_level: 4,
  max_repetition_level: 1,
  length: 12,
  precision: 10,
  scale: 4,
})";
  ASSERT_EQ(expected_descr, descr.ToString());
}

class TestSchemaDescriptor : public ::testing::Test {
 public:
  void setUp() {}

 protected:
  SchemaDescriptor descr_;
};

TEST_F(TestSchemaDescriptor, InitNonGroup) {
  NodePtr node = PrimitiveNode::Make("field", Repetition::OPTIONAL, Type::INT32);

  ASSERT_THROW(descr_.Init(node), ParquetException);
}

TEST_F(TestSchemaDescriptor, Equals) {
  NodePtr schema;

  NodePtr inta = Int32("a", Repetition::REQUIRED);
  NodePtr intb = Int64("b", Repetition::OPTIONAL);
  NodePtr intb2 = Int64("b2", Repetition::OPTIONAL);
  NodePtr intc = ByteArray("c", Repetition::REPEATED);

  NodePtr item1 = Int64("item1", Repetition::REQUIRED);
  NodePtr item2 = Boolean("item2", Repetition::OPTIONAL);
  NodePtr item3 = Int32("item3", Repetition::REPEATED);
  NodePtr list(GroupNode::Make("records", Repetition::REPEATED, {item1, item2, item3},
                               LogicalType::LIST));

  NodePtr bag(GroupNode::Make("bag", Repetition::OPTIONAL, {list}));
  NodePtr bag2(GroupNode::Make("bag", Repetition::REQUIRED, {list}));

  SchemaDescriptor descr1;
  descr1.Init(GroupNode::Make("schema", Repetition::REPEATED, {inta, intb, intc, bag}));

  ASSERT_TRUE(descr1.Equals(descr1));

  SchemaDescriptor descr2;
  descr2.Init(GroupNode::Make("schema", Repetition::REPEATED, {inta, intb, intc, bag2}));
  ASSERT_FALSE(descr1.Equals(descr2));

  SchemaDescriptor descr3;
  descr3.Init(GroupNode::Make("schema", Repetition::REPEATED, {inta, intb2, intc, bag}));
  ASSERT_FALSE(descr1.Equals(descr3));

  // Robust to name of parent node
  SchemaDescriptor descr4;
  descr4.Init(GroupNode::Make("SCHEMA", Repetition::REPEATED, {inta, intb, intc, bag}));
  ASSERT_TRUE(descr1.Equals(descr4));

  SchemaDescriptor descr5;
  descr5.Init(
      GroupNode::Make("schema", Repetition::REPEATED, {inta, intb, intc, bag, intb2}));
  ASSERT_FALSE(descr1.Equals(descr5));

  // Different max repetition / definition levels
  ColumnDescriptor col1(inta, 5, 1);
  ColumnDescriptor col2(inta, 6, 1);
  ColumnDescriptor col3(inta, 5, 2);

  ASSERT_TRUE(col1.Equals(col1));
  ASSERT_FALSE(col1.Equals(col2));
  ASSERT_FALSE(col1.Equals(col3));
}

TEST_F(TestSchemaDescriptor, BuildTree) {
  NodeVector fields;
  NodePtr schema;

  NodePtr inta = Int32("a", Repetition::REQUIRED);
  fields.push_back(inta);
  fields.push_back(Int64("b", Repetition::OPTIONAL));
  fields.push_back(ByteArray("c", Repetition::REPEATED));

  // 3-level list encoding
  NodePtr item1 = Int64("item1", Repetition::REQUIRED);
  NodePtr item2 = Boolean("item2", Repetition::OPTIONAL);
  NodePtr item3 = Int32("item3", Repetition::REPEATED);
  NodePtr list(GroupNode::Make("records", Repetition::REPEATED, {item1, item2, item3},
                               LogicalType::LIST));
  NodePtr bag(GroupNode::Make("bag", Repetition::OPTIONAL, {list}));
  fields.push_back(bag);

  schema = GroupNode::Make("schema", Repetition::REPEATED, fields);

  descr_.Init(schema);

  int nleaves = 6;

  // 6 leaves
  ASSERT_EQ(nleaves, descr_.num_columns());

  //                             mdef mrep
  // required int32 a            0    0
  // optional int64 b            1    0
  // repeated byte_array c       1    1
  // optional group bag          1    0
  //   repeated group records    2    1
  //     required int64 item1    2    1
  //     optional boolean item2  3    1
  //     repeated int32 item3    3    2
  int16_t ex_max_def_levels[6] = {0, 1, 1, 2, 3, 3};
  int16_t ex_max_rep_levels[6] = {0, 0, 1, 1, 1, 2};

  for (int i = 0; i < nleaves; ++i) {
    const ColumnDescriptor* col = descr_.Column(i);
    EXPECT_EQ(ex_max_def_levels[i], col->max_definition_level()) << i;
    EXPECT_EQ(ex_max_rep_levels[i], col->max_repetition_level()) << i;
  }

  ASSERT_EQ(descr_.Column(0)->path()->ToDotString(), "a");
  ASSERT_EQ(descr_.Column(1)->path()->ToDotString(), "b");
  ASSERT_EQ(descr_.Column(2)->path()->ToDotString(), "c");
  ASSERT_EQ(descr_.Column(3)->path()->ToDotString(), "bag.records.item1");
  ASSERT_EQ(descr_.Column(4)->path()->ToDotString(), "bag.records.item2");
  ASSERT_EQ(descr_.Column(5)->path()->ToDotString(), "bag.records.item3");

  for (int i = 0; i < nleaves; ++i) {
    auto col = descr_.Column(i);
    ASSERT_EQ(i, descr_.ColumnIndex(*col->schema_node()));
  }

  // Test non-column nodes find
  NodePtr non_column_alien = Int32("alien", Repetition::REQUIRED);  // other path
  NodePtr non_column_familiar = Int32("a", Repetition::REPEATED);   // other node
  ASSERT_LT(descr_.ColumnIndex(*non_column_alien), 0);
  ASSERT_LT(descr_.ColumnIndex(*non_column_familiar), 0);

  ASSERT_EQ(inta.get(), descr_.GetColumnRoot(0));
  ASSERT_EQ(bag.get(), descr_.GetColumnRoot(3));
  ASSERT_EQ(bag.get(), descr_.GetColumnRoot(4));
  ASSERT_EQ(bag.get(), descr_.GetColumnRoot(5));

  ASSERT_EQ(schema.get(), descr_.group_node());

  // Init clears the leaves
  descr_.Init(schema);
  ASSERT_EQ(nleaves, descr_.num_columns());
}

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
  NodePtr list(
      GroupNode::Make("b", Repetition::REPEATED, {item1, item2}, LogicalType::LIST));
  NodePtr bag(GroupNode::Make("bag", Repetition::OPTIONAL, {list}));
  fields.push_back(bag);

  fields.push_back(PrimitiveNode::Make("c", Repetition::REQUIRED, Type::INT32,
                                       LogicalType::DECIMAL, -1, 3, 2));

  fields.push_back(PrimitiveNode::Make("d", Repetition::REQUIRED,
                                       DecimalAnnotation::Make(10, 5), Type::INT64, -1));

  NodePtr schema = GroupNode::Make("schema", Repetition::REPEATED, fields);

  std::string result = Print(schema);
  std::string expected = R"(message schema {
  required int32 a;
  optional group bag {
    repeated group b (List) {
      optional int64 item1;
      required boolean item2;
    }
  }
  required int32 c (Decimal(precision=3, scale=2));
  required int64 d (Decimal(precision=10, scale=5));
}
)";
  ASSERT_EQ(expected, result);
}

static void ConfirmFactoryEquivalence(
    LogicalType::type converted_type,
    const std::shared_ptr<const LogicalAnnotation>& from_make,
    std::function<bool(const std::shared_ptr<const LogicalAnnotation>&)> check_is_type) {
  std::shared_ptr<const LogicalAnnotation> from_converted_type =
      LogicalAnnotation::FromConvertedType(converted_type);
  ASSERT_EQ(from_converted_type->type(), from_make->type())
      << from_make->ToString() << " annotations unexpectedly do not match on type";
  ASSERT_TRUE(from_converted_type->Equals(*from_make))
      << from_make->ToString() << " annotations unexpectedly not equivalent";
  ASSERT_TRUE(check_is_type(from_converted_type))
      << from_converted_type->ToString()
      << " annotation (from converted type) does not have expected type property";
  ASSERT_TRUE(check_is_type(from_make))
      << from_make->ToString()
      << " annotation (from Make()) does not have expected type property";
  return;
}

TEST(TestLogicalAnnotationConstruction, FactoryEquivalence) {
  // For each legacy converted type, ensure that the equivalent annotation object
  // can be obtained from either the base class's FromConvertedType() factory method or
  // the annotation type class's Make() method (accessed via convenience methods on the
  // base class) and that these annotation objects are equivalent

  struct ConfirmFactoryEquivalenceArguments {
    LogicalType::type converted_type;
    std::shared_ptr<const LogicalAnnotation> annotation;
    std::function<bool(const std::shared_ptr<const LogicalAnnotation>&)> check_is_type;
  };

  auto check_is_string = [](const std::shared_ptr<const LogicalAnnotation>& annotation) {
    return annotation->is_string();
  };
  auto check_is_map = [](const std::shared_ptr<const LogicalAnnotation>& annotation) {
    return annotation->is_map();
  };
  auto check_is_list = [](const std::shared_ptr<const LogicalAnnotation>& annotation) {
    return annotation->is_list();
  };
  auto check_is_enum = [](const std::shared_ptr<const LogicalAnnotation>& annotation) {
    return annotation->is_enum();
  };
  auto check_is_date = [](const std::shared_ptr<const LogicalAnnotation>& annotation) {
    return annotation->is_date();
  };
  auto check_is_time = [](const std::shared_ptr<const LogicalAnnotation>& annotation) {
    return annotation->is_time();
  };
  auto check_is_timestamp =
      [](const std::shared_ptr<const LogicalAnnotation>& annotation) {
        return annotation->is_timestamp();
      };
  auto check_is_int = [](const std::shared_ptr<const LogicalAnnotation>& annotation) {
    return annotation->is_int();
  };
  auto check_is_JSON = [](const std::shared_ptr<const LogicalAnnotation>& annotation) {
    return annotation->is_JSON();
  };
  auto check_is_BSON = [](const std::shared_ptr<const LogicalAnnotation>& annotation) {
    return annotation->is_BSON();
  };
  auto check_is_interval =
      [](const std::shared_ptr<const LogicalAnnotation>& annotation) {
        return annotation->is_interval();
      };
  auto check_is_none = [](const std::shared_ptr<const LogicalAnnotation>& annotation) {
    return annotation->is_none();
  };

  std::vector<ConfirmFactoryEquivalenceArguments> cases = {
      {LogicalType::UTF8, LogicalAnnotation::String(), check_is_string},
      {LogicalType::MAP, LogicalAnnotation::Map(), check_is_map},
      {LogicalType::MAP_KEY_VALUE, LogicalAnnotation::Map(), check_is_map},
      {LogicalType::LIST, LogicalAnnotation::List(), check_is_list},
      {LogicalType::ENUM, LogicalAnnotation::Enum(), check_is_enum},
      {LogicalType::DATE, LogicalAnnotation::Date(), check_is_date},
      {LogicalType::TIME_MILLIS,
       LogicalAnnotation::Time(true, LogicalAnnotation::TimeUnit::MILLIS), check_is_time},
      {LogicalType::TIME_MICROS,
       LogicalAnnotation::Time(true, LogicalAnnotation::TimeUnit::MICROS), check_is_time},
      {LogicalType::TIMESTAMP_MILLIS,
       LogicalAnnotation::Timestamp(true, LogicalAnnotation::TimeUnit::MILLIS),
       check_is_timestamp},
      {LogicalType::TIMESTAMP_MICROS,
       LogicalAnnotation::Timestamp(true, LogicalAnnotation::TimeUnit::MICROS),
       check_is_timestamp},
      {LogicalType::UINT_8, LogicalAnnotation::Int(8, false), check_is_int},
      {LogicalType::UINT_16, LogicalAnnotation::Int(16, false), check_is_int},
      {LogicalType::UINT_32, LogicalAnnotation::Int(32, false), check_is_int},
      {LogicalType::UINT_64, LogicalAnnotation::Int(64, false), check_is_int},
      {LogicalType::INT_8, LogicalAnnotation::Int(8, true), check_is_int},
      {LogicalType::INT_16, LogicalAnnotation::Int(16, true), check_is_int},
      {LogicalType::INT_32, LogicalAnnotation::Int(32, true), check_is_int},
      {LogicalType::INT_64, LogicalAnnotation::Int(64, true), check_is_int},
      {LogicalType::JSON, LogicalAnnotation::JSON(), check_is_JSON},
      {LogicalType::BSON, LogicalAnnotation::BSON(), check_is_BSON},
      {LogicalType::INTERVAL, LogicalAnnotation::Interval(), check_is_interval},
      {LogicalType::NONE, LogicalAnnotation::None(), check_is_none}};

  for (const ConfirmFactoryEquivalenceArguments& c : cases) {
    ConfirmFactoryEquivalence(c.converted_type, c.annotation, c.check_is_type);
  }

  // LogicalType::DECIMAL, LogicalAnnotation::Decimal, is_decimal
  schema::DecimalMetadata converted_decimal_metadata;
  converted_decimal_metadata.isset = true;
  converted_decimal_metadata.precision = 10;
  converted_decimal_metadata.scale = 4;
  std::shared_ptr<const LogicalAnnotation> from_converted_type =
      LogicalAnnotation::FromConvertedType(LogicalType::DECIMAL,
                                           converted_decimal_metadata);
  std::shared_ptr<const LogicalAnnotation> from_make = LogicalAnnotation::Decimal(10, 4);
  ASSERT_EQ(from_converted_type->type(), from_make->type());
  ASSERT_TRUE(from_converted_type->Equals(*from_make));
  ASSERT_TRUE(from_converted_type->is_decimal());
  ASSERT_TRUE(from_make->is_decimal());
  ASSERT_TRUE(LogicalAnnotation::Decimal(16)->Equals(*LogicalAnnotation::Decimal(16, 0)));
}

static void ConfirmConvertedTypeCompatibility(
    const std::shared_ptr<const LogicalAnnotation>& original,
    LogicalType::type expected_converted_type) {
  ASSERT_TRUE(original->is_valid())
      << original->ToString() << " annotation unexpectedly is not valid";
  schema::DecimalMetadata converted_decimal_metadata;
  LogicalType::type converted_type =
      original->ToConvertedType(&converted_decimal_metadata);
  ASSERT_EQ(converted_type, expected_converted_type)
      << original->ToString()
      << " annotation unexpectedly returns incorrect converted type";
  ASSERT_FALSE(converted_decimal_metadata.isset)
      << original->ToString()
      << " annotation unexpectedly returns converted decimal metatdata that is set";
  ASSERT_TRUE(original->is_compatible(converted_type, converted_decimal_metadata))
      << original->ToString()
      << " annotation unexpectedly is incompatible with converted type and decimal "
         "metadata it returned";
  ASSERT_FALSE(original->is_compatible(converted_type, {true, 1, 1}))
      << original->ToString()
      << " annotation unexpectedly is compatible with converted decimal metadata that is "
         "set";
  ASSERT_TRUE(original->is_compatible(converted_type))
      << original->ToString()
      << " annotation unexpectedly is incompatible with converted type it returned";
  std::shared_ptr<const LogicalAnnotation> reconstructed =
      LogicalAnnotation::FromConvertedType(converted_type, converted_decimal_metadata);
  ASSERT_TRUE(reconstructed->is_valid()) << "Reconstructed " << reconstructed->ToString()
                                         << " annotation unexpectedly is not valid";
  ASSERT_TRUE(reconstructed->Equals(*original))
      << "Reconstructed annotation (" << reconstructed->ToString()
      << ") unexpectedly not equivalent to original annotation (" << original->ToString()
      << ")";
  return;
}

TEST(TestLogicalAnnotationConstruction, ConvertedTypeCompatibility) {
  // For each legacy converted type, ensure that the equivalent logical annotation
  // emits correct, compatible converted type information and that the emitted
  // information can be used to reconstruct another equivalent logical annotation.

  struct ExpectedConvertedType {
    std::shared_ptr<const LogicalAnnotation> annotation;
    LogicalType::type converted_type;
  };

  std::vector<ExpectedConvertedType> cases = {
      {LogicalAnnotation::String(), LogicalType::UTF8},
      {LogicalAnnotation::Map(), LogicalType::MAP},
      {LogicalAnnotation::List(), LogicalType::LIST},
      {LogicalAnnotation::Enum(), LogicalType::ENUM},
      {LogicalAnnotation::Date(), LogicalType::DATE},
      {LogicalAnnotation::Time(true, LogicalAnnotation::TimeUnit::MILLIS),
       LogicalType::TIME_MILLIS},
      {LogicalAnnotation::Time(true, LogicalAnnotation::TimeUnit::MICROS),
       LogicalType::TIME_MICROS},
      {LogicalAnnotation::Timestamp(true, LogicalAnnotation::TimeUnit::MILLIS),
       LogicalType::TIMESTAMP_MILLIS},
      {LogicalAnnotation::Timestamp(true, LogicalAnnotation::TimeUnit::MICROS),
       LogicalType::TIMESTAMP_MICROS},
      {LogicalAnnotation::Int(8, false), LogicalType::UINT_8},
      {LogicalAnnotation::Int(16, false), LogicalType::UINT_16},
      {LogicalAnnotation::Int(32, false), LogicalType::UINT_32},
      {LogicalAnnotation::Int(64, false), LogicalType::UINT_64},
      {LogicalAnnotation::Int(8, true), LogicalType::INT_8},
      {LogicalAnnotation::Int(16, true), LogicalType::INT_16},
      {LogicalAnnotation::Int(32, true), LogicalType::INT_32},
      {LogicalAnnotation::Int(64, true), LogicalType::INT_64},
      {LogicalAnnotation::JSON(), LogicalType::JSON},
      {LogicalAnnotation::BSON(), LogicalType::BSON},
      {LogicalAnnotation::Interval(), LogicalType::INTERVAL},
      {LogicalAnnotation::None(), LogicalType::NONE}};

  for (const ExpectedConvertedType& c : cases) {
    ConfirmConvertedTypeCompatibility(c.annotation, c.converted_type);
  }

  // Special cases ...

  std::shared_ptr<const LogicalAnnotation> original;
  LogicalType::type converted_type;
  schema::DecimalMetadata converted_decimal_metadata;
  std::shared_ptr<const LogicalAnnotation> reconstructed;

  // DECIMAL
  std::memset(&converted_decimal_metadata, 0x00, sizeof(converted_decimal_metadata));
  original = LogicalAnnotation::Decimal(6, 2);
  ASSERT_TRUE(original->is_valid());
  converted_type = original->ToConvertedType(&converted_decimal_metadata);
  ASSERT_EQ(converted_type, LogicalType::DECIMAL);
  ASSERT_TRUE(converted_decimal_metadata.isset);
  ASSERT_EQ(converted_decimal_metadata.precision, 6);
  ASSERT_EQ(converted_decimal_metadata.scale, 2);
  ASSERT_TRUE(original->is_compatible(converted_type, converted_decimal_metadata));
  reconstructed =
      LogicalAnnotation::FromConvertedType(converted_type, converted_decimal_metadata);
  ASSERT_TRUE(reconstructed->is_valid());
  ASSERT_TRUE(reconstructed->Equals(*original));

  // Unknown
  original = LogicalAnnotation::Unknown();
  ASSERT_TRUE(original->is_invalid());
  ASSERT_FALSE(original->is_valid());
  converted_type = original->ToConvertedType(&converted_decimal_metadata);
  ASSERT_EQ(converted_type, LogicalType::NA);
  ASSERT_FALSE(converted_decimal_metadata.isset);
  ASSERT_TRUE(original->is_compatible(converted_type, converted_decimal_metadata));
  ASSERT_TRUE(original->is_compatible(converted_type));
  reconstructed =
      LogicalAnnotation::FromConvertedType(converted_type, converted_decimal_metadata);
  ASSERT_TRUE(reconstructed->is_invalid());
  ASSERT_TRUE(reconstructed->Equals(*original));
}

static void ConfirmNewTypeIncompatibility(
    const std::shared_ptr<const LogicalAnnotation>& annotation,
    std::function<bool(const std::shared_ptr<const LogicalAnnotation>&)> check_is_type) {
  ASSERT_TRUE(annotation->is_valid())
      << annotation->ToString() << " annotation unexpectedly is not valid";
  ASSERT_TRUE(check_is_type(annotation))
      << annotation->ToString() << " annotation is not expected annotation type";
  schema::DecimalMetadata converted_decimal_metadata;
  LogicalType::type converted_type =
      annotation->ToConvertedType(&converted_decimal_metadata);
  ASSERT_EQ(converted_type, LogicalType::NONE)
      << annotation->ToString() << " annotation converted type unexpectedly is not NONE";
  ASSERT_FALSE(converted_decimal_metadata.isset)
      << annotation->ToString()
      << " annotation converted decimal metadata unexpectedly is set";
  return;
}

TEST(TestLogicalAnnotationConstruction, NewTypeIncompatibility) {
  // For each new logical annotation type, ensure that the logical annotation
  // correctly reports that it has no legacy equivalent

  struct ConfirmNewTypeIncompatibilityArguments {
    std::shared_ptr<const LogicalAnnotation> annotation;
    std::function<bool(const std::shared_ptr<const LogicalAnnotation>&)> check_is_type;
  };

  auto check_is_UUID = [](const std::shared_ptr<const LogicalAnnotation>& annotation) {
    return annotation->is_UUID();
  };
  auto check_is_null = [](const std::shared_ptr<const LogicalAnnotation>& annotation) {
    return annotation->is_null();
  };
  auto check_is_time = [](const std::shared_ptr<const LogicalAnnotation>& annotation) {
    return annotation->is_time();
  };
  auto check_is_timestamp =
      [](const std::shared_ptr<const LogicalAnnotation>& annotation) {
        return annotation->is_timestamp();
      };

  std::vector<ConfirmNewTypeIncompatibilityArguments> cases = {
      {LogicalAnnotation::UUID(), check_is_UUID},
      {LogicalAnnotation::Null(), check_is_null},
      {LogicalAnnotation::Time(false, LogicalAnnotation::TimeUnit::MILLIS),
       check_is_time},
      {LogicalAnnotation::Time(false, LogicalAnnotation::TimeUnit::MICROS),
       check_is_time},
      {LogicalAnnotation::Time(false, LogicalAnnotation::TimeUnit::NANOS), check_is_time},
      {LogicalAnnotation::Time(true, LogicalAnnotation::TimeUnit::NANOS), check_is_time},
      {LogicalAnnotation::Timestamp(false, LogicalAnnotation::TimeUnit::MILLIS),
       check_is_timestamp},
      {LogicalAnnotation::Timestamp(false, LogicalAnnotation::TimeUnit::MICROS),
       check_is_timestamp},
      {LogicalAnnotation::Timestamp(false, LogicalAnnotation::TimeUnit::NANOS),
       check_is_timestamp},
      {LogicalAnnotation::Timestamp(true, LogicalAnnotation::TimeUnit::NANOS),
       check_is_timestamp},
  };

  for (const ConfirmNewTypeIncompatibilityArguments& c : cases) {
    ConfirmNewTypeIncompatibility(c.annotation, c.check_is_type);
  }
}

TEST(TestLogicalAnnotationConstruction, FactoryExceptions) {
  // Ensure that annotation construction catches invalid arguments

  std::vector<std::function<void()>> cases = {
      []() {
        TimeAnnotation::Make(true, LogicalAnnotation::TimeUnit::UNKNOWN);
      },  // Invalid TimeUnit
      []() {
        TimestampAnnotation::Make(true, LogicalAnnotation::TimeUnit::UNKNOWN);
      },                                          // Invalid TimeUnit
      []() { IntAnnotation::Make(-1, false); },   // Invalid bit width
      []() { IntAnnotation::Make(0, false); },    // Invalid bit width
      []() { IntAnnotation::Make(1, false); },    // Invalid bit width
      []() { IntAnnotation::Make(65, false); },   // Invalid bit width
      []() { DecimalAnnotation::Make(-1); },      // Invalid precision
      []() { DecimalAnnotation::Make(0); },       // Invalid precision
      []() { DecimalAnnotation::Make(0, 0); },    // Invalid precision
      []() { DecimalAnnotation::Make(10, -1); },  // Invalid scale
      []() { DecimalAnnotation::Make(10, 11); }   // Invalid scale
  };

  for (auto f : cases) {
    ASSERT_ANY_THROW(f());
  }
}

static void ConfirmAnnotationProperties(
    const std::shared_ptr<const LogicalAnnotation>& annotation, bool nested,
    bool serialized, bool valid) {
  ASSERT_TRUE(annotation->is_nested() == nested)
      << annotation->ToString() << " annotation has incorrect nested() property";
  ASSERT_TRUE(annotation->is_serialized() == serialized)
      << annotation->ToString() << " annotation has incorrect serialized() property";
  ASSERT_TRUE(annotation->is_valid() == valid)
      << annotation->ToString() << " annotation has incorrect valid() property";
  ASSERT_TRUE(annotation->is_nonnested() != nested)
      << annotation->ToString() << " annotation has incorrect nonnested() property";
  ASSERT_TRUE(annotation->is_invalid() != valid)
      << annotation->ToString() << " annotation has incorrect invalid() property";
  return;
}

TEST(TestLogicalAnnotationOperation, AnnotationProperties) {
  // For each annotation type, ensure that the correct general properties are reported

  struct ExpectedProperties {
    std::shared_ptr<const LogicalAnnotation> annotation;
    bool nested;
    bool serialized;
    bool valid;
  };

  std::vector<ExpectedProperties> cases = {
      {StringAnnotation::Make(), false, true, true},
      {MapAnnotation::Make(), true, true, true},
      {ListAnnotation::Make(), true, true, true},
      {EnumAnnotation::Make(), false, true, true},
      {DecimalAnnotation::Make(16, 6), false, true, true},
      {DateAnnotation::Make(), false, true, true},
      {TimeAnnotation::Make(true, LogicalAnnotation::TimeUnit::MICROS), false, true,
       true},
      {TimestampAnnotation::Make(true, LogicalAnnotation::TimeUnit::MICROS), false, true,
       true},
      {IntervalAnnotation::Make(), false, true, true},
      {IntAnnotation::Make(8, false), false, true, true},
      {IntAnnotation::Make(64, true), false, true, true},
      {NullAnnotation::Make(), false, true, true},
      {JSONAnnotation::Make(), false, true, true},
      {BSONAnnotation::Make(), false, true, true},
      {UUIDAnnotation::Make(), false, true, true},
      {NoAnnotation::Make(), false, false, true},
      {UnknownAnnotation::Make(), false, false, false},
  };

  for (const ExpectedProperties& c : cases) {
    ConfirmAnnotationProperties(c.annotation, c.nested, c.serialized, c.valid);
  }
}

static constexpr int PHYSICAL_TYPE_COUNT = 8;

static Type::type physical_type[PHYSICAL_TYPE_COUNT] = {
    Type::BOOLEAN, Type::INT32,  Type::INT64,      Type::INT96,
    Type::FLOAT,   Type::DOUBLE, Type::BYTE_ARRAY, Type::FIXED_LEN_BYTE_ARRAY};

static void ConfirmSinglePrimitiveTypeApplicability(
    const std::shared_ptr<const LogicalAnnotation>& annotation,
    Type::type applicable_type) {
  for (int i = 0; i < PHYSICAL_TYPE_COUNT; ++i) {
    if (physical_type[i] == applicable_type) {
      ASSERT_TRUE(annotation->is_applicable(physical_type[i]))
          << annotation->ToString()
          << " annotation unexpectedly inapplicable to physical type "
          << TypeToString(physical_type[i]);
    } else {
      ASSERT_FALSE(annotation->is_applicable(physical_type[i]))
          << annotation->ToString()
          << " annotation unexpectedly applicable to physical type "
          << TypeToString(physical_type[i]);
    }
  }
  return;
}

static void ConfirmAnyPrimitiveTypeApplicability(
    const std::shared_ptr<const LogicalAnnotation>& annotation) {
  for (int i = 0; i < PHYSICAL_TYPE_COUNT; ++i) {
    ASSERT_TRUE(annotation->is_applicable(physical_type[i]))
        << annotation->ToString()
        << " annotation unexpectedly inapplicable to physical type "
        << TypeToString(physical_type[i]);
  }
  return;
}

static void ConfirmNoPrimitiveTypeApplicability(
    const std::shared_ptr<const LogicalAnnotation>& annotation) {
  for (int i = 0; i < PHYSICAL_TYPE_COUNT; ++i) {
    ASSERT_FALSE(annotation->is_applicable(physical_type[i]))
        << annotation->ToString()
        << " annotation unexpectedly applicable to physical type "
        << TypeToString(physical_type[i]);
  }
  return;
}

TEST(TestLogicalAnnotationOperation, AnnotationApplicability) {
  // Check that each logical annotation type correctly reports which
  // underlying primitive type(s) it can be applied to

  struct ExpectedApplicability {
    std::shared_ptr<const LogicalAnnotation> annotation;
    Type::type applicable_type;
  };

  std::vector<ExpectedApplicability> single_type_cases = {
      {LogicalAnnotation::String(), Type::BYTE_ARRAY},
      {LogicalAnnotation::Enum(), Type::BYTE_ARRAY},
      {LogicalAnnotation::Date(), Type::INT32},
      {LogicalAnnotation::Time(true, LogicalAnnotation::TimeUnit::MILLIS), Type::INT32},
      {LogicalAnnotation::Time(true, LogicalAnnotation::TimeUnit::MICROS), Type::INT64},
      {LogicalAnnotation::Time(true, LogicalAnnotation::TimeUnit::NANOS), Type::INT64},
      {LogicalAnnotation::Timestamp(true, LogicalAnnotation::TimeUnit::MILLIS),
       Type::INT64},
      {LogicalAnnotation::Timestamp(true, LogicalAnnotation::TimeUnit::MICROS),
       Type::INT64},
      {LogicalAnnotation::Timestamp(true, LogicalAnnotation::TimeUnit::NANOS),
       Type::INT64},
      {LogicalAnnotation::Int(8, false), Type::INT32},
      {LogicalAnnotation::Int(16, false), Type::INT32},
      {LogicalAnnotation::Int(32, false), Type::INT32},
      {LogicalAnnotation::Int(64, false), Type::INT64},
      {LogicalAnnotation::Int(8, true), Type::INT32},
      {LogicalAnnotation::Int(16, true), Type::INT32},
      {LogicalAnnotation::Int(32, true), Type::INT32},
      {LogicalAnnotation::Int(64, true), Type::INT64},
      {LogicalAnnotation::JSON(), Type::BYTE_ARRAY},
      {LogicalAnnotation::BSON(), Type::BYTE_ARRAY}};

  for (const ExpectedApplicability& c : single_type_cases) {
    ConfirmSinglePrimitiveTypeApplicability(c.annotation, c.applicable_type);
  }

  std::vector<std::shared_ptr<const LogicalAnnotation>> no_type_cases = {
      LogicalAnnotation::Map(), LogicalAnnotation::List()};

  for (auto c : no_type_cases) {
    ConfirmNoPrimitiveTypeApplicability(c);
  }

  std::vector<std::shared_ptr<const LogicalAnnotation>> any_type_cases = {
      LogicalAnnotation::Null(), LogicalAnnotation::None(), LogicalAnnotation::Unknown()};

  for (auto c : any_type_cases) {
    ConfirmAnyPrimitiveTypeApplicability(c);
  }

  // Fixed binary, exact length cases ...

  struct InapplicableType {
    Type::type physical_type;
    int physical_length;
  };

  std::vector<InapplicableType> inapplicable_types = {{Type::FIXED_LEN_BYTE_ARRAY, 8},
                                                      {Type::FIXED_LEN_BYTE_ARRAY, 20},
                                                      {Type::BOOLEAN, -1},
                                                      {Type::INT32, -1},
                                                      {Type::INT64, -1},
                                                      {Type::INT96, -1},
                                                      {Type::FLOAT, -1},
                                                      {Type::DOUBLE, -1},
                                                      {Type::BYTE_ARRAY, -1}};

  std::shared_ptr<const LogicalAnnotation> annotation;

  annotation = LogicalAnnotation::Interval();
  ASSERT_TRUE(annotation->is_applicable(Type::FIXED_LEN_BYTE_ARRAY, 12));
  for (const InapplicableType& t : inapplicable_types) {
    ASSERT_FALSE(annotation->is_applicable(t.physical_type, t.physical_length));
  }

  annotation = LogicalAnnotation::UUID();
  ASSERT_TRUE(annotation->is_applicable(Type::FIXED_LEN_BYTE_ARRAY, 16));
  for (const InapplicableType& t : inapplicable_types) {
    ASSERT_FALSE(annotation->is_applicable(t.physical_type, t.physical_length));
  }
}

TEST(TestLogicalAnnotationOperation, DecimalAnnotationApplicability) {
  // Check that the decimal logical annotation type correctly reports which
  // underlying primitive type(s) it can be applied to

  std::shared_ptr<const LogicalAnnotation> annotation;

  for (int32_t precision = 1; precision <= 9; ++precision) {
    annotation = DecimalAnnotation::Make(precision, 0);
    ASSERT_TRUE(annotation->is_applicable(Type::INT32))
        << annotation->ToString() << " unexpectedly inapplicable to physical type INT32";
  }
  annotation = DecimalAnnotation::Make(10, 0);
  ASSERT_FALSE(annotation->is_applicable(Type::INT32))
      << annotation->ToString() << " unexpectedly applicable to physical type INT32";

  for (int32_t precision = 1; precision <= 18; ++precision) {
    annotation = DecimalAnnotation::Make(precision, 0);
    ASSERT_TRUE(annotation->is_applicable(Type::INT64))
        << annotation->ToString() << " unexpectedly inapplicable to physical type INT64";
  }
  annotation = DecimalAnnotation::Make(19, 0);
  ASSERT_FALSE(annotation->is_applicable(Type::INT64))
      << annotation->ToString() << " unexpectedly applicable to physical type INT64";

  for (int32_t precision = 1; precision <= 36; ++precision) {
    annotation = DecimalAnnotation::Make(precision, 0);
    ASSERT_TRUE(annotation->is_applicable(Type::BYTE_ARRAY))
        << annotation->ToString()
        << " unexpectedly inapplicable to physical type BYTE_ARRAY";
  }

  struct PrecisionLimits {
    int32_t physical_length;
    int32_t precision_limit;
  };

  std::vector<PrecisionLimits> cases = {{1, 2},   {2, 4},   {3, 6},   {4, 9},  {8, 18},
                                        {10, 23}, {16, 38}, {20, 47}, {32, 76}};

  for (const PrecisionLimits& c : cases) {
    int32_t precision;
    for (precision = 1; precision <= c.precision_limit; ++precision) {
      annotation = DecimalAnnotation::Make(precision, 0);
      ASSERT_TRUE(
          annotation->is_applicable(Type::FIXED_LEN_BYTE_ARRAY, c.physical_length))
          << annotation->ToString()
          << " unexpectedly inapplicable to physical type FIXED_LEN_BYTE_ARRAY with "
             "length "
          << c.physical_length;
    }
    annotation = DecimalAnnotation::Make(precision, 0);
    ASSERT_FALSE(annotation->is_applicable(Type::FIXED_LEN_BYTE_ARRAY, c.physical_length))
        << annotation->ToString()
        << " unexpectedly applicable to physical type FIXED_LEN_BYTE_ARRAY with length "
        << c.physical_length;
  }

  ASSERT_FALSE((DecimalAnnotation::Make(16, 6))->is_applicable(Type::BOOLEAN));
  ASSERT_FALSE((DecimalAnnotation::Make(16, 6))->is_applicable(Type::FLOAT));
  ASSERT_FALSE((DecimalAnnotation::Make(16, 6))->is_applicable(Type::DOUBLE));
}

TEST(TestLogicalAnnotationOperation, AnnotationRepresentation) {
  // Ensure that each logical annotation type prints a correct string and
  // JSON representation

  struct ExpectedRepresentation {
    std::shared_ptr<const LogicalAnnotation> annotation;
    const char* string_representation;
    const char* JSON_representation;
  };

  std::vector<ExpectedRepresentation> cases = {
      {LogicalAnnotation::Unknown(), "Unknown", R"({"Type": "Unknown"})"},
      {LogicalAnnotation::String(), "String", R"({"Type": "String"})"},
      {LogicalAnnotation::Map(), "Map", R"({"Type": "Map"})"},
      {LogicalAnnotation::List(), "List", R"({"Type": "List"})"},
      {LogicalAnnotation::Enum(), "Enum", R"({"Type": "Enum"})"},
      {LogicalAnnotation::Decimal(10, 4), "Decimal(precision=10, scale=4)",
       R"({"Type": "Decimal", "precision": 10, "scale": 4})"},
      {LogicalAnnotation::Decimal(10), "Decimal(precision=10, scale=0)",
       R"({"Type": "Decimal", "precision": 10, "scale": 0})"},
      {LogicalAnnotation::Date(), "Date", R"({"Type": "Date"})"},
      {LogicalAnnotation::Time(true, LogicalAnnotation::TimeUnit::MILLIS),
       "Time(isAdjustedToUTC=true, timeUnit=milliseconds)",
       R"({"Type": "Time", "isAdjustedToUTC": true, "timeUnit": "milliseconds"})"},
      {LogicalAnnotation::Time(true, LogicalAnnotation::TimeUnit::MICROS),
       "Time(isAdjustedToUTC=true, timeUnit=microseconds)",
       R"({"Type": "Time", "isAdjustedToUTC": true, "timeUnit": "microseconds"})"},
      {LogicalAnnotation::Time(true, LogicalAnnotation::TimeUnit::NANOS),
       "Time(isAdjustedToUTC=true, timeUnit=nanoseconds)",
       R"({"Type": "Time", "isAdjustedToUTC": true, "timeUnit": "nanoseconds"})"},
      {LogicalAnnotation::Time(false, LogicalAnnotation::TimeUnit::MILLIS),
       "Time(isAdjustedToUTC=false, timeUnit=milliseconds)",
       R"({"Type": "Time", "isAdjustedToUTC": false, "timeUnit": "milliseconds"})"},
      {LogicalAnnotation::Time(false, LogicalAnnotation::TimeUnit::MICROS),
       "Time(isAdjustedToUTC=false, timeUnit=microseconds)",
       R"({"Type": "Time", "isAdjustedToUTC": false, "timeUnit": "microseconds"})"},
      {LogicalAnnotation::Time(false, LogicalAnnotation::TimeUnit::NANOS),
       "Time(isAdjustedToUTC=false, timeUnit=nanoseconds)",
       R"({"Type": "Time", "isAdjustedToUTC": false, "timeUnit": "nanoseconds"})"},
      {LogicalAnnotation::Timestamp(true, LogicalAnnotation::TimeUnit::MILLIS),
       "Timestamp(isAdjustedToUTC=true, timeUnit=milliseconds)",
       R"({"Type": "Timestamp", "isAdjustedToUTC": true, "timeUnit": "milliseconds"})"},
      {LogicalAnnotation::Timestamp(true, LogicalAnnotation::TimeUnit::MICROS),
       "Timestamp(isAdjustedToUTC=true, timeUnit=microseconds)",
       R"({"Type": "Timestamp", "isAdjustedToUTC": true, "timeUnit": "microseconds"})"},
      {LogicalAnnotation::Timestamp(true, LogicalAnnotation::TimeUnit::NANOS),
       "Timestamp(isAdjustedToUTC=true, timeUnit=nanoseconds)",
       R"({"Type": "Timestamp", "isAdjustedToUTC": true, "timeUnit": "nanoseconds"})"},
      {LogicalAnnotation::Timestamp(false, LogicalAnnotation::TimeUnit::MILLIS),
       "Timestamp(isAdjustedToUTC=false, timeUnit=milliseconds)",
       R"({"Type": "Timestamp", "isAdjustedToUTC": false, "timeUnit": "milliseconds"})"},
      {LogicalAnnotation::Timestamp(false, LogicalAnnotation::TimeUnit::MICROS),
       "Timestamp(isAdjustedToUTC=false, timeUnit=microseconds)",
       R"({"Type": "Timestamp", "isAdjustedToUTC": false, "timeUnit": "microseconds"})"},
      {LogicalAnnotation::Timestamp(false, LogicalAnnotation::TimeUnit::NANOS),
       "Timestamp(isAdjustedToUTC=false, timeUnit=nanoseconds)",
       R"({"Type": "Timestamp", "isAdjustedToUTC": false, "timeUnit": "nanoseconds"})"},
      {LogicalAnnotation::Interval(), "Interval", R"({"Type": "Interval"})"},
      {LogicalAnnotation::Int(8, false), "Int(bitWidth=8, isSigned=false)",
       R"({"Type": "Int", "bitWidth": 8, "isSigned": false})"},
      {LogicalAnnotation::Int(16, false), "Int(bitWidth=16, isSigned=false)",
       R"({"Type": "Int", "bitWidth": 16, "isSigned": false})"},
      {LogicalAnnotation::Int(32, false), "Int(bitWidth=32, isSigned=false)",
       R"({"Type": "Int", "bitWidth": 32, "isSigned": false})"},
      {LogicalAnnotation::Int(64, false), "Int(bitWidth=64, isSigned=false)",
       R"({"Type": "Int", "bitWidth": 64, "isSigned": false})"},
      {LogicalAnnotation::Int(8, true), "Int(bitWidth=8, isSigned=true)",
       R"({"Type": "Int", "bitWidth": 8, "isSigned": true})"},
      {LogicalAnnotation::Int(16, true), "Int(bitWidth=16, isSigned=true)",
       R"({"Type": "Int", "bitWidth": 16, "isSigned": true})"},
      {LogicalAnnotation::Int(32, true), "Int(bitWidth=32, isSigned=true)",
       R"({"Type": "Int", "bitWidth": 32, "isSigned": true})"},
      {LogicalAnnotation::Int(64, true), "Int(bitWidth=64, isSigned=true)",
       R"({"Type": "Int", "bitWidth": 64, "isSigned": true})"},
      {LogicalAnnotation::Null(), "Null", R"({"Type": "Null"})"},
      {LogicalAnnotation::JSON(), "JSON", R"({"Type": "JSON"})"},
      {LogicalAnnotation::BSON(), "BSON", R"({"Type": "BSON"})"},
      {LogicalAnnotation::UUID(), "UUID", R"({"Type": "UUID"})"},
      {LogicalAnnotation::None(), "None", R"({"Type": "None"})"},
  };

  for (const ExpectedRepresentation& c : cases) {
    ASSERT_STREQ(c.annotation->ToString().c_str(), c.string_representation);
    ASSERT_STREQ(c.annotation->ToJSON().c_str(), c.JSON_representation);
  }
}

TEST(TestLogicalAnnotationOperation, AnnotationSortOrder) {
  // Ensure that each logical annotation type reports the correct sort order

  struct ExpectedSortOrder {
    std::shared_ptr<const LogicalAnnotation> annotation;
    SortOrder::type sort_order;
  };

  std::vector<ExpectedSortOrder> cases = {
      {LogicalAnnotation::Unknown(), SortOrder::UNKNOWN},
      {LogicalAnnotation::String(), SortOrder::UNSIGNED},
      {LogicalAnnotation::Map(), SortOrder::UNKNOWN},
      {LogicalAnnotation::List(), SortOrder::UNKNOWN},
      {LogicalAnnotation::Enum(), SortOrder::UNSIGNED},
      {LogicalAnnotation::Decimal(8, 2), SortOrder::SIGNED},
      {LogicalAnnotation::Date(), SortOrder::SIGNED},
      {LogicalAnnotation::Time(true, LogicalAnnotation::TimeUnit::MILLIS),
       SortOrder::SIGNED},
      {LogicalAnnotation::Time(true, LogicalAnnotation::TimeUnit::MICROS),
       SortOrder::SIGNED},
      {LogicalAnnotation::Time(true, LogicalAnnotation::TimeUnit::NANOS),
       SortOrder::SIGNED},
      {LogicalAnnotation::Time(false, LogicalAnnotation::TimeUnit::MILLIS),
       SortOrder::SIGNED},
      {LogicalAnnotation::Time(false, LogicalAnnotation::TimeUnit::MICROS),
       SortOrder::SIGNED},
      {LogicalAnnotation::Time(false, LogicalAnnotation::TimeUnit::NANOS),
       SortOrder::SIGNED},
      {LogicalAnnotation::Timestamp(true, LogicalAnnotation::TimeUnit::MILLIS),
       SortOrder::SIGNED},
      {LogicalAnnotation::Timestamp(true, LogicalAnnotation::TimeUnit::MICROS),
       SortOrder::SIGNED},
      {LogicalAnnotation::Timestamp(true, LogicalAnnotation::TimeUnit::NANOS),
       SortOrder::SIGNED},
      {LogicalAnnotation::Timestamp(false, LogicalAnnotation::TimeUnit::MILLIS),
       SortOrder::SIGNED},
      {LogicalAnnotation::Timestamp(false, LogicalAnnotation::TimeUnit::MICROS),
       SortOrder::SIGNED},
      {LogicalAnnotation::Timestamp(false, LogicalAnnotation::TimeUnit::NANOS),
       SortOrder::SIGNED},
      {LogicalAnnotation::Interval(), SortOrder::UNKNOWN},
      {LogicalAnnotation::Int(8, false), SortOrder::UNSIGNED},
      {LogicalAnnotation::Int(16, false), SortOrder::UNSIGNED},
      {LogicalAnnotation::Int(32, false), SortOrder::UNSIGNED},
      {LogicalAnnotation::Int(64, false), SortOrder::UNSIGNED},
      {LogicalAnnotation::Int(8, true), SortOrder::SIGNED},
      {LogicalAnnotation::Int(16, true), SortOrder::SIGNED},
      {LogicalAnnotation::Int(32, true), SortOrder::SIGNED},
      {LogicalAnnotation::Int(64, true), SortOrder::SIGNED},
      {LogicalAnnotation::Null(), SortOrder::UNKNOWN},
      {LogicalAnnotation::JSON(), SortOrder::UNSIGNED},
      {LogicalAnnotation::BSON(), SortOrder::UNSIGNED},
      {LogicalAnnotation::UUID(), SortOrder::UNSIGNED},
      {LogicalAnnotation::None(), SortOrder::UNKNOWN}};

  for (const ExpectedSortOrder& c : cases) {
    ASSERT_EQ(c.annotation->sort_order(), c.sort_order)
        << c.annotation->ToString() << " annotation has incorrect sort order";
  }
}

static void ConfirmPrimitiveNodeFactoryEquivalence(
    const std::shared_ptr<const LogicalAnnotation>& logical_annotation,
    LogicalType::type converted_type, Type::type physical_type, int physical_length,
    int precision, int scale) {
  std::string name = "something";
  Repetition::type repetition = Repetition::REQUIRED;
  NodePtr from_converted_type = PrimitiveNode::Make(
      name, repetition, physical_type, converted_type, physical_length, precision, scale);
  NodePtr from_logical_annotation = PrimitiveNode::Make(
      name, repetition, logical_annotation, physical_type, physical_length);
  ASSERT_TRUE(from_converted_type->Equals(from_logical_annotation.get()))
      << "Primitive node constructed with converted type "
      << LogicalTypeToString(converted_type)
      << " unexpectedly not equivalent to primitive node constructed with logical "
         "annotation "
      << logical_annotation->ToString();
  return;
}

static void ConfirmGroupNodeFactoryEquivalence(
    std::string name, const std::shared_ptr<const LogicalAnnotation>& logical_annotation,
    LogicalType::type converted_type) {
  Repetition::type repetition = Repetition::OPTIONAL;
  NodePtr from_converted_type = GroupNode::Make(name, repetition, {}, converted_type);
  NodePtr from_logical_annotation =
      GroupNode::Make(name, repetition, {}, logical_annotation);
  ASSERT_TRUE(from_converted_type->Equals(from_logical_annotation.get()))
      << "Group node constructed with converted type "
      << LogicalTypeToString(converted_type)
      << " unexpectedly not equivalent to group node constructed with logical annotation "
      << logical_annotation->ToString();
  return;
}

TEST(TestSchemaNodeCreation, FactoryEquivalence) {
  // Ensure that the Node factory methods produce equivalent results regardless
  // of whether they are given a converted type or a logical annotation.

  // Primitive nodes ...

  struct PrimitiveNodeFactoryArguments {
    std::shared_ptr<const LogicalAnnotation> annotation;
    LogicalType::type converted_type;
    Type::type physical_type;
    int physical_length;
    int precision;
    int scale;
  };

  std::vector<PrimitiveNodeFactoryArguments> cases = {
      {LogicalAnnotation::String(), LogicalType::UTF8, Type::BYTE_ARRAY, -1, -1, -1},
      {LogicalAnnotation::Enum(), LogicalType::ENUM, Type::BYTE_ARRAY, -1, -1, -1},
      {LogicalAnnotation::Decimal(16, 6), LogicalType::DECIMAL, Type::INT64, -1, 16, 6},
      {LogicalAnnotation::Date(), LogicalType::DATE, Type::INT32, -1, -1, -1},
      {LogicalAnnotation::Time(true, LogicalAnnotation::TimeUnit::MILLIS),
       LogicalType::TIME_MILLIS, Type::INT32, -1, -1, -1},
      {LogicalAnnotation::Time(true, LogicalAnnotation::TimeUnit::MICROS),
       LogicalType::TIME_MICROS, Type::INT64, -1, -1, -1},
      {LogicalAnnotation::Timestamp(true, LogicalAnnotation::TimeUnit::MILLIS),
       LogicalType::TIMESTAMP_MILLIS, Type::INT64, -1, -1, -1},
      {LogicalAnnotation::Timestamp(true, LogicalAnnotation::TimeUnit::MICROS),
       LogicalType::TIMESTAMP_MICROS, Type::INT64, -1, -1, -1},
      {LogicalAnnotation::Interval(), LogicalType::INTERVAL, Type::FIXED_LEN_BYTE_ARRAY,
       12, -1, -1},
      {LogicalAnnotation::Int(8, false), LogicalType::UINT_8, Type::INT32, -1, -1, -1},
      {LogicalAnnotation::Int(8, true), LogicalType::INT_8, Type::INT32, -1, -1, -1},
      {LogicalAnnotation::Int(16, false), LogicalType::UINT_16, Type::INT32, -1, -1, -1},
      {LogicalAnnotation::Int(16, true), LogicalType::INT_16, Type::INT32, -1, -1, -1},
      {LogicalAnnotation::Int(32, false), LogicalType::UINT_32, Type::INT32, -1, -1, -1},
      {LogicalAnnotation::Int(32, true), LogicalType::INT_32, Type::INT32, -1, -1, -1},
      {LogicalAnnotation::Int(64, false), LogicalType::UINT_64, Type::INT64, -1, -1, -1},
      {LogicalAnnotation::Int(64, true), LogicalType::INT_64, Type::INT64, -1, -1, -1},
      {LogicalAnnotation::JSON(), LogicalType::JSON, Type::BYTE_ARRAY, -1, -1, -1},
      {LogicalAnnotation::BSON(), LogicalType::BSON, Type::BYTE_ARRAY, -1, -1, -1},
      {LogicalAnnotation::None(), LogicalType::NONE, Type::INT64, -1, -1, -1}};

  for (const PrimitiveNodeFactoryArguments& c : cases) {
    ConfirmPrimitiveNodeFactoryEquivalence(c.annotation, c.converted_type,
                                           c.physical_type, c.physical_length,
                                           c.precision, c.scale);
  }

  // Group nodes ...
  ConfirmGroupNodeFactoryEquivalence("map", LogicalAnnotation::Map(), LogicalType::MAP);
  ConfirmGroupNodeFactoryEquivalence("list", LogicalAnnotation::List(),
                                     LogicalType::LIST);
}

TEST(TestSchemaNodeCreation, FactoryExceptions) {
  // Ensure that the Node factory method that accepts an annotation refuses to create
  // an object if compatibility conditions are not met

  // Nested annotation on non-group node ...
  ASSERT_ANY_THROW(PrimitiveNode::Make("map", Repetition::REQUIRED, MapAnnotation::Make(),
                                       Type::INT64));
  // Incompatible primitive type ...
  ASSERT_ANY_THROW(PrimitiveNode::Make("string", Repetition::REQUIRED,
                                       StringAnnotation::Make(), Type::BOOLEAN));
  // Incompatible primitive length ...
  ASSERT_ANY_THROW(PrimitiveNode::Make("interval", Repetition::REQUIRED,
                                       IntervalAnnotation::Make(),
                                       Type::FIXED_LEN_BYTE_ARRAY, 11));
  // Primitive too small for given precision ...
  ASSERT_ANY_THROW(PrimitiveNode::Make("decimal", Repetition::REQUIRED,
                                       DecimalAnnotation::Make(16, 6), Type::INT32));
  // Incompatible primitive length ...
  ASSERT_ANY_THROW(PrimitiveNode::Make("uuid", Repetition::REQUIRED,
                                       UUIDAnnotation::Make(), Type::FIXED_LEN_BYTE_ARRAY,
                                       64));
  // Non-positive length argument for fixed length binary ...
  ASSERT_ANY_THROW(PrimitiveNode::Make("negative_length", Repetition::REQUIRED,
                                       NoAnnotation::Make(), Type::FIXED_LEN_BYTE_ARRAY,
                                       -16));
  // Non-positive length argument for fixed length binary ...
  ASSERT_ANY_THROW(PrimitiveNode::Make("zero_length", Repetition::REQUIRED,
                                       NoAnnotation::Make(), Type::FIXED_LEN_BYTE_ARRAY,
                                       0));
  // Non-nested annotation on group node ...
  ASSERT_ANY_THROW(
      GroupNode::Make("list", Repetition::REPEATED, {}, JSONAnnotation::Make()));

  // nullptr annotation arguments convert to NoAnnotation/LogicalType::NONE
  std::shared_ptr<const LogicalAnnotation> empty;
  NodePtr node;
  ASSERT_NO_THROW(
      node = PrimitiveNode::Make("value", Repetition::REQUIRED, empty, Type::DOUBLE));
  ASSERT_TRUE(node->logical_annotation()->is_none());
  ASSERT_EQ(node->logical_type(), LogicalType::NONE);
  ASSERT_NO_THROW(node = GroupNode::Make("items", Repetition::REPEATED, {}, empty));
  ASSERT_TRUE(node->logical_annotation()->is_none());
  ASSERT_EQ(node->logical_type(), LogicalType::NONE);

  // Invalid LogicalType in deserialized element ...
  node = PrimitiveNode::Make("string", Repetition::REQUIRED, StringAnnotation::Make(),
                             Type::BYTE_ARRAY);
  ASSERT_EQ(node->logical_annotation()->type(), LogicalAnnotation::Type::STRING);
  ASSERT_TRUE(node->logical_annotation()->is_valid());
  ASSERT_TRUE(node->logical_annotation()->is_serialized());
  format::SchemaElement string_intermediary;
  node->ToParquet(&string_intermediary);
  // ... corrupt the Thrift intermediary ....
  string_intermediary.logicalType.__isset.STRING = false;
  ASSERT_ANY_THROW(node = PrimitiveNode::FromParquet(&string_intermediary, 1));

  // Invalid TimeUnit in deserialized TimeAnnotation ...
  node = PrimitiveNode::Make(
      "time", Repetition::REQUIRED,
      TimeAnnotation::Make(true, LogicalAnnotation::TimeUnit::NANOS), Type::INT64);
  format::SchemaElement time_intermediary;
  node->ToParquet(&time_intermediary);
  // ... corrupt the Thrift intermediary ....
  time_intermediary.logicalType.TIME.unit.__isset.NANOS = false;
  ASSERT_ANY_THROW(PrimitiveNode::FromParquet(&time_intermediary, 1));

  // Invalid TimeUnit in deserialized TimestampAnnotation ...
  node = PrimitiveNode::Make(
      "timestamp", Repetition::REQUIRED,
      TimestampAnnotation::Make(true, LogicalAnnotation::TimeUnit::NANOS), Type::INT64);
  format::SchemaElement timestamp_intermediary;
  node->ToParquet(&timestamp_intermediary);
  // ... corrupt the Thrift intermediary ....
  timestamp_intermediary.logicalType.TIMESTAMP.unit.__isset.NANOS = false;
  ASSERT_ANY_THROW(PrimitiveNode::FromParquet(&timestamp_intermediary, 1));
}

struct SchemaElementConstructionArguments {
  std::string name;
  std::shared_ptr<const LogicalAnnotation> annotation;
  Type::type physical_type;
  int physical_length;
  bool expect_converted_type;
  LogicalType::type converted_type;
  bool expect_logicalType;
  std::function<bool()> check_logicalType;
};

class TestSchemaElementConstruction : public ::testing::Test {
 public:
  TestSchemaElementConstruction* Reconstruct(
      const SchemaElementConstructionArguments& c) {
    // Make node, create serializable Thrift object from it ...
    node_ = PrimitiveNode::Make(c.name, Repetition::REQUIRED, c.annotation,
                                c.physical_type, c.physical_length);
    element_.reset(new format::SchemaElement);
    node_->ToParquet(element_.get());

    // ... then set aside some values for later inspection.
    name_ = c.name;
    expect_converted_type_ = c.expect_converted_type;
    converted_type_ = c.converted_type;
    expect_logicalType_ = c.expect_logicalType;
    check_logicalType_ = c.check_logicalType;
    return this;
  }

  void Inspect() {
    ASSERT_EQ(element_->name, name_);
    if (expect_converted_type_) {
      ASSERT_TRUE(element_->__isset.converted_type)
          << node_->logical_annotation()->ToString()
          << " annotation unexpectedly failed to generate a converted type in the Thrift "
             "intermediate object";
      ASSERT_EQ(element_->converted_type, ToThrift(converted_type_))
          << node_->logical_annotation()->ToString()
          << " annotation unexpectedly failed to generate correct converted type in the "
             "Thrift intermediate object";
    } else {
      ASSERT_FALSE(element_->__isset.converted_type)
          << node_->logical_annotation()->ToString()
          << " annotation unexpectedly generated a converted type in the Thrift "
             "intermediate object";
    }
    if (expect_logicalType_) {
      ASSERT_TRUE(element_->__isset.logicalType)
          << node_->logical_annotation()->ToString()
          << " annotation unexpectedly failed to genverate a logicalType in the Thrift "
             "intermediate object";
      ASSERT_TRUE(check_logicalType_()) << node_->logical_annotation()->ToString()
                                        << " annotation generated incorrect logicalType "
                                           "settings in the Thrift intermediate object";
    } else {
      ASSERT_FALSE(element_->__isset.logicalType)
          << node_->logical_annotation()->ToString()
          << " annotation unexpectedly generated a logicalType in the Thrift "
             "intermediate object";
    }
    return;
  }

 protected:
  NodePtr node_;
  std::unique_ptr<format::SchemaElement> element_;
  std::string name_;
  bool expect_converted_type_;
  LogicalType::type converted_type_;  // expected converted type in Thrift object
  bool expect_logicalType_;
  std::function<bool()> check_logicalType_;  // specialized (by annotation type)
                                             // logicalType check for Thrift object
};

/*
 * The Test*SchemaElementConstruction suites confirm that the logical type
 * and converted type members of the Thrift intermediate message object
 * (format::SchemaElement) that is created upon serialization of an annotated
 * schema node are correctly populated.
 */

TEST_F(TestSchemaElementConstruction, SimpleCases) {
  auto check_nothing = []() {
    return true;
  };  // used for annotations that don't expect a logicalType to be set

  std::vector<SchemaElementConstructionArguments> cases = {
      {"string", LogicalAnnotation::String(), Type::BYTE_ARRAY, -1, true,
       LogicalType::UTF8, true,
       [this]() { return element_->logicalType.__isset.STRING; }},
      {"enum", LogicalAnnotation::Enum(), Type::BYTE_ARRAY, -1, true, LogicalType::ENUM,
       true, [this]() { return element_->logicalType.__isset.ENUM; }},
      {"date", LogicalAnnotation::Date(), Type::INT32, -1, true, LogicalType::DATE, true,
       [this]() { return element_->logicalType.__isset.DATE; }},
      {"interval", LogicalAnnotation::Interval(), Type::FIXED_LEN_BYTE_ARRAY, 12, true,
       LogicalType::INTERVAL, false, check_nothing},
      {"null", LogicalAnnotation::Null(), Type::DOUBLE, -1, false, LogicalType::NA, true,
       [this]() { return element_->logicalType.__isset.UNKNOWN; }},
      {"json", LogicalAnnotation::JSON(), Type::BYTE_ARRAY, -1, true, LogicalType::JSON,
       true, [this]() { return element_->logicalType.__isset.JSON; }},
      {"bson", LogicalAnnotation::BSON(), Type::BYTE_ARRAY, -1, true, LogicalType::BSON,
       true, [this]() { return element_->logicalType.__isset.BSON; }},
      {"uuid", LogicalAnnotation::UUID(), Type::FIXED_LEN_BYTE_ARRAY, 16, false,
       LogicalType::NA, true, [this]() { return element_->logicalType.__isset.UUID; }},
      {"none", LogicalAnnotation::None(), Type::INT64, -1, false, LogicalType::NA, false,
       check_nothing},
      {"unknown", LogicalAnnotation::Unknown(), Type::INT64, -1, true, LogicalType::NA,
       false, check_nothing}};

  for (const SchemaElementConstructionArguments& c : cases) {
    this->Reconstruct(c)->Inspect();
  }
}

class TestDecimalSchemaElementConstruction : public TestSchemaElementConstruction {
 public:
  TestDecimalSchemaElementConstruction* Reconstruct(
      const SchemaElementConstructionArguments& c) {
    TestSchemaElementConstruction::Reconstruct(c);
    const auto& decimal_annotation =
        checked_cast<const DecimalAnnotation&>(*c.annotation);
    precision_ = decimal_annotation.precision();
    scale_ = decimal_annotation.scale();
    return this;
  }

  void Inspect() {
    TestSchemaElementConstruction::Inspect();
    ASSERT_EQ(element_->precision, precision_);
    ASSERT_EQ(element_->scale, scale_);
    ASSERT_EQ(element_->logicalType.DECIMAL.precision, precision_);
    ASSERT_EQ(element_->logicalType.DECIMAL.scale, scale_);
    return;
  }

 protected:
  int32_t precision_;
  int32_t scale_;
};

TEST_F(TestDecimalSchemaElementConstruction, DecimalCases) {
  auto check_DECIMAL = [this]() { return element_->logicalType.__isset.DECIMAL; };

  std::vector<SchemaElementConstructionArguments> cases = {
      {"decimal", LogicalAnnotation::Decimal(16, 6), Type::INT64, -1, true,
       LogicalType::DECIMAL, true, check_DECIMAL},
      {"decimal", LogicalAnnotation::Decimal(1, 0), Type::INT32, -1, true,
       LogicalType::DECIMAL, true, check_DECIMAL},
      {"decimal", LogicalAnnotation::Decimal(10), Type::INT64, -1, true,
       LogicalType::DECIMAL, true, check_DECIMAL},
      {"decimal", LogicalAnnotation::Decimal(11, 11), Type::INT64, -1, true,
       LogicalType::DECIMAL, true, check_DECIMAL},
  };

  for (const SchemaElementConstructionArguments& c : cases) {
    this->Reconstruct(c)->Inspect();
  }
}

class TestTemporalSchemaElementConstruction : public TestSchemaElementConstruction {
 public:
  template <typename T>
  TestTemporalSchemaElementConstruction* Reconstruct(
      const SchemaElementConstructionArguments& c) {
    TestSchemaElementConstruction::Reconstruct(c);
    const auto& t = checked_cast<const T&>(*c.annotation);
    adjusted_ = t.is_adjusted_to_utc();
    unit_ = t.time_unit();
    return this;
  }

  template <typename T>
  void Inspect() {
    FAIL() << "Invalid typename specified in test suite";
    return;
  }

 protected:
  bool adjusted_;
  LogicalAnnotation::TimeUnit::unit unit_;
};

template <>
void TestTemporalSchemaElementConstruction::Inspect<format::TimeType>() {
  TestSchemaElementConstruction::Inspect();
  ASSERT_EQ(element_->logicalType.TIME.isAdjustedToUTC, adjusted_);
  switch (unit_) {
    case LogicalAnnotation::TimeUnit::MILLIS:
      ASSERT_TRUE(element_->logicalType.TIME.unit.__isset.MILLIS);
      break;
    case LogicalAnnotation::TimeUnit::MICROS:
      ASSERT_TRUE(element_->logicalType.TIME.unit.__isset.MICROS);
      break;
    case LogicalAnnotation::TimeUnit::NANOS:
      ASSERT_TRUE(element_->logicalType.TIME.unit.__isset.NANOS);
      break;
    case LogicalAnnotation::TimeUnit::UNKNOWN:
    default:
      FAIL() << "Invalid time unit in test case";
  }
  return;
}

template <>
void TestTemporalSchemaElementConstruction::Inspect<format::TimestampType>() {
  TestSchemaElementConstruction::Inspect();
  ASSERT_EQ(element_->logicalType.TIMESTAMP.isAdjustedToUTC, adjusted_);
  switch (unit_) {
    case LogicalAnnotation::TimeUnit::MILLIS:
      ASSERT_TRUE(element_->logicalType.TIMESTAMP.unit.__isset.MILLIS);
      break;
    case LogicalAnnotation::TimeUnit::MICROS:
      ASSERT_TRUE(element_->logicalType.TIMESTAMP.unit.__isset.MICROS);
      break;
    case LogicalAnnotation::TimeUnit::NANOS:
      ASSERT_TRUE(element_->logicalType.TIMESTAMP.unit.__isset.NANOS);
      break;
    case LogicalAnnotation::TimeUnit::UNKNOWN:
    default:
      FAIL() << "Invalid time unit in test case";
  }
  return;
}

TEST_F(TestTemporalSchemaElementConstruction, TemporalCases) {
  auto check_TIME = [this]() { return element_->logicalType.__isset.TIME; };

  std::vector<SchemaElementConstructionArguments> time_cases = {
      {"time_T_ms", LogicalAnnotation::Time(true, LogicalAnnotation::TimeUnit::MILLIS),
       Type::INT32, -1, true, LogicalType::TIME_MILLIS, true, check_TIME},
      {"time_F_ms", LogicalAnnotation::Time(false, LogicalAnnotation::TimeUnit::MILLIS),
       Type::INT32, -1, false, LogicalType::NA, true, check_TIME},
      {"time_T_us", LogicalAnnotation::Time(true, LogicalAnnotation::TimeUnit::MICROS),
       Type::INT64, -1, true, LogicalType::TIME_MICROS, true, check_TIME},
      {"time_F_us", LogicalAnnotation::Time(false, LogicalAnnotation::TimeUnit::MICROS),
       Type::INT64, -1, false, LogicalType::NA, true, check_TIME},
      {"time_T_ns", LogicalAnnotation::Time(true, LogicalAnnotation::TimeUnit::NANOS),
       Type::INT64, -1, false, LogicalType::NA, true, check_TIME},
      {"time_F_ns", LogicalAnnotation::Time(false, LogicalAnnotation::TimeUnit::NANOS),
       Type::INT64, -1, false, LogicalType::NA, true, check_TIME},
  };

  for (const SchemaElementConstructionArguments& c : time_cases) {
    this->Reconstruct<TimeAnnotation>(c)->Inspect<format::TimeType>();
  }

  auto check_TIMESTAMP = [this]() { return element_->logicalType.__isset.TIMESTAMP; };

  std::vector<SchemaElementConstructionArguments> timestamp_cases = {
      {"timestamp_T_ms",
       LogicalAnnotation::Timestamp(true, LogicalAnnotation::TimeUnit::MILLIS),
       Type::INT64, -1, true, LogicalType::TIMESTAMP_MILLIS, true, check_TIMESTAMP},
      {"timestamp_F_ms",
       LogicalAnnotation::Timestamp(false, LogicalAnnotation::TimeUnit::MILLIS),
       Type::INT64, -1, false, LogicalType::NA, true, check_TIMESTAMP},
      {"timestamp_T_us",
       LogicalAnnotation::Timestamp(true, LogicalAnnotation::TimeUnit::MICROS),
       Type::INT64, -1, true, LogicalType::TIMESTAMP_MICROS, true, check_TIMESTAMP},
      {"timestamp_F_us",
       LogicalAnnotation::Timestamp(false, LogicalAnnotation::TimeUnit::MICROS),
       Type::INT64, -1, false, LogicalType::NA, true, check_TIMESTAMP},
      {"timestamp_T_ns",
       LogicalAnnotation::Timestamp(true, LogicalAnnotation::TimeUnit::NANOS),
       Type::INT64, -1, false, LogicalType::NA, true, check_TIMESTAMP},
      {"timestamp_F_ns",
       LogicalAnnotation::Timestamp(false, LogicalAnnotation::TimeUnit::NANOS),
       Type::INT64, -1, false, LogicalType::NA, true, check_TIMESTAMP},
  };

  for (const SchemaElementConstructionArguments& c : timestamp_cases) {
    this->Reconstruct<TimestampAnnotation>(c)->Inspect<format::TimestampType>();
  }
}

class TestIntegerSchemaElementConstruction : public TestSchemaElementConstruction {
 public:
  TestIntegerSchemaElementConstruction* Reconstruct(
      const SchemaElementConstructionArguments& c) {
    TestSchemaElementConstruction::Reconstruct(c);
    const auto& int_annotation = checked_cast<const IntAnnotation&>(*c.annotation);
    width_ = int_annotation.bit_width();
    signed_ = int_annotation.is_signed();
    return this;
  }

  void Inspect() {
    TestSchemaElementConstruction::Inspect();
    ASSERT_EQ(element_->logicalType.INTEGER.bitWidth, width_);
    ASSERT_EQ(element_->logicalType.INTEGER.isSigned, signed_);
    return;
  }

 protected:
  int width_;
  bool signed_;
};

TEST_F(TestIntegerSchemaElementConstruction, IntegerCases) {
  auto check_INTEGER = [this]() { return element_->logicalType.__isset.INTEGER; };

  std::vector<SchemaElementConstructionArguments> cases = {
      {"uint8", LogicalAnnotation::Int(8, false), Type::INT32, -1, true,
       LogicalType::UINT_8, true, check_INTEGER},
      {"uint16", LogicalAnnotation::Int(16, false), Type::INT32, -1, true,
       LogicalType::UINT_16, true, check_INTEGER},
      {"uint32", LogicalAnnotation::Int(32, false), Type::INT32, -1, true,
       LogicalType::UINT_32, true, check_INTEGER},
      {"uint64", LogicalAnnotation::Int(64, false), Type::INT64, -1, true,
       LogicalType::UINT_64, true, check_INTEGER},
      {"int8", LogicalAnnotation::Int(8, true), Type::INT32, -1, true, LogicalType::INT_8,
       true, check_INTEGER},
      {"int16", LogicalAnnotation::Int(16, true), Type::INT32, -1, true,
       LogicalType::INT_16, true, check_INTEGER},
      {"int32", LogicalAnnotation::Int(32, true), Type::INT32, -1, true,
       LogicalType::INT_32, true, check_INTEGER},
      {"int64", LogicalAnnotation::Int(64, true), Type::INT64, -1, true,
       LogicalType::INT_64, true, check_INTEGER},
  };

  for (const SchemaElementConstructionArguments& c : cases) {
    this->Reconstruct(c)->Inspect();
  }
}

TEST(TestLogicalAnnotationSerialization, SchemaElementNestedCases) {
  // Confirm that the intermediate Thrift objects created during node serialization
  // contain correct ConvertedType and LogicalType information

  NodePtr string_node = PrimitiveNode::Make("string", Repetition::REQUIRED,
                                            StringAnnotation::Make(), Type::BYTE_ARRAY);
  NodePtr date_node = PrimitiveNode::Make("date", Repetition::REQUIRED,
                                          DateAnnotation::Make(), Type::INT32);
  NodePtr json_node = PrimitiveNode::Make("json", Repetition::REQUIRED,
                                          JSONAnnotation::Make(), Type::BYTE_ARRAY);
  NodePtr uuid_node =
      PrimitiveNode::Make("uuid", Repetition::REQUIRED, UUIDAnnotation::Make(),
                          Type::FIXED_LEN_BYTE_ARRAY, 16);
  NodePtr timestamp_node = PrimitiveNode::Make(
      "timestamp", Repetition::REQUIRED,
      TimestampAnnotation::Make(false, LogicalAnnotation::TimeUnit::NANOS), Type::INT64);
  NodePtr int_node = PrimitiveNode::Make("int", Repetition::REQUIRED,
                                         IntAnnotation::Make(64, false), Type::INT64);
  NodePtr decimal_node = PrimitiveNode::Make("decimal", Repetition::REQUIRED,
                                             DecimalAnnotation::Make(16, 6), Type::INT64);

  NodePtr list_node = GroupNode::Make("list", Repetition::REPEATED,
                                      {string_node, date_node, json_node, uuid_node,
                                       timestamp_node, int_node, decimal_node},
                                      ListAnnotation::Make());
  std::vector<format::SchemaElement> list_elements;
  ToParquet(reinterpret_cast<GroupNode*>(list_node.get()), &list_elements);
  ASSERT_EQ(list_elements[0].name, "list");
  ASSERT_TRUE(list_elements[0].__isset.converted_type);
  ASSERT_TRUE(list_elements[0].__isset.logicalType);
  ASSERT_EQ(list_elements[0].converted_type, ToThrift(LogicalType::LIST));
  ASSERT_TRUE(list_elements[0].logicalType.__isset.LIST);
  ASSERT_TRUE(list_elements[1].logicalType.__isset.STRING);
  ASSERT_TRUE(list_elements[2].logicalType.__isset.DATE);
  ASSERT_TRUE(list_elements[3].logicalType.__isset.JSON);
  ASSERT_TRUE(list_elements[4].logicalType.__isset.UUID);
  ASSERT_TRUE(list_elements[5].logicalType.__isset.TIMESTAMP);
  ASSERT_TRUE(list_elements[6].logicalType.__isset.INTEGER);
  ASSERT_TRUE(list_elements[7].logicalType.__isset.DECIMAL);

  NodePtr map_node =
      GroupNode::Make("map", Repetition::REQUIRED, {}, MapAnnotation::Make());
  std::vector<format::SchemaElement> map_elements;
  ToParquet(reinterpret_cast<GroupNode*>(map_node.get()), &map_elements);
  ASSERT_EQ(map_elements[0].name, "map");
  ASSERT_TRUE(map_elements[0].__isset.converted_type);
  ASSERT_TRUE(map_elements[0].__isset.logicalType);
  ASSERT_EQ(map_elements[0].converted_type, ToThrift(LogicalType::MAP));
  ASSERT_TRUE(map_elements[0].logicalType.__isset.MAP);
}

static void ConfirmPrimitiveNodeRoundtrip(
    const std::shared_ptr<const LogicalAnnotation>& annotation, Type::type physical_type,
    int physical_length) {
  std::shared_ptr<Node> original = PrimitiveNode::Make(
      "something", Repetition::REQUIRED, annotation, physical_type, physical_length);
  format::SchemaElement intermediary;
  original->ToParquet(&intermediary);
  std::unique_ptr<Node> recovered = PrimitiveNode::FromParquet(&intermediary, 1);
  ASSERT_TRUE(original->Equals(recovered.get()))
      << "Recovered primitive node unexpectedly not equivalent to original primitive "
         "node constructed with logical annotation "
      << annotation->ToString();
  return;
}

static void ConfirmGroupNodeRoundtrip(
    std::string name, const std::shared_ptr<const LogicalAnnotation>& annotation) {
  NodeVector node_vector;
  std::shared_ptr<Node> original =
      GroupNode::Make(name, Repetition::REQUIRED, node_vector, annotation);
  std::vector<format::SchemaElement> elements;
  ToParquet(reinterpret_cast<GroupNode*>(original.get()), &elements);
  std::unique_ptr<Node> recovered =
      GroupNode::FromParquet(&(elements[0]), 1, node_vector);
  ASSERT_TRUE(original->Equals(recovered.get()))
      << "Recovered group node unexpectedly not equivalent to original group node "
         "constructed with logical annotation "
      << annotation->ToString();
  return;
}

TEST(TestLogicalAnnotationSerialization, Roundtrips) {
  // Confirm that Thrift serialization-deserialization of nodes with logical
  // annotations produces equivalent reconstituted nodes

  // Primitive nodes ...
  struct AnnotatedPrimitiveNodeFactoryArguments {
    std::shared_ptr<const LogicalAnnotation> annotation;
    Type::type physical_type;
    int physical_length;
  };

  std::vector<AnnotatedPrimitiveNodeFactoryArguments> cases = {
      {LogicalAnnotation::String(), Type::BYTE_ARRAY, -1},
      {LogicalAnnotation::Enum(), Type::BYTE_ARRAY, -1},
      {LogicalAnnotation::Decimal(16, 6), Type::INT64, -1},
      {LogicalAnnotation::Date(), Type::INT32, -1},
      {LogicalAnnotation::Time(true, LogicalAnnotation::TimeUnit::MILLIS), Type::INT32,
       -1},
      {LogicalAnnotation::Time(true, LogicalAnnotation::TimeUnit::MICROS), Type::INT64,
       -1},
      {LogicalAnnotation::Time(true, LogicalAnnotation::TimeUnit::NANOS), Type::INT64,
       -1},
      {LogicalAnnotation::Time(false, LogicalAnnotation::TimeUnit::MILLIS), Type::INT32,
       -1},
      {LogicalAnnotation::Time(false, LogicalAnnotation::TimeUnit::MICROS), Type::INT64,
       -1},
      {LogicalAnnotation::Time(false, LogicalAnnotation::TimeUnit::NANOS), Type::INT64,
       -1},
      {LogicalAnnotation::Timestamp(true, LogicalAnnotation::TimeUnit::MILLIS),
       Type::INT64, -1},
      {LogicalAnnotation::Timestamp(true, LogicalAnnotation::TimeUnit::MICROS),
       Type::INT64, -1},
      {LogicalAnnotation::Timestamp(true, LogicalAnnotation::TimeUnit::NANOS),
       Type::INT64, -1},
      {LogicalAnnotation::Timestamp(false, LogicalAnnotation::TimeUnit::MILLIS),
       Type::INT64, -1},
      {LogicalAnnotation::Timestamp(false, LogicalAnnotation::TimeUnit::MICROS),
       Type::INT64, -1},
      {LogicalAnnotation::Timestamp(false, LogicalAnnotation::TimeUnit::NANOS),
       Type::INT64, -1},
      {LogicalAnnotation::Interval(), Type::FIXED_LEN_BYTE_ARRAY, 12},
      {LogicalAnnotation::Int(8, false), Type::INT32, -1},
      {LogicalAnnotation::Int(16, false), Type::INT32, -1},
      {LogicalAnnotation::Int(32, false), Type::INT32, -1},
      {LogicalAnnotation::Int(64, false), Type::INT64, -1},
      {LogicalAnnotation::Int(8, true), Type::INT32, -1},
      {LogicalAnnotation::Int(16, true), Type::INT32, -1},
      {LogicalAnnotation::Int(32, true), Type::INT32, -1},
      {LogicalAnnotation::Int(64, true), Type::INT64, -1},
      {LogicalAnnotation::Null(), Type::BOOLEAN, -1},
      {LogicalAnnotation::JSON(), Type::BYTE_ARRAY, -1},
      {LogicalAnnotation::BSON(), Type::BYTE_ARRAY, -1},
      {LogicalAnnotation::UUID(), Type::FIXED_LEN_BYTE_ARRAY, 16},
      {LogicalAnnotation::None(), Type::BOOLEAN, -1}};

  for (const AnnotatedPrimitiveNodeFactoryArguments& c : cases) {
    ConfirmPrimitiveNodeRoundtrip(c.annotation, c.physical_type, c.physical_length);
  }

  // Group nodes ...
  ConfirmGroupNodeRoundtrip("map", LogicalAnnotation::Map());
  ConfirmGroupNodeRoundtrip("list", LogicalAnnotation::List());
}

}  // namespace schema

}  // namespace parquet
