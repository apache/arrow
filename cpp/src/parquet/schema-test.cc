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

  PrimitiveNode node2("bar", Repetition::OPTIONAL, Type::BYTE_ARRAY, ConvertedType::UTF8);

  ASSERT_EQ("foo", node1.name());

  ASSERT_TRUE(node1.is_primitive());
  ASSERT_FALSE(node1.is_group());

  ASSERT_EQ(Repetition::REPEATED, node1.repetition());
  ASSERT_EQ(Repetition::OPTIONAL, node2.repetition());

  ASSERT_EQ(Node::PRIMITIVE, node1.node_type());

  ASSERT_EQ(Type::INT32, node1.physical_type());
  ASSERT_EQ(Type::BYTE_ARRAY, node2.physical_type());

  // logical types
  ASSERT_EQ(ConvertedType::NONE, node1.converted_type());
  ASSERT_EQ(ConvertedType::UTF8, node2.converted_type());

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
  ASSERT_EQ(ConvertedType::NONE, prim_node_->converted_type());

  // Test a logical type
  elt = NewPrimitive(name_, FieldRepetitionType::REQUIRED, Type::BYTE_ARRAY, 0);
  elt.__set_converted_type(format::ConvertedType::UTF8);

  ASSERT_NO_FATAL_FAILURE(Convert(&elt));
  ASSERT_EQ(Repetition::REQUIRED, prim_node_->repetition());
  ASSERT_EQ(Type::BYTE_ARRAY, prim_node_->physical_type());
  ASSERT_EQ(ConvertedType::UTF8, prim_node_->converted_type());

  // FIXED_LEN_BYTE_ARRAY
  elt = NewPrimitive(name_, FieldRepetitionType::OPTIONAL, Type::FIXED_LEN_BYTE_ARRAY, 0);
  elt.__set_type_length(16);

  ASSERT_NO_FATAL_FAILURE(Convert(&elt));
  ASSERT_EQ(name_, prim_node_->name());
  ASSERT_EQ(id_, prim_node_->id());
  ASSERT_EQ(Repetition::OPTIONAL, prim_node_->repetition());
  ASSERT_EQ(Type::FIXED_LEN_BYTE_ARRAY, prim_node_->physical_type());
  ASSERT_EQ(16, prim_node_->type_length());

  // format::ConvertedType::Decimal
  elt = NewPrimitive(name_, FieldRepetitionType::OPTIONAL, Type::FIXED_LEN_BYTE_ARRAY, 0);
  elt.__set_converted_type(format::ConvertedType::DECIMAL);
  elt.__set_type_length(6);
  elt.__set_scale(2);
  elt.__set_precision(12);

  ASSERT_NO_FATAL_FAILURE(Convert(&elt));
  ASSERT_EQ(Type::FIXED_LEN_BYTE_ARRAY, prim_node_->physical_type());
  ASSERT_EQ(ConvertedType::DECIMAL, prim_node_->converted_type());
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
                      ConvertedType::DECIMAL, 12, 4, 2);

  PrimitiveNode flba2("foo", Repetition::REQUIRED, Type::FIXED_LEN_BYTE_ARRAY,
                      ConvertedType::DECIMAL, 1, 4, 2);
  flba2.SetTypeLength(12);

  PrimitiveNode flba3("foo", Repetition::REQUIRED, Type::FIXED_LEN_BYTE_ARRAY,
                      ConvertedType::DECIMAL, 1, 4, 2);
  flba3.SetTypeLength(16);

  PrimitiveNode flba4("foo", Repetition::REQUIRED, Type::FIXED_LEN_BYTE_ARRAY,
                      ConvertedType::DECIMAL, 12, 4, 0);

  PrimitiveNode flba5("foo", Repetition::REQUIRED, Type::FIXED_LEN_BYTE_ARRAY,
                      ConvertedType::NONE, 12, 4, 0);

  ASSERT_TRUE(flba1.Equals(&flba2));
  ASSERT_FALSE(flba1.Equals(&flba3));
  ASSERT_FALSE(flba1.Equals(&flba4));
  ASSERT_FALSE(flba1.Equals(&flba5));
}

TEST_F(TestPrimitiveNode, PhysicalLogicalMapping) {
  ASSERT_NO_THROW(PrimitiveNode::Make("foo", Repetition::REQUIRED, Type::INT32,
                                      ConvertedType::INT_32));
  ASSERT_NO_THROW(PrimitiveNode::Make("foo", Repetition::REQUIRED, Type::BYTE_ARRAY,
                                      ConvertedType::JSON));
  ASSERT_THROW(
      PrimitiveNode::Make("foo", Repetition::REQUIRED, Type::INT32, ConvertedType::JSON),
      ParquetException);
  ASSERT_NO_THROW(PrimitiveNode::Make("foo", Repetition::REQUIRED, Type::INT64,
                                      ConvertedType::TIMESTAMP_MILLIS));
  ASSERT_THROW(PrimitiveNode::Make("foo", Repetition::REQUIRED, Type::INT32,
                                   ConvertedType::INT_64),
               ParquetException);
  ASSERT_THROW(PrimitiveNode::Make("foo", Repetition::REQUIRED, Type::BYTE_ARRAY,
                                   ConvertedType::INT_8),
               ParquetException);
  ASSERT_THROW(PrimitiveNode::Make("foo", Repetition::REQUIRED, Type::BYTE_ARRAY,
                                   ConvertedType::INTERVAL),
               ParquetException);
  ASSERT_THROW(PrimitiveNode::Make("foo", Repetition::REQUIRED,
                                   Type::FIXED_LEN_BYTE_ARRAY, ConvertedType::ENUM),
               ParquetException);
  ASSERT_NO_THROW(PrimitiveNode::Make("foo", Repetition::REQUIRED, Type::BYTE_ARRAY,
                                      ConvertedType::ENUM));
  ASSERT_THROW(
      PrimitiveNode::Make("foo", Repetition::REQUIRED, Type::FIXED_LEN_BYTE_ARRAY,
                          ConvertedType::DECIMAL, 0, 2, 4),
      ParquetException);
  ASSERT_THROW(PrimitiveNode::Make("foo", Repetition::REQUIRED, Type::FLOAT,
                                   ConvertedType::DECIMAL, 0, 2, 4),
               ParquetException);
  ASSERT_THROW(
      PrimitiveNode::Make("foo", Repetition::REQUIRED, Type::FIXED_LEN_BYTE_ARRAY,
                          ConvertedType::DECIMAL, 0, 4, 0),
      ParquetException);
  ASSERT_THROW(
      PrimitiveNode::Make("foo", Repetition::REQUIRED, Type::FIXED_LEN_BYTE_ARRAY,
                          ConvertedType::DECIMAL, 10, 0, 4),
      ParquetException);
  ASSERT_THROW(
      PrimitiveNode::Make("foo", Repetition::REQUIRED, Type::FIXED_LEN_BYTE_ARRAY,
                          ConvertedType::DECIMAL, 10, 4, -1),
      ParquetException);
  ASSERT_THROW(
      PrimitiveNode::Make("foo", Repetition::REQUIRED, Type::FIXED_LEN_BYTE_ARRAY,
                          ConvertedType::DECIMAL, 10, 2, 4),
      ParquetException);
  ASSERT_NO_THROW(PrimitiveNode::Make("foo", Repetition::REQUIRED,
                                      Type::FIXED_LEN_BYTE_ARRAY, ConvertedType::DECIMAL,
                                      10, 6, 4));
  ASSERT_NO_THROW(PrimitiveNode::Make("foo", Repetition::REQUIRED,
                                      Type::FIXED_LEN_BYTE_ARRAY, ConvertedType::INTERVAL,
                                      12));
  ASSERT_THROW(
      PrimitiveNode::Make("foo", Repetition::REQUIRED, Type::FIXED_LEN_BYTE_ARRAY,
                          ConvertedType::INTERVAL, 10),
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
  GroupNode node2("bar", Repetition::OPTIONAL, fields, ConvertedType::LIST);

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
  ASSERT_EQ(ConvertedType::NONE, node1.converted_type());
  ASSERT_EQ(ConvertedType::LIST, node2.converted_type());
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
  elt.__set_converted_type(format::ConvertedType::LIST);
  elements.push_back(elt);
  elements.push_back(NewPrimitive("item", FieldRepetitionType::OPTIONAL, Type::INT64, 4));

  ASSERT_NO_FATAL_FAILURE(Convert(&elements[0], static_cast<int>(elements.size())));

  // Construct the expected schema
  NodeVector fields;
  fields.push_back(Int32("a", Repetition::REQUIRED));

  // 3-level list encoding
  NodePtr item = Int64("item");
  NodePtr list(GroupNode::Make("b", Repetition::REPEATED, {item}, ConvertedType::LIST));
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
                                     ConvertedType::DECIMAL, -1, 8, 4);
  NodePtr group =
      GroupNode::Make("group", Repetition::REPEATED, {node}, ConvertedType::LIST);
  Flatten(reinterpret_cast<GroupNode*>(group.get()));
  ASSERT_EQ("decimal", elements_[1].name);
  ASSERT_TRUE(elements_[1].__isset.precision);
  ASSERT_TRUE(elements_[1].__isset.scale);

  elements_.clear();
  // ... including those created with new logical types
  node = PrimitiveNode::Make("decimal", Repetition::REQUIRED,
                             DecimalLogicalType::Make(10, 5), Type::INT64, -1);
  group = GroupNode::Make("group", Repetition::REPEATED, {node}, ListLogicalType::Make());
  Flatten(reinterpret_cast<GroupNode*>(group.get()));
  ASSERT_EQ("decimal", elements_[1].name);
  ASSERT_TRUE(elements_[1].__isset.precision);
  ASSERT_TRUE(elements_[1].__isset.scale);

  elements_.clear();
  // Not for integers with no logical type
  group = GroupNode::Make("group", Repetition::REPEATED, {Int64("int64")},
                          ConvertedType::LIST);
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
  elt.__set_converted_type(format::ConvertedType::LIST);
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
  NodePtr list(GroupNode::Make("b", Repetition::REPEATED, {item}, ConvertedType::LIST));
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
                                     ConvertedType::UTF8);
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
  converted_type: UTF8,
  logical_type: String,
  max_definition_level: 4,
  max_repetition_level: 1,
})";
  ASSERT_EQ(expected_descr, descr.ToString());

  // Test FIXED_LEN_BYTE_ARRAY
  node = PrimitiveNode::Make("name", Repetition::OPTIONAL, Type::FIXED_LEN_BYTE_ARRAY,
                             ConvertedType::DECIMAL, 12, 10, 4);
  descr = ColumnDescriptor(node, 4, 1);

  ASSERT_EQ(Type::FIXED_LEN_BYTE_ARRAY, descr.physical_type());
  ASSERT_EQ(12, descr.type_length());

  expected_descr = R"(column descriptor = {
  name: name,
  path: ,
  physical_type: FIXED_LEN_BYTE_ARRAY,
  converted_type: DECIMAL,
  logical_type: Decimal(precision=10, scale=4),
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
                               ConvertedType::LIST));

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
                               ConvertedType::LIST));
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
      GroupNode::Make("b", Repetition::REPEATED, {item1, item2}, ConvertedType::LIST));
  NodePtr bag(GroupNode::Make("bag", Repetition::OPTIONAL, {list}));
  fields.push_back(bag);

  fields.push_back(PrimitiveNode::Make("c", Repetition::REQUIRED, Type::INT32,
                                       ConvertedType::DECIMAL, -1, 3, 2));

  fields.push_back(PrimitiveNode::Make("d", Repetition::REQUIRED,
                                       DecimalLogicalType::Make(10, 5), Type::INT64, -1));

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
    ConvertedType::type converted_type,
    const std::shared_ptr<const LogicalType>& from_make,
    std::function<bool(const std::shared_ptr<const LogicalType>&)> check_is_type) {
  std::shared_ptr<const LogicalType> from_converted_type =
      LogicalType::FromConvertedType(converted_type);
  ASSERT_EQ(from_converted_type->type(), from_make->type())
      << from_make->ToString() << " logical types unexpectedly do not match on type";
  ASSERT_TRUE(from_converted_type->Equals(*from_make))
      << from_make->ToString() << " logical types unexpectedly not equivalent";
  ASSERT_TRUE(check_is_type(from_converted_type))
      << from_converted_type->ToString()
      << " logical type (from converted type) does not have expected type property";
  ASSERT_TRUE(check_is_type(from_make))
      << from_make->ToString()
      << " logical type (from Make()) does not have expected type property";
  return;
}

TEST(TestLogicalTypeConstruction, FactoryEquivalence) {
  // For each legacy converted type, ensure that the equivalent logical type object
  // can be obtained from either the base class's FromConvertedType() factory method or
  // the logical type type class's Make() method (accessed via convenience methods on the
  // base class) and that these logical type objects are equivalent

  struct ConfirmFactoryEquivalenceArguments {
    ConvertedType::type converted_type;
    std::shared_ptr<const LogicalType> logical_type;
    std::function<bool(const std::shared_ptr<const LogicalType>&)> check_is_type;
  };

  auto check_is_string = [](const std::shared_ptr<const LogicalType>& logical_type) {
    return logical_type->is_string();
  };
  auto check_is_map = [](const std::shared_ptr<const LogicalType>& logical_type) {
    return logical_type->is_map();
  };
  auto check_is_list = [](const std::shared_ptr<const LogicalType>& logical_type) {
    return logical_type->is_list();
  };
  auto check_is_enum = [](const std::shared_ptr<const LogicalType>& logical_type) {
    return logical_type->is_enum();
  };
  auto check_is_date = [](const std::shared_ptr<const LogicalType>& logical_type) {
    return logical_type->is_date();
  };
  auto check_is_time = [](const std::shared_ptr<const LogicalType>& logical_type) {
    return logical_type->is_time();
  };
  auto check_is_timestamp = [](const std::shared_ptr<const LogicalType>& logical_type) {
    return logical_type->is_timestamp();
  };
  auto check_is_int = [](const std::shared_ptr<const LogicalType>& logical_type) {
    return logical_type->is_int();
  };
  auto check_is_JSON = [](const std::shared_ptr<const LogicalType>& logical_type) {
    return logical_type->is_JSON();
  };
  auto check_is_BSON = [](const std::shared_ptr<const LogicalType>& logical_type) {
    return logical_type->is_BSON();
  };
  auto check_is_interval = [](const std::shared_ptr<const LogicalType>& logical_type) {
    return logical_type->is_interval();
  };
  auto check_is_none = [](const std::shared_ptr<const LogicalType>& logical_type) {
    return logical_type->is_none();
  };

  std::vector<ConfirmFactoryEquivalenceArguments> cases = {
      {ConvertedType::UTF8, LogicalType::String(), check_is_string},
      {ConvertedType::MAP, LogicalType::Map(), check_is_map},
      {ConvertedType::MAP_KEY_VALUE, LogicalType::Map(), check_is_map},
      {ConvertedType::LIST, LogicalType::List(), check_is_list},
      {ConvertedType::ENUM, LogicalType::Enum(), check_is_enum},
      {ConvertedType::DATE, LogicalType::Date(), check_is_date},
      {ConvertedType::TIME_MILLIS, LogicalType::Time(true, LogicalType::TimeUnit::MILLIS),
       check_is_time},
      {ConvertedType::TIME_MICROS, LogicalType::Time(true, LogicalType::TimeUnit::MICROS),
       check_is_time},
      {ConvertedType::TIMESTAMP_MILLIS,
       LogicalType::Timestamp(true, LogicalType::TimeUnit::MILLIS), check_is_timestamp},
      {ConvertedType::TIMESTAMP_MICROS,
       LogicalType::Timestamp(true, LogicalType::TimeUnit::MICROS), check_is_timestamp},
      {ConvertedType::UINT_8, LogicalType::Int(8, false), check_is_int},
      {ConvertedType::UINT_16, LogicalType::Int(16, false), check_is_int},
      {ConvertedType::UINT_32, LogicalType::Int(32, false), check_is_int},
      {ConvertedType::UINT_64, LogicalType::Int(64, false), check_is_int},
      {ConvertedType::INT_8, LogicalType::Int(8, true), check_is_int},
      {ConvertedType::INT_16, LogicalType::Int(16, true), check_is_int},
      {ConvertedType::INT_32, LogicalType::Int(32, true), check_is_int},
      {ConvertedType::INT_64, LogicalType::Int(64, true), check_is_int},
      {ConvertedType::JSON, LogicalType::JSON(), check_is_JSON},
      {ConvertedType::BSON, LogicalType::BSON(), check_is_BSON},
      {ConvertedType::INTERVAL, LogicalType::Interval(), check_is_interval},
      {ConvertedType::NONE, LogicalType::None(), check_is_none}};

  for (const ConfirmFactoryEquivalenceArguments& c : cases) {
    ConfirmFactoryEquivalence(c.converted_type, c.logical_type, c.check_is_type);
  }

  // ConvertedType::DECIMAL, LogicalType::Decimal, is_decimal
  schema::DecimalMetadata converted_decimal_metadata;
  converted_decimal_metadata.isset = true;
  converted_decimal_metadata.precision = 10;
  converted_decimal_metadata.scale = 4;
  std::shared_ptr<const LogicalType> from_converted_type =
      LogicalType::FromConvertedType(ConvertedType::DECIMAL, converted_decimal_metadata);
  std::shared_ptr<const LogicalType> from_make = LogicalType::Decimal(10, 4);
  ASSERT_EQ(from_converted_type->type(), from_make->type());
  ASSERT_TRUE(from_converted_type->Equals(*from_make));
  ASSERT_TRUE(from_converted_type->is_decimal());
  ASSERT_TRUE(from_make->is_decimal());
  ASSERT_TRUE(LogicalType::Decimal(16)->Equals(*LogicalType::Decimal(16, 0)));
}

static void ConfirmConvertedTypeCompatibility(
    const std::shared_ptr<const LogicalType>& original,
    ConvertedType::type expected_converted_type) {
  ASSERT_TRUE(original->is_valid())
      << original->ToString() << " logical type unexpectedly is not valid";
  schema::DecimalMetadata converted_decimal_metadata;
  ConvertedType::type converted_type =
      original->ToConvertedType(&converted_decimal_metadata);
  ASSERT_EQ(converted_type, expected_converted_type)
      << original->ToString()
      << " logical type unexpectedly returns incorrect converted type";
  ASSERT_FALSE(converted_decimal_metadata.isset)
      << original->ToString()
      << " logical type unexpectedly returns converted decimal metatdata that is set";
  ASSERT_TRUE(original->is_compatible(converted_type, converted_decimal_metadata))
      << original->ToString()
      << " logical type unexpectedly is incompatible with converted type and decimal "
         "metadata it returned";
  ASSERT_FALSE(original->is_compatible(converted_type, {true, 1, 1}))
      << original->ToString()
      << " logical type unexpectedly is compatible with converted decimal metadata that "
         "is "
         "set";
  ASSERT_TRUE(original->is_compatible(converted_type))
      << original->ToString()
      << " logical type unexpectedly is incompatible with converted type it returned";
  std::shared_ptr<const LogicalType> reconstructed =
      LogicalType::FromConvertedType(converted_type, converted_decimal_metadata);
  ASSERT_TRUE(reconstructed->is_valid()) << "Reconstructed " << reconstructed->ToString()
                                         << " logical type unexpectedly is not valid";
  ASSERT_TRUE(reconstructed->Equals(*original))
      << "Reconstructed logical type (" << reconstructed->ToString()
      << ") unexpectedly not equivalent to original logical type ("
      << original->ToString() << ")";
  return;
}

TEST(TestLogicalTypeConstruction, ConvertedTypeCompatibility) {
  // For each legacy converted type, ensure that the equivalent logical type
  // emits correct, compatible converted type information and that the emitted
  // information can be used to reconstruct another equivalent logical type.

  struct ExpectedConvertedType {
    std::shared_ptr<const LogicalType> logical_type;
    ConvertedType::type converted_type;
  };

  std::vector<ExpectedConvertedType> cases = {
      {LogicalType::String(), ConvertedType::UTF8},
      {LogicalType::Map(), ConvertedType::MAP},
      {LogicalType::List(), ConvertedType::LIST},
      {LogicalType::Enum(), ConvertedType::ENUM},
      {LogicalType::Date(), ConvertedType::DATE},
      {LogicalType::Time(true, LogicalType::TimeUnit::MILLIS),
       ConvertedType::TIME_MILLIS},
      {LogicalType::Time(true, LogicalType::TimeUnit::MICROS),
       ConvertedType::TIME_MICROS},
      {LogicalType::Timestamp(true, LogicalType::TimeUnit::MILLIS),
       ConvertedType::TIMESTAMP_MILLIS},
      {LogicalType::Timestamp(true, LogicalType::TimeUnit::MICROS),
       ConvertedType::TIMESTAMP_MICROS},
      {LogicalType::Int(8, false), ConvertedType::UINT_8},
      {LogicalType::Int(16, false), ConvertedType::UINT_16},
      {LogicalType::Int(32, false), ConvertedType::UINT_32},
      {LogicalType::Int(64, false), ConvertedType::UINT_64},
      {LogicalType::Int(8, true), ConvertedType::INT_8},
      {LogicalType::Int(16, true), ConvertedType::INT_16},
      {LogicalType::Int(32, true), ConvertedType::INT_32},
      {LogicalType::Int(64, true), ConvertedType::INT_64},
      {LogicalType::JSON(), ConvertedType::JSON},
      {LogicalType::BSON(), ConvertedType::BSON},
      {LogicalType::Interval(), ConvertedType::INTERVAL},
      {LogicalType::None(), ConvertedType::NONE}};

  for (const ExpectedConvertedType& c : cases) {
    ConfirmConvertedTypeCompatibility(c.logical_type, c.converted_type);
  }

  // Special cases ...

  std::shared_ptr<const LogicalType> original;
  ConvertedType::type converted_type;
  schema::DecimalMetadata converted_decimal_metadata;
  std::shared_ptr<const LogicalType> reconstructed;

  // DECIMAL
  std::memset(&converted_decimal_metadata, 0x00, sizeof(converted_decimal_metadata));
  original = LogicalType::Decimal(6, 2);
  ASSERT_TRUE(original->is_valid());
  converted_type = original->ToConvertedType(&converted_decimal_metadata);
  ASSERT_EQ(converted_type, ConvertedType::DECIMAL);
  ASSERT_TRUE(converted_decimal_metadata.isset);
  ASSERT_EQ(converted_decimal_metadata.precision, 6);
  ASSERT_EQ(converted_decimal_metadata.scale, 2);
  ASSERT_TRUE(original->is_compatible(converted_type, converted_decimal_metadata));
  reconstructed =
      LogicalType::FromConvertedType(converted_type, converted_decimal_metadata);
  ASSERT_TRUE(reconstructed->is_valid());
  ASSERT_TRUE(reconstructed->Equals(*original));

  // Unknown
  original = LogicalType::Unknown();
  ASSERT_TRUE(original->is_invalid());
  ASSERT_FALSE(original->is_valid());
  converted_type = original->ToConvertedType(&converted_decimal_metadata);
  ASSERT_EQ(converted_type, ConvertedType::NA);
  ASSERT_FALSE(converted_decimal_metadata.isset);
  ASSERT_TRUE(original->is_compatible(converted_type, converted_decimal_metadata));
  ASSERT_TRUE(original->is_compatible(converted_type));
  reconstructed =
      LogicalType::FromConvertedType(converted_type, converted_decimal_metadata);
  ASSERT_TRUE(reconstructed->is_invalid());
  ASSERT_TRUE(reconstructed->Equals(*original));
}

static void ConfirmNewTypeIncompatibility(
    const std::shared_ptr<const LogicalType>& logical_type,
    std::function<bool(const std::shared_ptr<const LogicalType>&)> check_is_type) {
  ASSERT_TRUE(logical_type->is_valid())
      << logical_type->ToString() << " logical type unexpectedly is not valid";
  ASSERT_TRUE(check_is_type(logical_type))
      << logical_type->ToString() << " logical type is not expected logical type";
  schema::DecimalMetadata converted_decimal_metadata;
  ConvertedType::type converted_type =
      logical_type->ToConvertedType(&converted_decimal_metadata);
  ASSERT_EQ(converted_type, ConvertedType::NONE)
      << logical_type->ToString()
      << " logical type converted type unexpectedly is not NONE";
  ASSERT_FALSE(converted_decimal_metadata.isset)
      << logical_type->ToString()
      << " logical type converted decimal metadata unexpectedly is set";
  return;
}

TEST(TestLogicalTypeConstruction, NewTypeIncompatibility) {
  // For each new logical type, ensure that the type
  // correctly reports that it has no legacy equivalent

  struct ConfirmNewTypeIncompatibilityArguments {
    std::shared_ptr<const LogicalType> logical_type;
    std::function<bool(const std::shared_ptr<const LogicalType>&)> check_is_type;
  };

  auto check_is_UUID = [](const std::shared_ptr<const LogicalType>& logical_type) {
    return logical_type->is_UUID();
  };
  auto check_is_null = [](const std::shared_ptr<const LogicalType>& logical_type) {
    return logical_type->is_null();
  };
  auto check_is_time = [](const std::shared_ptr<const LogicalType>& logical_type) {
    return logical_type->is_time();
  };
  auto check_is_timestamp = [](const std::shared_ptr<const LogicalType>& logical_type) {
    return logical_type->is_timestamp();
  };

  std::vector<ConfirmNewTypeIncompatibilityArguments> cases = {
      {LogicalType::UUID(), check_is_UUID},
      {LogicalType::Null(), check_is_null},
      {LogicalType::Time(false, LogicalType::TimeUnit::MILLIS), check_is_time},
      {LogicalType::Time(false, LogicalType::TimeUnit::MICROS), check_is_time},
      {LogicalType::Time(false, LogicalType::TimeUnit::NANOS), check_is_time},
      {LogicalType::Time(true, LogicalType::TimeUnit::NANOS), check_is_time},
      {LogicalType::Timestamp(false, LogicalType::TimeUnit::NANOS), check_is_timestamp},
      {LogicalType::Timestamp(true, LogicalType::TimeUnit::NANOS), check_is_timestamp},
  };

  for (const ConfirmNewTypeIncompatibilityArguments& c : cases) {
    ConfirmNewTypeIncompatibility(c.logical_type, c.check_is_type);
  }
}

TEST(TestLogicalTypeConstruction, FactoryExceptions) {
  // Ensure that logical type construction catches invalid arguments

  std::vector<std::function<void()>> cases = {
      []() {
        TimeLogicalType::Make(true, LogicalType::TimeUnit::UNKNOWN);
      },  // Invalid TimeUnit
      []() {
        TimestampLogicalType::Make(true, LogicalType::TimeUnit::UNKNOWN);
      },                                           // Invalid TimeUnit
      []() { IntLogicalType::Make(-1, false); },   // Invalid bit width
      []() { IntLogicalType::Make(0, false); },    // Invalid bit width
      []() { IntLogicalType::Make(1, false); },    // Invalid bit width
      []() { IntLogicalType::Make(65, false); },   // Invalid bit width
      []() { DecimalLogicalType::Make(-1); },      // Invalid precision
      []() { DecimalLogicalType::Make(0); },       // Invalid precision
      []() { DecimalLogicalType::Make(0, 0); },    // Invalid precision
      []() { DecimalLogicalType::Make(10, -1); },  // Invalid scale
      []() { DecimalLogicalType::Make(10, 11); }   // Invalid scale
  };

  for (auto f : cases) {
    ASSERT_ANY_THROW(f());
  }
}

static void ConfirmLogicalTypeProperties(
    const std::shared_ptr<const LogicalType>& logical_type, bool nested, bool serialized,
    bool valid) {
  ASSERT_TRUE(logical_type->is_nested() == nested)
      << logical_type->ToString() << " logical type has incorrect nested() property";
  ASSERT_TRUE(logical_type->is_serialized() == serialized)
      << logical_type->ToString() << " logical type has incorrect serialized() property";
  ASSERT_TRUE(logical_type->is_valid() == valid)
      << logical_type->ToString() << " logical type has incorrect valid() property";
  ASSERT_TRUE(logical_type->is_nonnested() != nested)
      << logical_type->ToString() << " logical type has incorrect nonnested() property";
  ASSERT_TRUE(logical_type->is_invalid() != valid)
      << logical_type->ToString() << " logical type has incorrect invalid() property";
  return;
}

TEST(TestLogicalTypeOperation, LogicalTypeProperties) {
  // For each logical type, ensure that the correct general properties are reported

  struct ExpectedProperties {
    std::shared_ptr<const LogicalType> logical_type;
    bool nested;
    bool serialized;
    bool valid;
  };

  std::vector<ExpectedProperties> cases = {
      {StringLogicalType::Make(), false, true, true},
      {MapLogicalType::Make(), true, true, true},
      {ListLogicalType::Make(), true, true, true},
      {EnumLogicalType::Make(), false, true, true},
      {DecimalLogicalType::Make(16, 6), false, true, true},
      {DateLogicalType::Make(), false, true, true},
      {TimeLogicalType::Make(true, LogicalType::TimeUnit::MICROS), false, true, true},
      {TimestampLogicalType::Make(true, LogicalType::TimeUnit::MICROS), false, true,
       true},
      {IntervalLogicalType::Make(), false, true, true},
      {IntLogicalType::Make(8, false), false, true, true},
      {IntLogicalType::Make(64, true), false, true, true},
      {NullLogicalType::Make(), false, true, true},
      {JSONLogicalType::Make(), false, true, true},
      {BSONLogicalType::Make(), false, true, true},
      {UUIDLogicalType::Make(), false, true, true},
      {NoLogicalType::Make(), false, false, true},
      {UnknownLogicalType::Make(), false, false, false},
  };

  for (const ExpectedProperties& c : cases) {
    ConfirmLogicalTypeProperties(c.logical_type, c.nested, c.serialized, c.valid);
  }
}

static constexpr int PHYSICAL_TYPE_COUNT = 8;

static Type::type physical_type[PHYSICAL_TYPE_COUNT] = {
    Type::BOOLEAN, Type::INT32,  Type::INT64,      Type::INT96,
    Type::FLOAT,   Type::DOUBLE, Type::BYTE_ARRAY, Type::FIXED_LEN_BYTE_ARRAY};

static void ConfirmSinglePrimitiveTypeApplicability(
    const std::shared_ptr<const LogicalType>& logical_type, Type::type applicable_type) {
  for (int i = 0; i < PHYSICAL_TYPE_COUNT; ++i) {
    if (physical_type[i] == applicable_type) {
      ASSERT_TRUE(logical_type->is_applicable(physical_type[i]))
          << logical_type->ToString()
          << " logical type unexpectedly inapplicable to physical type "
          << TypeToString(physical_type[i]);
    } else {
      ASSERT_FALSE(logical_type->is_applicable(physical_type[i]))
          << logical_type->ToString()
          << " logical type unexpectedly applicable to physical type "
          << TypeToString(physical_type[i]);
    }
  }
  return;
}

static void ConfirmAnyPrimitiveTypeApplicability(
    const std::shared_ptr<const LogicalType>& logical_type) {
  for (int i = 0; i < PHYSICAL_TYPE_COUNT; ++i) {
    ASSERT_TRUE(logical_type->is_applicable(physical_type[i]))
        << logical_type->ToString()
        << " logical type unexpectedly inapplicable to physical type "
        << TypeToString(physical_type[i]);
  }
  return;
}

static void ConfirmNoPrimitiveTypeApplicability(
    const std::shared_ptr<const LogicalType>& logical_type) {
  for (int i = 0; i < PHYSICAL_TYPE_COUNT; ++i) {
    ASSERT_FALSE(logical_type->is_applicable(physical_type[i]))
        << logical_type->ToString()
        << " logical type unexpectedly applicable to physical type "
        << TypeToString(physical_type[i]);
  }
  return;
}

TEST(TestLogicalTypeOperation, LogicalTypeApplicability) {
  // Check that each logical type correctly reports which
  // underlying primitive type(s) it can be applied to

  struct ExpectedApplicability {
    std::shared_ptr<const LogicalType> logical_type;
    Type::type applicable_type;
  };

  std::vector<ExpectedApplicability> single_type_cases = {
      {LogicalType::String(), Type::BYTE_ARRAY},
      {LogicalType::Enum(), Type::BYTE_ARRAY},
      {LogicalType::Date(), Type::INT32},
      {LogicalType::Time(true, LogicalType::TimeUnit::MILLIS), Type::INT32},
      {LogicalType::Time(true, LogicalType::TimeUnit::MICROS), Type::INT64},
      {LogicalType::Time(true, LogicalType::TimeUnit::NANOS), Type::INT64},
      {LogicalType::Timestamp(true, LogicalType::TimeUnit::MILLIS), Type::INT64},
      {LogicalType::Timestamp(true, LogicalType::TimeUnit::MICROS), Type::INT64},
      {LogicalType::Timestamp(true, LogicalType::TimeUnit::NANOS), Type::INT64},
      {LogicalType::Int(8, false), Type::INT32},
      {LogicalType::Int(16, false), Type::INT32},
      {LogicalType::Int(32, false), Type::INT32},
      {LogicalType::Int(64, false), Type::INT64},
      {LogicalType::Int(8, true), Type::INT32},
      {LogicalType::Int(16, true), Type::INT32},
      {LogicalType::Int(32, true), Type::INT32},
      {LogicalType::Int(64, true), Type::INT64},
      {LogicalType::JSON(), Type::BYTE_ARRAY},
      {LogicalType::BSON(), Type::BYTE_ARRAY}};

  for (const ExpectedApplicability& c : single_type_cases) {
    ConfirmSinglePrimitiveTypeApplicability(c.logical_type, c.applicable_type);
  }

  std::vector<std::shared_ptr<const LogicalType>> no_type_cases = {LogicalType::Map(),
                                                                   LogicalType::List()};

  for (auto c : no_type_cases) {
    ConfirmNoPrimitiveTypeApplicability(c);
  }

  std::vector<std::shared_ptr<const LogicalType>> any_type_cases = {
      LogicalType::Null(), LogicalType::None(), LogicalType::Unknown()};

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

  std::shared_ptr<const LogicalType> logical_type;

  logical_type = LogicalType::Interval();
  ASSERT_TRUE(logical_type->is_applicable(Type::FIXED_LEN_BYTE_ARRAY, 12));
  for (const InapplicableType& t : inapplicable_types) {
    ASSERT_FALSE(logical_type->is_applicable(t.physical_type, t.physical_length));
  }

  logical_type = LogicalType::UUID();
  ASSERT_TRUE(logical_type->is_applicable(Type::FIXED_LEN_BYTE_ARRAY, 16));
  for (const InapplicableType& t : inapplicable_types) {
    ASSERT_FALSE(logical_type->is_applicable(t.physical_type, t.physical_length));
  }
}

TEST(TestLogicalTypeOperation, DecimalLogicalTypeApplicability) {
  // Check that the decimal logical type correctly reports which
  // underlying primitive type(s) it can be applied to

  std::shared_ptr<const LogicalType> logical_type;

  for (int32_t precision = 1; precision <= 9; ++precision) {
    logical_type = DecimalLogicalType::Make(precision, 0);
    ASSERT_TRUE(logical_type->is_applicable(Type::INT32))
        << logical_type->ToString()
        << " unexpectedly inapplicable to physical type INT32";
  }
  logical_type = DecimalLogicalType::Make(10, 0);
  ASSERT_FALSE(logical_type->is_applicable(Type::INT32))
      << logical_type->ToString() << " unexpectedly applicable to physical type INT32";

  for (int32_t precision = 1; precision <= 18; ++precision) {
    logical_type = DecimalLogicalType::Make(precision, 0);
    ASSERT_TRUE(logical_type->is_applicable(Type::INT64))
        << logical_type->ToString()
        << " unexpectedly inapplicable to physical type INT64";
  }
  logical_type = DecimalLogicalType::Make(19, 0);
  ASSERT_FALSE(logical_type->is_applicable(Type::INT64))
      << logical_type->ToString() << " unexpectedly applicable to physical type INT64";

  for (int32_t precision = 1; precision <= 36; ++precision) {
    logical_type = DecimalLogicalType::Make(precision, 0);
    ASSERT_TRUE(logical_type->is_applicable(Type::BYTE_ARRAY))
        << logical_type->ToString()
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
      logical_type = DecimalLogicalType::Make(precision, 0);
      ASSERT_TRUE(
          logical_type->is_applicable(Type::FIXED_LEN_BYTE_ARRAY, c.physical_length))
          << logical_type->ToString()
          << " unexpectedly inapplicable to physical type FIXED_LEN_BYTE_ARRAY with "
             "length "
          << c.physical_length;
    }
    logical_type = DecimalLogicalType::Make(precision, 0);
    ASSERT_FALSE(
        logical_type->is_applicable(Type::FIXED_LEN_BYTE_ARRAY, c.physical_length))
        << logical_type->ToString()
        << " unexpectedly applicable to physical type FIXED_LEN_BYTE_ARRAY with length "
        << c.physical_length;
  }

  ASSERT_FALSE((DecimalLogicalType::Make(16, 6))->is_applicable(Type::BOOLEAN));
  ASSERT_FALSE((DecimalLogicalType::Make(16, 6))->is_applicable(Type::FLOAT));
  ASSERT_FALSE((DecimalLogicalType::Make(16, 6))->is_applicable(Type::DOUBLE));
}

TEST(TestLogicalTypeOperation, LogicalTypeRepresentation) {
  // Ensure that each logical type prints a correct string and
  // JSON representation

  struct ExpectedRepresentation {
    std::shared_ptr<const LogicalType> logical_type;
    const char* string_representation;
    const char* JSON_representation;
  };

  std::vector<ExpectedRepresentation> cases = {
      {LogicalType::Unknown(), "Unknown", R"({"Type": "Unknown"})"},
      {LogicalType::String(), "String", R"({"Type": "String"})"},
      {LogicalType::Map(), "Map", R"({"Type": "Map"})"},
      {LogicalType::List(), "List", R"({"Type": "List"})"},
      {LogicalType::Enum(), "Enum", R"({"Type": "Enum"})"},
      {LogicalType::Decimal(10, 4), "Decimal(precision=10, scale=4)",
       R"({"Type": "Decimal", "precision": 10, "scale": 4})"},
      {LogicalType::Decimal(10), "Decimal(precision=10, scale=0)",
       R"({"Type": "Decimal", "precision": 10, "scale": 0})"},
      {LogicalType::Date(), "Date", R"({"Type": "Date"})"},
      {LogicalType::Time(true, LogicalType::TimeUnit::MILLIS),
       "Time(isAdjustedToUTC=true, timeUnit=milliseconds)",
       R"({"Type": "Time", "isAdjustedToUTC": true, "timeUnit": "milliseconds"})"},
      {LogicalType::Time(true, LogicalType::TimeUnit::MICROS),
       "Time(isAdjustedToUTC=true, timeUnit=microseconds)",
       R"({"Type": "Time", "isAdjustedToUTC": true, "timeUnit": "microseconds"})"},
      {LogicalType::Time(true, LogicalType::TimeUnit::NANOS),
       "Time(isAdjustedToUTC=true, timeUnit=nanoseconds)",
       R"({"Type": "Time", "isAdjustedToUTC": true, "timeUnit": "nanoseconds"})"},
      {LogicalType::Time(false, LogicalType::TimeUnit::MILLIS),
       "Time(isAdjustedToUTC=false, timeUnit=milliseconds)",
       R"({"Type": "Time", "isAdjustedToUTC": false, "timeUnit": "milliseconds"})"},
      {LogicalType::Time(false, LogicalType::TimeUnit::MICROS),
       "Time(isAdjustedToUTC=false, timeUnit=microseconds)",
       R"({"Type": "Time", "isAdjustedToUTC": false, "timeUnit": "microseconds"})"},
      {LogicalType::Time(false, LogicalType::TimeUnit::NANOS),
       "Time(isAdjustedToUTC=false, timeUnit=nanoseconds)",
       R"({"Type": "Time", "isAdjustedToUTC": false, "timeUnit": "nanoseconds"})"},
      {LogicalType::Timestamp(true, LogicalType::TimeUnit::MILLIS),
       "Timestamp(isAdjustedToUTC=true, timeUnit=milliseconds, "
       "force_set_converted_type=false)",
       R"({"Type": "Timestamp", "isAdjustedToUTC": true, "timeUnit": "milliseconds", "force_set_converted_type": false})"},
      {LogicalType::Timestamp(true, LogicalType::TimeUnit::MICROS),
       "Timestamp(isAdjustedToUTC=true, timeUnit=microseconds, "
       "force_set_converted_type=false)",
       R"({"Type": "Timestamp", "isAdjustedToUTC": true, "timeUnit": "microseconds", "force_set_converted_type": false})"},
      {LogicalType::Timestamp(true, LogicalType::TimeUnit::NANOS),
       "Timestamp(isAdjustedToUTC=true, timeUnit=nanoseconds, "
       "force_set_converted_type=false)",
       R"({"Type": "Timestamp", "isAdjustedToUTC": true, "timeUnit": "nanoseconds", "force_set_converted_type": false})"},
      {LogicalType::Timestamp(false, LogicalType::TimeUnit::MILLIS, true),
       "Timestamp(isAdjustedToUTC=false, timeUnit=milliseconds, "
       "force_set_converted_type=true)",
       R"({"Type": "Timestamp", "isAdjustedToUTC": false, "timeUnit": "milliseconds", "force_set_converted_type": true})"},
      {LogicalType::Timestamp(false, LogicalType::TimeUnit::MICROS),
       "Timestamp(isAdjustedToUTC=false, timeUnit=microseconds, "
       "force_set_converted_type=false)",
       R"({"Type": "Timestamp", "isAdjustedToUTC": false, "timeUnit": "microseconds", "force_set_converted_type": false})"},
      {LogicalType::Timestamp(false, LogicalType::TimeUnit::NANOS),
       "Timestamp(isAdjustedToUTC=false, timeUnit=nanoseconds, "
       "force_set_converted_type=false)",
       R"({"Type": "Timestamp", "isAdjustedToUTC": false, "timeUnit": "nanoseconds", "force_set_converted_type": false})"},
      {LogicalType::Interval(), "Interval", R"({"Type": "Interval"})"},
      {LogicalType::Int(8, false), "Int(bitWidth=8, isSigned=false)",
       R"({"Type": "Int", "bitWidth": 8, "isSigned": false})"},
      {LogicalType::Int(16, false), "Int(bitWidth=16, isSigned=false)",
       R"({"Type": "Int", "bitWidth": 16, "isSigned": false})"},
      {LogicalType::Int(32, false), "Int(bitWidth=32, isSigned=false)",
       R"({"Type": "Int", "bitWidth": 32, "isSigned": false})"},
      {LogicalType::Int(64, false), "Int(bitWidth=64, isSigned=false)",
       R"({"Type": "Int", "bitWidth": 64, "isSigned": false})"},
      {LogicalType::Int(8, true), "Int(bitWidth=8, isSigned=true)",
       R"({"Type": "Int", "bitWidth": 8, "isSigned": true})"},
      {LogicalType::Int(16, true), "Int(bitWidth=16, isSigned=true)",
       R"({"Type": "Int", "bitWidth": 16, "isSigned": true})"},
      {LogicalType::Int(32, true), "Int(bitWidth=32, isSigned=true)",
       R"({"Type": "Int", "bitWidth": 32, "isSigned": true})"},
      {LogicalType::Int(64, true), "Int(bitWidth=64, isSigned=true)",
       R"({"Type": "Int", "bitWidth": 64, "isSigned": true})"},
      {LogicalType::Null(), "Null", R"({"Type": "Null"})"},
      {LogicalType::JSON(), "JSON", R"({"Type": "JSON"})"},
      {LogicalType::BSON(), "BSON", R"({"Type": "BSON"})"},
      {LogicalType::UUID(), "UUID", R"({"Type": "UUID"})"},
      {LogicalType::None(), "None", R"({"Type": "None"})"},
  };

  for (const ExpectedRepresentation& c : cases) {
    ASSERT_STREQ(c.logical_type->ToString().c_str(), c.string_representation);
    ASSERT_STREQ(c.logical_type->ToJSON().c_str(), c.JSON_representation);
  }
}

TEST(TestLogicalTypeOperation, LogicalTypeSortOrder) {
  // Ensure that each logical type reports the correct sort order

  struct ExpectedSortOrder {
    std::shared_ptr<const LogicalType> logical_type;
    SortOrder::type sort_order;
  };

  std::vector<ExpectedSortOrder> cases = {
      {LogicalType::Unknown(), SortOrder::UNKNOWN},
      {LogicalType::String(), SortOrder::UNSIGNED},
      {LogicalType::Map(), SortOrder::UNKNOWN},
      {LogicalType::List(), SortOrder::UNKNOWN},
      {LogicalType::Enum(), SortOrder::UNSIGNED},
      {LogicalType::Decimal(8, 2), SortOrder::SIGNED},
      {LogicalType::Date(), SortOrder::SIGNED},
      {LogicalType::Time(true, LogicalType::TimeUnit::MILLIS), SortOrder::SIGNED},
      {LogicalType::Time(true, LogicalType::TimeUnit::MICROS), SortOrder::SIGNED},
      {LogicalType::Time(true, LogicalType::TimeUnit::NANOS), SortOrder::SIGNED},
      {LogicalType::Time(false, LogicalType::TimeUnit::MILLIS), SortOrder::SIGNED},
      {LogicalType::Time(false, LogicalType::TimeUnit::MICROS), SortOrder::SIGNED},
      {LogicalType::Time(false, LogicalType::TimeUnit::NANOS), SortOrder::SIGNED},
      {LogicalType::Timestamp(true, LogicalType::TimeUnit::MILLIS), SortOrder::SIGNED},
      {LogicalType::Timestamp(true, LogicalType::TimeUnit::MICROS), SortOrder::SIGNED},
      {LogicalType::Timestamp(true, LogicalType::TimeUnit::NANOS), SortOrder::SIGNED},
      {LogicalType::Timestamp(false, LogicalType::TimeUnit::MILLIS), SortOrder::SIGNED},
      {LogicalType::Timestamp(false, LogicalType::TimeUnit::MICROS), SortOrder::SIGNED},
      {LogicalType::Timestamp(false, LogicalType::TimeUnit::NANOS), SortOrder::SIGNED},
      {LogicalType::Interval(), SortOrder::UNKNOWN},
      {LogicalType::Int(8, false), SortOrder::UNSIGNED},
      {LogicalType::Int(16, false), SortOrder::UNSIGNED},
      {LogicalType::Int(32, false), SortOrder::UNSIGNED},
      {LogicalType::Int(64, false), SortOrder::UNSIGNED},
      {LogicalType::Int(8, true), SortOrder::SIGNED},
      {LogicalType::Int(16, true), SortOrder::SIGNED},
      {LogicalType::Int(32, true), SortOrder::SIGNED},
      {LogicalType::Int(64, true), SortOrder::SIGNED},
      {LogicalType::Null(), SortOrder::UNKNOWN},
      {LogicalType::JSON(), SortOrder::UNSIGNED},
      {LogicalType::BSON(), SortOrder::UNSIGNED},
      {LogicalType::UUID(), SortOrder::UNSIGNED},
      {LogicalType::None(), SortOrder::UNKNOWN}};

  for (const ExpectedSortOrder& c : cases) {
    ASSERT_EQ(c.logical_type->sort_order(), c.sort_order)
        << c.logical_type->ToString() << " logical type has incorrect sort order";
  }
}

static void ConfirmPrimitiveNodeFactoryEquivalence(
    const std::shared_ptr<const LogicalType>& logical_type,
    ConvertedType::type converted_type, Type::type physical_type, int physical_length,
    int precision, int scale) {
  std::string name = "something";
  Repetition::type repetition = Repetition::REQUIRED;
  NodePtr from_converted_type = PrimitiveNode::Make(
      name, repetition, physical_type, converted_type, physical_length, precision, scale);
  NodePtr from_logical_type =
      PrimitiveNode::Make(name, repetition, logical_type, physical_type, physical_length);
  ASSERT_TRUE(from_converted_type->Equals(from_logical_type.get()))
      << "Primitive node constructed with converted type "
      << ConvertedTypeToString(converted_type)
      << " unexpectedly not equivalent to primitive node constructed with logical "
         "type "
      << logical_type->ToString();
  return;
}

static void ConfirmGroupNodeFactoryEquivalence(
    std::string name, const std::shared_ptr<const LogicalType>& logical_type,
    ConvertedType::type converted_type) {
  Repetition::type repetition = Repetition::OPTIONAL;
  NodePtr from_converted_type = GroupNode::Make(name, repetition, {}, converted_type);
  NodePtr from_logical_type = GroupNode::Make(name, repetition, {}, logical_type);
  ASSERT_TRUE(from_converted_type->Equals(from_logical_type.get()))
      << "Group node constructed with converted type "
      << ConvertedTypeToString(converted_type)
      << " unexpectedly not equivalent to group node constructed with logical type "
      << logical_type->ToString();
  return;
}

TEST(TestSchemaNodeCreation, FactoryEquivalence) {
  // Ensure that the Node factory methods produce equivalent results regardless
  // of whether they are given a converted type or a logical type.

  // Primitive nodes ...

  struct PrimitiveNodeFactoryArguments {
    std::shared_ptr<const LogicalType> logical_type;
    ConvertedType::type converted_type;
    Type::type physical_type;
    int physical_length;
    int precision;
    int scale;
  };

  std::vector<PrimitiveNodeFactoryArguments> cases = {
      {LogicalType::String(), ConvertedType::UTF8, Type::BYTE_ARRAY, -1, -1, -1},
      {LogicalType::Enum(), ConvertedType::ENUM, Type::BYTE_ARRAY, -1, -1, -1},
      {LogicalType::Decimal(16, 6), ConvertedType::DECIMAL, Type::INT64, -1, 16, 6},
      {LogicalType::Date(), ConvertedType::DATE, Type::INT32, -1, -1, -1},
      {LogicalType::Time(true, LogicalType::TimeUnit::MILLIS), ConvertedType::TIME_MILLIS,
       Type::INT32, -1, -1, -1},
      {LogicalType::Time(true, LogicalType::TimeUnit::MICROS), ConvertedType::TIME_MICROS,
       Type::INT64, -1, -1, -1},
      {LogicalType::Timestamp(true, LogicalType::TimeUnit::MILLIS),
       ConvertedType::TIMESTAMP_MILLIS, Type::INT64, -1, -1, -1},
      {LogicalType::Timestamp(true, LogicalType::TimeUnit::MICROS),
       ConvertedType::TIMESTAMP_MICROS, Type::INT64, -1, -1, -1},
      {LogicalType::Interval(), ConvertedType::INTERVAL, Type::FIXED_LEN_BYTE_ARRAY, 12,
       -1, -1},
      {LogicalType::Int(8, false), ConvertedType::UINT_8, Type::INT32, -1, -1, -1},
      {LogicalType::Int(8, true), ConvertedType::INT_8, Type::INT32, -1, -1, -1},
      {LogicalType::Int(16, false), ConvertedType::UINT_16, Type::INT32, -1, -1, -1},
      {LogicalType::Int(16, true), ConvertedType::INT_16, Type::INT32, -1, -1, -1},
      {LogicalType::Int(32, false), ConvertedType::UINT_32, Type::INT32, -1, -1, -1},
      {LogicalType::Int(32, true), ConvertedType::INT_32, Type::INT32, -1, -1, -1},
      {LogicalType::Int(64, false), ConvertedType::UINT_64, Type::INT64, -1, -1, -1},
      {LogicalType::Int(64, true), ConvertedType::INT_64, Type::INT64, -1, -1, -1},
      {LogicalType::JSON(), ConvertedType::JSON, Type::BYTE_ARRAY, -1, -1, -1},
      {LogicalType::BSON(), ConvertedType::BSON, Type::BYTE_ARRAY, -1, -1, -1},
      {LogicalType::None(), ConvertedType::NONE, Type::INT64, -1, -1, -1}};

  for (const PrimitiveNodeFactoryArguments& c : cases) {
    ConfirmPrimitiveNodeFactoryEquivalence(c.logical_type, c.converted_type,
                                           c.physical_type, c.physical_length,
                                           c.precision, c.scale);
  }

  // Group nodes ...
  ConfirmGroupNodeFactoryEquivalence("map", LogicalType::Map(), ConvertedType::MAP);
  ConfirmGroupNodeFactoryEquivalence("list", LogicalType::List(), ConvertedType::LIST);
}

TEST(TestSchemaNodeCreation, FactoryExceptions) {
  // Ensure that the Node factory method that accepts a logical type refuses to create
  // an object if compatibility conditions are not met

  // Nested logical type on non-group node ...
  ASSERT_ANY_THROW(PrimitiveNode::Make("map", Repetition::REQUIRED,
                                       MapLogicalType::Make(), Type::INT64));
  // Incompatible primitive type ...
  ASSERT_ANY_THROW(PrimitiveNode::Make("string", Repetition::REQUIRED,
                                       StringLogicalType::Make(), Type::BOOLEAN));
  // Incompatible primitive length ...
  ASSERT_ANY_THROW(PrimitiveNode::Make("interval", Repetition::REQUIRED,
                                       IntervalLogicalType::Make(),
                                       Type::FIXED_LEN_BYTE_ARRAY, 11));
  // Primitive too small for given precision ...
  ASSERT_ANY_THROW(PrimitiveNode::Make("decimal", Repetition::REQUIRED,
                                       DecimalLogicalType::Make(16, 6), Type::INT32));
  // Incompatible primitive length ...
  ASSERT_ANY_THROW(PrimitiveNode::Make("uuid", Repetition::REQUIRED,
                                       UUIDLogicalType::Make(),
                                       Type::FIXED_LEN_BYTE_ARRAY, 64));
  // Non-positive length argument for fixed length binary ...
  ASSERT_ANY_THROW(PrimitiveNode::Make("negative_length", Repetition::REQUIRED,
                                       NoLogicalType::Make(), Type::FIXED_LEN_BYTE_ARRAY,
                                       -16));
  // Non-positive length argument for fixed length binary ...
  ASSERT_ANY_THROW(PrimitiveNode::Make("zero_length", Repetition::REQUIRED,
                                       NoLogicalType::Make(), Type::FIXED_LEN_BYTE_ARRAY,
                                       0));
  // Non-nested logical type on group node ...
  ASSERT_ANY_THROW(
      GroupNode::Make("list", Repetition::REPEATED, {}, JSONLogicalType::Make()));

  // nullptr logical type arguments convert to NoLogicalType/ConvertedType::NONE
  std::shared_ptr<const LogicalType> empty;
  NodePtr node;
  ASSERT_NO_THROW(
      node = PrimitiveNode::Make("value", Repetition::REQUIRED, empty, Type::DOUBLE));
  ASSERT_TRUE(node->logical_type()->is_none());
  ASSERT_EQ(node->converted_type(), ConvertedType::NONE);
  ASSERT_NO_THROW(node = GroupNode::Make("items", Repetition::REPEATED, {}, empty));
  ASSERT_TRUE(node->logical_type()->is_none());
  ASSERT_EQ(node->converted_type(), ConvertedType::NONE);

  // Invalid ConvertedType in deserialized element ...
  node = PrimitiveNode::Make("string", Repetition::REQUIRED, StringLogicalType::Make(),
                             Type::BYTE_ARRAY);
  ASSERT_EQ(node->logical_type()->type(), LogicalType::Type::STRING);
  ASSERT_TRUE(node->logical_type()->is_valid());
  ASSERT_TRUE(node->logical_type()->is_serialized());
  format::SchemaElement string_intermediary;
  node->ToParquet(&string_intermediary);
  // ... corrupt the Thrift intermediary ....
  string_intermediary.logicalType.__isset.STRING = false;
  ASSERT_ANY_THROW(node = PrimitiveNode::FromParquet(&string_intermediary, 1));

  // Invalid TimeUnit in deserialized TimeLogicalType ...
  node = PrimitiveNode::Make("time", Repetition::REQUIRED,
                             TimeLogicalType::Make(true, LogicalType::TimeUnit::NANOS),
                             Type::INT64);
  format::SchemaElement time_intermediary;
  node->ToParquet(&time_intermediary);
  // ... corrupt the Thrift intermediary ....
  time_intermediary.logicalType.TIME.unit.__isset.NANOS = false;
  ASSERT_ANY_THROW(PrimitiveNode::FromParquet(&time_intermediary, 1));

  // Invalid TimeUnit in deserialized TimestampLogicalType ...
  node = PrimitiveNode::Make(
      "timestamp", Repetition::REQUIRED,
      TimestampLogicalType::Make(true, LogicalType::TimeUnit::NANOS), Type::INT64);
  format::SchemaElement timestamp_intermediary;
  node->ToParquet(&timestamp_intermediary);
  // ... corrupt the Thrift intermediary ....
  timestamp_intermediary.logicalType.TIMESTAMP.unit.__isset.NANOS = false;
  ASSERT_ANY_THROW(PrimitiveNode::FromParquet(&timestamp_intermediary, 1));
}

struct SchemaElementConstructionArguments {
  std::string name;
  std::shared_ptr<const LogicalType> logical_type;
  Type::type physical_type;
  int physical_length;
  bool expect_converted_type;
  ConvertedType::type converted_type;
  bool expect_logicalType;
  std::function<bool()> check_logicalType;
};

class TestSchemaElementConstruction : public ::testing::Test {
 public:
  TestSchemaElementConstruction* Reconstruct(
      const SchemaElementConstructionArguments& c) {
    // Make node, create serializable Thrift object from it ...
    node_ = PrimitiveNode::Make(c.name, Repetition::REQUIRED, c.logical_type,
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
          << node_->logical_type()->ToString()
          << " logical type unexpectedly failed to generate a converted type in the "
             "Thrift "
             "intermediate object";
      ASSERT_EQ(element_->converted_type, ToThrift(converted_type_))
          << node_->logical_type()->ToString()
          << " logical type unexpectedly failed to generate correct converted type in "
             "the "
             "Thrift intermediate object";
    } else {
      ASSERT_FALSE(element_->__isset.converted_type)
          << node_->logical_type()->ToString()
          << " logical type unexpectedly generated a converted type in the Thrift "
             "intermediate object";
    }
    if (expect_logicalType_) {
      ASSERT_TRUE(element_->__isset.logicalType)
          << node_->logical_type()->ToString()
          << " logical type unexpectedly failed to genverate a logicalType in the Thrift "
             "intermediate object";
      ASSERT_TRUE(check_logicalType_())
          << node_->logical_type()->ToString()
          << " logical type generated incorrect logicalType "
             "settings in the Thrift intermediate object";
    } else {
      ASSERT_FALSE(element_->__isset.logicalType)
          << node_->logical_type()->ToString()
          << " logical type unexpectedly generated a logicalType in the Thrift "
             "intermediate object";
    }
    return;
  }

 protected:
  NodePtr node_;
  std::unique_ptr<format::SchemaElement> element_;
  std::string name_;
  bool expect_converted_type_;
  ConvertedType::type converted_type_;  // expected converted type in Thrift object
  bool expect_logicalType_;
  std::function<bool()> check_logicalType_;  // specialized (by logical type)
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
  };  // used for logical types that don't expect a logicalType to be set

  std::vector<SchemaElementConstructionArguments> cases = {
      {"string", LogicalType::String(), Type::BYTE_ARRAY, -1, true, ConvertedType::UTF8,
       true, [this]() { return element_->logicalType.__isset.STRING; }},
      {"enum", LogicalType::Enum(), Type::BYTE_ARRAY, -1, true, ConvertedType::ENUM, true,
       [this]() { return element_->logicalType.__isset.ENUM; }},
      {"date", LogicalType::Date(), Type::INT32, -1, true, ConvertedType::DATE, true,
       [this]() { return element_->logicalType.__isset.DATE; }},
      {"interval", LogicalType::Interval(), Type::FIXED_LEN_BYTE_ARRAY, 12, true,
       ConvertedType::INTERVAL, false, check_nothing},
      {"null", LogicalType::Null(), Type::DOUBLE, -1, false, ConvertedType::NA, true,
       [this]() { return element_->logicalType.__isset.UNKNOWN; }},
      {"json", LogicalType::JSON(), Type::BYTE_ARRAY, -1, true, ConvertedType::JSON, true,
       [this]() { return element_->logicalType.__isset.JSON; }},
      {"bson", LogicalType::BSON(), Type::BYTE_ARRAY, -1, true, ConvertedType::BSON, true,
       [this]() { return element_->logicalType.__isset.BSON; }},
      {"uuid", LogicalType::UUID(), Type::FIXED_LEN_BYTE_ARRAY, 16, false,
       ConvertedType::NA, true, [this]() { return element_->logicalType.__isset.UUID; }},
      {"none", LogicalType::None(), Type::INT64, -1, false, ConvertedType::NA, false,
       check_nothing},
      {"unknown", LogicalType::Unknown(), Type::INT64, -1, true, ConvertedType::NA, false,
       check_nothing}};

  for (const SchemaElementConstructionArguments& c : cases) {
    this->Reconstruct(c)->Inspect();
  }
}

class TestDecimalSchemaElementConstruction : public TestSchemaElementConstruction {
 public:
  TestDecimalSchemaElementConstruction* Reconstruct(
      const SchemaElementConstructionArguments& c) {
    TestSchemaElementConstruction::Reconstruct(c);
    const auto& decimal_logical_type =
        checked_cast<const DecimalLogicalType&>(*c.logical_type);
    precision_ = decimal_logical_type.precision();
    scale_ = decimal_logical_type.scale();
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
      {"decimal", LogicalType::Decimal(16, 6), Type::INT64, -1, true,
       ConvertedType::DECIMAL, true, check_DECIMAL},
      {"decimal", LogicalType::Decimal(1, 0), Type::INT32, -1, true,
       ConvertedType::DECIMAL, true, check_DECIMAL},
      {"decimal", LogicalType::Decimal(10), Type::INT64, -1, true, ConvertedType::DECIMAL,
       true, check_DECIMAL},
      {"decimal", LogicalType::Decimal(11, 11), Type::INT64, -1, true,
       ConvertedType::DECIMAL, true, check_DECIMAL},
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
    const auto& t = checked_cast<const T&>(*c.logical_type);
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
  LogicalType::TimeUnit::unit unit_;
};

template <>
void TestTemporalSchemaElementConstruction::Inspect<format::TimeType>() {
  TestSchemaElementConstruction::Inspect();
  ASSERT_EQ(element_->logicalType.TIME.isAdjustedToUTC, adjusted_);
  switch (unit_) {
    case LogicalType::TimeUnit::MILLIS:
      ASSERT_TRUE(element_->logicalType.TIME.unit.__isset.MILLIS);
      break;
    case LogicalType::TimeUnit::MICROS:
      ASSERT_TRUE(element_->logicalType.TIME.unit.__isset.MICROS);
      break;
    case LogicalType::TimeUnit::NANOS:
      ASSERT_TRUE(element_->logicalType.TIME.unit.__isset.NANOS);
      break;
    case LogicalType::TimeUnit::UNKNOWN:
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
    case LogicalType::TimeUnit::MILLIS:
      ASSERT_TRUE(element_->logicalType.TIMESTAMP.unit.__isset.MILLIS);
      break;
    case LogicalType::TimeUnit::MICROS:
      ASSERT_TRUE(element_->logicalType.TIMESTAMP.unit.__isset.MICROS);
      break;
    case LogicalType::TimeUnit::NANOS:
      ASSERT_TRUE(element_->logicalType.TIMESTAMP.unit.__isset.NANOS);
      break;
    case LogicalType::TimeUnit::UNKNOWN:
    default:
      FAIL() << "Invalid time unit in test case";
  }
  return;
}

TEST_F(TestTemporalSchemaElementConstruction, TemporalCases) {
  auto check_TIME = [this]() { return element_->logicalType.__isset.TIME; };

  std::vector<SchemaElementConstructionArguments> time_cases = {
      {"time_T_ms", LogicalType::Time(true, LogicalType::TimeUnit::MILLIS), Type::INT32,
       -1, true, ConvertedType::TIME_MILLIS, true, check_TIME},
      {"time_F_ms", LogicalType::Time(false, LogicalType::TimeUnit::MILLIS), Type::INT32,
       -1, false, ConvertedType::NA, true, check_TIME},
      {"time_T_us", LogicalType::Time(true, LogicalType::TimeUnit::MICROS), Type::INT64,
       -1, true, ConvertedType::TIME_MICROS, true, check_TIME},
      {"time_F_us", LogicalType::Time(false, LogicalType::TimeUnit::MICROS), Type::INT64,
       -1, false, ConvertedType::NA, true, check_TIME},
      {"time_T_ns", LogicalType::Time(true, LogicalType::TimeUnit::NANOS), Type::INT64,
       -1, false, ConvertedType::NA, true, check_TIME},
      {"time_F_ns", LogicalType::Time(false, LogicalType::TimeUnit::NANOS), Type::INT64,
       -1, false, ConvertedType::NA, true, check_TIME},
  };

  for (const SchemaElementConstructionArguments& c : time_cases) {
    this->Reconstruct<TimeLogicalType>(c)->Inspect<format::TimeType>();
  }

  auto check_TIMESTAMP = [this]() { return element_->logicalType.__isset.TIMESTAMP; };

  std::vector<SchemaElementConstructionArguments> timestamp_cases = {
      {"timestamp_T_ms", LogicalType::Timestamp(true, LogicalType::TimeUnit::MILLIS),
       Type::INT64, -1, true, ConvertedType::TIMESTAMP_MILLIS, true, check_TIMESTAMP},
      {"timestamp_F_ms", LogicalType::Timestamp(false, LogicalType::TimeUnit::MILLIS),
       Type::INT64, -1, false, ConvertedType::NA, true, check_TIMESTAMP},
      {"timestamp_F_ms_force",
       LogicalType::Timestamp(false, LogicalType::TimeUnit::MILLIS,
                              /*force_set_converted_type=*/true),
       Type::INT64, -1, true, ConvertedType::TIMESTAMP_MILLIS, true, check_TIMESTAMP},
      {"timestamp_T_us", LogicalType::Timestamp(true, LogicalType::TimeUnit::MICROS),
       Type::INT64, -1, true, ConvertedType::TIMESTAMP_MICROS, true, check_TIMESTAMP},
      {"timestamp_F_us", LogicalType::Timestamp(false, LogicalType::TimeUnit::MICROS),
       Type::INT64, -1, false, ConvertedType::NA, true, check_TIMESTAMP},
      {"timestamp_F_us_force",
       LogicalType::Timestamp(false, LogicalType::TimeUnit::MILLIS,
                              /*force_set_converted_type=*/true),
       Type::INT64, -1, true, ConvertedType::TIMESTAMP_MILLIS, true, check_TIMESTAMP},
      {"timestamp_T_ns", LogicalType::Timestamp(true, LogicalType::TimeUnit::NANOS),
       Type::INT64, -1, false, ConvertedType::NA, true, check_TIMESTAMP},
      {"timestamp_F_ns", LogicalType::Timestamp(false, LogicalType::TimeUnit::NANOS),
       Type::INT64, -1, false, ConvertedType::NA, true, check_TIMESTAMP},
  };

  for (const SchemaElementConstructionArguments& c : timestamp_cases) {
    this->Reconstruct<TimestampLogicalType>(c)->Inspect<format::TimestampType>();
  }
}

class TestIntegerSchemaElementConstruction : public TestSchemaElementConstruction {
 public:
  TestIntegerSchemaElementConstruction* Reconstruct(
      const SchemaElementConstructionArguments& c) {
    TestSchemaElementConstruction::Reconstruct(c);
    const auto& int_logical_type = checked_cast<const IntLogicalType&>(*c.logical_type);
    width_ = int_logical_type.bit_width();
    signed_ = int_logical_type.is_signed();
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
      {"uint8", LogicalType::Int(8, false), Type::INT32, -1, true, ConvertedType::UINT_8,
       true, check_INTEGER},
      {"uint16", LogicalType::Int(16, false), Type::INT32, -1, true,
       ConvertedType::UINT_16, true, check_INTEGER},
      {"uint32", LogicalType::Int(32, false), Type::INT32, -1, true,
       ConvertedType::UINT_32, true, check_INTEGER},
      {"uint64", LogicalType::Int(64, false), Type::INT64, -1, true,
       ConvertedType::UINT_64, true, check_INTEGER},
      {"int8", LogicalType::Int(8, true), Type::INT32, -1, true, ConvertedType::INT_8,
       true, check_INTEGER},
      {"int16", LogicalType::Int(16, true), Type::INT32, -1, true, ConvertedType::INT_16,
       true, check_INTEGER},
      {"int32", LogicalType::Int(32, true), Type::INT32, -1, true, ConvertedType::INT_32,
       true, check_INTEGER},
      {"int64", LogicalType::Int(64, true), Type::INT64, -1, true, ConvertedType::INT_64,
       true, check_INTEGER},
  };

  for (const SchemaElementConstructionArguments& c : cases) {
    this->Reconstruct(c)->Inspect();
  }
}

TEST(TestLogicalTypeSerialization, SchemaElementNestedCases) {
  // Confirm that the intermediate Thrift objects created during node serialization
  // contain correct ConvertedType and ConvertedType information

  NodePtr string_node = PrimitiveNode::Make("string", Repetition::REQUIRED,
                                            StringLogicalType::Make(), Type::BYTE_ARRAY);
  NodePtr date_node = PrimitiveNode::Make("date", Repetition::REQUIRED,
                                          DateLogicalType::Make(), Type::INT32);
  NodePtr json_node = PrimitiveNode::Make("json", Repetition::REQUIRED,
                                          JSONLogicalType::Make(), Type::BYTE_ARRAY);
  NodePtr uuid_node =
      PrimitiveNode::Make("uuid", Repetition::REQUIRED, UUIDLogicalType::Make(),
                          Type::FIXED_LEN_BYTE_ARRAY, 16);
  NodePtr timestamp_node = PrimitiveNode::Make(
      "timestamp", Repetition::REQUIRED,
      TimestampLogicalType::Make(false, LogicalType::TimeUnit::NANOS), Type::INT64);
  NodePtr int_node = PrimitiveNode::Make("int", Repetition::REQUIRED,
                                         IntLogicalType::Make(64, false), Type::INT64);
  NodePtr decimal_node = PrimitiveNode::Make(
      "decimal", Repetition::REQUIRED, DecimalLogicalType::Make(16, 6), Type::INT64);

  NodePtr list_node = GroupNode::Make("list", Repetition::REPEATED,
                                      {string_node, date_node, json_node, uuid_node,
                                       timestamp_node, int_node, decimal_node},
                                      ListLogicalType::Make());
  std::vector<format::SchemaElement> list_elements;
  ToParquet(reinterpret_cast<GroupNode*>(list_node.get()), &list_elements);
  ASSERT_EQ(list_elements[0].name, "list");
  ASSERT_TRUE(list_elements[0].__isset.converted_type);
  ASSERT_TRUE(list_elements[0].__isset.logicalType);
  ASSERT_EQ(list_elements[0].converted_type, ToThrift(ConvertedType::LIST));
  ASSERT_TRUE(list_elements[0].logicalType.__isset.LIST);
  ASSERT_TRUE(list_elements[1].logicalType.__isset.STRING);
  ASSERT_TRUE(list_elements[2].logicalType.__isset.DATE);
  ASSERT_TRUE(list_elements[3].logicalType.__isset.JSON);
  ASSERT_TRUE(list_elements[4].logicalType.__isset.UUID);
  ASSERT_TRUE(list_elements[5].logicalType.__isset.TIMESTAMP);
  ASSERT_TRUE(list_elements[6].logicalType.__isset.INTEGER);
  ASSERT_TRUE(list_elements[7].logicalType.__isset.DECIMAL);

  NodePtr map_node =
      GroupNode::Make("map", Repetition::REQUIRED, {}, MapLogicalType::Make());
  std::vector<format::SchemaElement> map_elements;
  ToParquet(reinterpret_cast<GroupNode*>(map_node.get()), &map_elements);
  ASSERT_EQ(map_elements[0].name, "map");
  ASSERT_TRUE(map_elements[0].__isset.converted_type);
  ASSERT_TRUE(map_elements[0].__isset.logicalType);
  ASSERT_EQ(map_elements[0].converted_type, ToThrift(ConvertedType::MAP));
  ASSERT_TRUE(map_elements[0].logicalType.__isset.MAP);
}

static void ConfirmPrimitiveNodeRoundtrip(
    const std::shared_ptr<const LogicalType>& logical_type, Type::type physical_type,
    int physical_length) {
  std::shared_ptr<Node> original = PrimitiveNode::Make(
      "something", Repetition::REQUIRED, logical_type, physical_type, physical_length);
  format::SchemaElement intermediary;
  original->ToParquet(&intermediary);
  std::unique_ptr<Node> recovered = PrimitiveNode::FromParquet(&intermediary, 1);
  ASSERT_TRUE(original->Equals(recovered.get()))
      << "Recovered primitive node unexpectedly not equivalent to original primitive "
         "node constructed with logical type "
      << logical_type->ToString();
  return;
}

static void ConfirmGroupNodeRoundtrip(
    std::string name, const std::shared_ptr<const LogicalType>& logical_type) {
  NodeVector node_vector;
  std::shared_ptr<Node> original =
      GroupNode::Make(name, Repetition::REQUIRED, node_vector, logical_type);
  std::vector<format::SchemaElement> elements;
  ToParquet(reinterpret_cast<GroupNode*>(original.get()), &elements);
  std::unique_ptr<Node> recovered =
      GroupNode::FromParquet(&(elements[0]), 1, node_vector);
  ASSERT_TRUE(original->Equals(recovered.get()))
      << "Recovered group node unexpectedly not equivalent to original group node "
         "constructed with logical type "
      << logical_type->ToString();
  return;
}

TEST(TestLogicalTypeSerialization, Roundtrips) {
  // Confirm that Thrift serialization-deserialization of nodes with logical
  // types produces equivalent reconstituted nodes

  // Primitive nodes ...
  struct AnnotatedPrimitiveNodeFactoryArguments {
    std::shared_ptr<const LogicalType> logical_type;
    Type::type physical_type;
    int physical_length;
  };

  std::vector<AnnotatedPrimitiveNodeFactoryArguments> cases = {
      {LogicalType::String(), Type::BYTE_ARRAY, -1},
      {LogicalType::Enum(), Type::BYTE_ARRAY, -1},
      {LogicalType::Decimal(16, 6), Type::INT64, -1},
      {LogicalType::Date(), Type::INT32, -1},
      {LogicalType::Time(true, LogicalType::TimeUnit::MILLIS), Type::INT32, -1},
      {LogicalType::Time(true, LogicalType::TimeUnit::MICROS), Type::INT64, -1},
      {LogicalType::Time(true, LogicalType::TimeUnit::NANOS), Type::INT64, -1},
      {LogicalType::Time(false, LogicalType::TimeUnit::MILLIS), Type::INT32, -1},
      {LogicalType::Time(false, LogicalType::TimeUnit::MICROS), Type::INT64, -1},
      {LogicalType::Time(false, LogicalType::TimeUnit::NANOS), Type::INT64, -1},
      {LogicalType::Timestamp(true, LogicalType::TimeUnit::MILLIS), Type::INT64, -1},
      {LogicalType::Timestamp(true, LogicalType::TimeUnit::MICROS), Type::INT64, -1},
      {LogicalType::Timestamp(true, LogicalType::TimeUnit::NANOS), Type::INT64, -1},
      {LogicalType::Timestamp(false, LogicalType::TimeUnit::MILLIS), Type::INT64, -1},
      {LogicalType::Timestamp(false, LogicalType::TimeUnit::MICROS), Type::INT64, -1},
      {LogicalType::Timestamp(false, LogicalType::TimeUnit::NANOS), Type::INT64, -1},
      {LogicalType::Interval(), Type::FIXED_LEN_BYTE_ARRAY, 12},
      {LogicalType::Int(8, false), Type::INT32, -1},
      {LogicalType::Int(16, false), Type::INT32, -1},
      {LogicalType::Int(32, false), Type::INT32, -1},
      {LogicalType::Int(64, false), Type::INT64, -1},
      {LogicalType::Int(8, true), Type::INT32, -1},
      {LogicalType::Int(16, true), Type::INT32, -1},
      {LogicalType::Int(32, true), Type::INT32, -1},
      {LogicalType::Int(64, true), Type::INT64, -1},
      {LogicalType::Null(), Type::BOOLEAN, -1},
      {LogicalType::JSON(), Type::BYTE_ARRAY, -1},
      {LogicalType::BSON(), Type::BYTE_ARRAY, -1},
      {LogicalType::UUID(), Type::FIXED_LEN_BYTE_ARRAY, 16},
      {LogicalType::None(), Type::BOOLEAN, -1}};

  for (const AnnotatedPrimitiveNodeFactoryArguments& c : cases) {
    ConfirmPrimitiveNodeRoundtrip(c.logical_type, c.physical_type, c.physical_length);
  }

  // Group nodes ...
  ConfirmGroupNodeRoundtrip("map", LogicalType::Map());
  ConfirmGroupNodeRoundtrip("list", LogicalType::List());
}

}  // namespace schema

}  // namespace parquet
