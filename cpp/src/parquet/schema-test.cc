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
#include <iosfwd>
#include <memory>
#include <string>
#include <vector>

#include "parquet/exception.h"
#include "parquet/parquet_types.h"
#include "parquet/schema-internal.h"
#include "parquet/schema.h"
#include "parquet/types.h"

using std::string;
using std::vector;

namespace parquet {

using format::ConvertedType;
using format::FieldRepetitionType;
using format::SchemaElement;

namespace schema {

static inline SchemaElement NewPrimitive(const std::string& name,
    FieldRepetitionType::type repetition, format::Type::type type, int id = 0) {
  SchemaElement result;
  result.__set_name(name);
  result.__set_repetition_type(repetition);
  result.__set_type(type);
  result.__set_num_children(0);

  return result;
}

static inline SchemaElement NewGroup(const std::string& name,
    FieldRepetitionType::type repetition, int num_children, int id = 0) {
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
  SchemaElement elt =
      NewPrimitive(name_, FieldRepetitionType::OPTIONAL, format::Type::INT32, 0);
  Convert(&elt);
  ASSERT_EQ(name_, prim_node_->name());
  ASSERT_EQ(id_, prim_node_->id());
  ASSERT_EQ(Repetition::OPTIONAL, prim_node_->repetition());
  ASSERT_EQ(Type::INT32, prim_node_->physical_type());
  ASSERT_EQ(LogicalType::NONE, prim_node_->logical_type());

  // Test a logical type
  elt = NewPrimitive(name_, FieldRepetitionType::REQUIRED, format::Type::BYTE_ARRAY, 0);
  elt.__set_converted_type(ConvertedType::UTF8);

  Convert(&elt);
  ASSERT_EQ(Repetition::REQUIRED, prim_node_->repetition());
  ASSERT_EQ(Type::BYTE_ARRAY, prim_node_->physical_type());
  ASSERT_EQ(LogicalType::UTF8, prim_node_->logical_type());

  // FIXED_LEN_BYTE_ARRAY
  elt = NewPrimitive(
      name_, FieldRepetitionType::OPTIONAL, format::Type::FIXED_LEN_BYTE_ARRAY, 0);
  elt.__set_type_length(16);

  Convert(&elt);
  ASSERT_EQ(name_, prim_node_->name());
  ASSERT_EQ(id_, prim_node_->id());
  ASSERT_EQ(Repetition::OPTIONAL, prim_node_->repetition());
  ASSERT_EQ(Type::FIXED_LEN_BYTE_ARRAY, prim_node_->physical_type());
  ASSERT_EQ(16, prim_node_->type_length());

  // ConvertedType::Decimal
  elt = NewPrimitive(
      name_, FieldRepetitionType::OPTIONAL, format::Type::FIXED_LEN_BYTE_ARRAY, 0);
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
  ASSERT_NO_THROW(
      PrimitiveNode::Make("foo", Repetition::REQUIRED, Type::INT32, LogicalType::INT_32));
  ASSERT_NO_THROW(PrimitiveNode::Make(
      "foo", Repetition::REQUIRED, Type::BYTE_ARRAY, LogicalType::JSON));
  ASSERT_THROW(
      PrimitiveNode::Make("foo", Repetition::REQUIRED, Type::INT32, LogicalType::JSON),
      ParquetException);
  ASSERT_NO_THROW(PrimitiveNode::Make(
      "foo", Repetition::REQUIRED, Type::INT64, LogicalType::TIMESTAMP_MILLIS));
  ASSERT_THROW(
      PrimitiveNode::Make("foo", Repetition::REQUIRED, Type::INT32, LogicalType::INT_64),
      ParquetException);
  ASSERT_THROW(PrimitiveNode::Make(
                   "foo", Repetition::REQUIRED, Type::BYTE_ARRAY, LogicalType::INT_8),
      ParquetException);
  ASSERT_THROW(PrimitiveNode::Make(
                   "foo", Repetition::REQUIRED, Type::BYTE_ARRAY, LogicalType::INTERVAL),
      ParquetException);
  ASSERT_THROW(PrimitiveNode::Make("foo", Repetition::REQUIRED,
                   Type::FIXED_LEN_BYTE_ARRAY, LogicalType::ENUM),
      ParquetException);
  ASSERT_NO_THROW(PrimitiveNode::Make(
      "foo", Repetition::REQUIRED, Type::BYTE_ARRAY, LogicalType::ENUM));
  ASSERT_THROW(PrimitiveNode::Make("foo", Repetition::REQUIRED,
                   Type::FIXED_LEN_BYTE_ARRAY, LogicalType::DECIMAL, 0, 2, 4),
      ParquetException);
  ASSERT_THROW(PrimitiveNode::Make("foo", Repetition::REQUIRED, Type::FLOAT,
                   LogicalType::DECIMAL, 0, 2, 4),
      ParquetException);
  ASSERT_THROW(PrimitiveNode::Make("foo", Repetition::REQUIRED,
                   Type::FIXED_LEN_BYTE_ARRAY, LogicalType::DECIMAL, 0, 4, 0),
      ParquetException);
  ASSERT_THROW(PrimitiveNode::Make("foo", Repetition::REQUIRED,
                   Type::FIXED_LEN_BYTE_ARRAY, LogicalType::DECIMAL, 10, 0, 4),
      ParquetException);
  ASSERT_THROW(PrimitiveNode::Make("foo", Repetition::REQUIRED,
                   Type::FIXED_LEN_BYTE_ARRAY, LogicalType::DECIMAL, 10, 4, -1),
      ParquetException);
  ASSERT_THROW(PrimitiveNode::Make("foo", Repetition::REQUIRED,
                   Type::FIXED_LEN_BYTE_ARRAY, LogicalType::DECIMAL, 10, 2, 4),
      ParquetException);
  ASSERT_NO_THROW(PrimitiveNode::Make("foo", Repetition::REQUIRED,
      Type::FIXED_LEN_BYTE_ARRAY, LogicalType::DECIMAL, 10, 6, 4));
  ASSERT_NO_THROW(PrimitiveNode::Make("foo", Repetition::REQUIRED,
      Type::FIXED_LEN_BYTE_ARRAY, LogicalType::INTERVAL, 12));
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
    if (field->parent() != node) { return false; }
    if (field->is_group()) {
      const GroupNode* group = static_cast<GroupNode*>(field.get());
      if (!check_for_parent_consistency(group)) { return false; }
    }
  }
  return true;
}

TEST_F(TestSchemaConverter, NestedExample) {
  SchemaElement elt;
  std::vector<SchemaElement> elements;
  elements.push_back(NewGroup(name_, FieldRepetitionType::REPEATED, 2, 0));

  // A primitive one
  elements.push_back(
      NewPrimitive("a", FieldRepetitionType::REQUIRED, format::Type::INT32, 1));

  // A group
  elements.push_back(NewGroup("bag", FieldRepetitionType::OPTIONAL, 1, 2));

  // 3-level list encoding, by hand
  elt = NewGroup("b", FieldRepetitionType::REPEATED, 1, 3);
  elt.__set_converted_type(ConvertedType::LIST);
  elements.push_back(elt);
  elements.push_back(
      NewPrimitive("item", FieldRepetitionType::OPTIONAL, format::Type::INT64, 4));

  Convert(&elements[0], elements.size());

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

TEST_F(TestSchemaConverter, InvalidRoot) {
  // According to the Parquet specification, the first element in the
  // list<SchemaElement> is a group whose children (and their descendants)
  // contain all of the rest of the flattened schema elements. If the first
  // element is not a group, it is a malformed Parquet file.

  SchemaElement elements[2];
  elements[0] =
      NewPrimitive("not-a-group", FieldRepetitionType::REQUIRED, format::Type::INT32, 0);
  ASSERT_THROW(Convert(elements, 2), ParquetException);

  // While the Parquet spec indicates that the root group should have REPEATED
  // repetition type, some implementations may return REQUIRED or OPTIONAL
  // groups as the first element. These tests check that this is okay as a
  // practicality matter.
  elements[0] = NewGroup("not-repeated", FieldRepetitionType::REQUIRED, 1, 0);
  elements[1] = NewPrimitive("a", FieldRepetitionType::REQUIRED, format::Type::INT32, 1);
  Convert(elements, 2);

  elements[0] = NewGroup("not-repeated", FieldRepetitionType::OPTIONAL, 1, 0);
  Convert(elements, 2);
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
  NodePtr node = PrimitiveNode::Make(
      "decimal", Repetition::REQUIRED, Type::INT64, LogicalType::DECIMAL, -1, 8, 4);
  NodePtr group =
      GroupNode::Make("group", Repetition::REPEATED, {node}, LogicalType::LIST);
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
  elements.push_back(
      NewPrimitive("a", FieldRepetitionType::REQUIRED, format::Type::INT32, 1));

  // A group
  elements.push_back(NewGroup("bag", FieldRepetitionType::OPTIONAL, 1, 2));

  // 3-level list encoding, by hand
  elt = NewGroup("b", FieldRepetitionType::REPEATED, 1, 3);
  elt.__set_converted_type(ConvertedType::LIST);
  elements.push_back(elt);
  elements.push_back(
      NewPrimitive("item", FieldRepetitionType::OPTIONAL, format::Type::INT64, 4));

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
  NodePtr node = PrimitiveNode::Make(
      "name", Repetition::OPTIONAL, Type::BYTE_ARRAY, LogicalType::UTF8);
  ColumnDescriptor descr(node, 4, 1);

  ASSERT_EQ("name", descr.name());
  ASSERT_EQ(4, descr.max_definition_level());
  ASSERT_EQ(1, descr.max_repetition_level());

  ASSERT_EQ(Type::BYTE_ARRAY, descr.physical_type());

  ASSERT_EQ(-1, descr.type_length());

  // Test FIXED_LEN_BYTE_ARRAY
  node = PrimitiveNode::Make("name", Repetition::OPTIONAL, Type::FIXED_LEN_BYTE_ARRAY,
      LogicalType::DECIMAL, 12, 10, 4);
  descr = ColumnDescriptor(node, 4, 1);

  ASSERT_EQ(Type::FIXED_LEN_BYTE_ARRAY, descr.physical_type());
  ASSERT_EQ(12, descr.type_length());
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
  NodePtr list(GroupNode::Make(
      "records", Repetition::REPEATED, {item1, item2, item3}, LogicalType::LIST));

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
  NodePtr list(GroupNode::Make(
      "records", Repetition::REPEATED, {item1, item2, item3}, LogicalType::LIST));
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

  ASSERT_EQ(inta.get(), descr_.GetColumnRoot(0).get());
  ASSERT_EQ(bag.get(), descr_.GetColumnRoot(3).get());
  ASSERT_EQ(bag.get(), descr_.GetColumnRoot(4).get());
  ASSERT_EQ(bag.get(), descr_.GetColumnRoot(5).get());

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

  fields.push_back(PrimitiveNode::Make(
      "c", Repetition::REQUIRED, Type::INT32, LogicalType::DECIMAL, -1, 3, 2));

  NodePtr schema = GroupNode::Make("schema", Repetition::REPEATED, fields);

  std::string result = Print(schema);
  std::string expected = R"(message schema {
  required int32 a;
  optional group bag {
    repeated group b (LIST) {
      optional int64 item1;
      required boolean item2;
    }
  }
  required int32 c (DECIMAL(3,2));
}
)";
  ASSERT_EQ(expected, result);
}

}  // namespace schema
}  // namespace parquet
