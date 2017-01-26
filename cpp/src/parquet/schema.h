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

// This module contains the logical parquet-cpp types (independent of Thrift
// structures), schema nodes, and related type tools

#ifndef PARQUET_SCHEMA_TYPES_H
#define PARQUET_SCHEMA_TYPES_H

#include <cstdint>
#include <memory>
#include <ostream>
#include <string>
#include <unordered_map>
#include <vector>

#include "parquet/types.h"
#include "parquet/util/macros.h"
#include "parquet/util/visibility.h"

namespace parquet {

class SchemaDescriptor;

namespace schema {

// List encodings: using the terminology from Impala to define different styles
// of representing logical lists (a.k.a. ARRAY types) in Parquet schemas. Since
// the converted type named in the Parquet metadata is ConvertedType::LIST we
// use that terminology here. It also helps distinguish from the *_ARRAY
// primitive types.
//
// One-level encoding: Only allows required lists with required cells
//   repeated value_type name
//
// Two-level encoding: Enables optional lists with only required cells
//   <required/optional> group list
//     repeated value_type item
//
// Three-level encoding: Enables optional lists with optional cells
//   <required/optional> group bag
//     repeated group list
//       <required/optional> value_type item
//
// 2- and 1-level encoding are respectively equivalent to 3-level encoding with
// the non-repeated nodes set to required.
//
// The "official" encoding recommended in the Parquet spec is the 3-level, and
// we use that as the default when creating list types. For semantic completeness
// we allow the other two. Since all types of encodings will occur "in the
// wild" we need to be able to interpret the associated definition levels in
// the context of the actual encoding used in the file.
//
// NB: Some Parquet writers may not set ConvertedType::LIST on the repeated
// SchemaElement, which could make things challenging if we are trying to infer
// that a sequence of nodes semantically represents an array according to one
// of these encodings (versus a struct containing an array). We should refuse
// the temptation to guess, as they say.
struct ListEncoding {
  enum type { ONE_LEVEL, TWO_LEVEL, THREE_LEVEL };
};

struct DecimalMetadata {
  bool isset;
  int32_t scale;
  int32_t precision;
};

class PARQUET_EXPORT ColumnPath {
 public:
  ColumnPath() : path_() {}
  explicit ColumnPath(const std::vector<std::string>& path) : path_(path) {}
  explicit ColumnPath(std::vector<std::string>&& path) : path_(path) {}

  static std::shared_ptr<ColumnPath> FromDotString(const std::string& dotstring);

  std::shared_ptr<ColumnPath> extend(const std::string& node_name) const;
  std::string ToDotString() const;
  const std::vector<std::string>& ToDotVector() const;

 protected:
  std::vector<std::string> path_;
};

class GroupNode;

// Base class for logical schema types. A type has a name, repetition level,
// and optionally a logical type (ConvertedType in Parquet metadata parlance)
class PARQUET_EXPORT Node {
 public:
  enum type { PRIMITIVE, GROUP };

  Node(Node::type type, const std::string& name, Repetition::type repetition,
      LogicalType::type logical_type = LogicalType::NONE, int id = -1)
      : type_(type),
        name_(name),
        repetition_(repetition),
        logical_type_(logical_type),
        id_(id),
        parent_(nullptr) {}

  virtual ~Node() {}

  bool is_primitive() const { return type_ == Node::PRIMITIVE; }

  bool is_group() const { return type_ == Node::GROUP; }

  bool is_optional() const { return repetition_ == Repetition::OPTIONAL; }

  bool is_repeated() const { return repetition_ == Repetition::REPEATED; }

  bool is_required() const { return repetition_ == Repetition::REQUIRED; }

  virtual bool Equals(const Node* other) const = 0;

  const std::string& name() const { return name_; }

  Node::type node_type() const { return type_; }

  Repetition::type repetition() const { return repetition_; }

  LogicalType::type logical_type() const { return logical_type_; }

  int id() const { return id_; }

  const Node* parent() const { return parent_; }

  // ToParquet returns an opaque void* to avoid exporting
  // parquet::SchemaElement into the public API
  virtual void ToParquet(void* opaque_element) const = 0;

  // Node::Visitor abstract class for walking schemas with the visitor pattern
  class Visitor {
   public:
    virtual ~Visitor() {}

    virtual void Visit(Node* node) = 0;
  };
  class ConstVisitor {
   public:
    virtual ~ConstVisitor() {}

    virtual void Visit(const Node* node) = 0;
  };

  virtual void Visit(Visitor* visitor) = 0;
  virtual void VisitConst(ConstVisitor* visitor) const = 0;

 protected:
  friend class GroupNode;

  Node::type type_;
  std::string name_;
  Repetition::type repetition_;
  LogicalType::type logical_type_;
  int id_;
  // Nodes should not be shared, they have a single parent.
  const Node* parent_;

  bool EqualsInternal(const Node* other) const;
  void SetParent(const Node* p_parent);
};

// Save our breath all over the place with these typedefs
typedef std::shared_ptr<Node> NodePtr;
typedef std::vector<NodePtr> NodeVector;

// A type that is one of the primitive Parquet storage types. In addition to
// the other type metadata (name, repetition level, logical type), also has the
// physical storage type and their type-specific metadata (byte width, decimal
// parameters)
class PARQUET_EXPORT PrimitiveNode : public Node {
 public:
  // FromParquet accepts an opaque void* to avoid exporting
  // parquet::SchemaElement into the public API
  static std::unique_ptr<Node> FromParquet(const void* opaque_element, int id);

  static inline NodePtr Make(const std::string& name, Repetition::type repetition,
      Type::type type, LogicalType::type logical_type = LogicalType::NONE,
      int length = -1, int precision = -1, int scale = -1) {
    return NodePtr(new PrimitiveNode(
        name, repetition, type, logical_type, length, precision, scale));
  }

  bool Equals(const Node* other) const override;

  Type::type physical_type() const { return physical_type_; }

  int32_t type_length() const { return type_length_; }

  const DecimalMetadata& decimal_metadata() const { return decimal_metadata_; }

  void ToParquet(void* opaque_element) const override;
  void Visit(Visitor* visitor) override;
  void VisitConst(ConstVisitor* visitor) const override;

 private:
  PrimitiveNode(const std::string& name, Repetition::type repetition, Type::type type,
      LogicalType::type logical_type = LogicalType::NONE, int length = -1,
      int precision = -1, int scale = -1, int id = -1);

  Type::type physical_type_;
  int32_t type_length_;
  DecimalMetadata decimal_metadata_;

  // For FIXED_LEN_BYTE_ARRAY
  void SetTypeLength(int32_t length) { type_length_ = length; }

  // For Decimal logical type: Precision and scale
  void SetDecimalMetadata(int32_t scale, int32_t precision) {
    decimal_metadata_.scale = scale;
    decimal_metadata_.precision = precision;
  }

  bool EqualsInternal(const PrimitiveNode* other) const;

  FRIEND_TEST(TestPrimitiveNode, Attrs);
  FRIEND_TEST(TestPrimitiveNode, Equals);
  FRIEND_TEST(TestPrimitiveNode, PhysicalLogicalMapping);
  FRIEND_TEST(TestPrimitiveNode, FromParquet);
};

class PARQUET_EXPORT GroupNode : public Node {
 public:
  // Like PrimitiveNode, GroupNode::FromParquet accepts an opaque void* to avoid exporting
  // parquet::SchemaElement into the public API
  static std::unique_ptr<Node> FromParquet(
      const void* opaque_element, int id, const NodeVector& fields);

  static inline NodePtr Make(const std::string& name, Repetition::type repetition,
      const NodeVector& fields, LogicalType::type logical_type = LogicalType::NONE) {
    return NodePtr(new GroupNode(name, repetition, fields, logical_type));
  }

  bool Equals(const Node* other) const override;

  const NodePtr& field(int i) const { return fields_[i]; }

  int field_count() const { return fields_.size(); }

  void ToParquet(void* opaque_element) const override;
  void Visit(Visitor* visitor) override;
  void VisitConst(ConstVisitor* visitor) const override;

 private:
  GroupNode(const std::string& name, Repetition::type repetition,
      const NodeVector& fields, LogicalType::type logical_type = LogicalType::NONE,
      int id = -1)
      : Node(Node::GROUP, name, repetition, logical_type, id), fields_(fields) {
    for (NodePtr& field : fields_) {
      field->SetParent(this);
    }
  }

  NodeVector fields_;
  bool EqualsInternal(const GroupNode* other) const;

  FRIEND_TEST(TestGroupNode, Attrs);
  FRIEND_TEST(TestGroupNode, Equals);
};

// ----------------------------------------------------------------------
// Convenience primitive type factory functions

#define PRIMITIVE_FACTORY(FuncName, TYPE)                                            \
  static inline NodePtr FuncName(                                                    \
      const std::string& name, Repetition::type repetition = Repetition::OPTIONAL) { \
    return PrimitiveNode::Make(name, repetition, Type::TYPE);                        \
  }

PRIMITIVE_FACTORY(Boolean, BOOLEAN);
PRIMITIVE_FACTORY(Int32, INT32);
PRIMITIVE_FACTORY(Int64, INT64);
PRIMITIVE_FACTORY(Int96, INT96);
PRIMITIVE_FACTORY(Float, FLOAT);
PRIMITIVE_FACTORY(Double, DOUBLE);
PRIMITIVE_FACTORY(ByteArray, BYTE_ARRAY);

void PARQUET_EXPORT PrintSchema(
    const schema::Node* schema, std::ostream& stream, int indent_width = 2);

}  // namespace schema

// The ColumnDescriptor encapsulates information necessary to interpret
// primitive column data in the context of a particular schema. We have to
// examine the node structure of a column's path to the root in the schema tree
// to be able to reassemble the nested structure from the repetition and
// definition levels.
class PARQUET_EXPORT ColumnDescriptor {
 public:
  ColumnDescriptor(const schema::NodePtr& node, int16_t max_definition_level,
      int16_t max_repetition_level, const SchemaDescriptor* schema_descr = nullptr);

  bool Equals(const ColumnDescriptor& other) const;

  int16_t max_definition_level() const { return max_definition_level_; }

  int16_t max_repetition_level() const { return max_repetition_level_; }

  Type::type physical_type() const { return primitive_node_->physical_type(); }

  LogicalType::type logical_type() const { return primitive_node_->logical_type(); }

  const std::string& name() const { return primitive_node_->name(); }

  const std::shared_ptr<schema::ColumnPath> path() const;

  const schema::NodePtr& schema_node() const { return node_; }

  int type_length() const;

  int type_precision() const;

  int type_scale() const;

 private:
  schema::NodePtr node_;
  const schema::PrimitiveNode* primitive_node_;

  int16_t max_definition_level_;
  int16_t max_repetition_level_;

  // When this descriptor is part of a real schema (and not being used for
  // testing purposes), maintain a link back to the parent SchemaDescriptor to
  // enable reverse graph traversals
  const SchemaDescriptor* schema_descr_;
};

// Container for the converted Parquet schema with a computed information from
// the schema analysis needed for file reading
//
// * Column index to Node
// * Max repetition / definition levels for each primitive node
//
// The ColumnDescriptor objects produced by this class can be used to assist in
// the reconstruction of fully materialized data structures from the
// repetition-definition level encoding of nested data
//
// TODO(wesm): this object can be recomputed from a Schema
class PARQUET_EXPORT SchemaDescriptor {
 public:
  SchemaDescriptor() {}
  ~SchemaDescriptor() {}

  // Analyze the schema
  void Init(std::unique_ptr<schema::Node> schema);
  void Init(const schema::NodePtr& schema);

  const ColumnDescriptor* Column(int i) const;

  bool Equals(const SchemaDescriptor& other) const;

  // The number of physical columns appearing in the file
  int num_columns() const { return leaves_.size(); }

  const schema::NodePtr& schema_root() const { return schema_; }

  const schema::GroupNode* group_node() const { return group_node_; }

  // Returns the root (child of the schema root) node of the leaf(column) node
  const schema::NodePtr& GetColumnRoot(int i) const;

  const std::string& name() const { return group_node_->name(); }

 private:
  friend class ColumnDescriptor;

  schema::NodePtr schema_;
  const schema::GroupNode* group_node_;

  void BuildTree(const schema::NodePtr& node, int16_t max_def_level,
      int16_t max_rep_level, const schema::NodePtr& base);

  // Result of leaf node / tree analysis
  std::vector<ColumnDescriptor> leaves_;

  // Mapping between leaf nodes and root group of leaf (first node
  // below the schema's root group)
  //
  // For example, the leaf `a.b.c.d` would have a link back to `a`
  //
  // -- a  <------
  // -- -- b     |
  // -- -- -- c  |
  // -- -- -- -- d
  std::unordered_map<int, const schema::NodePtr> leaf_to_base_;
};

}  // namespace parquet

#endif  // PARQUET_SCHEMA_TYPES_H
