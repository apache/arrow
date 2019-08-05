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

#include "arrow/colfmt/api.h"
#include "arrow/array/builder_primitive.h"
#include "arrow/array/builder_nested.h"
#include "arrow/array/builder_binary.h"
#include "arrow/util/checked_cast.h"
#include "arrow/util/stl.h"
#include <iostream>

using arrow::internal::checked_cast;
using arrow::internal::make_unique;

// ColumnMap implementation
namespace {

int FindColumn(const std::vector<arrow::ColumnMap::Column>& columns,
               const std::shared_ptr<arrow::Field>& field) {
  // First, binary search is used (equality is by pointer value).
  // This helps to locate elements fast when the objects from the schema are queried for.
  int low = 0;
  int high = static_cast<int>(columns.size()) - 1;
  while (low <= high) {
    int mid = (low + high) / 2;
    if (columns[mid].field.get() < field.get()) {
      low = mid + 1;
    } else if (columns[mid].field.get() > field.get()) {
      high = mid - 1;
    } else {
      return mid;
    }
  }

  // If desired element is not found, linear search is attempted
  // by using Field::Equals(). This is more expensive.
  for (int i = 0; i < static_cast<int>(columns.size()); i++) {
    if (columns[i].field->Equals(*field)) {
      return i;
    }
  }

  return -low-1;
}

} // anonymous namespace

namespace arrow {

void ColumnMap::Put(const Column& column) {
  int i = FindColumn(columns_, column.field);
  if (i < 0) {
    i = -i-1;
    columns_.insert(columns_.begin() + i, Column());
  }
  columns_[i] = column;
}

void ColumnMap::Put(const std::shared_ptr<Field>& field,
                    const std::shared_ptr<Int16Array>& rep_levels,
                    const std::shared_ptr<Int16Array>& def_levels,
                    const std::shared_ptr<Array>& values) {
  Column column;
  column.field = field;
  column.rep_levels = rep_levels;
  column.def_levels = def_levels;
  column.values = values;

  Put(column);
}

Result<ColumnMap::Column> ColumnMap::Find(const std::shared_ptr<Field>& field) const {
  int i = FindColumn(columns_, field);
  if (i >= 0) {
    return Result<Column>(columns_[i]);
  } else {
    return Status::Invalid("Column not found");
  }
}

Result<ColumnMap::Column> ColumnMap::Get(int i) const {
  if (i >= 0 && i < size()) {
    return Result<Column>(columns_[i]);
  } else {
    return Status::IndexError("Column index out of range");
  }
}

// Utility functions and classes
namespace {

template<typename data_type>
struct supports_basic_append {
  static constexpr bool value =
    has_c_type<data_type>::value ||
    std::is_same<data_type, BooleanType>::value ||
    std::is_base_of<FixedSizeBinaryType, data_type>::value;
};

/// Copy single value from array to builder, generic version
template<typename data_type>
typename std::enable_if<supports_basic_append<data_type>::value, Status>::type
CopyValue(const typename TypeTraits<data_type>::ArrayType& array, int64_t i,
          typename TypeTraits<data_type>::BuilderType* builder) {
  return builder->Append(array.Value(i));
}


/// Copy single value from array to builder, specialization for BinaryType
template<typename data_type>
typename std::enable_if<std::is_base_of<BinaryType, data_type>::value, Status>::type
CopyValue(const typename TypeTraits<data_type>::ArrayType& array, int64_t i,
          typename TypeTraits<data_type>::BuilderType* builder) {
  int32_t length;
  const uint8_t* value = array.GetValue(i, &length);
  return builder->Append(value, length);
}


#define PRIMITIVE_CASE_LIST(CASE) \
  CASE(TimestampType) \
  CASE(BooleanType) \
  CASE(Int8Type) \
  CASE(UInt8Type) \
  CASE(Int16Type) \
  CASE(UInt16Type) \
  CASE(Int32Type) \
  CASE(UInt32Type) \
  CASE(Int64Type) \
  CASE(UInt64Type) \
  CASE(FloatType) \
  CASE(DoubleType) \
  CASE(BinaryType) \
  CASE(StringType) \
  CASE(FixedSizeBinaryType) \
  CASE(Date32Type) \
  CASE(Date64Type) \
  CASE(Time32Type) \
  CASE(Time64Type)

} // anonymous namespace


// Shredder implementation
namespace {

class ShredNode {
 public:
  ShredNode(const std::shared_ptr<Field>& field, MemoryPool* pool)
      : field_(field),
        rep_levels_builder_(std::make_shared<Int16Builder>(pool)),
        def_levels_builder_(std::make_shared<Int16Builder>(pool)) {
  }
  virtual ~ShredNode() {
  }

  static Result<std::unique_ptr<ShredNode>>
  CreateTree(const std::shared_ptr<Field>& field,
             int16_t rep_level, int16_t def_level,
             MemoryPool* pool);

  const std::shared_ptr<Field>& field() const { return field_; }
  int num_children() const { return static_cast<int>(children_.size()); }
  ShredNode* child(int i) const { return children_[i].get(); }

  virtual Status Handle(const Array& array, int64_t i, int16_t rep_level) = 0;
  virtual Status Finish(ColumnMap* colmap) = 0;

  Status FinishRecursive(ColumnMap* colmap) {
    RETURN_NOT_OK(Finish(colmap));
    for (int i = 0; i < num_children(); i++) {
      RETURN_NOT_OK(child(i)->FinishRecursive(colmap));
    }
    return Status::OK();
  }

  // Append given pair of levels recursively to this node and all nodes below
  Status AppendLevelsRecursive(int16_t rep_level, int16_t def_level) {
    RETURN_NOT_OK(rep_levels_builder_->Append(rep_level));
    RETURN_NOT_OK(def_levels_builder_->Append(def_level));
    for (int i = 0; i < num_children(); i++) {
      RETURN_NOT_OK(child(i)->AppendLevelsRecursive(rep_level, def_level));
    }
    return Status::OK();
  }


  std::shared_ptr<Field> field_;
  // Repetition level starts with 0 in root element and increases by one in
  // every list _element_ node.
  int16_t rep_level_{-1};
  // Minimal definition level at which this node is not NULL and not empty (if node
  // is a list).
  int16_t def_level_{-1};
  std::shared_ptr<Int16Builder> rep_levels_builder_;
  std::shared_ptr<Int16Builder> def_levels_builder_;
  std::vector<std::unique_ptr<ShredNode>> children_;
};


template<typename data_type>
class PrimitiveShredNode : public ShredNode {
 public:
  using PrimitiveBuilder = typename TypeTraits<data_type>::BuilderType;
  using PrimitiveArray = typename TypeTraits<data_type>::ArrayType;

  using ShredNode::ShredNode;

  static Result<std::unique_ptr<ShredNode>>
  Create(const std::shared_ptr<Field>& field,
         int16_t rep_level, int16_t def_level,
         MemoryPool* pool) {
    auto node = make_unique<PrimitiveShredNode<data_type>>(field, pool);
    node->rep_level_ = rep_level;
    if (field->nullable()) {
      def_level++;
    }
    node->def_level_ = def_level;
    node->values_builder_ = std::make_shared<PrimitiveBuilder>(field->type(), pool);
    return Result<std::unique_ptr<ShredNode>>(std::move(node));
  }

  Status Handle(const Array& array, int64_t i, int16_t rep_level) override {
    return Handle(checked_cast<const PrimitiveArray&>(array), i, rep_level);
  }

  Status Handle(const PrimitiveArray& array, int64_t i, int16_t rep_level) {
    RETURN_NOT_OK(rep_levels_builder_->Append(rep_level));
    if (array.IsNull(i)) {
      RETURN_NOT_OK(def_levels_builder_->Append(int16_t(def_level_ - 1)));
    } else {
      RETURN_NOT_OK(def_levels_builder_->Append(def_level_));
      RETURN_NOT_OK(CopyValue<data_type>(array, i, values_builder_.get()));
    }
    return Status::OK();
  }

  Status Finish(ColumnMap* colmap) override {
    std::shared_ptr<Array> rep_levels_tmp;
    std::shared_ptr<Array> def_levels_tmp;
    std::shared_ptr<Array> tmp_values;
    RETURN_NOT_OK(rep_levels_builder_->Finish(&rep_levels_tmp));
    RETURN_NOT_OK(def_levels_builder_->Finish(&def_levels_tmp));
    RETURN_NOT_OK(values_builder_->Finish(&tmp_values));

    ColumnMap::Column column;
    column.field = field_;
    column.rep_levels = std::static_pointer_cast<Int16Array>(rep_levels_tmp);
    column.def_levels = std::static_pointer_cast<Int16Array>(def_levels_tmp);
    column.values = tmp_values;

    colmap->Put(column);

    return Status::OK();
  }

  std::shared_ptr<PrimitiveBuilder> values_builder_;
};

class StructShredNode : public ShredNode {
 public:
  using ShredNode::ShredNode;

  static Result<std::unique_ptr<ShredNode>>
  Create(const std::shared_ptr<Field>& field,
         int16_t rep_level, int16_t def_level,
         MemoryPool* pool) {
    auto node = make_unique<StructShredNode>(field, pool);
    node->rep_level_ = rep_level;
    if (field->nullable()) {
      def_level++;
    }
    node->def_level_ = def_level;
    node->children_.resize(field->type()->num_children());
    for (int i = 0; i < field->type()->num_children(); i++) {
      Result<std::unique_ptr<ShredNode>> child =
        CreateTree(field->type()->child(i), rep_level, def_level, pool);
      RETURN_NOT_OK(child.status());
      node->children_[i] = std::move(child).ValueOrDie();
    }
    return Result<std::unique_ptr<ShredNode>>(std::move(node));
  }

  Status Handle(const Array& array, int64_t i, int16_t rep_level) override {
    return Handle(checked_cast<const StructArray&>(array), i, rep_level);
  }

  Status Handle(const StructArray& array, int64_t i, int16_t rep_level) {
    if (array.IsNull(i)) {
      RETURN_NOT_OK(AppendLevelsRecursive(rep_level, int16_t(def_level_ - 1)));
    } else {
      RETURN_NOT_OK(rep_levels_builder_->Append(rep_level));
      RETURN_NOT_OK(def_levels_builder_->Append(def_level_));
      for (int j = 0; j < num_children(); j++) {
        RETURN_NOT_OK(children_[j]->Handle(*array.field(j), i, rep_level));
      }
    }
    return Status::OK();
  }

  Status Finish(ColumnMap* colmap) override {
    std::shared_ptr<Array> rep_levels_tmp;
    std::shared_ptr<Array> def_levels_tmp;
    RETURN_NOT_OK(rep_levels_builder_->Finish(&rep_levels_tmp));
    RETURN_NOT_OK(def_levels_builder_->Finish(&def_levels_tmp));

    ColumnMap::Column column;
    column.field = field_;
    column.rep_levels = std::static_pointer_cast<Int16Array>(rep_levels_tmp);
    column.def_levels = std::static_pointer_cast<Int16Array>(def_levels_tmp);

    colmap->Put(column);

    return Status::OK();
  }
};

class ListShredNode : public ShredNode {
 public:
  using ShredNode::ShredNode;

  static Result<std::unique_ptr<ShredNode>>
  Create(const std::shared_ptr<Field>& field,
         int16_t rep_level, int16_t def_level,
         MemoryPool* pool) {
    if (field->type()->num_children() != 1) {
      return Status::Invalid("List fields must have exactly one child: ", field->name());
    }
    auto node = make_unique<ListShredNode>(field, pool);
    node->rep_level_ = ++rep_level;
    if (field->nullable()) {
      def_level++;
    }
    def_level++; // empty list
    node->def_level_ = def_level;

    Result<std::unique_ptr<ShredNode>> child =
      CreateTree(field->type()->child(0), rep_level, def_level, pool);
    RETURN_NOT_OK(child.status());
    node->children_.push_back(std::move(child).ValueOrDie());
    return Result<std::unique_ptr<ShredNode>>(std::move(node));
  }

  Status Handle(const Array& array, int64_t i, int16_t rep_level) override {
    return Handle(checked_cast<const ListArray&>(array), i, rep_level);
  }

  Status Handle(const ListArray& array, int64_t i, int16_t rep_level) {
    if (array.IsNull(i)) {
      RETURN_NOT_OK(AppendLevelsRecursive(rep_level, int16_t(def_level_ - 2)));
    } else if (array.value_length(i) == 0) {
      RETURN_NOT_OK(AppendLevelsRecursive(rep_level, int16_t(def_level_ - 1)));
    } else {
      RETURN_NOT_OK(rep_levels_builder_->Append(rep_level));
      RETURN_NOT_OK(def_levels_builder_->Append(def_level_));
      int32_t offset = array.value_offset(i);
      RETURN_NOT_OK(child(0)->Handle(*array.values(), offset, rep_level));
      for (int64_t j = 1; j < array.value_length(i); j++) {
        RETURN_NOT_OK(child(0)->Handle(*array.values(), offset + j, rep_level_));
      }
    }
    return Status::OK();
  }

  // only levels
  Status Finish(ColumnMap* colmap) override {
    std::shared_ptr<Array> rep_levels_tmp;
    std::shared_ptr<Array> def_levels_tmp;
    RETURN_NOT_OK(rep_levels_builder_->Finish(&rep_levels_tmp));
    RETURN_NOT_OK(def_levels_builder_->Finish(&def_levels_tmp));

    ColumnMap::Column column;
    column.field = field_;
    column.rep_levels = std::static_pointer_cast<Int16Array>(rep_levels_tmp);
    column.def_levels = std::static_pointer_cast<Int16Array>(def_levels_tmp);

    colmap->Put(column);

    return Status::OK();
  }
};

Result<std::unique_ptr<ShredNode>> ShredNode::CreateTree(const std::shared_ptr<Field>& field,
                                                         int16_t rep_level, int16_t def_level,
                                                         MemoryPool* pool) {
  switch (field->type()->id()) {
    case Type::STRUCT:
      return StructShredNode::Create(field, rep_level, def_level, pool);
    case Type::LIST:
      return ListShredNode::Create(field, rep_level, def_level, pool);
#define CASE(data_type) \
    case data_type::type_id: \
      return PrimitiveShredNode<data_type>::Create(field, rep_level, def_level, pool);
    PRIMITIVE_CASE_LIST(CASE)
#undef CASE
    default:
      return Status::Invalid("Unsupported type: ", field->type()->name());
  }
}


} // anonymous namespace

class Shredder::Impl {
 public:
  Impl(std::unique_ptr<ShredNode> root) : root_(std::move(root)) {}

  std::unique_ptr<ShredNode> root_;
};

Result<std::shared_ptr<Shredder>> Shredder::Create(const std::shared_ptr<Field>& schema,
                                                   MemoryPool* pool) {
  Result<std::unique_ptr<ShredNode>> root =
    ShredNode::CreateTree(schema, 0, 0, pool);
  RETURN_NOT_OK(root.status());

  std::unique_ptr<Impl> impl =
    make_unique<Impl>(std::move(root).ValueOrDie());

  std::shared_ptr<Shredder> shredder(new Shredder(std::move(impl)));

  return Result<std::shared_ptr<Shredder>>(shredder);
}

Shredder::Shredder(std::unique_ptr<Impl> impl) : impl_(std::move(impl)) {
}

Shredder::~Shredder() {
}

Status Shredder::Shred(const Array& array) {
  if (!array.type()->Equals(impl_->root_->field()->type())) {
    return Status::Invalid("Array schema doesn't match shredder schema");
  }
  for (int64_t i = 0; i < array.length(); i++) {
    RETURN_NOT_OK(impl_->root_->Handle(array, i, 0));
  }
  return Status::OK();
}

Status Shredder::Shred(const ChunkedArray& array) {
  for (int i = 0; i < array.num_chunks(); i++) {
    RETURN_NOT_OK(Shred(*array.chunk(i)));
  }
  return Status::OK();
}

Result<ColumnMap> Shredder::Finish() {
  ColumnMap colmap;
  RETURN_NOT_OK(impl_->root_->FinishRecursive(&colmap));
  return Result<ColumnMap>(colmap);
}

const std::shared_ptr<Field>& Shredder::schema() const {
  return impl_->root_->field();
}


// Stitcher implementation
namespace {

// Stitching algorithm transitions through a series of states.
//
// Every state is given by pair of nodes (base, leaf), where
// "leaf" is the left-most leaf of the subtree starting in "base".
// At every state, we go down from "base" node to "leaf" node
// and append elements on the way.
// This process is controlled by the next definition level read from
// def_levels array of the "leaf" node.
// If at some point def_level instructs us to append NULL (or empty list), we stop.
// If we reach "leaf" node, we consume and append value.
//
// Next we look at the next repetition level in rep_levels array of the leaf node
// and use prebuilt FSM tables to perform transition to the next state (base1, leaf1).
//
// The process is terminated when level arrays become exhausted.
//
// Below example demonstrates schema and possible sequence of states.
// Nodes with [] are lists, other non-leaf nodes are structs.
//
//         A
//    _____|____
//   |     |    |
//   B     C[]  D
//  _|_    |    |
// |   |   G    H[]
// E   F  _|_   |
//       |   |  L
//       I   K
//
// 1. (A,B,E) -- A is "base" node, "E" is leaf node
// 2. (F)     -- F is both "base" and leaf node
// 3. (C,G,I)
// 4. (K)
// 5. (G, I) -- second element in C[]
// 6. (K)
// 7. (G, I) -- third element in C[]
// 8. (K)
// 9. (D, H, L)
// 10. (L) -- second element in H[]
// 11. (L) -- third element in H[]

class StitchNode {
 public:
  StitchNode(const std::shared_ptr<Field>& field)
      : field_(field) {
  }
  virtual ~StitchNode() {}

  static Result<std::unique_ptr<StitchNode>>
  CreateTree(std::shared_ptr<Field> field,
             int16_t rep_level, int16_t def_level,
             MemoryPool* pool);

  const std::shared_ptr<Field>& field() const { return field_; }
  int num_children() const { return static_cast<int>(children_.size()); }
  StitchNode* child(int i) { return children_[i].get(); }

  StitchNode* left_most_leaf() {
    StitchNode* node = this;
    while (node->num_children() > 0) {
      node = node->child(0);
    }
    return node;
  }

  /// Return top-most ancestor with the given repetition level
  StitchNode* ancestor(int16_t rep_level) {
    StitchNode* node = this;
    while (node->parent_ && node->parent_->rep_level_ >= rep_level) {
      node = node->parent_;
    }
    return node;
  }

  int child_position(StitchNode* immediate_child) {
    for (int i = 0; i < num_children(); i++) {
      if (child(i) == immediate_child) {
        return i;
      }
    }
    return -1;
  }

  // Given that current node is the most recently processed leaf node,
  // find next "base" node.
  StitchNode* next_base() {
    StitchNode* node = this;
    while (node->parent_) {
      int pos = node->parent_->child_position(node);
      if (pos + 1 < node->parent_->num_children()) {
        return node->parent_->child(pos + 1);
      } else {
        node = node->parent_;
      }
    }
    return nullptr;
  }

  bool has_more_levels() const {
    return def_levels_ != NULL && levels_pos_ < def_levels_->length();
  }

  virtual bool has_more_values() const = 0;

  // Build FSM tables for all leaf nodes
  void BuildTransitionTablesRecursive() {
    if (num_children() > 0) {
      for (int i = 0; i < num_children(); i++) {
        child(i)->BuildTransitionTablesRecursive();
      }
    } else {
      // we have leaf node
      replevel2base_.resize(rep_level_ + 1);
      replevel2leaf_.resize(rep_level_ + 1);
      StitchNode* base = next_base();
      if (!base) {
        // last field; make tree root new base
        base = ancestor(0);
      }
      StitchNode* next = base->left_most_leaf();
      for (int16_t rl = 0; rl <= rep_level_; rl++) {
        // Actually, we need to compare rl with the repetition level of the lowest
        // common ancestor of the current leaf node and the next leaf node in a schema.
        // But since base node is an immediate child of the lowest common ancestor,
        // they both have identical repetition levels.
        if (rl <= base->rep_level_) {
          replevel2base_[rl] = base;
          replevel2leaf_[rl] = next;
        } else {
          replevel2base_[rl] = ancestor(rl);
          replevel2leaf_[rl] = replevel2base_[rl]->left_most_leaf();
        }
      }
    }
  }

  virtual std::shared_ptr<ArrayBuilder> builder() = 0;

  // Appends element (NULL, empty list, value) to current builder depending on def_level
  virtual Status Append(int16_t def_level, bool* godown) = 0;

  // Given that current node is base node, and also given def_level of the left-most leaf node,
  // go along the path from base node to leaf node and Append() elements
  // as required by def_level.
  Status AppendBranch(int16_t def_level) {
    StitchNode* node = this;
    while (true) {
      bool godown;
      RETURN_NOT_OK(node->Append(def_level, &godown));
      if (!godown) {
        break;
      }
      node = node->child(0);
    }

    return Status::OK();
  }

  // After processing current leaf node, peek its next rep_level and
  // make transition to next state [base .. leaf]
  Status NextState(StitchNode** base, StitchNode** leaf) {
    if (levels_pos_ < rep_levels_->length()) {
      int16_t rep_level = rep_levels_->Value(levels_pos_);
      if (rep_level >= (int)replevel2base_.size()) {
        return Status::Invalid("Invalid repetition level");
      }
      *base = replevel2base_[rep_level];
      *leaf = replevel2leaf_[rep_level];
    } else {
      // this will move us to the next leaf in schema
      *base = replevel2base_[0];
      *leaf = replevel2leaf_[0];
    }
    return Status::OK();
  }

  // Set input levels and values arrays for all leaf fields
  Status SetColumnDataRecursive(const ColumnMap& colmap) {
    if (num_children() > 0) {
      for (int i = 0; i < num_children(); i++) {
        RETURN_NOT_OK(child(i)->SetColumnDataRecursive(colmap));
      }
    } else {
      Result<ColumnMap::Column> column = colmap.Find(field_);
      if (!column.ok()) {
        return Status::Invalid("No data for field ", field_->name());
      }
      RETURN_NOT_OK(SetLevels(column.ValueOrDie().rep_levels,
                              column.ValueOrDie().def_levels));
      RETURN_NOT_OK(SetValues(column.ValueOrDie().values));
    }
    return Status::OK();
  }

  // Check and set levels arrays for input
  Status SetLevels(const std::shared_ptr<Int16Array>& rep_levels,
                   const std::shared_ptr<Int16Array>& def_levels) {
    if (rep_levels == nullptr) {
      return Status::Invalid("No repetition levels for field ", field_->name());
    }
    if (def_levels == nullptr) {
      return Status::Invalid("No definition levels for field ", field_->name());
    }
    if (rep_levels->length() != def_levels->length()) {
      return Status::Invalid("Different number of repetition vs definition levels, field ",
                             field_->name());
    }
    for (int64_t i = 0; i < rep_levels->length(); i++) {
      if (rep_levels->Value(i) < 0 || rep_levels->Value(i) > rep_level_) {
        return Status::Invalid("Invalid repetition level, field ", field_->name());
      }
      if (def_levels->Value(i) < 0 || def_levels->Value(i) > def_level_) {
        return Status::Invalid("Invalid definition level, field ", field_->name());
      }
    }
    this->rep_levels_ = rep_levels;
    this->def_levels_ = def_levels;
    this->levels_pos_ = 0;
    return Status::OK();
  }

  // Check and set values array for input
  virtual Status SetValues(const std::shared_ptr<Array>& values) = 0;

  // Check that all input data was consumed
  Status CheckConsumedRecursive() {
    if (num_children() > 0) {
      for (int i = 0; i < num_children(); i++) {
        RETURN_NOT_OK(child(i)->CheckConsumedRecursive());
      }
    } else {
      if (has_more_levels()) {
        return Status::Invalid("Not all levels were consumed, field ",
                               field_->name());
      }
      if (has_more_values()) {
        return Status::Invalid("Not all values were consumed, field ",
                               field_->name());
      }
    }
    return Status::OK();
  }

  void DebugPrint(std::ostream& out, int indent = 0) {
    out << std::string(indent, ' ') << field_->name() << ' ' << field_->type()->name();
    out << " rep:" << rep_level_;
    out << " def:" << def_level_;
    if (num_children() == 0) {
      out << ' ';
      out << '[';
      for (int16_t rl = 0; rl <= rep_level_; rl++) {
        if (rl > 0) {
          out << ' ';
        }
        out << replevel2base_[rl]->field_->name();
        out << "->";
        out << replevel2leaf_[rl]->field_->name();
      }
      out << ']';
    }
    out << std::endl;
    for (int i = 0; i < num_children(); i++) {
      child(i)->DebugPrint(out, indent + 2);
    }
  }

  std::shared_ptr<Field> field_;
  std::vector<std::unique_ptr<StitchNode>> children_;
  StitchNode* parent_{nullptr};
  int16_t rep_level_{-1};
  int16_t def_level_{-1};

  // input data
  std::shared_ptr<Int16Array> rep_levels_;
  std::shared_ptr<Int16Array> def_levels_;
  int64_t levels_pos_{0};

  // FSM tables
  std::vector<StitchNode*> replevel2base_;
  std::vector<StitchNode*> replevel2leaf_;
};


template<typename data_type>
class PrimitiveStitchNode : public StitchNode {
 public:
  using PrimitiveBuilder = typename TypeTraits<data_type>::BuilderType;
  using PrimitiveArray = typename TypeTraits<data_type>::ArrayType;

  using StitchNode::StitchNode;

  static Result<std::unique_ptr<StitchNode>>
  Create(const std::shared_ptr<Field>& field,
         int16_t rep_level, int16_t def_level,
         MemoryPool* pool) {
    auto node = make_unique<PrimitiveStitchNode<data_type>>(field);

    node->rep_level_ = rep_level;
    if (field->nullable()) {
      def_level++;
    }
    node->def_level_ = def_level;

    node->builder_ = std::make_shared<PrimitiveBuilder>(node->field()->type(), pool);

    return Result<std::unique_ptr<StitchNode>>(std::move(node));
  }

  std::shared_ptr<ArrayBuilder> builder() override {
    return builder_;
  }

  bool has_more_values() const override {
    return values_pos_ < values_->length();
  }

  Status Append(int16_t def_level, bool* godown) override {
    if (def_level < def_level_) {
      RETURN_NOT_OK(builder_->AppendNull());
    } else if (def_level == def_level_) {
      if ((int64_t)values_pos_ >= values_->length()) {
        return Status::Invalid("Not enough values, field ", field_->name());
      }
      RETURN_NOT_OK(CopyValue<data_type>(*values_, values_pos_++, builder_.get()));
    } else {
      return Status::Invalid("Bad definition level ", def_level,
                             " for field ", field_->name());
    }
    *godown = false;
    return Status::OK();
  }

  Status SetValues(const std::shared_ptr<Array>& values) override {
    if (values == nullptr) {
      return Status::Invalid("No values for field ", field_->name());
    }
    if (!values->type()->Equals(field_->type())) {
      return Status::Invalid("Incorrect value type for field ", field_->name());
    }
    this->values_ = std::static_pointer_cast<PrimitiveArray>(values);
    this->values_pos_ = 0;
    return Status::OK();
  }

  std::shared_ptr<PrimitiveBuilder> builder_;
  std::shared_ptr<PrimitiveArray> values_;
  int64_t values_pos_{0};
};

class StructStitchNode : public StitchNode {
 public:
  using StitchNode::StitchNode;

  static Result<std::unique_ptr<StitchNode>>
  Create(const std::shared_ptr<Field>& field,
         int16_t rep_level, int16_t def_level,
         MemoryPool* pool) {
    auto node = make_unique<StructStitchNode>(field);

    node->rep_level_ = rep_level;
    if (field->nullable()) {
      def_level++;
    }
    node->def_level_ = def_level;

    node->children_.resize(field->type()->num_children());
    for (int i = 0; i < field->type()->num_children(); i++) {
      Result<std::unique_ptr<StitchNode>> child =
        CreateTree(field->type()->child(i), rep_level, def_level, pool);
      RETURN_NOT_OK(child.status());
      node->children_[i] = std::move(child).ValueOrDie();
      node->children_[i]->parent_ = node.get();
    }

    std::vector<std::shared_ptr<ArrayBuilder>> field_builders;
    for (int i = 0; i < node->num_children(); i++) {
      field_builders.push_back(node->child(i)->builder());
    }
    node->builder_ = std::make_shared<StructBuilder>(node->field()->type(), pool,
                                                     field_builders);

    return Result<std::unique_ptr<StitchNode>>(std::move(node));
  }


  std::shared_ptr<ArrayBuilder> builder() override {
    return builder_;
  }

  bool has_more_values() const override {
    return false;
  }

  Status Append(int16_t def_level, bool* godown) override {
    RETURN_NOT_OK(builder_->Append(def_level >= def_level_));
    *godown = (num_children() > 0);
    return Status::OK();
  }

  Status SetValues(const std::shared_ptr<Array>& values) override {
    if (values != nullptr) {
      return Status::Invalid("Struct node can't have values");
    }
    return Status::OK();
  }

  std::shared_ptr<StructBuilder> builder_;
};

class ListStitchNode : public StitchNode {
public:
  using StitchNode::StitchNode;

  static Result<std::unique_ptr<StitchNode>>
  Create(std::shared_ptr<Field> field,
         int16_t rep_level, int16_t def_level,
         MemoryPool* pool) {
    if (field->type()->num_children() != 1) {
      return Status::Invalid("List nodes must have exactly one child");
    }

    auto node = make_unique<ListStitchNode>(field);

    node->rep_level_ = rep_level++;
    if (field->nullable()) {
      def_level++;
    }
    def_level++; // empty list
    node->def_level_ = def_level;

    node->children_.resize(1);
    Result<std::unique_ptr<StitchNode>> child =
      CreateTree(field->type()->child(0), rep_level, def_level, pool);
    RETURN_NOT_OK(child.status());
    node->children_[0] = std::move(child).ValueOrDie();
    node->children_[0]->parent_ = node.get();

    node->builder_ = std::make_shared<ListBuilder>(pool, node->child(0)->builder(),
                                                   node->field()->type());

    return Result<std::unique_ptr<StitchNode>>(std::move(node));
  }

  bool has_more_values() const override {
    return false;
  }

  std::shared_ptr<ArrayBuilder> builder() override {
    return builder_;
  }

  Status Append(int16_t def_level, bool* godown) override {
    // (def_level == def_level_ - 2) is NULL list
    // (def_level == def_level_ - 1) is empty list
    // (def_level >= def_level_) is non-empty list
    RETURN_NOT_OK(builder_->Append(def_level >= def_level_ - 1));
    *godown = (def_level >= def_level_);
    return Status::OK();
  }

  Status SetValues(const std::shared_ptr<Array>& values) override {
    if (values != nullptr) {
      return Status::Invalid("List node can't have values");
    }
    return Status::OK();
  }

  std::shared_ptr<ListBuilder> builder_;
};

Result<std::unique_ptr<StitchNode>>
StitchNode::CreateTree(std::shared_ptr<Field> field,
                       int16_t rep_level, int16_t def_level,
                       MemoryPool* pool) {
  switch (field->type()->id()) {
    case Type::STRUCT:
      return StructStitchNode::Create(field, rep_level, def_level, pool);
    case Type::LIST:
      return ListStitchNode::Create(field, rep_level, def_level, pool);
#define CASE(data_type) \
    case data_type::type_id: \
      return PrimitiveStitchNode<data_type>::Create(field, rep_level, def_level, pool);
    PRIMITIVE_CASE_LIST(CASE)
#undef CASE
    default:
      return Status::NotImplemented("Unsupported type: ", field->type()->name());
  }
}


} // anonymous namespace

class Stitcher::Impl {
public:
  Impl(std::unique_ptr<StitchNode>&& root, MemoryPool* pool)
      : root_(std::move(root)),
        pool_(pool) {
  }

  std::unique_ptr<StitchNode> root_;
  MemoryPool* pool_;
};

Stitcher::Stitcher(std::unique_ptr<Impl> impl)
    : impl_(std::move(impl)) {
}

Stitcher::~Stitcher() {
}

Result<std::shared_ptr<Stitcher>> Stitcher::Create(const std::shared_ptr<Field>& schema,
                                                   MemoryPool* pool) {
  Result<std::unique_ptr<StitchNode>> root =
    StitchNode::CreateTree(schema, 0, 0, pool);
  RETURN_NOT_OK(root.status());

  root.ValueOrDie()->BuildTransitionTablesRecursive();

  std::unique_ptr<Impl> impl = make_unique<Impl>(std::move(root).ValueOrDie(), pool);

  std::shared_ptr<Stitcher> stitcher(new Stitcher(std::move(impl)));

  return Result<std::shared_ptr<Stitcher>>(stitcher);
}

Status Stitcher::Stitch(const ColumnMap& colmap) {
  RETURN_NOT_OK(impl_->root_->SetColumnDataRecursive(colmap));

  StitchNode* base = impl_->root_.get();
  StitchNode* leaf = base->left_most_leaf();
  while (leaf->has_more_levels()) {
    int16_t def_level = leaf->def_levels_->Value(leaf->levels_pos_++);
    RETURN_NOT_OK(base->AppendBranch(def_level));
    RETURN_NOT_OK(leaf->NextState(&base, &leaf));
  }

  RETURN_NOT_OK(impl_->root_->CheckConsumedRecursive());

  return Status::OK();
}

Result<std::shared_ptr<Array>> Stitcher::Finish() {
  std::shared_ptr<Array> array;
  RETURN_NOT_OK(impl_->root_->builder()->Finish(&array));
  return Result<std::shared_ptr<Array>>(array);
}

const std::shared_ptr<Field>& Stitcher::schema() const {
  return impl_->root_->field();
}

void Stitcher::DebugPrint(std::ostream& out) const {
  impl_->root_->DebugPrint(out);
}


}  // namespace arrow
