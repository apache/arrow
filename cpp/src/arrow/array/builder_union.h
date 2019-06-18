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

#pragma once

#include <memory>
#include <string>
#include <vector>

#include "arrow/array.h"
#include "arrow/array/builder_base.h"
#include "arrow/buffer-builder.h"

namespace arrow {

/// \class DenseUnionBuilder
///
/// You need to call AppendChild for each of the children builders you want
/// to use. The function will return an int8_t, which is the type tag
/// associated with that child. You can then call Append with that tag
/// (followed by an append on the child builder) to add elements to
/// the union array.
///
/// You can either specify the type when the UnionBuilder is constructed
/// or let the UnionBuilder infer the type at runtime (by omitting the
/// type argument from the constructor).
///
/// This API is EXPERIMENTAL.
class ARROW_EXPORT DenseUnionBuilder : public ArrayBuilder {
 public:
  /// Use this constructor to incrementally build the union array along
  /// with types, offsets, and null bitmap.
  explicit DenseUnionBuilder(MemoryPool* pool,
                             std::vector<std::shared_ptr<ArrayBuilder>> children,
                             const std::shared_ptr<DataType>& type);

  Status AppendNull() final {
    ARROW_RETURN_NOT_OK(types_builder_.Append(0));
    ARROW_RETURN_NOT_OK(offsets_builder_.Append(0));
    return AppendToBitmap(false);
  }

  Status AppendNulls(int64_t length) final {
    ARROW_RETURN_NOT_OK(types_builder_.Append(length, 0));
    ARROW_RETURN_NOT_OK(offsets_builder_.Append(length, 0));
    return AppendToBitmap(length, false);
  }

  /// \brief Append an element to the UnionArray. This must be followed
  ///        by an append to the appropriate child builder.
  ///
  /// \param[in] type index of the child the value will be appended
  /// The corresponding child builder must be appended to independently
  /// after this method is called.
  Status Append(int8_t type) {
    ARROW_RETURN_NOT_OK(types_builder_.Append(type));
    auto offset = static_cast<int32_t>(children_[type_id_to_child_num_[type]]->length());
    ARROW_RETURN_NOT_OK(offsets_builder_.Append(offset));
    return AppendToBitmap(true);
  }

  Status FinishInternal(std::shared_ptr<ArrayData>* out) override;

  /// \cond FALSE
  using ArrayBuilder::Finish;
  /// \endcond

  Status Finish(std::shared_ptr<UnionArray>* out) { return FinishTyped(out); }

  /// \brief Make a new child builder available to the UnionArray
  ///
  /// \param[in] child the child builder
  /// \param[in] field_name the name of the field in the union array type
  /// if type inference is used
  /// \return child index, which is the "type" argument that needs
  /// to be passed to the "Append" method to add a new element to
  /// the union array.
  int8_t AppendChild(const std::shared_ptr<ArrayBuilder>& child,
                     const std::string& field_name = "") {
    // force type inferrence in Finish
    type_ = NULLPTR;

    children_.push_back(child);
    field_names_.push_back(field_name);
    auto child_num = static_cast<int8_t>(children_.size() - 1);
    // search for an available type_id
    // FIXME(bkietz) far from optimal
    auto max_type = static_cast<int8_t>(type_id_to_child_num_.size());
    for (int8_t type = 0; type < max_type; ++type) {
      if (type_id_to_child_num_[type] == -1) {
        type_id_to_child_num_[type] = child_num;
        return type;
      }
    }
    type_id_to_child_num_.push_back(child_num);
    return max_type;
  }

 private:
  const UnionType* union_type_;
  TypedBufferBuilder<int8_t> types_builder_;
  TypedBufferBuilder<int32_t> offsets_builder_;
  std::vector<std::string> field_names_;
  std::vector<int8_t> type_id_to_child_num_;
};

/// \class SparseUnionBuilder
///
/// You need to call AppendChild for each of the children builders you want
/// to use. The function will return an int8_t, which is the type tag
/// associated with that child. You can then call Append with that tag
/// (followed by an append on the child builder) to add elements to
/// the union array.
///
/// You can either specify the type when the UnionBuilder is constructed
/// or let the UnionBuilder infer the type at runtime (by omitting the
/// type argument from the constructor).
///
/// This API is EXPERIMENTAL.
class ARROW_EXPORT SparseUnionBuilder : public ArrayBuilder {
 public:
  /// Use this constructor to incrementally build the union array along
  /// with types, offsets, and null bitmap.
  explicit SparseUnionBuilder(MemoryPool* pool,
                              std::vector<std::shared_ptr<ArrayBuilder>> children,
                              const std::shared_ptr<DataType>& type);

  Status AppendNull() final {
    ARROW_RETURN_NOT_OK(types_builder_.Append(0));
    return AppendToBitmap(false);
  }

  Status AppendNulls(int64_t length) final {
    ARROW_RETURN_NOT_OK(types_builder_.Append(length, 0));
    return AppendToBitmap(length, false);
  }

  /// \brief Append an element to the UnionArray. This must be followed
  ///        by an append to the appropriate child builder.
  ///
  /// \param[in] type index of the child the value will be appended
  /// The corresponding child builder must be appended to independently
  /// after this method is called.
  Status Append(int8_t type) {
    ARROW_RETURN_NOT_OK(types_builder_.Append(type));
    return AppendToBitmap(true);
  }

  Status FinishInternal(std::shared_ptr<ArrayData>* out) override;

  /// \cond FALSE
  using ArrayBuilder::Finish;
  /// \endcond

  Status Finish(std::shared_ptr<UnionArray>* out) { return FinishTyped(out); }

  /// \brief Make a new child builder available to the UnionArray
  ///
  /// \param[in] child the child builder
  /// \param[in] field_name the name of the field in the union array type
  /// if type inference is used
  /// \return child index, which is the "type" argument that needs
  /// to be passed to the "Append" method to add a new element to
  /// the union array.
  int8_t AppendChild(const std::shared_ptr<ArrayBuilder>& child,
                     const std::string& field_name = "") {
    // force type inferrence in Finish
    type_ = NULLPTR;

    children_.push_back(child);
    field_names_.push_back(field_name);
    auto child_num = static_cast<int8_t>(children_.size() - 1);
    // search for an available type_id
    // FIXME(bkietz) far from optimal
    auto max_type = static_cast<int8_t>(type_id_to_child_num_.size());
    for (int8_t type = 0; type < max_type; ++type) {
      if (type_id_to_child_num_[type] == -1) {
        type_id_to_child_num_[type] = child_num;
        return type;
      }
    }
    type_id_to_child_num_.push_back(child_num);
    return max_type;
  }

 private:
  const UnionType* union_type_;
  TypedBufferBuilder<int8_t> types_builder_;
  std::vector<std::string> field_names_;
  std::vector<int8_t> type_id_to_child_num_;
};

}  // namespace arrow
