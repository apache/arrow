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

class ARROW_EXPORT BasicUnionBuilder : public ArrayBuilder {
 public:
  Status FinishInternal(std::shared_ptr<ArrayData>* out) override;

  /// \cond FALSE
  using ArrayBuilder::Finish;
  /// \endcond

  Status Finish(std::shared_ptr<UnionArray>* out) { return FinishTyped(out); }

  /// \brief Make a new child builder available to the UnionArray
  ///
  /// \param[in] new_child the child builder
  /// \param[in] field_name the name of the field in the union array type
  /// if type inference is used
  /// \return child index, which is the "type" argument that needs
  /// to be passed to the "Append" method to add a new element to
  /// the union array.
  int8_t AppendChild(const std::shared_ptr<ArrayBuilder>& new_child,
                     const std::string& field_name = "");

 protected:
  /// Use this constructor to initialize the UnionBuilder with no child builders,
  /// allowing type to be inferred. You will need to call AppendChild for each of the
  /// children builders you want to use.
  explicit BasicUnionBuilder(MemoryPool* pool, UnionMode::type mode)
      : ArrayBuilder(NULLPTR, pool), mode_(mode), types_builder_(pool) {}

  /// Use this constructor to specify the type explicitly.
  /// You can still add child builders to the union after using this constructor
  BasicUnionBuilder(MemoryPool* pool, UnionMode::type mode,
                    const std::vector<std::shared_ptr<ArrayBuilder>>& children,
                    const std::shared_ptr<DataType>& type);

  UnionMode::type mode_;
  std::vector<std::shared_ptr<ArrayBuilder>> type_id_to_children_;
  // for all type_id < dense_type_id_, type_id_to_children_[type_id] != nullptr
  int8_t dense_type_id_ = 0;
  TypedBufferBuilder<int8_t> types_builder_;
  std::vector<std::string> field_names_;
};

/// \class DenseUnionBuilder
///
/// This API is EXPERIMENTAL.
class ARROW_EXPORT DenseUnionBuilder : public BasicUnionBuilder {
 public:
  /// Use this constructor to initialize the UnionBuilder with no child builders,
  /// allowing type to be inferred. You will need to call AppendChild for each of the
  /// children builders you want to use.
  explicit DenseUnionBuilder(MemoryPool* pool)
      : BasicUnionBuilder(pool, UnionMode::DENSE), offsets_builder_(pool) {}

  /// Use this constructor to specify the type explicitly.
  /// You can still add child builders to the union after using this constructor
  DenseUnionBuilder(MemoryPool* pool,
                    const std::vector<std::shared_ptr<ArrayBuilder>>& children,
                    const std::shared_ptr<DataType>& type)
      : BasicUnionBuilder(pool, UnionMode::DENSE, children, type),
        offsets_builder_(pool) {}

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
  /// \param[in] next_type type_id of the child to which the next value will be appended.
  ///
  /// The corresponding child builder must be appended to independently after this method
  /// is called.
  Status Append(int8_t next_type) {
    ARROW_RETURN_NOT_OK(types_builder_.Append(next_type));
    if (type_id_to_children_[next_type]->length() == kListMaximumElements) {
      return Status::CapacityError(
          "a dense UnionArray cannot contain more than 2^31 - 1 elements from a single "
          "child");
    }
    auto offset = static_cast<int32_t>(type_id_to_children_[next_type]->length());
    ARROW_RETURN_NOT_OK(offsets_builder_.Append(offset));
    return AppendToBitmap(true);
  }

  Status FinishInternal(std::shared_ptr<ArrayData>* out) override {
    ARROW_RETURN_NOT_OK(BasicUnionBuilder::FinishInternal(out));
    return offsets_builder_.Finish(&(*out)->buffers[2]);
  }

 private:
  TypedBufferBuilder<int32_t> offsets_builder_;
};

/// \class SparseUnionBuilder
///
/// This API is EXPERIMENTAL.
class ARROW_EXPORT SparseUnionBuilder : public BasicUnionBuilder {
 public:
  /// Use this constructor to initialize the UnionBuilder with no child builders,
  /// allowing type to be inferred. You will need to call AppendChild for each of the
  /// children builders you want to use.
  explicit SparseUnionBuilder(MemoryPool* pool)
      : BasicUnionBuilder(pool, UnionMode::SPARSE) {}

  /// Use this constructor to specify the type explicitly.
  /// You can still add child builders to the union after using this constructor
  SparseUnionBuilder(MemoryPool* pool,
                     const std::vector<std::shared_ptr<ArrayBuilder>>& children,
                     const std::shared_ptr<DataType>& type)
      : BasicUnionBuilder(pool, UnionMode::SPARSE, children, type) {}

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
  /// \param[in] next_type type_id of the child to which the next value will be appended.
  ///
  /// The corresponding child builder must be appended to independently after this method
  /// is called, and all other child builders must have null appended
  Status Append(int8_t next_type) {
    ARROW_RETURN_NOT_OK(types_builder_.Append(next_type));
    return AppendToBitmap(true);
  }
};

}  // namespace arrow
