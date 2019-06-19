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

#include "arrow/array/builder_union.h"

#include <utility>

#include "arrow/util/checked_cast.h"
#include "arrow/util/logging.h"

namespace arrow {

using internal::checked_cast;

DenseUnionBuilder::DenseUnionBuilder(MemoryPool* pool)
    : ArrayBuilder(nullptr, pool), types_builder_(pool), offsets_builder_(pool) {}

DenseUnionBuilder::DenseUnionBuilder(MemoryPool* pool,
                                     std::vector<std::shared_ptr<ArrayBuilder>> children,
                                     const std::shared_ptr<DataType>& type)
    : ArrayBuilder(type, pool), types_builder_(pool), offsets_builder_(pool) {
  auto union_type = checked_cast<const UnionType*>(type.get());
  DCHECK_NE(union_type, nullptr);
  type_id_to_child_num_.resize(union_type->max_type_code() + 1, -1);
  DCHECK_EQ(union_type->mode(), UnionMode::DENSE);
  int child_i = 0;
  for (auto type_id : union_type->type_codes()) {
    type_id_to_child_num_[type_id] = child_i++;
  }
  children_ = std::move(children);
}

Status DenseUnionBuilder::FinishInternal(std::shared_ptr<ArrayData>* out) {
  std::shared_ptr<Buffer> types, offsets, null_bitmap;
  RETURN_NOT_OK(null_bitmap_builder_.Finish(&null_bitmap));
  RETURN_NOT_OK(types_builder_.Finish(&types));
  RETURN_NOT_OK(offsets_builder_.Finish(&offsets));

  std::vector<std::shared_ptr<ArrayData>> child_data(children_.size());
  for (size_t i = 0; i < children_.size(); ++i) {
    std::shared_ptr<ArrayData> data;
    RETURN_NOT_OK(children_[i]->FinishInternal(&data));
    child_data[i] = data;
  }

  // If the type has not been specified in the constructor, infer it
  if (!type_) {
    std::vector<std::shared_ptr<Field>> fields;
    std::vector<uint8_t> type_ids;
    for (size_t i = 0; i < children_.size(); ++i) {
      fields.push_back(field(field_names_[i], children_[i]->type()));
      type_ids.push_back(static_cast<uint8_t>(i));
    }
    type_ = union_(fields, type_ids, UnionMode::DENSE);
  }

  *out = ArrayData::Make(type_, length(), {null_bitmap, types, offsets}, null_count_);
  (*out)->child_data = std::move(child_data);
  return Status::OK();
}

SparseUnionBuilder::SparseUnionBuilder(MemoryPool* pool)
    : ArrayBuilder(nullptr, pool), types_builder_(pool) {}

SparseUnionBuilder::SparseUnionBuilder(
    MemoryPool* pool, std::vector<std::shared_ptr<ArrayBuilder>> children,
    const std::shared_ptr<DataType>& type)
    : ArrayBuilder(type, pool), types_builder_(pool) {
  auto union_type = checked_cast<const UnionType*>(type.get());
  DCHECK_NE(union_type, nullptr);
  type_id_to_child_num_.resize(union_type->max_type_code() + 1, -1);
  DCHECK_EQ(union_type->mode(), UnionMode::SPARSE);
  int child_i = 0;
  for (auto type_id : union_type->type_codes()) {
    type_id_to_child_num_[type_id] = child_i++;
  }
  children_ = std::move(children);
  for (auto&& child : children_) {
    DCHECK_EQ(child->length(), 0);
  }
}

Status SparseUnionBuilder::FinishInternal(std::shared_ptr<ArrayData>* out) {
  std::shared_ptr<Buffer> types, offsets, null_bitmap;
  RETURN_NOT_OK(null_bitmap_builder_.Finish(&null_bitmap));
  RETURN_NOT_OK(types_builder_.Finish(&types));

  std::vector<std::shared_ptr<ArrayData>> child_data(children_.size());
  for (size_t i = 0; i < children_.size(); ++i) {
    std::shared_ptr<ArrayData> data;
    RETURN_NOT_OK(children_[i]->FinishInternal(&data));
    child_data[i] = data;
  }

  // If the type has not been specified in the constructor, infer it
  if (!type_) {
    std::vector<std::shared_ptr<Field>> fields;
    std::vector<uint8_t> type_ids;
    for (size_t i = 0; i < children_.size(); ++i) {
      fields.push_back(field(field_names_[i], children_[i]->type()));
      type_ids.push_back(static_cast<uint8_t>(i));
    }
    type_ = union_(fields, type_ids, UnionMode::SPARSE);
  }

  *out = ArrayData::Make(type_, length(), {null_bitmap, types, offsets}, null_count_);
  (*out)->child_data = std::move(child_data);
  return Status::OK();
}

int8_t SparseUnionBuilder::AppendChild(const std::shared_ptr<ArrayBuilder>& child,
                                       const std::string& field_name) {
  // force type inferrence in Finish
  type_ = NULLPTR;
  DCHECK_EQ(child->length(), length_);

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

}  // namespace arrow
