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

DenseUnionBuilder::DenseUnionBuilder(MemoryPool* pool,
                                     std::vector<std::shared_ptr<ArrayBuilder>> children,
                                     const std::shared_ptr<DataType>& type)
    : ArrayBuilder(type, pool),
      union_type_(checked_cast<const UnionType*>(type.get())),
      types_builder_(pool),
      offsets_builder_(pool),
      type_id_to_child_num_(union_type_ ? union_type_->max_type_code() + 1 : 0, -1) {
  if (union_type_) {
    DCHECK_EQ(union_type_->mode(), UnionMode::DENSE);
    int child_i = 0;
    for (auto type_id : union_type_->type_codes()) {
      type_id_to_child_num_[type_id] = child_i++;
    }
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

SparseUnionBuilder::SparseUnionBuilder(
    MemoryPool* pool, std::vector<std::shared_ptr<ArrayBuilder>> children,
    const std::shared_ptr<DataType>& type)
    : ArrayBuilder(type, pool),
      union_type_(checked_cast<const UnionType*>(type.get())),
      types_builder_(pool),
      type_id_to_child_num_(union_type_ ? union_type_->max_type_code() + 1 : 0, -1) {
  if (union_type_) {
    DCHECK_EQ(union_type_->mode(), UnionMode::SPARSE);
    int child_i = 0;
    for (auto type_id : union_type_->type_codes()) {
      type_id_to_child_num_[type_id] = child_i++;
    }
  }
  children_ = std::move(children);
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

}  // namespace arrow
