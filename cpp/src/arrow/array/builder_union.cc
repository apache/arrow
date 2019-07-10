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

Status BasicUnionBuilder::FinishInternal(std::shared_ptr<ArrayData>* out) {
  std::shared_ptr<Buffer> types, null_bitmap;
  RETURN_NOT_OK(null_bitmap_builder_.Finish(&null_bitmap));
  RETURN_NOT_OK(types_builder_.Finish(&types));

  // If the type has not been specified in the constructor, gather type_ids
  std::vector<uint8_t> type_ids;
  if (type_ == nullptr) {
    for (size_t i = 0; i < children_.size(); ++i) {
      type_ids.push_back(static_cast<uint8_t>(i));
    }
  } else {
    type_ids = checked_cast<const UnionType&>(*type_).type_codes();
  }

  std::vector<std::shared_ptr<ArrayData>> child_data(type_ids.size());
  for (size_t i = 0; i < type_ids.size(); ++i) {
    RETURN_NOT_OK(children_[type_ids[i]]->FinishInternal(&child_data[i]));
  }

  // If the type has not been specified in the constructor, infer it
  if (type_ == nullptr) {
    std::vector<std::shared_ptr<Field>> fields;
    auto field_names_it = field_names_.begin();
    for (auto&& data : child_data) {
      fields.push_back(field(*field_names_it++, data->type));
    }
    type_ = union_(fields, type_ids, mode_);
  }

  *out = ArrayData::Make(type_, length(), {null_bitmap, types, nullptr}, null_count_);
  (*out)->child_data = std::move(child_data);
  return Status::OK();
}

BasicUnionBuilder::BasicUnionBuilder(
    MemoryPool* pool, UnionMode::type mode,
    const std::vector<std::shared_ptr<ArrayBuilder>>& children,
    const std::shared_ptr<DataType>& type)
    : ArrayBuilder(type, pool), mode_(mode), types_builder_(pool) {
  auto union_type = checked_cast<const UnionType*>(type.get());
  DCHECK_NE(union_type, nullptr);
  DCHECK_EQ(union_type->mode(), mode);

  // NB: children_ is indexed by the child array's type_id, *not* by the index
  // of the child_data in the Finished array data
  children_.resize(union_type->max_type_code() + 1, nullptr);

  auto field_it = type->children().begin();
  auto children_it = children.begin();
  for (auto type_id : union_type->type_codes()) {
    children_[type_id] = *children_it++;
    field_names_.push_back((*field_it++)->name());
  }
  DCHECK_EQ(children_it, children.end());
  DCHECK_EQ(field_it, type->children().end());
}

int8_t BasicUnionBuilder::AppendChild(const std::shared_ptr<ArrayBuilder>& new_child,
                                      const std::string& field_name) {
  // force type inferrence in Finish
  type_ = nullptr;

  field_names_.push_back(field_name);

  for (int8_t type_id = dense_type_id_; static_cast<size_t>(type_id) < children_.size();
       ++type_id) {
    if (children_[type_id] == NULLPTR) {
      children_[type_id] = new_child;
      return type_id;
    }
    dense_type_id_ = type_id;
  }

  children_.push_back(new_child);
  return dense_type_id_++;
}

}  // namespace arrow
