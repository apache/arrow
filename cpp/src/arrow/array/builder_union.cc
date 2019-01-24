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

#include "arrow/util/logging.h"

namespace arrow {

UnionBuilder::UnionBuilder(MemoryPool* pool,
                           const std::vector<std::shared_ptr<ArrayBuilder>>& children)
    : ArrayBuilder(nullptr, pool), types_builder_(pool), offsets_builder_(pool) {
  children_ = children;
}

Status UnionBuilder::FinishInternal(std::shared_ptr<ArrayData>* out) {
  std::shared_ptr<Buffer> types;
  RETURN_NOT_OK(types_builder_.Finish(&types));
  std::shared_ptr<Buffer> offsets;
  RETURN_NOT_OK(offsets_builder_.Finish(&offsets));

  std::shared_ptr<Buffer> null_bitmap;
  RETURN_NOT_OK(null_bitmap_builder_.Finish(&null_bitmap));

  std::vector<std::shared_ptr<Field>> fields;
  std::vector<std::shared_ptr<ArrayData>> child_data(children_.size());
  std::vector<uint8_t> type_ids;
  for (size_t i = 0; i < children_.size(); ++i) {
    if (children_[i]) {
      std::shared_ptr<ArrayData> data;
      RETURN_NOT_OK(children_[i]->FinishInternal(&data));
      child_data[i] = data;
      fields.push_back(::arrow::field("", children_[i]->type()));
      type_ids.push_back(static_cast<uint8_t>(i));
    } else {
      fields.push_back(::arrow::field("", ::arrow::null()));
      child_data[i] = ArrayData::Make(::arrow::null(), 0);
      type_ids.push_back(static_cast<uint8_t>(i));
    }
  }

  type_ = ::arrow::union_(fields, type_ids, UnionMode::DENSE);

  *out = ArrayData::Make(type_, length(), {null_bitmap, types, offsets}, null_count_);
  (*out)->child_data = std::move(child_data);
  return Status::OK();
}

}  // namespace arrow
