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

#include "arrow/array/builder_nested.h"

#include <cstddef>
#include <cstdint>
#include <utility>
#include <vector>

#include "arrow/buffer.h"
#include "arrow/status.h"
#include "arrow/type.h"
#include "arrow/util/checked_cast.h"
#include "arrow/util/logging.h"

namespace arrow {

// ----------------------------------------------------------------------
// MapBuilder

MapBuilder::MapBuilder(MemoryPool* pool, const std::shared_ptr<ArrayBuilder>& key_builder,
                       std::shared_ptr<ArrayBuilder> const& item_builder,
                       const std::shared_ptr<DataType>& type)
    : ArrayBuilder(pool), key_builder_(key_builder), item_builder_(item_builder) {
  auto map_type = internal::checked_cast<const MapType*>(type.get());
  keys_sorted_ = map_type->keys_sorted();

  std::vector<std::shared_ptr<ArrayBuilder>> child_builders{key_builder, item_builder};
  auto struct_builder =
      std::make_shared<StructBuilder>(map_type->value_type(), pool, child_builders);

  list_builder_ =
      std::make_shared<ListBuilder>(pool, struct_builder, struct_builder->type());
}

MapBuilder::MapBuilder(MemoryPool* pool, const std::shared_ptr<ArrayBuilder>& key_builder,
                       const std::shared_ptr<ArrayBuilder>& item_builder,
                       bool keys_sorted)
    : MapBuilder(pool, key_builder, item_builder,
                 map(key_builder->type(), item_builder->type(), keys_sorted)) {}

MapBuilder::MapBuilder(MemoryPool* pool,
                       const std::shared_ptr<ArrayBuilder>& struct_builder,
                       const std::shared_ptr<DataType>& type)
    : ArrayBuilder(pool) {
  auto map_type = internal::checked_cast<const MapType*>(type.get());
  keys_sorted_ = map_type->keys_sorted();
  key_builder_ = struct_builder->child_builder(0);
  item_builder_ = struct_builder->child_builder(1);
  list_builder_ =
      std::make_shared<ListBuilder>(pool, struct_builder, struct_builder->type());
}

Status MapBuilder::Resize(int64_t capacity) {
  RETURN_NOT_OK(list_builder_->Resize(capacity));
  capacity_ = list_builder_->capacity();
  return Status::OK();
}

void MapBuilder::Reset() {
  list_builder_->Reset();
  ArrayBuilder::Reset();
}

Status MapBuilder::FinishInternal(std::shared_ptr<ArrayData>* out) {
  ARROW_CHECK_EQ(item_builder_->length(), key_builder_->length())
      << "keys and items builders don't have the same size in MapBuilder";
  RETURN_NOT_OK(AdjustStructBuilderLength());
  RETURN_NOT_OK(list_builder_->FinishInternal(out));
  (*out)->type = type();
  ArrayBuilder::Reset();
  return Status::OK();
}

Status MapBuilder::AppendValues(const int32_t* offsets, int64_t length,
                                const uint8_t* valid_bytes) {
  DCHECK_EQ(item_builder_->length(), key_builder_->length());
  RETURN_NOT_OK(AdjustStructBuilderLength());
  RETURN_NOT_OK(list_builder_->AppendValues(offsets, length, valid_bytes));
  length_ = list_builder_->length();
  null_count_ = list_builder_->null_count();
  return Status::OK();
}

Status MapBuilder::Append() {
  DCHECK_EQ(item_builder_->length(), key_builder_->length());
  RETURN_NOT_OK(AdjustStructBuilderLength());
  RETURN_NOT_OK(list_builder_->Append());
  length_ = list_builder_->length();
  return Status::OK();
}

Status MapBuilder::AppendNull() {
  DCHECK_EQ(item_builder_->length(), key_builder_->length());
  RETURN_NOT_OK(AdjustStructBuilderLength());
  RETURN_NOT_OK(list_builder_->AppendNull());
  length_ = list_builder_->length();
  null_count_ = list_builder_->null_count();
  return Status::OK();
}

Status MapBuilder::AppendNulls(int64_t length) {
  DCHECK_EQ(item_builder_->length(), key_builder_->length());
  RETURN_NOT_OK(AdjustStructBuilderLength());
  RETURN_NOT_OK(list_builder_->AppendNulls(length));
  length_ = list_builder_->length();
  null_count_ = list_builder_->null_count();
  return Status::OK();
}

Status MapBuilder::AppendEmptyValue() {
  DCHECK_EQ(item_builder_->length(), key_builder_->length());
  RETURN_NOT_OK(AdjustStructBuilderLength());
  RETURN_NOT_OK(list_builder_->AppendEmptyValue());
  length_ = list_builder_->length();
  null_count_ = list_builder_->null_count();
  return Status::OK();
}

Status MapBuilder::AppendEmptyValues(int64_t length) {
  DCHECK_EQ(item_builder_->length(), key_builder_->length());
  RETURN_NOT_OK(AdjustStructBuilderLength());
  RETURN_NOT_OK(list_builder_->AppendEmptyValues(length));
  length_ = list_builder_->length();
  null_count_ = list_builder_->null_count();
  return Status::OK();
}

Status MapBuilder::AdjustStructBuilderLength() {
  // If key/item builders have been appended, adjust struct builder length
  // to match. Struct and key are non-nullable, append all valid values.
  auto struct_builder =
      internal::checked_cast<StructBuilder*>(list_builder_->value_builder());
  if (struct_builder->length() < key_builder_->length()) {
    int64_t length_diff = key_builder_->length() - struct_builder->length();
    RETURN_NOT_OK(struct_builder->AppendValues(length_diff, NULLPTR));
  }
  return Status::OK();
}

// ----------------------------------------------------------------------
// FixedSizeListBuilder

FixedSizeListBuilder::FixedSizeListBuilder(
    MemoryPool* pool, const std::shared_ptr<ArrayBuilder>& value_builder,
    const std::shared_ptr<DataType>& type)
    : ArrayBuilder(pool),
      value_field_(type->field(0)),
      list_size_(
          internal::checked_cast<const FixedSizeListType*>(type.get())->list_size()),
      value_builder_(value_builder) {}

FixedSizeListBuilder::FixedSizeListBuilder(
    MemoryPool* pool, const std::shared_ptr<ArrayBuilder>& value_builder,
    int32_t list_size)
    : FixedSizeListBuilder(pool, value_builder,
                           fixed_size_list(value_builder->type(), list_size)) {}

void FixedSizeListBuilder::Reset() {
  ArrayBuilder::Reset();
  value_builder_->Reset();
}

Status FixedSizeListBuilder::Append() {
  RETURN_NOT_OK(Reserve(1));
  UnsafeAppendToBitmap(true);
  return Status::OK();
}

Status FixedSizeListBuilder::AppendValues(int64_t length, const uint8_t* valid_bytes) {
  RETURN_NOT_OK(Reserve(length));
  UnsafeAppendToBitmap(valid_bytes, length);
  return Status::OK();
}

Status FixedSizeListBuilder::AppendNull() {
  RETURN_NOT_OK(Reserve(1));
  UnsafeAppendToBitmap(false);
  return value_builder_->AppendNulls(list_size_);
}

Status FixedSizeListBuilder::AppendNulls(int64_t length) {
  RETURN_NOT_OK(Reserve(length));
  UnsafeAppendToBitmap(length, false);
  return value_builder_->AppendNulls(list_size_ * length);
}

Status FixedSizeListBuilder::ValidateOverflow(int64_t new_elements) {
  auto new_length = value_builder_->length() + new_elements;
  if (new_elements != list_size_) {
    return Status::Invalid("Length of item not correct: expected ", list_size_,
                           " but got array of size ", new_elements);
  }
  if (new_length > maximum_elements()) {
    return Status::CapacityError("array cannot contain more than ", maximum_elements(),
                                 " elements, have ", new_elements);
  }
  return Status::OK();
}

Status FixedSizeListBuilder::AppendEmptyValue() {
  RETURN_NOT_OK(Reserve(1));
  UnsafeAppendToBitmap(true);
  return value_builder_->AppendEmptyValues(list_size_);
}

Status FixedSizeListBuilder::AppendEmptyValues(int64_t length) {
  RETURN_NOT_OK(Reserve(length));
  UnsafeAppendToBitmap(length, true);
  return value_builder_->AppendEmptyValues(list_size_ * length);
}

Status FixedSizeListBuilder::Resize(int64_t capacity) {
  RETURN_NOT_OK(CheckCapacity(capacity));
  return ArrayBuilder::Resize(capacity);
}

Status FixedSizeListBuilder::FinishInternal(std::shared_ptr<ArrayData>* out) {
  std::shared_ptr<ArrayData> items;

  if (value_builder_->length() == 0) {
    // Try to make sure we get a non-null values buffer (ARROW-2744)
    RETURN_NOT_OK(value_builder_->Resize(0));
  }
  RETURN_NOT_OK(value_builder_->FinishInternal(&items));

  std::shared_ptr<Buffer> null_bitmap;
  RETURN_NOT_OK(null_bitmap_builder_.Finish(&null_bitmap));
  *out = ArrayData::Make(type(), length_, {null_bitmap}, {std::move(items)}, null_count_);
  Reset();
  return Status::OK();
}

// ----------------------------------------------------------------------
// Struct

StructBuilder::StructBuilder(const std::shared_ptr<DataType>& type, MemoryPool* pool,
                             std::vector<std::shared_ptr<ArrayBuilder>> field_builders)
    : ArrayBuilder(pool), type_(type) {
  children_ = std::move(field_builders);
}

void StructBuilder::Reset() {
  ArrayBuilder::Reset();
  for (const auto& field_builder : children_) {
    field_builder->Reset();
  }
}

Status StructBuilder::FinishInternal(std::shared_ptr<ArrayData>* out) {
  std::shared_ptr<Buffer> null_bitmap;
  RETURN_NOT_OK(null_bitmap_builder_.Finish(&null_bitmap));

  std::vector<std::shared_ptr<ArrayData>> child_data(children_.size());
  for (size_t i = 0; i < children_.size(); ++i) {
    if (length_ == 0) {
      // Try to make sure the child buffers are initialized
      RETURN_NOT_OK(children_[i]->Resize(0));
    }
    RETURN_NOT_OK(children_[i]->FinishInternal(&child_data[i]));
  }

  *out = ArrayData::Make(type(), length_, {null_bitmap}, null_count_);
  (*out)->child_data = std::move(child_data);

  capacity_ = length_ = null_count_ = 0;
  return Status::OK();
}

std::shared_ptr<DataType> StructBuilder::type() const {
  DCHECK_EQ(type_->fields().size(), children_.size());
  std::vector<std::shared_ptr<Field>> fields(children_.size());
  for (int i = 0; i < static_cast<int>(fields.size()); ++i) {
    fields[i] = type_->field(i)->WithType(children_[i]->type());
  }
  return struct_(std::move(fields));
}

}  // namespace arrow
