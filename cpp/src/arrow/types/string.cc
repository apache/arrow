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

#include "arrow/types/string.h"

#include <cstring>
#include <sstream>
#include <string>

#include "arrow/type.h"

namespace arrow {

static std::shared_ptr<DataType> kBinary = std::make_shared<BinaryType>();
static std::shared_ptr<DataType> kString = std::make_shared<StringType>();

BinaryArray::BinaryArray(int32_t length, const std::shared_ptr<Buffer>& offsets,
    const std::shared_ptr<Buffer>& data, int32_t null_count,
    const std::shared_ptr<Buffer>& null_bitmap)
    : BinaryArray(kBinary, length, offsets, data, null_count, null_bitmap) {}

BinaryArray::BinaryArray(const TypePtr& type, int32_t length,
    const std::shared_ptr<Buffer>& offsets, const std::shared_ptr<Buffer>& data,
    int32_t null_count, const std::shared_ptr<Buffer>& null_bitmap)
    : Array(type, length, null_count, null_bitmap),
      offset_buffer_(offsets),
      offsets_(reinterpret_cast<const int32_t*>(offset_buffer_->data())),
      data_buffer_(data),
      data_(nullptr) {
  if (data_buffer_ != nullptr) { data_ = data_buffer_->data(); }
}

Status BinaryArray::Validate() const {
  // TODO(wesm): what to do here?
  return Status::OK();
}

bool BinaryArray::EqualsExact(const BinaryArray& other) const {
  if (!Array::EqualsExact(other)) { return false; }

  bool equal_offsets =
      offset_buffer_->Equals(*other.offset_buffer_, (length_ + 1) * sizeof(int32_t));
  if (!equal_offsets) { return false; }

  if (!data_buffer_ && !(other.data_buffer_)) { return true; }

  return data_buffer_->Equals(*other.data_buffer_, data_buffer_->size());
}

bool BinaryArray::Equals(const std::shared_ptr<Array>& arr) const {
  if (this == arr.get()) { return true; }
  if (this->type_enum() != arr->type_enum()) { return false; }
  return EqualsExact(*static_cast<const BinaryArray*>(arr.get()));
}

bool BinaryArray::RangeEquals(int32_t start_idx, int32_t end_idx, int32_t other_start_idx,
    const std::shared_ptr<Array>& arr) const {
  if (this == arr.get()) { return true; }
  if (!arr) { return false; }
  if (this->type_enum() != arr->type_enum()) { return false; }
  const auto other = static_cast<const BinaryArray*>(arr.get());
  for (int32_t i = start_idx, o_i = other_start_idx; i < end_idx; ++i, ++o_i) {
    const bool is_null = IsNull(i);
    if (is_null != arr->IsNull(o_i)) { return false; }
    if (is_null) continue;
    const int32_t begin_offset = offset(i);
    const int32_t end_offset = offset(i + 1);
    const int32_t other_begin_offset = other->offset(o_i);
    const int32_t other_end_offset = other->offset(o_i + 1);
    // Underlying can't be equal if the size isn't equal
    if (end_offset - begin_offset != other_end_offset - other_begin_offset) {
      return false;
    }

    if (std::memcmp(data_ + begin_offset, other->data_ + other_begin_offset,
            end_offset - begin_offset)) {
      return false;
    }
  }
  return true;
}

Status BinaryArray::Accept(ArrayVisitor* visitor) const {
  return visitor->Visit(*this);
}

StringArray::StringArray(int32_t length, const std::shared_ptr<Buffer>& offsets,
    const std::shared_ptr<Buffer>& data, int32_t null_count,
    const std::shared_ptr<Buffer>& null_bitmap)
    : BinaryArray(kString, length, offsets, data, null_count, null_bitmap) {}

Status StringArray::Validate() const {
  // TODO(emkornfield) Validate proper UTF8 code points?
  return BinaryArray::Validate();
}

Status StringArray::Accept(ArrayVisitor* visitor) const {
  return visitor->Visit(*this);
}

// This used to be a static member variable of BinaryBuilder, but it can cause
// valgrind to report a (spurious?) memory leak when needed in other shared
// libraries. The problem came up while adding explicit visibility to libarrow
// and libparquet_arrow
static TypePtr kBinaryValueType = TypePtr(new UInt8Type());

BinaryBuilder::BinaryBuilder(MemoryPool* pool, const TypePtr& type)
    : ListBuilder(pool, std::make_shared<UInt8Builder>(pool, kBinaryValueType), type) {
  byte_builder_ = static_cast<UInt8Builder*>(value_builder_.get());
}

Status BinaryBuilder::Finish(std::shared_ptr<Array>* out) {
  std::shared_ptr<Array> result;
  RETURN_NOT_OK(ListBuilder::Finish(&result));

  const auto list = std::dynamic_pointer_cast<ListArray>(result);
  auto values = std::dynamic_pointer_cast<UInt8Array>(list->values());

  *out = std::make_shared<BinaryArray>(list->length(), list->offsets(), values->data(),
      list->null_count(), list->null_bitmap());
  return Status::OK();
}

Status StringBuilder::Finish(std::shared_ptr<Array>* out) {
  std::shared_ptr<Array> result;
  RETURN_NOT_OK(ListBuilder::Finish(&result));

  const auto list = std::dynamic_pointer_cast<ListArray>(result);
  auto values = std::dynamic_pointer_cast<UInt8Array>(list->values());

  *out = std::make_shared<StringArray>(list->length(), list->offsets(), values->data(),
      list->null_count(), list->null_bitmap());
  return Status::OK();
}

}  // namespace arrow
