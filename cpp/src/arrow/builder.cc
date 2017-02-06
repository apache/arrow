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

#include "arrow/builder.h"

#include <cstdint>
#include <cstring>
#include <limits>

#include "arrow/array.h"
#include "arrow/buffer.h"
#include "arrow/status.h"
#include "arrow/type.h"
#include "arrow/type_traits.h"
#include "arrow/util/bit-util.h"
#include "arrow/util/logging.h"

namespace arrow {

Status ArrayBuilder::AppendToBitmap(bool is_valid) {
  if (length_ == capacity_) {
    // If the capacity was not already a multiple of 2, do so here
    // TODO(emkornfield) doubling isn't great default allocation practice
    // see https://github.com/facebook/folly/blob/master/folly/docs/FBVector.md
    // fo discussion
    RETURN_NOT_OK(Resize(BitUtil::NextPower2(capacity_ + 1)));
  }
  UnsafeAppendToBitmap(is_valid);
  return Status::OK();
}

Status ArrayBuilder::AppendToBitmap(const uint8_t* valid_bytes, int32_t length) {
  RETURN_NOT_OK(Reserve(length));

  UnsafeAppendToBitmap(valid_bytes, length);
  return Status::OK();
}

Status ArrayBuilder::Init(int32_t capacity) {
  int32_t to_alloc = BitUtil::CeilByte(capacity) / 8;
  null_bitmap_ = std::make_shared<PoolBuffer>(pool_);
  RETURN_NOT_OK(null_bitmap_->Resize(to_alloc));
  // Buffers might allocate more then necessary to satisfy padding requirements
  const int byte_capacity = null_bitmap_->capacity();
  capacity_ = capacity;
  null_bitmap_data_ = null_bitmap_->mutable_data();
  memset(null_bitmap_data_, 0, byte_capacity);
  return Status::OK();
}

Status ArrayBuilder::Resize(int32_t new_bits) {
  if (!null_bitmap_) { return Init(new_bits); }
  int32_t new_bytes = BitUtil::CeilByte(new_bits) / 8;
  int32_t old_bytes = null_bitmap_->size();
  RETURN_NOT_OK(null_bitmap_->Resize(new_bytes));
  null_bitmap_data_ = null_bitmap_->mutable_data();
  // The buffer might be overpadded to deal with padding according to the spec
  const int32_t byte_capacity = null_bitmap_->capacity();
  capacity_ = new_bits;
  if (old_bytes < new_bytes) {
    memset(null_bitmap_data_ + old_bytes, 0, byte_capacity - old_bytes);
  }
  return Status::OK();
}

Status ArrayBuilder::Advance(int32_t elements) {
  if (length_ + elements > capacity_) {
    return Status::Invalid("Builder must be expanded");
  }
  length_ += elements;
  return Status::OK();
}

Status ArrayBuilder::Reserve(int32_t elements) {
  if (length_ + elements > capacity_) {
    // TODO(emkornfield) power of 2 growth is potentially suboptimal
    int32_t new_capacity = BitUtil::NextPower2(length_ + elements);
    return Resize(new_capacity);
  }
  return Status::OK();
}

Status ArrayBuilder::SetNotNull(int32_t length) {
  RETURN_NOT_OK(Reserve(length));
  UnsafeSetNotNull(length);
  return Status::OK();
}

void ArrayBuilder::UnsafeAppendToBitmap(bool is_valid) {
  if (is_valid) {
    BitUtil::SetBit(null_bitmap_data_, length_);
  } else {
    ++null_count_;
  }
  ++length_;
}

void ArrayBuilder::UnsafeAppendToBitmap(const uint8_t* valid_bytes, int32_t length) {
  if (valid_bytes == nullptr) {
    UnsafeSetNotNull(length);
    return;
  }
  for (int32_t i = 0; i < length; ++i) {
    // TODO(emkornfield) Optimize for large values of length?
    UnsafeAppendToBitmap(valid_bytes[i] > 0);
  }
}

void ArrayBuilder::UnsafeSetNotNull(int32_t length) {
  const int32_t new_length = length + length_;
  // TODO(emkornfield) Optimize for large values of length?
  for (int32_t i = length_; i < new_length; ++i) {
    BitUtil::SetBit(null_bitmap_data_, i);
  }
  length_ = new_length;
}

template <typename T>
Status PrimitiveBuilder<T>::Init(int32_t capacity) {
  RETURN_NOT_OK(ArrayBuilder::Init(capacity));
  data_ = std::make_shared<PoolBuffer>(pool_);

  int64_t nbytes = TypeTraits<T>::bytes_required(capacity);
  RETURN_NOT_OK(data_->Resize(nbytes));
  // TODO(emkornfield) valgrind complains without this
  memset(data_->mutable_data(), 0, nbytes);

  raw_data_ = reinterpret_cast<value_type*>(data_->mutable_data());
  return Status::OK();
}

template <typename T>
Status PrimitiveBuilder<T>::Resize(int32_t capacity) {
  // XXX: Set floor size for now
  if (capacity < kMinBuilderCapacity) { capacity = kMinBuilderCapacity; }

  if (capacity_ == 0) {
    RETURN_NOT_OK(Init(capacity));
  } else {
    RETURN_NOT_OK(ArrayBuilder::Resize(capacity));
    const int64_t old_bytes = data_->size();
    const int64_t new_bytes = TypeTraits<T>::bytes_required(capacity);
    RETURN_NOT_OK(data_->Resize(new_bytes));
    raw_data_ = reinterpret_cast<value_type*>(data_->mutable_data());
    // TODO(emkornfield) valgrind complains without this
    memset(data_->mutable_data() + old_bytes, 0, new_bytes - old_bytes);
  }
  return Status::OK();
}

template <typename T>
Status PrimitiveBuilder<T>::Append(
    const value_type* values, int32_t length, const uint8_t* valid_bytes) {
  RETURN_NOT_OK(Reserve(length));

  if (length > 0) {
    memcpy(raw_data_ + length_, values, TypeTraits<T>::bytes_required(length));
  }

  // length_ is update by these
  ArrayBuilder::UnsafeAppendToBitmap(valid_bytes, length);

  return Status::OK();
}

template <typename T>
Status PrimitiveBuilder<T>::Finish(std::shared_ptr<Array>* out) {
  const int64_t bytes_required = TypeTraits<T>::bytes_required(length_);
  if (bytes_required > 0 && bytes_required < data_->size()) {
    // Trim buffers
    RETURN_NOT_OK(data_->Resize(bytes_required));
  }
  *out = std::make_shared<typename TypeTraits<T>::ArrayType>(
      type_, length_, data_, null_bitmap_, null_count_);

  data_ = null_bitmap_ = nullptr;
  capacity_ = length_ = null_count_ = 0;
  return Status::OK();
}

template class PrimitiveBuilder<UInt8Type>;
template class PrimitiveBuilder<UInt16Type>;
template class PrimitiveBuilder<UInt32Type>;
template class PrimitiveBuilder<UInt64Type>;
template class PrimitiveBuilder<Int8Type>;
template class PrimitiveBuilder<Int16Type>;
template class PrimitiveBuilder<Int32Type>;
template class PrimitiveBuilder<Int64Type>;
template class PrimitiveBuilder<DateType>;
template class PrimitiveBuilder<TimestampType>;
template class PrimitiveBuilder<TimeType>;
template class PrimitiveBuilder<HalfFloatType>;
template class PrimitiveBuilder<FloatType>;
template class PrimitiveBuilder<DoubleType>;

BooleanBuilder::BooleanBuilder(MemoryPool* pool)
    : ArrayBuilder(pool, boolean()), data_(nullptr), raw_data_(nullptr) {}

BooleanBuilder::BooleanBuilder(MemoryPool* pool, const std::shared_ptr<DataType>& type)
    : BooleanBuilder(pool) {
  DCHECK_EQ(Type::BOOL, type->type);
}

Status BooleanBuilder::Init(int32_t capacity) {
  RETURN_NOT_OK(ArrayBuilder::Init(capacity));
  data_ = std::make_shared<PoolBuffer>(pool_);

  int64_t nbytes = BitUtil::BytesForBits(capacity);
  RETURN_NOT_OK(data_->Resize(nbytes));
  // TODO(emkornfield) valgrind complains without this
  memset(data_->mutable_data(), 0, nbytes);

  raw_data_ = reinterpret_cast<uint8_t*>(data_->mutable_data());
  return Status::OK();
}

Status BooleanBuilder::Resize(int32_t capacity) {
  // XXX: Set floor size for now
  if (capacity < kMinBuilderCapacity) { capacity = kMinBuilderCapacity; }

  if (capacity_ == 0) {
    RETURN_NOT_OK(Init(capacity));
  } else {
    RETURN_NOT_OK(ArrayBuilder::Resize(capacity));
    const int64_t old_bytes = data_->size();
    const int64_t new_bytes = BitUtil::BytesForBits(capacity);

    RETURN_NOT_OK(data_->Resize(new_bytes));
    raw_data_ = reinterpret_cast<uint8_t*>(data_->mutable_data());
    memset(data_->mutable_data() + old_bytes, 0, new_bytes - old_bytes);
  }
  return Status::OK();
}

Status BooleanBuilder::Finish(std::shared_ptr<Array>* out) {
  const int64_t bytes_required = BitUtil::BytesForBits(length_);

  if (bytes_required > 0 && bytes_required < data_->size()) {
    // Trim buffers
    RETURN_NOT_OK(data_->Resize(bytes_required));
  }
  *out = std::make_shared<BooleanArray>(type_, length_, data_, null_bitmap_, null_count_);

  data_ = null_bitmap_ = nullptr;
  capacity_ = length_ = null_count_ = 0;
  return Status::OK();
}

Status BooleanBuilder::Append(
    const uint8_t* values, int32_t length, const uint8_t* valid_bytes) {
  RETURN_NOT_OK(Reserve(length));

  for (int i = 0; i < length; ++i) {
    // Skip reading from unitialised memory
    // TODO: This actually is only to keep valgrind happy but may or may not
    // have a performance impact.
    if ((valid_bytes != nullptr) && !valid_bytes[i]) continue;

    if (values[i] > 0) {
      BitUtil::SetBit(raw_data_, length_ + i);
    } else {
      BitUtil::ClearBit(raw_data_, length_ + i);
    }
  }

  // this updates length_
  ArrayBuilder::UnsafeAppendToBitmap(valid_bytes, length);
  return Status::OK();
}

// ----------------------------------------------------------------------
// ListBuilder

ListBuilder::ListBuilder(
    MemoryPool* pool, std::shared_ptr<ArrayBuilder> value_builder, const TypePtr& type)
    : ArrayBuilder(
          pool, type ? type : std::static_pointer_cast<DataType>(
                                  std::make_shared<ListType>(value_builder->type()))),
      offset_builder_(pool),
      value_builder_(value_builder) {}

ListBuilder::ListBuilder(
    MemoryPool* pool, std::shared_ptr<Array> values, const TypePtr& type)
    : ArrayBuilder(pool, type ? type : std::static_pointer_cast<DataType>(
                                           std::make_shared<ListType>(values->type()))),
      offset_builder_(pool),
      values_(values) {}

Status ListBuilder::Init(int32_t elements) {
  DCHECK_LT(elements, std::numeric_limits<int32_t>::max());
  RETURN_NOT_OK(ArrayBuilder::Init(elements));
  // one more then requested for offsets
  return offset_builder_.Resize((elements + 1) * sizeof(int32_t));
}

Status ListBuilder::Resize(int32_t capacity) {
  DCHECK_LT(capacity, std::numeric_limits<int32_t>::max());
  // one more then requested for offsets
  RETURN_NOT_OK(offset_builder_.Resize((capacity + 1) * sizeof(int32_t)));
  return ArrayBuilder::Resize(capacity);
}

Status ListBuilder::Finish(std::shared_ptr<Array>* out) {
  std::shared_ptr<Array> items = values_;
  if (!items) { RETURN_NOT_OK(value_builder_->Finish(&items)); }

  RETURN_NOT_OK(offset_builder_.Append<int32_t>(items->length()));
  std::shared_ptr<Buffer> offsets = offset_builder_.Finish();

  *out = std::make_shared<ListArray>(
      type_, length_, offsets, items, null_bitmap_, null_count_);

  Reset();

  return Status::OK();
}

void ListBuilder::Reset() {
  capacity_ = length_ = null_count_ = 0;
  null_bitmap_ = nullptr;
}

std::shared_ptr<ArrayBuilder> ListBuilder::value_builder() const {
  DCHECK(!values_) << "Using value builder is pointless when values_ is set";
  return value_builder_;
}

// ----------------------------------------------------------------------
// String and binary

BinaryBuilder::BinaryBuilder(MemoryPool* pool)
    : ListBuilder(pool, std::make_shared<UInt8Builder>(pool, uint8()), binary()) {
  byte_builder_ = static_cast<UInt8Builder*>(value_builder_.get());
}

BinaryBuilder::BinaryBuilder(MemoryPool* pool, const TypePtr& type)
    : ListBuilder(pool, std::make_shared<UInt8Builder>(pool, uint8()), type) {
  byte_builder_ = static_cast<UInt8Builder*>(value_builder_.get());
}

Status BinaryBuilder::Finish(std::shared_ptr<Array>* out) {
  std::shared_ptr<Array> result;
  RETURN_NOT_OK(ListBuilder::Finish(&result));

  const auto list = std::dynamic_pointer_cast<ListArray>(result);
  auto values = std::dynamic_pointer_cast<UInt8Array>(list->values());

  *out = std::make_shared<BinaryArray>(list->length(), list->value_offsets(),
      values->data(), list->null_bitmap(), list->null_count());
  return Status::OK();
}

StringBuilder::StringBuilder(MemoryPool* pool) : BinaryBuilder(pool, utf8()) {}

Status StringBuilder::Finish(std::shared_ptr<Array>* out) {
  std::shared_ptr<Array> result;
  RETURN_NOT_OK(ListBuilder::Finish(&result));

  const auto list = std::dynamic_pointer_cast<ListArray>(result);
  auto values = std::dynamic_pointer_cast<UInt8Array>(list->values());

  *out = std::make_shared<StringArray>(list->length(), list->value_offsets(),
      values->data(), list->null_bitmap(), list->null_count());
  return Status::OK();
}

// ----------------------------------------------------------------------
// Struct

Status StructBuilder::Finish(std::shared_ptr<Array>* out) {
  std::vector<std::shared_ptr<Array>> fields(field_builders_.size());
  for (size_t i = 0; i < field_builders_.size(); ++i) {
    RETURN_NOT_OK(field_builders_[i]->Finish(&fields[i]));
  }

  *out = std::make_shared<StructArray>(type_, length_, fields, null_bitmap_, null_count_);

  null_bitmap_ = nullptr;
  capacity_ = length_ = null_count_ = 0;

  return Status::OK();
}

std::shared_ptr<ArrayBuilder> StructBuilder::field_builder(int pos) const {
  DCHECK_GT(field_builders_.size(), 0);
  return field_builders_[pos];
}

// ----------------------------------------------------------------------
// Helper functions

#define BUILDER_CASE(ENUM, BuilderType) \
  case Type::ENUM:                      \
    out->reset(new BuilderType(pool));  \
    return Status::OK();

// Initially looked at doing this with vtables, but shared pointers makes it
// difficult
//
// TODO(wesm): come up with a less monolithic strategy
Status MakeBuilder(MemoryPool* pool, const std::shared_ptr<DataType>& type,
    std::shared_ptr<ArrayBuilder>* out) {
  switch (type->type) {
    BUILDER_CASE(UINT8, UInt8Builder);
    BUILDER_CASE(INT8, Int8Builder);
    BUILDER_CASE(UINT16, UInt16Builder);
    BUILDER_CASE(INT16, Int16Builder);
    BUILDER_CASE(UINT32, UInt32Builder);
    BUILDER_CASE(INT32, Int32Builder);
    BUILDER_CASE(UINT64, UInt64Builder);
    BUILDER_CASE(INT64, Int64Builder);
    BUILDER_CASE(DATE, DateBuilder);
    case Type::TIMESTAMP:
      out->reset(new TimestampBuilder(pool, type));
      return Status::OK();
    case Type::TIME:
      out->reset(new TimeBuilder(pool, type));
      return Status::OK();
      BUILDER_CASE(BOOL, BooleanBuilder);
      BUILDER_CASE(FLOAT, FloatBuilder);
      BUILDER_CASE(DOUBLE, DoubleBuilder);
      BUILDER_CASE(STRING, StringBuilder);
      BUILDER_CASE(BINARY, BinaryBuilder);
    case Type::LIST: {
      std::shared_ptr<ArrayBuilder> value_builder;
      std::shared_ptr<DataType> value_type =
          static_cast<ListType*>(type.get())->value_type();
      RETURN_NOT_OK(MakeBuilder(pool, value_type, &value_builder));
      out->reset(new ListBuilder(pool, value_builder));
      return Status::OK();
    }

    case Type::STRUCT: {
      std::vector<FieldPtr>& fields = type->children_;
      std::vector<std::shared_ptr<ArrayBuilder>> values_builder;

      for (auto it : fields) {
        std::shared_ptr<ArrayBuilder> builder;
        RETURN_NOT_OK(MakeBuilder(pool, it->type, &builder));
        values_builder.push_back(builder);
      }
      out->reset(new StructBuilder(pool, type, values_builder));
      return Status::OK();
    }

    default:
      return Status::NotImplemented(type->ToString());
  }
}

}  // namespace arrow
