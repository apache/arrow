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

Status ArrayBuilder::AppendToBitmap(const uint8_t* valid_bytes, int64_t length) {
  RETURN_NOT_OK(Reserve(length));

  UnsafeAppendToBitmap(valid_bytes, length);
  return Status::OK();
}

Status ArrayBuilder::Init(int64_t capacity) {
  int64_t to_alloc = BitUtil::CeilByte(capacity) / 8;
  null_bitmap_ = std::make_shared<PoolBuffer>(pool_);
  RETURN_NOT_OK(null_bitmap_->Resize(to_alloc));
  // Buffers might allocate more then necessary to satisfy padding requirements
  const int64_t byte_capacity = null_bitmap_->capacity();
  capacity_ = capacity;
  null_bitmap_data_ = null_bitmap_->mutable_data();
  memset(null_bitmap_data_, 0, static_cast<size_t>(byte_capacity));
  return Status::OK();
}

Status ArrayBuilder::Resize(int64_t new_bits) {
  if (!null_bitmap_) { return Init(new_bits); }
  int64_t new_bytes = BitUtil::CeilByte(new_bits) / 8;
  int64_t old_bytes = null_bitmap_->size();
  RETURN_NOT_OK(null_bitmap_->Resize(new_bytes));
  null_bitmap_data_ = null_bitmap_->mutable_data();
  // The buffer might be overpadded to deal with padding according to the spec
  const int64_t byte_capacity = null_bitmap_->capacity();
  capacity_ = new_bits;
  if (old_bytes < new_bytes) {
    memset(
        null_bitmap_data_ + old_bytes, 0, static_cast<size_t>(byte_capacity - old_bytes));
  }
  return Status::OK();
}

Status ArrayBuilder::Advance(int64_t elements) {
  if (length_ + elements > capacity_) {
    return Status::Invalid("Builder must be expanded");
  }
  length_ += elements;
  return Status::OK();
}

Status ArrayBuilder::Reserve(int64_t elements) {
  if (length_ + elements > capacity_) {
    // TODO(emkornfield) power of 2 growth is potentially suboptimal
    int64_t new_capacity = BitUtil::NextPower2(length_ + elements);
    return Resize(new_capacity);
  }
  return Status::OK();
}

Status ArrayBuilder::SetNotNull(int64_t length) {
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

void ArrayBuilder::UnsafeAppendToBitmap(const uint8_t* valid_bytes, int64_t length) {
  if (valid_bytes == nullptr) {
    UnsafeSetNotNull(length);
    return;
  }

  int64_t byte_offset = length_ / 8;
  int64_t bit_offset = length_ % 8;
  uint8_t bitset = null_bitmap_data_[byte_offset];

  for (int64_t i = 0; i < length; ++i) {
    if (bit_offset == 8) {
      bit_offset = 0;
      null_bitmap_data_[byte_offset] = bitset;
      byte_offset++;
      // TODO: Except for the last byte, this shouldn't be needed
      bitset = null_bitmap_data_[byte_offset];
    }

    if (valid_bytes[i]) {
      bitset |= BitUtil::kBitmask[bit_offset];
    } else {
      bitset &= BitUtil::kFlippedBitmask[bit_offset];
      ++null_count_;
    }

    bit_offset++;
  }
  if (bit_offset != 0) { null_bitmap_data_[byte_offset] = bitset; }
  length_ += length;
}

void ArrayBuilder::UnsafeSetNotNull(int64_t length) {
  const int64_t new_length = length + length_;

  // Fill up the bytes until we have a byte alignment
  int64_t pad_to_byte = 8 - (length_ % 8);
  if (pad_to_byte == 8) { pad_to_byte = 0; }
  for (int64_t i = 0; i < pad_to_byte; ++i) {
    BitUtil::SetBit(null_bitmap_data_, i);
  }

  // Fast bitsetting
  int64_t fast_length = (length - pad_to_byte) / 8;
  memset(null_bitmap_data_ + ((length_ + pad_to_byte) / 8), 255,
      static_cast<size_t>(fast_length));

  // Trailing bytes
  for (int64_t i = length_ + pad_to_byte + (fast_length * 8); i < new_length; ++i) {
    BitUtil::SetBit(null_bitmap_data_, i);
  }

  length_ = new_length;
}

template <typename T>
Status PrimitiveBuilder<T>::Init(int64_t capacity) {
  RETURN_NOT_OK(ArrayBuilder::Init(capacity));
  data_ = std::make_shared<PoolBuffer>(pool_);

  int64_t nbytes = TypeTraits<T>::bytes_required(capacity);
  RETURN_NOT_OK(data_->Resize(nbytes));
  // TODO(emkornfield) valgrind complains without this
  memset(data_->mutable_data(), 0, static_cast<size_t>(nbytes));

  raw_data_ = reinterpret_cast<value_type*>(data_->mutable_data());
  return Status::OK();
}

template <typename T>
Status PrimitiveBuilder<T>::Resize(int64_t capacity) {
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
    memset(
        data_->mutable_data() + old_bytes, 0, static_cast<size_t>(new_bytes - old_bytes));
  }
  return Status::OK();
}

template <typename T>
Status PrimitiveBuilder<T>::Append(
    const value_type* values, int64_t length, const uint8_t* valid_bytes) {
  RETURN_NOT_OK(Reserve(length));

  if (length > 0) {
    std::memcpy(raw_data_ + length_, values,
        static_cast<std::size_t>(TypeTraits<T>::bytes_required(length)));
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
template class PrimitiveBuilder<Date32Type>;
template class PrimitiveBuilder<Date64Type>;
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

Status BooleanBuilder::Init(int64_t capacity) {
  RETURN_NOT_OK(ArrayBuilder::Init(capacity));
  data_ = std::make_shared<PoolBuffer>(pool_);

  int64_t nbytes = BitUtil::BytesForBits(capacity);
  RETURN_NOT_OK(data_->Resize(nbytes));
  // TODO(emkornfield) valgrind complains without this
  memset(data_->mutable_data(), 0, static_cast<size_t>(nbytes));

  raw_data_ = reinterpret_cast<uint8_t*>(data_->mutable_data());
  return Status::OK();
}

Status BooleanBuilder::Resize(int64_t capacity) {
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
    memset(
        data_->mutable_data() + old_bytes, 0, static_cast<size_t>(new_bytes - old_bytes));
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
    const uint8_t* values, int64_t length, const uint8_t* valid_bytes) {
  RETURN_NOT_OK(Reserve(length));

  for (int64_t i = 0; i < length; ++i) {
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

ListBuilder::ListBuilder(MemoryPool* pool, std::shared_ptr<ArrayBuilder> value_builder,
    const std::shared_ptr<DataType>& type)
    : ArrayBuilder(
          pool, type ? type : std::static_pointer_cast<DataType>(
                                  std::make_shared<ListType>(value_builder->type()))),
      offset_builder_(pool),
      value_builder_(value_builder) {}

ListBuilder::ListBuilder(MemoryPool* pool, std::shared_ptr<Array> values,
    const std::shared_ptr<DataType>& type)
    : ArrayBuilder(pool, type ? type : std::static_pointer_cast<DataType>(
                                           std::make_shared<ListType>(values->type()))),
      offset_builder_(pool),
      values_(values) {}

Status ListBuilder::Append(
    const int32_t* offsets, int64_t length, const uint8_t* valid_bytes) {
  RETURN_NOT_OK(Reserve(length));
  UnsafeAppendToBitmap(valid_bytes, length);
  offset_builder_.UnsafeAppend<int32_t>(offsets, length);
  return Status::OK();
}

Status ListBuilder::Append(bool is_valid) {
  RETURN_NOT_OK(Reserve(1));
  UnsafeAppendToBitmap(is_valid);
  RETURN_NOT_OK(
      offset_builder_.Append<int32_t>(static_cast<int32_t>(value_builder_->length())));
  return Status::OK();
}

Status ListBuilder::Init(int64_t elements) {
  DCHECK_LT(elements, std::numeric_limits<int64_t>::max());
  RETURN_NOT_OK(ArrayBuilder::Init(elements));
  // one more then requested for offsets
  return offset_builder_.Resize((elements + 1) * sizeof(int64_t));
}

Status ListBuilder::Resize(int64_t capacity) {
  DCHECK_LT(capacity, std::numeric_limits<int64_t>::max());
  // one more then requested for offsets
  RETURN_NOT_OK(offset_builder_.Resize((capacity + 1) * sizeof(int64_t)));
  return ArrayBuilder::Resize(capacity);
}

Status ListBuilder::Finish(std::shared_ptr<Array>* out) {
  std::shared_ptr<Array> items = values_;
  if (!items) { RETURN_NOT_OK(value_builder_->Finish(&items)); }

  RETURN_NOT_OK(offset_builder_.Append<int64_t>(items->length()));
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

BinaryBuilder::BinaryBuilder(MemoryPool* pool, const std::shared_ptr<DataType>& type)
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
// Fixed width binary

FixedWidthBinaryBuilder::FixedWidthBinaryBuilder(
    MemoryPool* pool, const std::shared_ptr<DataType>& type)
    : ArrayBuilder(pool, type), byte_builder_(pool) {
  DCHECK(type->type == Type::FIXED_WIDTH_BINARY);
  byte_width_ = static_cast<const FixedWidthBinaryType&>(*type).byte_width();
}

Status FixedWidthBinaryBuilder::Append(const uint8_t* value) {
  RETURN_NOT_OK(Reserve(1));
  UnsafeAppendToBitmap(true);
  return byte_builder_.Append(value, byte_width_);
}

Status FixedWidthBinaryBuilder::Append(
    const uint8_t* data, int64_t length, const uint8_t* valid_bytes) {
  RETURN_NOT_OK(Reserve(length));
  UnsafeAppendToBitmap(valid_bytes, length);
  return byte_builder_.Append(data, length * byte_width_);
}

Status FixedWidthBinaryBuilder::Append(const std::string& value) {
  return Append(reinterpret_cast<const uint8_t*>(value.c_str()));
}

Status FixedWidthBinaryBuilder::AppendNull() {
  RETURN_NOT_OK(Reserve(1));
  UnsafeAppendToBitmap(false);
  return byte_builder_.Advance(byte_width_);
}

Status FixedWidthBinaryBuilder::Init(int64_t elements) {
  DCHECK_LT(elements, std::numeric_limits<int64_t>::max());
  RETURN_NOT_OK(ArrayBuilder::Init(elements));
  return byte_builder_.Resize(elements * byte_width_);
}

Status FixedWidthBinaryBuilder::Resize(int64_t capacity) {
  DCHECK_LT(capacity, std::numeric_limits<int64_t>::max());
  RETURN_NOT_OK(byte_builder_.Resize(capacity * byte_width_));
  return ArrayBuilder::Resize(capacity);
}

Status FixedWidthBinaryBuilder::Finish(std::shared_ptr<Array>* out) {
  std::shared_ptr<Buffer> data = byte_builder_.Finish();
  *out = std::make_shared<FixedWidthBinaryArray>(
      type_, length_, data, null_bitmap_, null_count_);
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
    BUILDER_CASE(DATE32, Date32Builder);
    BUILDER_CASE(DATE64, Date64Builder);
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
