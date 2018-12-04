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

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <sstream>
#include <utility>
#include <vector>

#include "arrow/array.h"
#include "arrow/buffer.h"
#include "arrow/builder.h"
#include "arrow/status.h"
#include "arrow/type.h"
#include "arrow/type_traits.h"
#include "arrow/util/bit-util.h"
#include "arrow/util/checked_cast.h"
#include "arrow/util/int-util.h"
#include "arrow/util/logging.h"

namespace arrow {

using internal::checked_cast;

Status ArrayBuilder::TrimBuffer(const int64_t bytes_filled, ResizableBuffer* buffer) {
  if (buffer) {
    if (bytes_filled < buffer->size()) {
      // Trim buffer
      RETURN_NOT_OK(buffer->Resize(bytes_filled));
    }
    // zero the padding
    buffer->ZeroPadding();
  } else {
    // Null buffers are allowed in place of 0-byte buffers
    DCHECK_EQ(bytes_filled, 0);
  }
  return Status::OK();
}

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

Status ArrayBuilder::Resize(int64_t capacity) {
  // Target size of validity (null) bitmap data
  const int64_t new_bitmap_size = BitUtil::BytesForBits(capacity);
  RETURN_NOT_OK(CheckCapacity(capacity, capacity_));

  if (capacity_ == 0) {
    RETURN_NOT_OK(AllocateResizableBuffer(pool_, new_bitmap_size, &null_bitmap_));
    null_bitmap_data_ = null_bitmap_->mutable_data();

    // Padding is zeroed by AllocateResizableBuffer
    memset(null_bitmap_data_, 0, static_cast<size_t>(new_bitmap_size));
  } else {
    const int64_t old_bitmap_capacity = null_bitmap_->capacity();
    RETURN_NOT_OK(null_bitmap_->Resize(new_bitmap_size));

    const int64_t new_bitmap_capacity = null_bitmap_->capacity();
    null_bitmap_data_ = null_bitmap_->mutable_data();

    // Zero the region between the original capacity and the new capacity,
    // including padding, which has not been zeroed, unlike
    // AllocateResizableBuffer
    if (old_bitmap_capacity < new_bitmap_capacity) {
      memset(null_bitmap_data_ + old_bitmap_capacity, 0,
             static_cast<size_t>(new_bitmap_capacity - old_bitmap_capacity));
    }
  }
  capacity_ = capacity;
  return Status::OK();
}

Status ArrayBuilder::Advance(int64_t elements) {
  if (length_ + elements > capacity_) {
    return Status::Invalid("Builder must be expanded");
  }
  length_ += elements;
  return Status::OK();
}

Status ArrayBuilder::Finish(std::shared_ptr<Array>* out) {
  std::shared_ptr<ArrayData> internal_data;
  RETURN_NOT_OK(FinishInternal(&internal_data));
  *out = MakeArray(internal_data);
  return Status::OK();
}

Status ArrayBuilder::Reserve(int64_t additional_elements) {
  if (length_ + additional_elements > capacity_) {
    // TODO(emkornfield) power of 2 growth is potentially suboptimal
    int64_t new_size = BitUtil::NextPower2(length_ + additional_elements);
    return Resize(new_size);
  }
  return Status::OK();
}

void ArrayBuilder::Reset() {
  capacity_ = length_ = null_count_ = 0;
  null_bitmap_ = nullptr;
}

Status ArrayBuilder::SetNotNull(int64_t length) {
  RETURN_NOT_OK(Reserve(length));
  UnsafeSetNotNull(length);
  return Status::OK();
}

void ArrayBuilder::UnsafeAppendToBitmap(const uint8_t* valid_bytes, int64_t length) {
  if (valid_bytes == nullptr) {
    UnsafeSetNotNull(length);
    return;
  }
  UnsafeAppendToBitmap(valid_bytes, valid_bytes + length);
}

void ArrayBuilder::UnsafeAppendToBitmap(const std::vector<bool>& is_valid) {
  UnsafeAppendToBitmap(is_valid.begin(), is_valid.end());
}

void ArrayBuilder::UnsafeSetNotNull(int64_t length) {
  const int64_t new_length = length + length_;

  // Fill up the bytes until we have a byte alignment
  int64_t pad_to_byte = std::min<int64_t>(8 - (length_ % 8), length);

  if (pad_to_byte == 8) {
    pad_to_byte = 0;
  }
  for (int64_t i = length_; i < length_ + pad_to_byte; ++i) {
    BitUtil::SetBit(null_bitmap_data_, i);
  }

  // Fast bitsetting
  int64_t fast_length = (length - pad_to_byte) / 8;
  memset(null_bitmap_data_ + ((length_ + pad_to_byte) / 8), 0xFF,
         static_cast<size_t>(fast_length));

  // Trailing bits
  for (int64_t i = length_ + pad_to_byte + (fast_length * 8); i < new_length; ++i) {
    BitUtil::SetBit(null_bitmap_data_, i);
  }

  length_ = new_length;
}

// ----------------------------------------------------------------------
// Null builder

Status NullBuilder::FinishInternal(std::shared_ptr<ArrayData>* out) {
  *out = ArrayData::Make(null(), length_, {nullptr}, length_);
  length_ = null_count_ = 0;
  return Status::OK();
}

// ----------------------------------------------------------------------

template <typename T>
void PrimitiveBuilder<T>::Reset() {
  data_.reset();
  raw_data_ = nullptr;
}

template <typename T>
Status PrimitiveBuilder<T>::Resize(int64_t capacity) {
  RETURN_NOT_OK(CheckCapacity(capacity, capacity_));
  capacity = std::max(capacity, kMinBuilderCapacity);

  int64_t nbytes = TypeTraits<T>::bytes_required(capacity);
  if (capacity_ == 0) {
    RETURN_NOT_OK(AllocateResizableBuffer(pool_, nbytes, &data_));
  } else {
    RETURN_NOT_OK(data_->Resize(nbytes));
  }

  raw_data_ = reinterpret_cast<value_type*>(data_->mutable_data());
  return ArrayBuilder::Resize(capacity);
}

template <typename T>
Status PrimitiveBuilder<T>::AppendValues(const value_type* values, int64_t length,
                                         const uint8_t* valid_bytes) {
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
Status PrimitiveBuilder<T>::AppendValues(const value_type* values, int64_t length,
                                         const std::vector<bool>& is_valid) {
  RETURN_NOT_OK(Reserve(length));
  DCHECK_EQ(length, static_cast<int64_t>(is_valid.size()));

  if (length > 0) {
    std::memcpy(raw_data_ + length_, values,
                static_cast<std::size_t>(TypeTraits<T>::bytes_required(length)));
  }

  // length_ is update by these
  ArrayBuilder::UnsafeAppendToBitmap(is_valid);
  return Status::OK();
}

template <typename T>
Status PrimitiveBuilder<T>::AppendValues(const std::vector<value_type>& values,
                                         const std::vector<bool>& is_valid) {
  return AppendValues(values.data(), static_cast<int64_t>(values.size()), is_valid);
}

template <typename T>
Status PrimitiveBuilder<T>::AppendValues(const std::vector<value_type>& values) {
  return AppendValues(values.data(), static_cast<int64_t>(values.size()));
}

template <typename T>
Status PrimitiveBuilder<T>::FinishInternal(std::shared_ptr<ArrayData>* out) {
  RETURN_NOT_OK(TrimBuffer(BitUtil::BytesForBits(length_), null_bitmap_.get()));
  RETURN_NOT_OK(TrimBuffer(TypeTraits<T>::bytes_required(length_), data_.get()));

  *out = ArrayData::Make(type_, length_, {null_bitmap_, data_}, null_count_);

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
template class PrimitiveBuilder<Time32Type>;
template class PrimitiveBuilder<Time64Type>;
template class PrimitiveBuilder<TimestampType>;
template class PrimitiveBuilder<HalfFloatType>;
template class PrimitiveBuilder<FloatType>;
template class PrimitiveBuilder<DoubleType>;

BooleanBuilder::BooleanBuilder(MemoryPool* pool)
    : ArrayBuilder(boolean(), pool), data_(nullptr), raw_data_(nullptr) {}

BooleanBuilder::BooleanBuilder(const std::shared_ptr<DataType>& type, MemoryPool* pool)
    : BooleanBuilder(pool) {
  DCHECK_EQ(Type::BOOL, type->id());
}

void BooleanBuilder::Reset() {
  ArrayBuilder::Reset();
  data_.reset();
  raw_data_ = nullptr;
}

Status BooleanBuilder::Resize(int64_t capacity) {
  RETURN_NOT_OK(CheckCapacity(capacity, capacity_));
  capacity = std::max(capacity, kMinBuilderCapacity);

  const int64_t new_bitmap_size = BitUtil::BytesForBits(capacity);
  if (capacity_ == 0) {
    RETURN_NOT_OK(AllocateResizableBuffer(pool_, new_bitmap_size, &data_));
    raw_data_ = reinterpret_cast<uint8_t*>(data_->mutable_data());

    // We zero the memory for booleans to keep things simple; for some reason if
    // we do not, even though we may write every bit (through in-place | or &),
    // valgrind will still show a warning. If we do not zero the bytes here, we
    // will have to be careful to zero them in AppendNull and AppendNulls. Also,
    // zeroing the bits results in deterministic bits when each byte may have a
    // mix of nulls and not nulls.
    //
    // We only zero up to new_bitmap_size because the padding was zeroed by
    // AllocateResizableBuffer
    memset(raw_data_, 0, static_cast<size_t>(new_bitmap_size));
  } else {
    const int64_t old_bitmap_capacity = data_->capacity();
    RETURN_NOT_OK(data_->Resize(new_bitmap_size));
    const int64_t new_bitmap_capacity = data_->capacity();
    raw_data_ = reinterpret_cast<uint8_t*>(data_->mutable_data());

    // See comment above about why we zero memory for booleans
    memset(raw_data_ + old_bitmap_capacity, 0,
           static_cast<size_t>(new_bitmap_capacity - old_bitmap_capacity));
  }

  return ArrayBuilder::Resize(capacity);
}

Status BooleanBuilder::FinishInternal(std::shared_ptr<ArrayData>* out) {
  int64_t bit_offset = length_ % 8;
  if (bit_offset > 0) {
    // Adjust last byte
    data_->mutable_data()[length_ / 8] &= BitUtil::kPrecedingBitmask[bit_offset];
  }

  RETURN_NOT_OK(TrimBuffer(BitUtil::BytesForBits(length_), null_bitmap_.get()));
  RETURN_NOT_OK(TrimBuffer(BitUtil::BytesForBits(length_), data_.get()));

  *out = ArrayData::Make(boolean(), length_, {null_bitmap_, data_}, null_count_);

  data_ = null_bitmap_ = nullptr;
  capacity_ = length_ = null_count_ = 0;
  return Status::OK();
}

Status BooleanBuilder::AppendValues(const uint8_t* values, int64_t length,
                                    const uint8_t* valid_bytes) {
  RETURN_NOT_OK(Reserve(length));

  int64_t i = 0;
  internal::GenerateBitsUnrolled(raw_data_, length_, length,
                                 [values, &i]() -> bool { return values[i++] != 0; });

  // this updates length_
  ArrayBuilder::UnsafeAppendToBitmap(valid_bytes, length);
  return Status::OK();
}

Status BooleanBuilder::AppendValues(const uint8_t* values, int64_t length,
                                    const std::vector<bool>& is_valid) {
  RETURN_NOT_OK(Reserve(length));
  DCHECK_EQ(length, static_cast<int64_t>(is_valid.size()));

  int64_t i = 0;
  internal::GenerateBitsUnrolled(raw_data_, length_, length,
                                 [values, &i]() -> bool { return values[i++]; });

  // this updates length_
  ArrayBuilder::UnsafeAppendToBitmap(is_valid);
  return Status::OK();
}

Status BooleanBuilder::AppendValues(const std::vector<uint8_t>& values,
                                    const std::vector<bool>& is_valid) {
  return AppendValues(values.data(), static_cast<int64_t>(values.size()), is_valid);
}

Status BooleanBuilder::AppendValues(const std::vector<uint8_t>& values) {
  return AppendValues(values.data(), static_cast<int64_t>(values.size()));
}

Status BooleanBuilder::AppendValues(const std::vector<bool>& values,
                                    const std::vector<bool>& is_valid) {
  const int64_t length = static_cast<int64_t>(values.size());
  RETURN_NOT_OK(Reserve(length));
  DCHECK_EQ(length, static_cast<int64_t>(is_valid.size()));

  int64_t i = 0;
  internal::GenerateBitsUnrolled(raw_data_, length_, length,
                                 [&values, &i]() -> bool { return values[i++]; });

  // this updates length_
  ArrayBuilder::UnsafeAppendToBitmap(is_valid);
  return Status::OK();
}

Status BooleanBuilder::AppendValues(const std::vector<bool>& values) {
  const int64_t length = static_cast<int64_t>(values.size());
  RETURN_NOT_OK(Reserve(length));

  int64_t i = 0;
  internal::GenerateBitsUnrolled(raw_data_, length_, length,
                                 [&values, &i]() -> bool { return values[i++]; });

  // this updates length_
  ArrayBuilder::UnsafeSetNotNull(length);
  return Status::OK();
}

// ----------------------------------------------------------------------
// ListBuilder

ListBuilder::ListBuilder(MemoryPool* pool,
                         std::shared_ptr<ArrayBuilder> const& value_builder,
                         const std::shared_ptr<DataType>& type)
    : ArrayBuilder(type ? type
                        : std::static_pointer_cast<DataType>(
                              std::make_shared<ListType>(value_builder->type())),
                   pool),
      offsets_builder_(pool),
      value_builder_(value_builder) {}

Status ListBuilder::AppendValues(const int32_t* offsets, int64_t length,
                                 const uint8_t* valid_bytes) {
  RETURN_NOT_OK(Reserve(length));
  UnsafeAppendToBitmap(valid_bytes, length);
  offsets_builder_.UnsafeAppend(offsets, length);
  return Status::OK();
}

Status ListBuilder::AppendNextOffset() {
  int64_t num_values = value_builder_->length();
  if (ARROW_PREDICT_FALSE(num_values > kListMaximumElements)) {
    std::stringstream ss;
    ss << "ListArray cannot contain more then INT32_MAX - 1 child elements,"
       << " have " << num_values;
    return Status::CapacityError(ss.str());
  }
  return offsets_builder_.Append(static_cast<int32_t>(num_values));
}

Status ListBuilder::Append(bool is_valid) {
  RETURN_NOT_OK(Reserve(1));
  UnsafeAppendToBitmap(is_valid);
  return AppendNextOffset();
}

Status ListBuilder::Resize(int64_t capacity) {
  DCHECK_LE(capacity, kListMaximumElements);
  RETURN_NOT_OK(CheckCapacity(capacity, capacity_));

  // one more then requested for offsets
  RETURN_NOT_OK(offsets_builder_.Resize((capacity + 1) * sizeof(int32_t)));
  return ArrayBuilder::Resize(capacity);
}

Status ListBuilder::FinishInternal(std::shared_ptr<ArrayData>* out) {
  RETURN_NOT_OK(AppendNextOffset());

  // Offset padding zeroed by BufferBuilder
  std::shared_ptr<Buffer> offsets;
  RETURN_NOT_OK(offsets_builder_.Finish(&offsets));

  std::shared_ptr<ArrayData> items;
  if (values_) {
    items = values_->data();
  } else {
    if (value_builder_->length() == 0) {
      // Try to make sure we get a non-null values buffer (ARROW-2744)
      RETURN_NOT_OK(value_builder_->Resize(0));
    }
    RETURN_NOT_OK(value_builder_->FinishInternal(&items));
  }

  *out = ArrayData::Make(type_, length_, {null_bitmap_, offsets}, null_count_);
  (*out)->child_data.emplace_back(std::move(items));
  Reset();
  return Status::OK();
}

void ListBuilder::Reset() {
  ArrayBuilder::Reset();
  values_.reset();
  offsets_builder_.Reset();
  value_builder_->Reset();
}

ArrayBuilder* ListBuilder::value_builder() const {
  DCHECK(!values_) << "Using value builder is pointless when values_ is set";
  return value_builder_.get();
}

// ----------------------------------------------------------------------
// Struct

StructBuilder::StructBuilder(const std::shared_ptr<DataType>& type, MemoryPool* pool,
                             std::vector<std::shared_ptr<ArrayBuilder>>&& field_builders)
    : ArrayBuilder(type, pool), field_builders_(std::move(field_builders)) {}

void StructBuilder::Reset() {
  ArrayBuilder::Reset();
  for (const auto& field_builder : field_builders_) {
    field_builder->Reset();
  }
}

Status StructBuilder::FinishInternal(std::shared_ptr<ArrayData>* out) {
  RETURN_NOT_OK(TrimBuffer(BitUtil::BytesForBits(length_), null_bitmap_.get()));
  *out = ArrayData::Make(type_, length_, {null_bitmap_}, null_count_);

  (*out)->child_data.resize(field_builders_.size());
  for (size_t i = 0; i < field_builders_.size(); ++i) {
    if (length_ == 0) {
      // Try to make sure the child buffers are initialized
      RETURN_NOT_OK(field_builders_[i]->Resize(0));
    }
    RETURN_NOT_OK(field_builders_[i]->FinishInternal(&(*out)->child_data[i]));
  }

  null_bitmap_ = nullptr;
  capacity_ = length_ = null_count_ = 0;
  return Status::OK();
}

// ----------------------------------------------------------------------
// Helper functions

#define BUILDER_CASE(ENUM, BuilderType)      \
  case Type::ENUM:                           \
    out->reset(new BuilderType(type, pool)); \
    return Status::OK();

// Initially looked at doing this with vtables, but shared pointers makes it
// difficult
//
// TODO(wesm): come up with a less monolithic strategy
Status MakeBuilder(MemoryPool* pool, const std::shared_ptr<DataType>& type,
                   std::unique_ptr<ArrayBuilder>* out) {
  switch (type->id()) {
    case Type::NA: {
      out->reset(new NullBuilder(pool));
      return Status::OK();
    }
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
      BUILDER_CASE(TIME32, Time32Builder);
      BUILDER_CASE(TIME64, Time64Builder);
      BUILDER_CASE(TIMESTAMP, TimestampBuilder);
      BUILDER_CASE(BOOL, BooleanBuilder);
      BUILDER_CASE(HALF_FLOAT, HalfFloatBuilder);
      BUILDER_CASE(FLOAT, FloatBuilder);
      BUILDER_CASE(DOUBLE, DoubleBuilder);
      BUILDER_CASE(STRING, StringBuilder);
      BUILDER_CASE(BINARY, BinaryBuilder);
      BUILDER_CASE(FIXED_SIZE_BINARY, FixedSizeBinaryBuilder);
      BUILDER_CASE(DECIMAL, Decimal128Builder);
    case Type::LIST: {
      std::unique_ptr<ArrayBuilder> value_builder;
      std::shared_ptr<DataType> value_type =
          checked_cast<const ListType&>(*type).value_type();
      RETURN_NOT_OK(MakeBuilder(pool, value_type, &value_builder));
      out->reset(new ListBuilder(pool, std::move(value_builder)));
      return Status::OK();
    }

    case Type::STRUCT: {
      const std::vector<std::shared_ptr<Field>>& fields = type->children();
      std::vector<std::shared_ptr<ArrayBuilder>> values_builder;

      for (auto it : fields) {
        std::unique_ptr<ArrayBuilder> builder;
        RETURN_NOT_OK(MakeBuilder(pool, it->type(), &builder));
        values_builder.emplace_back(std::move(builder));
      }
      out->reset(new StructBuilder(type, pool, std::move(values_builder)));
      return Status::OK();
    }

    default: {
      std::stringstream ss;
      ss << "MakeBuilder: cannot construct builder for type " << type->ToString();
      return Status::NotImplemented(ss.str());
    }
  }
}

}  // namespace arrow
