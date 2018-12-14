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

#include "arrow/array/builder_primitive.h"

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <sstream>
#include <utility>
#include <vector>

#include "arrow/array.h"
#include "arrow/buffer.h"
#include "arrow/status.h"
#include "arrow/type.h"
#include "arrow/type_traits.h"
#include "arrow/util/bit-util.h"
#include "arrow/util/int-util.h"
#include "arrow/util/logging.h"

namespace arrow {

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

}  // namespace arrow
