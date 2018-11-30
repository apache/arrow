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
#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <numeric>
#include <sstream>
#include <utility>
#include <vector>

#include "arrow/array.h"
#include "arrow/buffer.h"
#include "arrow/status.h"
#include "arrow/type.h"
#include "arrow/type_traits.h"
#include "arrow/util/bit-util.h"
#include "arrow/util/checked_cast.h"
#include "arrow/util/decimal.h"
#include "arrow/util/hashing.h"
#include "arrow/util/int-util.h"
#include "arrow/util/logging.h"

namespace arrow {

using internal::AdaptiveIntBuilderBase;
using internal::checked_cast;

namespace {

Status TrimBuffer(const int64_t bytes_filled, ResizableBuffer* buffer) {
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

}  // namespace

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

static inline Status CheckCapacity(int64_t new_capacity, int64_t old_capacity) {
  if (new_capacity < 0) return Status::Invalid("Resize capacity must be positive");
  if (new_capacity < old_capacity) return Status::Invalid("Resize cannot downsize");

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

AdaptiveIntBuilderBase::AdaptiveIntBuilderBase(MemoryPool* pool)
    : ArrayBuilder(int64(), pool),
      data_(nullptr),
      raw_data_(nullptr),
      int_size_(1),
      pending_pos_(0),
      pending_has_nulls_(false) {}

void AdaptiveIntBuilderBase::Reset() {
  ArrayBuilder::Reset();
  data_.reset();
  raw_data_ = nullptr;
  pending_pos_ = 0;
  pending_has_nulls_ = false;
}

Status AdaptiveIntBuilderBase::Resize(int64_t capacity) {
  RETURN_NOT_OK(CheckCapacity(capacity, capacity_));
  capacity = std::max(capacity, kMinBuilderCapacity);

  int64_t nbytes = capacity * int_size_;
  if (capacity_ == 0) {
    RETURN_NOT_OK(AllocateResizableBuffer(pool_, nbytes, &data_));
  } else {
    RETURN_NOT_OK(data_->Resize(nbytes));
  }
  raw_data_ = reinterpret_cast<uint8_t*>(data_->mutable_data());

  return ArrayBuilder::Resize(capacity);
}

AdaptiveIntBuilder::AdaptiveIntBuilder(MemoryPool* pool) : AdaptiveIntBuilderBase(pool) {}

Status AdaptiveIntBuilder::FinishInternal(std::shared_ptr<ArrayData>* out) {
  RETURN_NOT_OK(CommitPendingData());

  std::shared_ptr<DataType> output_type;
  switch (int_size_) {
    case 1:
      output_type = int8();
      break;
    case 2:
      output_type = int16();
      break;
    case 4:
      output_type = int32();
      break;
    case 8:
      output_type = int64();
      break;
    default:
      DCHECK(false);
      return Status::NotImplemented("Only ints of size 1,2,4,8 are supported");
  }

  RETURN_NOT_OK(TrimBuffer(BitUtil::BytesForBits(length_), null_bitmap_.get()));
  RETURN_NOT_OK(TrimBuffer(length_ * int_size_, data_.get()));

  *out = ArrayData::Make(output_type, length_, {null_bitmap_, data_}, null_count_);

  data_ = null_bitmap_ = nullptr;
  capacity_ = length_ = null_count_ = 0;
  return Status::OK();
}

Status AdaptiveIntBuilder::CommitPendingData() {
  if (pending_pos_ == 0) {
    return Status::OK();
  }
  RETURN_NOT_OK(Reserve(pending_pos_));
  const uint8_t* valid_bytes = pending_has_nulls_ ? pending_valid_ : nullptr;
  RETURN_NOT_OK(AppendValuesInternal(reinterpret_cast<const int64_t*>(pending_data_),
                                     pending_pos_, valid_bytes));
  pending_has_nulls_ = false;
  pending_pos_ = 0;
  return Status::OK();
}

static constexpr int64_t kAdaptiveIntChunkSize = 8192;

Status AdaptiveIntBuilder::AppendValuesInternal(const int64_t* values, int64_t length,
                                                const uint8_t* valid_bytes) {
  while (length > 0) {
    // In case `length` is very large, we don't want to trash the cache by
    // scanning it twice (first to detect int width, second to copy the data).
    // Instead, process data in L2-cacheable chunks.
    const int64_t chunk_size = std::min(length, kAdaptiveIntChunkSize);

    uint8_t new_int_size;
    new_int_size = internal::DetectIntWidth(values, valid_bytes, chunk_size, int_size_);

    DCHECK_GE(new_int_size, int_size_);
    if (new_int_size > int_size_) {
      // This updates int_size_
      RETURN_NOT_OK(ExpandIntSize(new_int_size));
    }

    switch (int_size_) {
      case 1:
        internal::DowncastInts(values, reinterpret_cast<int8_t*>(raw_data_) + length_,
                               chunk_size);
        break;
      case 2:
        internal::DowncastInts(values, reinterpret_cast<int16_t*>(raw_data_) + length_,
                               chunk_size);
        break;
      case 4:
        internal::DowncastInts(values, reinterpret_cast<int32_t*>(raw_data_) + length_,
                               chunk_size);
        break;
      case 8:
        internal::DowncastInts(values, reinterpret_cast<int64_t*>(raw_data_) + length_,
                               chunk_size);
        break;
      default:
        DCHECK(false);
    }

    // This updates length_
    ArrayBuilder::UnsafeAppendToBitmap(valid_bytes, chunk_size);
    values += chunk_size;
    if (valid_bytes != nullptr) {
      valid_bytes += chunk_size;
    }
    length -= chunk_size;
  }

  return Status::OK();
}

Status AdaptiveUIntBuilder::CommitPendingData() {
  if (pending_pos_ == 0) {
    return Status::OK();
  }
  RETURN_NOT_OK(Reserve(pending_pos_));
  const uint8_t* valid_bytes = pending_has_nulls_ ? pending_valid_ : nullptr;
  RETURN_NOT_OK(AppendValuesInternal(pending_data_, pending_pos_, valid_bytes));
  pending_has_nulls_ = false;
  pending_pos_ = 0;
  return Status::OK();
}

Status AdaptiveIntBuilder::AppendValues(const int64_t* values, int64_t length,
                                        const uint8_t* valid_bytes) {
  RETURN_NOT_OK(CommitPendingData());
  RETURN_NOT_OK(Reserve(length));

  return AppendValuesInternal(values, length, valid_bytes);
}

template <typename new_type, typename old_type>
typename std::enable_if<sizeof(old_type) >= sizeof(new_type), Status>::type
AdaptiveIntBuilder::ExpandIntSizeInternal() {
  return Status::OK();
}

#define __LESS(a, b) (a) < (b)
template <typename new_type, typename old_type>
typename std::enable_if<__LESS(sizeof(old_type), sizeof(new_type)), Status>::type
AdaptiveIntBuilder::ExpandIntSizeInternal() {
  int_size_ = sizeof(new_type);
  RETURN_NOT_OK(Resize(data_->size() / sizeof(old_type)));
  raw_data_ = reinterpret_cast<uint8_t*>(data_->mutable_data());
  const old_type* src = reinterpret_cast<old_type*>(raw_data_);
  new_type* dst = reinterpret_cast<new_type*>(raw_data_);

  // By doing the backward copy, we ensure that no element is overriden during
  // the copy process and the copy stays in-place.
  std::copy_backward(src, src + length_, dst + length_);

  return Status::OK();
}
#undef __LESS

template <typename new_type>
Status AdaptiveIntBuilder::ExpandIntSizeN() {
  switch (int_size_) {
    case 1:
      RETURN_NOT_OK((ExpandIntSizeInternal<new_type, int8_t>()));
      break;
    case 2:
      RETURN_NOT_OK((ExpandIntSizeInternal<new_type, int16_t>()));
      break;
    case 4:
      RETURN_NOT_OK((ExpandIntSizeInternal<new_type, int32_t>()));
      break;
    case 8:
      RETURN_NOT_OK((ExpandIntSizeInternal<new_type, int64_t>()));
      break;
    default:
      DCHECK(false);
  }
  return Status::OK();
}

Status AdaptiveIntBuilder::ExpandIntSize(uint8_t new_int_size) {
  switch (new_int_size) {
    case 1:
      RETURN_NOT_OK((ExpandIntSizeN<int8_t>()));
      break;
    case 2:
      RETURN_NOT_OK((ExpandIntSizeN<int16_t>()));
      break;
    case 4:
      RETURN_NOT_OK((ExpandIntSizeN<int32_t>()));
      break;
    case 8:
      RETURN_NOT_OK((ExpandIntSizeN<int64_t>()));
      break;
    default:
      DCHECK(false);
  }
  return Status::OK();
}

AdaptiveUIntBuilder::AdaptiveUIntBuilder(MemoryPool* pool)
    : AdaptiveIntBuilderBase(pool) {}

Status AdaptiveUIntBuilder::FinishInternal(std::shared_ptr<ArrayData>* out) {
  RETURN_NOT_OK(CommitPendingData());

  std::shared_ptr<DataType> output_type;
  switch (int_size_) {
    case 1:
      output_type = uint8();
      break;
    case 2:
      output_type = uint16();
      break;
    case 4:
      output_type = uint32();
      break;
    case 8:
      output_type = uint64();
      break;
    default:
      DCHECK(false);
      return Status::NotImplemented("Only ints of size 1,2,4,8 are supported");
  }

  RETURN_NOT_OK(TrimBuffer(BitUtil::BytesForBits(length_), null_bitmap_.get()));
  RETURN_NOT_OK(TrimBuffer(length_ * int_size_, data_.get()));

  *out = ArrayData::Make(output_type, length_, {null_bitmap_, data_}, null_count_);

  data_ = null_bitmap_ = nullptr;
  capacity_ = length_ = null_count_ = 0;
  return Status::OK();
}

Status AdaptiveUIntBuilder::AppendValuesInternal(const uint64_t* values, int64_t length,
                                                 const uint8_t* valid_bytes) {
  while (length > 0) {
    // See AdaptiveIntBuilder::AppendValuesInternal
    const int64_t chunk_size = std::min(length, kAdaptiveIntChunkSize);

    uint8_t new_int_size;
    new_int_size = internal::DetectUIntWidth(values, valid_bytes, chunk_size, int_size_);

    DCHECK_GE(new_int_size, int_size_);
    if (new_int_size > int_size_) {
      // This updates int_size_
      RETURN_NOT_OK(ExpandIntSize(new_int_size));
    }

    switch (int_size_) {
      case 1:
        internal::DowncastUInts(values, reinterpret_cast<uint8_t*>(raw_data_) + length_,
                                chunk_size);
        break;
      case 2:
        internal::DowncastUInts(values, reinterpret_cast<uint16_t*>(raw_data_) + length_,
                                chunk_size);
        break;
      case 4:
        internal::DowncastUInts(values, reinterpret_cast<uint32_t*>(raw_data_) + length_,
                                chunk_size);
        break;
      case 8:
        internal::DowncastUInts(values, reinterpret_cast<uint64_t*>(raw_data_) + length_,
                                chunk_size);
        break;
      default:
        DCHECK(false);
    }

    // This updates length_
    ArrayBuilder::UnsafeAppendToBitmap(valid_bytes, chunk_size);
    values += chunk_size;
    if (valid_bytes != nullptr) {
      valid_bytes += chunk_size;
    }
    length -= chunk_size;
  }

  return Status::OK();
}

Status AdaptiveUIntBuilder::AppendValues(const uint64_t* values, int64_t length,
                                         const uint8_t* valid_bytes) {
  RETURN_NOT_OK(Reserve(length));

  return AppendValuesInternal(values, length, valid_bytes);
}

template <typename new_type, typename old_type>
typename std::enable_if<sizeof(old_type) >= sizeof(new_type), Status>::type
AdaptiveUIntBuilder::ExpandIntSizeInternal() {
  return Status::OK();
}

#define __LESS(a, b) (a) < (b)
template <typename new_type, typename old_type>
typename std::enable_if<__LESS(sizeof(old_type), sizeof(new_type)), Status>::type
AdaptiveUIntBuilder::ExpandIntSizeInternal() {
  int_size_ = sizeof(new_type);
  RETURN_NOT_OK(Resize(data_->size() / sizeof(old_type)));

  old_type* src = reinterpret_cast<old_type*>(raw_data_);
  new_type* dst = reinterpret_cast<new_type*>(raw_data_);
  // By doing the backward copy, we ensure that no element is overriden during
  // the copy process and the copy stays in-place.
  std::copy_backward(src, src + length_, dst + length_);

  return Status::OK();
}
#undef __LESS

template <typename new_type>
Status AdaptiveUIntBuilder::ExpandIntSizeN() {
  switch (int_size_) {
    case 1:
      RETURN_NOT_OK((ExpandIntSizeInternal<new_type, uint8_t>()));
      break;
    case 2:
      RETURN_NOT_OK((ExpandIntSizeInternal<new_type, uint16_t>()));
      break;
    case 4:
      RETURN_NOT_OK((ExpandIntSizeInternal<new_type, uint32_t>()));
      break;
    case 8:
      RETURN_NOT_OK((ExpandIntSizeInternal<new_type, uint64_t>()));
      break;
    default:
      DCHECK(false);
  }
  return Status::OK();
}

Status AdaptiveUIntBuilder::ExpandIntSize(uint8_t new_int_size) {
  switch (new_int_size) {
    case 1:
      RETURN_NOT_OK((ExpandIntSizeN<uint8_t>()));
      break;
    case 2:
      RETURN_NOT_OK((ExpandIntSizeN<uint16_t>()));
      break;
    case 4:
      RETURN_NOT_OK((ExpandIntSizeN<uint32_t>()));
      break;
    case 8:
      RETURN_NOT_OK((ExpandIntSizeN<uint64_t>()));
      break;
    default:
      DCHECK(false);
  }
  return Status::OK();
}

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
// DictionaryBuilder

template <typename T>
class DictionaryBuilder<T>::MemoTableImpl
    : public internal::HashTraits<T>::MemoTableType {
 public:
  using MemoTableType = typename internal::HashTraits<T>::MemoTableType;
  using MemoTableType::MemoTableType;
};

template <typename T>
DictionaryBuilder<T>::~DictionaryBuilder() {}

template <typename T>
DictionaryBuilder<T>::DictionaryBuilder(const std::shared_ptr<DataType>& type,
                                        MemoryPool* pool)
    : ArrayBuilder(type, pool), byte_width_(-1), values_builder_(pool) {
  DCHECK_EQ(T::type_id, type->id()) << "inconsistent type passed to DictionaryBuilder";
}

DictionaryBuilder<NullType>::DictionaryBuilder(const std::shared_ptr<DataType>& type,
                                               MemoryPool* pool)
    : ArrayBuilder(type, pool), values_builder_(pool) {
  DCHECK_EQ(Type::NA, type->id()) << "inconsistent type passed to DictionaryBuilder";
}

template <>
DictionaryBuilder<FixedSizeBinaryType>::DictionaryBuilder(
    const std::shared_ptr<DataType>& type, MemoryPool* pool)
    : ArrayBuilder(type, pool),
      byte_width_(checked_cast<const FixedSizeBinaryType&>(*type).byte_width()) {}

template <typename T>
void DictionaryBuilder<T>::Reset() {
  ArrayBuilder::Reset();
  values_builder_.Reset();
  memo_table_.reset();
  delta_offset_ = 0;
}

template <typename T>
Status DictionaryBuilder<T>::Resize(int64_t capacity) {
  RETURN_NOT_OK(CheckCapacity(capacity, capacity_));
  capacity = std::max(capacity, kMinBuilderCapacity);

  if (capacity_ == 0) {
    // Initialize hash table
    // XXX should we let the user pass additional size heuristics?
    memo_table_.reset(new MemoTableImpl(0));
    delta_offset_ = 0;
  }
  RETURN_NOT_OK(values_builder_.Resize(capacity));
  return ArrayBuilder::Resize(capacity);
}

Status DictionaryBuilder<NullType>::Resize(int64_t capacity) {
  RETURN_NOT_OK(CheckCapacity(capacity, capacity_));
  capacity = std::max(capacity, kMinBuilderCapacity);

  RETURN_NOT_OK(values_builder_.Resize(capacity));
  return ArrayBuilder::Resize(capacity);
}

template <typename T>
Status DictionaryBuilder<T>::Append(const Scalar& value) {
  RETURN_NOT_OK(Reserve(1));

  auto memo_index = memo_table_->GetOrInsert(value);
  RETURN_NOT_OK(values_builder_.Append(memo_index));

  return Status::OK();
}

template <typename T>
Status DictionaryBuilder<T>::AppendNull() {
  return values_builder_.AppendNull();
}

Status DictionaryBuilder<NullType>::AppendNull() { return values_builder_.AppendNull(); }

template <typename T>
Status DictionaryBuilder<T>::AppendArray(const Array& array) {
  const auto& numeric_array = checked_cast<const NumericArray<T>&>(array);
  for (int64_t i = 0; i < array.length(); i++) {
    if (array.IsNull(i)) {
      RETURN_NOT_OK(AppendNull());
    } else {
      RETURN_NOT_OK(Append(numeric_array.Value(i)));
    }
  }
  return Status::OK();
}

Status DictionaryBuilder<NullType>::AppendArray(const Array& array) {
  for (int64_t i = 0; i < array.length(); i++) {
    RETURN_NOT_OK(AppendNull());
  }
  return Status::OK();
}

template <typename T>
Status DictionaryBuilder<T>::FinishInternal(std::shared_ptr<ArrayData>* out) {
  // Finalize indices array
  RETURN_NOT_OK(values_builder_.FinishInternal(out));

  // Generate dictionary array from hash table contents
  std::shared_ptr<Array> dictionary;
  std::shared_ptr<ArrayData> dictionary_data;

  RETURN_NOT_OK(internal::DictionaryTraits<T>::GetDictionaryArrayData(
      pool_, type_, *memo_table_, delta_offset_, &dictionary_data));
  dictionary = MakeArray(dictionary_data);

  // Set type of array data to the right dictionary type
  (*out)->type = std::make_shared<DictionaryType>((*out)->type, dictionary);

  // Update internals for further uses of this DictionaryBuilder
  delta_offset_ = memo_table_->size();
  values_builder_.Reset();

  return Status::OK();
}

Status DictionaryBuilder<NullType>::FinishInternal(std::shared_ptr<ArrayData>* out) {
  std::shared_ptr<Array> dictionary = std::make_shared<NullArray>(0);

  RETURN_NOT_OK(values_builder_.FinishInternal(out));
  (*out)->type = std::make_shared<DictionaryType>((*out)->type, dictionary);

  return Status::OK();
}

//
// StringType and BinaryType specializations
//

#define BINARY_DICTIONARY_SPECIALIZATIONS(Type)                            \
                                                                           \
  template <>                                                              \
  Status DictionaryBuilder<Type>::AppendArray(const Array& array) {        \
    using ArrayType = typename TypeTraits<Type>::ArrayType;                \
    const ArrayType& binary_array = checked_cast<const ArrayType&>(array); \
    for (int64_t i = 0; i < array.length(); i++) {                         \
      if (array.IsNull(i)) {                                               \
        RETURN_NOT_OK(AppendNull());                                       \
      } else {                                                             \
        RETURN_NOT_OK(Append(binary_array.GetView(i)));                    \
      }                                                                    \
    }                                                                      \
    return Status::OK();                                                   \
  }

BINARY_DICTIONARY_SPECIALIZATIONS(StringType);
BINARY_DICTIONARY_SPECIALIZATIONS(BinaryType);

template <>
Status DictionaryBuilder<FixedSizeBinaryType>::AppendArray(const Array& array) {
  if (!type_->Equals(*array.type())) {
    return Status::Invalid("Cannot append FixedSizeBinary array with non-matching type");
  }

  const auto& typed_array = checked_cast<const FixedSizeBinaryArray&>(array);
  for (int64_t i = 0; i < array.length(); i++) {
    if (array.IsNull(i)) {
      RETURN_NOT_OK(AppendNull());
    } else {
      RETURN_NOT_OK(Append(typed_array.GetValue(i)));
    }
  }
  return Status::OK();
}

template class DictionaryBuilder<UInt8Type>;
template class DictionaryBuilder<UInt16Type>;
template class DictionaryBuilder<UInt32Type>;
template class DictionaryBuilder<UInt64Type>;
template class DictionaryBuilder<Int8Type>;
template class DictionaryBuilder<Int16Type>;
template class DictionaryBuilder<Int32Type>;
template class DictionaryBuilder<Int64Type>;
template class DictionaryBuilder<Date32Type>;
template class DictionaryBuilder<Date64Type>;
template class DictionaryBuilder<Time32Type>;
template class DictionaryBuilder<Time64Type>;
template class DictionaryBuilder<TimestampType>;
template class DictionaryBuilder<FloatType>;
template class DictionaryBuilder<DoubleType>;
template class DictionaryBuilder<FixedSizeBinaryType>;
template class DictionaryBuilder<BinaryType>;
template class DictionaryBuilder<StringType>;

// ----------------------------------------------------------------------
// Decimal128Builder

Decimal128Builder::Decimal128Builder(const std::shared_ptr<DataType>& type,
                                     MemoryPool* pool)
    : FixedSizeBinaryBuilder(type, pool) {}

Status Decimal128Builder::Append(const Decimal128& value) {
  RETURN_NOT_OK(FixedSizeBinaryBuilder::Reserve(1));
  return FixedSizeBinaryBuilder::Append(value.ToBytes());
}

Status Decimal128Builder::FinishInternal(std::shared_ptr<ArrayData>* out) {
  std::shared_ptr<Buffer> data;
  RETURN_NOT_OK(byte_builder_.Finish(&data));

  *out = ArrayData::Make(type_, length_, {null_bitmap_, data}, null_count_);

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
// String and binary

BinaryBuilder::BinaryBuilder(const std::shared_ptr<DataType>& type, MemoryPool* pool)
    : ArrayBuilder(type, pool), offsets_builder_(pool), value_data_builder_(pool) {}

BinaryBuilder::BinaryBuilder(MemoryPool* pool) : BinaryBuilder(binary(), pool) {}

Status BinaryBuilder::Resize(int64_t capacity) {
  DCHECK_LE(capacity, kListMaximumElements);
  RETURN_NOT_OK(CheckCapacity(capacity, capacity_));

  // one more then requested for offsets
  RETURN_NOT_OK(offsets_builder_.Resize((capacity + 1) * sizeof(int32_t)));
  return ArrayBuilder::Resize(capacity);
}

Status BinaryBuilder::ReserveData(int64_t elements) {
  if (value_data_length() + elements > value_data_capacity()) {
    if (value_data_length() + elements > kBinaryMemoryLimit) {
      return Status::CapacityError(
          "Cannot reserve capacity larger than 2^31 - 1 for binary");
    }
    RETURN_NOT_OK(value_data_builder_.Reserve(elements));
  }
  return Status::OK();
}

Status BinaryBuilder::AppendNextOffset() {
  const int64_t num_bytes = value_data_builder_.length();
  if (ARROW_PREDICT_FALSE(num_bytes > kBinaryMemoryLimit)) {
    std::stringstream ss;
    ss << "BinaryArray cannot contain more than " << kBinaryMemoryLimit << " bytes, have "
       << num_bytes;
    return Status::CapacityError(ss.str());
  }
  return offsets_builder_.Append(static_cast<int32_t>(num_bytes));
}

Status BinaryBuilder::Append(const uint8_t* value, int32_t length) {
  RETURN_NOT_OK(Reserve(1));
  RETURN_NOT_OK(AppendNextOffset());
  RETURN_NOT_OK(value_data_builder_.Append(value, length));

  UnsafeAppendToBitmap(true);
  return Status::OK();
}

Status BinaryBuilder::AppendNull() {
  RETURN_NOT_OK(AppendNextOffset());
  RETURN_NOT_OK(Reserve(1));

  UnsafeAppendToBitmap(false);
  return Status::OK();
}

Status BinaryBuilder::FinishInternal(std::shared_ptr<ArrayData>* out) {
  // Write final offset (values length)
  RETURN_NOT_OK(AppendNextOffset());

  // These buffers' padding zeroed by BufferBuilder
  std::shared_ptr<Buffer> offsets, value_data;
  RETURN_NOT_OK(offsets_builder_.Finish(&offsets));
  RETURN_NOT_OK(value_data_builder_.Finish(&value_data));

  *out = ArrayData::Make(type_, length_, {null_bitmap_, offsets, value_data}, null_count_,
                         0);
  Reset();
  return Status::OK();
}

void BinaryBuilder::Reset() {
  ArrayBuilder::Reset();
  offsets_builder_.Reset();
  value_data_builder_.Reset();
}

const uint8_t* BinaryBuilder::GetValue(int64_t i, int32_t* out_length) const {
  const int32_t* offsets = offsets_builder_.data();
  int32_t offset = offsets[i];
  if (i == (length_ - 1)) {
    *out_length = static_cast<int32_t>(value_data_builder_.length()) - offset;
  } else {
    *out_length = offsets[i + 1] - offset;
  }
  return value_data_builder_.data() + offset;
}

util::string_view BinaryBuilder::GetView(int64_t i) const {
  const int32_t* offsets = offsets_builder_.data();
  int32_t offset = offsets[i];
  int32_t value_length;
  if (i == (length_ - 1)) {
    value_length = static_cast<int32_t>(value_data_builder_.length()) - offset;
  } else {
    value_length = offsets[i + 1] - offset;
  }
  return util::string_view(
      reinterpret_cast<const char*>(value_data_builder_.data() + offset), value_length);
}

StringBuilder::StringBuilder(MemoryPool* pool) : BinaryBuilder(utf8(), pool) {}

Status StringBuilder::AppendValues(const std::vector<std::string>& values,
                                   const uint8_t* valid_bytes) {
  std::size_t total_length = std::accumulate(
      values.begin(), values.end(), 0ULL,
      [](uint64_t sum, const std::string& str) { return sum + str.size(); });
  RETURN_NOT_OK(Reserve(values.size()));
  RETURN_NOT_OK(value_data_builder_.Reserve(total_length));
  RETURN_NOT_OK(offsets_builder_.Reserve(values.size()));

  if (valid_bytes) {
    for (std::size_t i = 0; i < values.size(); ++i) {
      RETURN_NOT_OK(AppendNextOffset());
      if (valid_bytes[i]) {
        RETURN_NOT_OK(value_data_builder_.Append(
            reinterpret_cast<const uint8_t*>(values[i].data()), values[i].size()));
      }
    }
  } else {
    for (std::size_t i = 0; i < values.size(); ++i) {
      RETURN_NOT_OK(AppendNextOffset());
      RETURN_NOT_OK(value_data_builder_.Append(
          reinterpret_cast<const uint8_t*>(values[i].data()), values[i].size()));
    }
  }

  UnsafeAppendToBitmap(valid_bytes, values.size());
  return Status::OK();
}

Status StringBuilder::AppendValues(const char** values, int64_t length,
                                   const uint8_t* valid_bytes) {
  std::size_t total_length = 0;
  std::vector<std::size_t> value_lengths(length);
  bool have_null_value = false;
  for (int64_t i = 0; i < length; ++i) {
    if (values[i]) {
      auto value_length = strlen(values[i]);
      value_lengths[i] = value_length;
      total_length += value_length;
    } else {
      have_null_value = true;
    }
  }
  RETURN_NOT_OK(Reserve(length));
  RETURN_NOT_OK(value_data_builder_.Reserve(total_length));
  RETURN_NOT_OK(offsets_builder_.Reserve(length));

  if (valid_bytes) {
    int64_t valid_bytes_offset = 0;
    for (int64_t i = 0; i < length; ++i) {
      RETURN_NOT_OK(AppendNextOffset());
      if (valid_bytes[i]) {
        if (values[i]) {
          RETURN_NOT_OK(value_data_builder_.Append(
              reinterpret_cast<const uint8_t*>(values[i]), value_lengths[i]));
        } else {
          UnsafeAppendToBitmap(valid_bytes + valid_bytes_offset, i - valid_bytes_offset);
          UnsafeAppendToBitmap(false);
          valid_bytes_offset = i + 1;
        }
      }
    }
    UnsafeAppendToBitmap(valid_bytes + valid_bytes_offset, length - valid_bytes_offset);
  } else {
    if (have_null_value) {
      std::vector<uint8_t> valid_vector(length, 0);
      for (int64_t i = 0; i < length; ++i) {
        RETURN_NOT_OK(AppendNextOffset());
        if (values[i]) {
          RETURN_NOT_OK(value_data_builder_.Append(
              reinterpret_cast<const uint8_t*>(values[i]), value_lengths[i]));
          valid_vector[i] = 1;
        }
      }
      UnsafeAppendToBitmap(valid_vector.data(), length);
    } else {
      for (int64_t i = 0; i < length; ++i) {
        RETURN_NOT_OK(AppendNextOffset());
        RETURN_NOT_OK(value_data_builder_.Append(
            reinterpret_cast<const uint8_t*>(values[i]), value_lengths[i]));
      }
      UnsafeAppendToBitmap(nullptr, length);
    }
  }
  return Status::OK();
}

// ----------------------------------------------------------------------
// Fixed width binary

FixedSizeBinaryBuilder::FixedSizeBinaryBuilder(const std::shared_ptr<DataType>& type,
                                               MemoryPool* pool)
    : ArrayBuilder(type, pool),
      byte_width_(checked_cast<const FixedSizeBinaryType&>(*type).byte_width()),
      byte_builder_(pool) {}

#ifndef NDEBUG
void FixedSizeBinaryBuilder::CheckValueSize(int64_t size) {
  DCHECK_EQ(size, byte_width_) << "Appending wrong size to FixedSizeBinaryBuilder";
}
#endif

Status FixedSizeBinaryBuilder::AppendValues(const uint8_t* data, int64_t length,
                                            const uint8_t* valid_bytes) {
  RETURN_NOT_OK(Reserve(length));
  UnsafeAppendToBitmap(valid_bytes, length);
  return byte_builder_.Append(data, length * byte_width_);
}

Status FixedSizeBinaryBuilder::AppendNull() {
  RETURN_NOT_OK(Reserve(1));
  UnsafeAppendToBitmap(false);
  return byte_builder_.Advance(byte_width_);
}

void FixedSizeBinaryBuilder::Reset() {
  ArrayBuilder::Reset();
  byte_builder_.Reset();
}

Status FixedSizeBinaryBuilder::Resize(int64_t capacity) {
  RETURN_NOT_OK(CheckCapacity(capacity, capacity_));
  RETURN_NOT_OK(byte_builder_.Resize(capacity * byte_width_));
  return ArrayBuilder::Resize(capacity);
}

Status FixedSizeBinaryBuilder::FinishInternal(std::shared_ptr<ArrayData>* out) {
  std::shared_ptr<Buffer> data;
  RETURN_NOT_OK(byte_builder_.Finish(&data));

  *out = ArrayData::Make(type_, length_, {null_bitmap_, data}, null_count_);

  null_bitmap_ = nullptr;
  capacity_ = length_ = null_count_ = 0;
  return Status::OK();
}

const uint8_t* FixedSizeBinaryBuilder::GetValue(int64_t i) const {
  const uint8_t* data_ptr = byte_builder_.data();
  return data_ptr + i * byte_width_;
}

util::string_view FixedSizeBinaryBuilder::GetView(int64_t i) const {
  const uint8_t* data_ptr = byte_builder_.data();
  return util::string_view(reinterpret_cast<const char*>(data_ptr + i * byte_width_),
                           byte_width_);
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
