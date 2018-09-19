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

#ifndef ARROW_BUILDER_H
#define ARROW_BUILDER_H

#include <algorithm>
#include <array>
#include <cstdint>
#include <functional>
#include <limits>
#include <memory>
#include <string>
#include <vector>

#include "arrow/buffer.h"
#include "arrow/memory_pool.h"
#include "arrow/status.h"
#include "arrow/type.h"
#include "arrow/type_traits.h"
#include "arrow/util/bit-util.h"
#include "arrow/util/hash.h"
#include "arrow/util/macros.h"
#include "arrow/util/visibility.h"

namespace arrow {

class Array;
class Decimal128;

constexpr int64_t kBinaryMemoryLimit = std::numeric_limits<int32_t>::max() - 1;
constexpr int64_t kListMaximumElements = std::numeric_limits<int32_t>::max() - 1;

namespace internal {

struct ArrayData;

}  // namespace internal

constexpr int64_t kMinBuilderCapacity = 1 << 5;

/// Base class for all data array builders.
//
/// This class provides a facilities for incrementally building the null bitmap
/// (see Append methods) and as a side effect the current number of slots and
/// the null count.
class ARROW_EXPORT ArrayBuilder {
 public:
  explicit ArrayBuilder(const std::shared_ptr<DataType>& type, MemoryPool* pool)
      : type_(type),
        pool_(pool),
        null_bitmap_(NULLPTR),
        null_count_(0),
        null_bitmap_data_(NULLPTR),
        length_(0),
        capacity_(0) {}

  virtual ~ArrayBuilder() = default;

  /// For nested types. Since the objects are owned by this class instance, we
  /// skip shared pointers and just return a raw pointer
  ArrayBuilder* child(int i) { return children_[i].get(); }

  int num_children() const { return static_cast<int>(children_.size()); }

  int64_t length() const { return length_; }
  int64_t null_count() const { return null_count_; }
  int64_t capacity() const { return capacity_; }

  /// Append to null bitmap
  Status AppendToBitmap(bool is_valid);

  /// Vector append. Treat each zero byte as a null.   If valid_bytes is null
  /// assume all of length bits are valid.
  Status AppendToBitmap(const uint8_t* valid_bytes, int64_t length);

  /// Set the next length bits to not null (i.e. valid).
  Status SetNotNull(int64_t length);

  /// \brief Ensure that enough memory has been allocated to fit the indicated
  /// number of total elements in the builder, including any that have already
  /// been appended. Does not account for reallocations that may be due to
  /// variable size data, like binary values. To make space for incremental
  /// appends, use Reserve instead.
  /// \param[in] capacity the minimum number of additional array values
  /// \return Status
  virtual Status Resize(int64_t capacity);

  /// \brief Ensure that there is enough space allocated to add the indicated
  /// number of elements without any further calls to Resize. The memory
  /// allocated is rounded up to the next highest power of 2 similar to memory
  /// allocations in STL containers like std::vector
  /// \param[in] additional_capacity the number of additional array values
  /// \return Status
  Status Reserve(int64_t additional_capacity);

  /// Reset the builder.
  virtual void Reset();

  /// For cases where raw data was memcpy'd into the internal buffers, allows us
  /// to advance the length of the builder. It is your responsibility to use
  /// this function responsibly.
  Status Advance(int64_t elements);

  /// \brief Return result of builder as an internal generic ArrayData
  /// object. Resets builder except for dictionary builder
  ///
  /// \param[out] out the finalized ArrayData object
  /// \return Status
  virtual Status FinishInternal(std::shared_ptr<ArrayData>* out) = 0;

  /// \brief Return result of builder as an Array object.
  ///        Resets the builder except for DictionaryBuilder
  ///
  /// \param[out] out the finalized Array object
  /// \return Status
  Status Finish(std::shared_ptr<Array>* out);

  std::shared_ptr<DataType> type() const { return type_; }

  // Unsafe operations (don't check capacity/don't resize)

  // Append to null bitmap, update the length
  void UnsafeAppendToBitmap(bool is_valid) {
    if (is_valid) {
      BitUtil::SetBit(null_bitmap_data_, length_);
    } else {
      ++null_count_;
    }
    ++length_;
  }

  template <typename IterType>
  void UnsafeAppendToBitmap(const IterType& begin, const IterType& end) {
    int64_t byte_offset = length_ / 8;
    int64_t bit_offset = length_ % 8;
    uint8_t bitset = null_bitmap_data_[byte_offset];

    for (auto iter = begin; iter != end; ++iter) {
      if (bit_offset == 8) {
        bit_offset = 0;
        null_bitmap_data_[byte_offset] = bitset;
        byte_offset++;
        // TODO: Except for the last byte, this shouldn't be needed
        bitset = null_bitmap_data_[byte_offset];
      }

      if (*iter) {
        bitset |= BitUtil::kBitmask[bit_offset];
      } else {
        bitset &= BitUtil::kFlippedBitmask[bit_offset];
        ++null_count_;
      }

      bit_offset++;
    }

    if (bit_offset != 0) {
      null_bitmap_data_[byte_offset] = bitset;
    }

    length_ += std::distance(begin, end);
  }

 protected:
  ArrayBuilder() {}

  std::shared_ptr<DataType> type_;
  MemoryPool* pool_;

  // When null_bitmap are first appended to the builder, the null bitmap is allocated
  std::shared_ptr<ResizableBuffer> null_bitmap_;
  int64_t null_count_;
  uint8_t* null_bitmap_data_;

  // Array length, so far. Also, the index of the next element to be added
  int64_t length_;
  int64_t capacity_;

  // Child value array builders. These are owned by this class
  std::vector<std::unique_ptr<ArrayBuilder>> children_;

  // Vector append. Treat each zero byte as a nullzero. If valid_bytes is null
  // assume all of length bits are valid.
  void UnsafeAppendToBitmap(const uint8_t* valid_bytes, int64_t length);

  void UnsafeAppendToBitmap(const std::vector<bool>& is_valid);

  // Set the next length bits to not null (i.e. valid).
  void UnsafeSetNotNull(int64_t length);

 private:
  ARROW_DISALLOW_COPY_AND_ASSIGN(ArrayBuilder);
};

class ARROW_EXPORT NullBuilder : public ArrayBuilder {
 public:
  explicit NullBuilder(MemoryPool* pool ARROW_MEMORY_POOL_DEFAULT)
      : ArrayBuilder(null(), pool) {}

  Status AppendNull() {
    ++null_count_;
    ++length_;
    return Status::OK();
  }

  Status FinishInternal(std::shared_ptr<ArrayData>* out) override;
};

template <typename Type>
class ARROW_EXPORT PrimitiveBuilder : public ArrayBuilder {
 public:
  using value_type = typename Type::c_type;

  explicit PrimitiveBuilder(const std::shared_ptr<DataType>& type, MemoryPool* pool)
      : ArrayBuilder(type, pool), data_(NULLPTR), raw_data_(NULLPTR) {}

  using ArrayBuilder::Advance;

  /// Write nulls as uint8_t* (0 value indicates null) into pre-allocated memory
  /// The memory at the corresponding data slot is set to 0 to prevent uninitialized
  /// memory access
  Status AppendNulls(const uint8_t* valid_bytes, int64_t length) {
    ARROW_RETURN_NOT_OK(Reserve(length));
    memset(raw_data_ + length_, 0,
           static_cast<size_t>(TypeTraits<Type>::bytes_required(length)));
    UnsafeAppendToBitmap(valid_bytes, length);
    return Status::OK();
  }

  Status AppendNull() {
    ARROW_RETURN_NOT_OK(Reserve(1));
    memset(raw_data_ + length_, 0, sizeof(value_type));
    UnsafeAppendToBitmap(false);
    return Status::OK();
  }

  value_type GetValue(int64_t index) const {
    return reinterpret_cast<const value_type*>(data_->data())[index];
  }

  /// \brief Append a sequence of elements in one shot
  /// \param[in] values a contiguous C array of values
  /// \param[in] length the number of values to append
  /// \param[in] valid_bytes an optional sequence of bytes where non-zero
  /// indicates a valid (non-null) value
  /// \return Status
  Status AppendValues(const value_type* values, int64_t length,
                      const uint8_t* valid_bytes = NULLPTR);

  /// \brief Append a sequence of elements in one shot
  /// \param[in] values a contiguous C array of values
  /// \param[in] length the number of values to append
  /// \param[in] is_valid an std::vector<bool> indicating valid (1) or null
  /// (0). Equal in length to values
  /// \return Status
  Status AppendValues(const value_type* values, int64_t length,
                      const std::vector<bool>& is_valid);

  /// \brief Append a sequence of elements in one shot
  /// \param[in] values a std::vector of values
  /// \param[in] is_valid an std::vector<bool> indicating valid (1) or null
  /// (0). Equal in length to values
  /// \return Status
  Status AppendValues(const std::vector<value_type>& values,
                      const std::vector<bool>& is_valid);

  /// \brief Append a sequence of elements in one shot
  /// \param[in] values a std::vector of values
  /// \return Status
  Status AppendValues(const std::vector<value_type>& values);

  /// \brief Append a sequence of elements in one shot
  /// \param[in] values_begin InputIterator to the beginning of the values
  /// \param[in] values_end InputIterator pointing to the end of the values
  /// \return Status

  template <typename ValuesIter>
  Status AppendValues(ValuesIter values_begin, ValuesIter values_end) {
    int64_t length = static_cast<int64_t>(std::distance(values_begin, values_end));
    ARROW_RETURN_NOT_OK(Reserve(length));

    std::copy(values_begin, values_end, raw_data_ + length_);

    // this updates the length_
    UnsafeSetNotNull(length);
    return Status::OK();
  }

  /// \brief Append a sequence of elements in one shot, with a specified nullmap
  /// \param[in] values_begin InputIterator to the beginning of the values
  /// \param[in] values_end InputIterator pointing to the end of the values
  /// \param[in] valid_begin InputIterator with elements indication valid(1)
  ///  or null(0) values.
  /// \return Status
  template <typename ValuesIter, typename ValidIter>
  typename std::enable_if<!std::is_pointer<ValidIter>::value, Status>::type AppendValues(
      ValuesIter values_begin, ValuesIter values_end, ValidIter valid_begin) {
    static_assert(!is_null_pointer<ValidIter>::value,
                  "Don't pass a NULLPTR directly as valid_begin, use the 2-argument "
                  "version instead");
    int64_t length = static_cast<int64_t>(std::distance(values_begin, values_end));
    ARROW_RETURN_NOT_OK(Reserve(length));

    std::copy(values_begin, values_end, raw_data_ + length_);

    // this updates the length_
    UnsafeAppendToBitmap(valid_begin, std::next(valid_begin, length));
    return Status::OK();
  }

  /// \brief Append a sequence of elements in one shot, with a specified nullmap
  /// \param[in] values_begin InputIterator to the beginning of the values
  /// \param[in] values_end InputIterator pointing to the end of the values
  /// \param[in] valid_begin uint8_t* indication valid(1) or null(0) values.
  ///  nullptr indicates all values are valid.
  /// \return Status
  template <typename ValuesIter, typename ValidIter>
  typename std::enable_if<std::is_pointer<ValidIter>::value, Status>::type AppendValues(
      ValuesIter values_begin, ValuesIter values_end, ValidIter valid_begin) {
    int64_t length = static_cast<int64_t>(std::distance(values_begin, values_end));
    ARROW_RETURN_NOT_OK(Reserve(length));

    std::copy(values_begin, values_end, raw_data_ + length_);

    // this updates the length_
    if (valid_begin == NULLPTR) {
      UnsafeSetNotNull(length);
    } else {
      UnsafeAppendToBitmap(valid_begin, std::next(valid_begin, length));
    }

    return Status::OK();
  }

  Status FinishInternal(std::shared_ptr<ArrayData>* out) override;
  void Reset() override;

  Status Resize(int64_t capacity) override;

 protected:
  std::shared_ptr<ResizableBuffer> data_;
  value_type* raw_data_;
};

/// Base class for all Builders that emit an Array of a scalar numerical type.
template <typename T>
class ARROW_EXPORT NumericBuilder : public PrimitiveBuilder<T> {
 public:
  using typename PrimitiveBuilder<T>::value_type;
  using PrimitiveBuilder<T>::PrimitiveBuilder;

  template <typename T1 = T>
  explicit NumericBuilder(
      typename std::enable_if<TypeTraits<T1>::is_parameter_free, MemoryPool*>::type pool
          ARROW_MEMORY_POOL_DEFAULT)
      : PrimitiveBuilder<T1>(TypeTraits<T1>::type_singleton(), pool) {}

  using PrimitiveBuilder<T>::AppendValues;
  using PrimitiveBuilder<T>::Resize;
  using PrimitiveBuilder<T>::Reserve;

  /// Append a single scalar and increase the size if necessary.
  Status Append(const value_type val) {
    ARROW_RETURN_NOT_OK(ArrayBuilder::Reserve(1));
    UnsafeAppend(val);
    return Status::OK();
  }

  /// Append a single scalar under the assumption that the underlying Buffer is
  /// large enough.
  ///
  /// This method does not capacity-check; make sure to call Reserve
  /// beforehand.
  void UnsafeAppend(const value_type val) {
    BitUtil::SetBit(null_bitmap_data_, length_);
    raw_data_[length_++] = val;
  }

 protected:
  using PrimitiveBuilder<T>::length_;
  using PrimitiveBuilder<T>::null_bitmap_data_;
  using PrimitiveBuilder<T>::raw_data_;
};

// Builders

using UInt8Builder = NumericBuilder<UInt8Type>;
using UInt16Builder = NumericBuilder<UInt16Type>;
using UInt32Builder = NumericBuilder<UInt32Type>;
using UInt64Builder = NumericBuilder<UInt64Type>;

using Int8Builder = NumericBuilder<Int8Type>;
using Int16Builder = NumericBuilder<Int16Type>;
using Int32Builder = NumericBuilder<Int32Type>;
using Int64Builder = NumericBuilder<Int64Type>;
using TimestampBuilder = NumericBuilder<TimestampType>;
using Time32Builder = NumericBuilder<Time32Type>;
using Time64Builder = NumericBuilder<Time64Type>;
using Date32Builder = NumericBuilder<Date32Type>;
using Date64Builder = NumericBuilder<Date64Type>;

using HalfFloatBuilder = NumericBuilder<HalfFloatType>;
using FloatBuilder = NumericBuilder<FloatType>;
using DoubleBuilder = NumericBuilder<DoubleType>;

namespace internal {

class ARROW_EXPORT AdaptiveIntBuilderBase : public ArrayBuilder {
 public:
  explicit AdaptiveIntBuilderBase(MemoryPool* pool);

  /// Write nulls as uint8_t* (0 value indicates null) into pre-allocated memory
  Status AppendNulls(const uint8_t* valid_bytes, int64_t length) {
    ARROW_RETURN_NOT_OK(Reserve(length));
    memset(data_->mutable_data() + length_ * int_size_, 0, int_size_ * length);
    UnsafeAppendToBitmap(valid_bytes, length);
    return Status::OK();
  }

  Status AppendNull() {
    ARROW_RETURN_NOT_OK(Reserve(1));
    memset(data_->mutable_data() + length_ * int_size_, 0, int_size_);
    UnsafeAppendToBitmap(false);
    return Status::OK();
  }

  void Reset() override;
  Status Resize(int64_t capacity) override;

 protected:
  std::shared_ptr<ResizableBuffer> data_;
  uint8_t* raw_data_;

  uint8_t int_size_;
};

// Check if we would need to expand the underlying storage type
inline uint8_t ExpandedIntSize(int64_t val, uint8_t current_int_size) {
  if (current_int_size == 8 ||
      (current_int_size < 8 &&
       (val > static_cast<int64_t>(std::numeric_limits<int32_t>::max()) ||
        val < static_cast<int64_t>(std::numeric_limits<int32_t>::min())))) {
    return 8;
  } else if (current_int_size == 4 ||
             (current_int_size < 4 &&
              (val > static_cast<int64_t>(std::numeric_limits<int16_t>::max()) ||
               val < static_cast<int64_t>(std::numeric_limits<int16_t>::min())))) {
    return 4;
  } else if (current_int_size == 2 ||
             (current_int_size == 1 &&
              (val > static_cast<int64_t>(std::numeric_limits<int8_t>::max()) ||
               val < static_cast<int64_t>(std::numeric_limits<int8_t>::min())))) {
    return 2;
  } else {
    return 1;
  }
}

// Check if we would need to expand the underlying storage type
inline uint8_t ExpandedUIntSize(uint64_t val, uint8_t current_int_size) {
  if (current_int_size == 8 ||
      (current_int_size < 8 &&
       (val > static_cast<uint64_t>(std::numeric_limits<uint32_t>::max())))) {
    return 8;
  } else if (current_int_size == 4 ||
             (current_int_size < 4 &&
              (val > static_cast<uint64_t>(std::numeric_limits<uint16_t>::max())))) {
    return 4;
  } else if (current_int_size == 2 ||
             (current_int_size == 1 &&
              (val > static_cast<uint64_t>(std::numeric_limits<uint8_t>::max())))) {
    return 2;
  } else {
    return 1;
  }
}

}  // namespace internal

class ARROW_EXPORT AdaptiveUIntBuilder : public internal::AdaptiveIntBuilderBase {
 public:
  explicit AdaptiveUIntBuilder(MemoryPool* pool ARROW_MEMORY_POOL_DEFAULT);

  using ArrayBuilder::Advance;
  using internal::AdaptiveIntBuilderBase::Reset;

  /// Scalar append
  Status Append(const uint64_t val) {
    ARROW_RETURN_NOT_OK(Reserve(1));
    BitUtil::SetBit(null_bitmap_data_, length_);

    uint8_t new_int_size = internal::ExpandedUIntSize(val, int_size_);
    if (new_int_size != int_size_) {
      ARROW_RETURN_NOT_OK(ExpandIntSize(new_int_size));
    }

    switch (int_size_) {
      case 1:
        reinterpret_cast<uint8_t*>(raw_data_)[length_++] = static_cast<uint8_t>(val);
        break;
      case 2:
        reinterpret_cast<uint16_t*>(raw_data_)[length_++] = static_cast<uint16_t>(val);
        break;
      case 4:
        reinterpret_cast<uint32_t*>(raw_data_)[length_++] = static_cast<uint32_t>(val);
        break;
      case 8:
        reinterpret_cast<uint64_t*>(raw_data_)[length_++] = val;
        break;
      default:
        return Status::NotImplemented("This code shall never be reached");
    }
    return Status::OK();
  }

  /// \brief Append a sequence of elements in one shot
  /// \param[in] values a contiguous C array of values
  /// \param[in] length the number of values to append
  /// \param[in] valid_bytes an optional sequence of bytes where non-zero
  /// indicates a valid (non-null) value
  /// \return Status
  Status AppendValues(const uint64_t* values, int64_t length,
                      const uint8_t* valid_bytes = NULLPTR);

  Status FinishInternal(std::shared_ptr<ArrayData>* out) override;

 protected:
  Status ExpandIntSize(uint8_t new_int_size);

  template <typename new_type, typename old_type>
  typename std::enable_if<sizeof(old_type) >= sizeof(new_type), Status>::type
  ExpandIntSizeInternal();
#define __LESS(a, b) (a) < (b)
  template <typename new_type, typename old_type>
  typename std::enable_if<__LESS(sizeof(old_type), sizeof(new_type)), Status>::type
  ExpandIntSizeInternal();
#undef __LESS

  template <typename new_type>
  Status ExpandIntSizeN();
};

class ARROW_EXPORT AdaptiveIntBuilder : public internal::AdaptiveIntBuilderBase {
 public:
  explicit AdaptiveIntBuilder(MemoryPool* pool ARROW_MEMORY_POOL_DEFAULT);

  using ArrayBuilder::Advance;
  using internal::AdaptiveIntBuilderBase::Reset;

  /// Scalar append
  Status Append(const int64_t val) {
    ARROW_RETURN_NOT_OK(Reserve(1));
    BitUtil::SetBit(null_bitmap_data_, length_);

    uint8_t new_int_size = internal::ExpandedIntSize(val, int_size_);
    if (new_int_size != int_size_) {
      ARROW_RETURN_NOT_OK(ExpandIntSize(new_int_size));
    }

    switch (int_size_) {
      case 1:
        reinterpret_cast<int8_t*>(raw_data_)[length_++] = static_cast<int8_t>(val);
        break;
      case 2:
        reinterpret_cast<int16_t*>(raw_data_)[length_++] = static_cast<int16_t>(val);
        break;
      case 4:
        reinterpret_cast<int32_t*>(raw_data_)[length_++] = static_cast<int32_t>(val);
        break;
      case 8:
        reinterpret_cast<int64_t*>(raw_data_)[length_++] = val;
        break;
      default:
        return Status::NotImplemented("This code shall never be reached");
    }
    return Status::OK();
  }

  /// \brief Append a sequence of elements in one shot
  /// \param[in] values a contiguous C array of values
  /// \param[in] length the number of values to append
  /// \param[in] valid_bytes an optional sequence of bytes where non-zero
  /// indicates a valid (non-null) value
  /// \return Status
  Status AppendValues(const int64_t* values, int64_t length,
                      const uint8_t* valid_bytes = NULLPTR);

  Status FinishInternal(std::shared_ptr<ArrayData>* out) override;

 protected:
  Status ExpandIntSize(uint8_t new_int_size);

  template <typename new_type, typename old_type>
  typename std::enable_if<sizeof(old_type) >= sizeof(new_type), Status>::type
  ExpandIntSizeInternal();
#define __LESS(a, b) (a) < (b)
  template <typename new_type, typename old_type>
  typename std::enable_if<__LESS(sizeof(old_type), sizeof(new_type)), Status>::type
  ExpandIntSizeInternal();
#undef __LESS

  template <typename new_type>
  Status ExpandIntSizeN();
};

class ARROW_EXPORT BooleanBuilder : public ArrayBuilder {
 public:
  using value_type = bool;
  explicit BooleanBuilder(MemoryPool* pool ARROW_MEMORY_POOL_DEFAULT);

  explicit BooleanBuilder(const std::shared_ptr<DataType>& type, MemoryPool* pool);

  using ArrayBuilder::Advance;

  /// Write nulls as uint8_t* (0 value indicates null) into pre-allocated memory
  Status AppendNulls(const uint8_t* valid_bytes, int64_t length) {
    ARROW_RETURN_NOT_OK(Reserve(length));
    UnsafeAppendToBitmap(valid_bytes, length);

    return Status::OK();
  }

  Status AppendNull() {
    ARROW_RETURN_NOT_OK(Reserve(1));
    UnsafeAppendToBitmap(false);

    return Status::OK();
  }

  /// Scalar append
  Status Append(const bool val) {
    ARROW_RETURN_NOT_OK(Reserve(1));
    BitUtil::SetBit(null_bitmap_data_, length_);
    if (val) {
      BitUtil::SetBit(raw_data_, length_);
    } else {
      BitUtil::ClearBit(raw_data_, length_);
    }
    ++length_;
    return Status::OK();
  }

  Status Append(const uint8_t val) { return Append(val != 0); }

  /// \brief Append a sequence of elements in one shot
  /// \param[in] values a contiguous array of bytes (non-zero is 1)
  /// \param[in] length the number of values to append
  /// \param[in] valid_bytes an optional sequence of bytes where non-zero
  /// indicates a valid (non-null) value
  /// \return Status
  Status AppendValues(const uint8_t* values, int64_t length,
                      const uint8_t* valid_bytes = NULLPTR);

  /// \brief Append a sequence of elements in one shot
  /// \param[in] values a contiguous C array of values
  /// \param[in] length the number of values to append
  /// \param[in] is_valid an std::vector<bool> indicating valid (1) or null
  /// (0). Equal in length to values
  /// \return Status
  Status AppendValues(const uint8_t* values, int64_t length,
                      const std::vector<bool>& is_valid);

  /// \brief Append a sequence of elements in one shot
  /// \param[in] values a std::vector of bytes
  /// \param[in] is_valid an std::vector<bool> indicating valid (1) or null
  /// (0). Equal in length to values
  /// \return Status
  Status AppendValues(const std::vector<uint8_t>& values,
                      const std::vector<bool>& is_valid);

  /// \brief Append a sequence of elements in one shot
  /// \param[in] values a std::vector of bytes
  /// \return Status
  Status AppendValues(const std::vector<uint8_t>& values);

  /// \brief Append a sequence of elements in one shot
  /// \param[in] values an std::vector<bool> indicating true (1) or false
  /// \param[in] is_valid an std::vector<bool> indicating valid (1) or null
  /// (0). Equal in length to values
  /// \return Status
  Status AppendValues(const std::vector<bool>& values, const std::vector<bool>& is_valid);

  /// \brief Append a sequence of elements in one shot
  /// \param[in] values an std::vector<bool> indicating true (1) or false
  /// \return Status
  Status AppendValues(const std::vector<bool>& values);

  /// \brief Append a sequence of elements in one shot
  /// \param[in] values_begin InputIterator to the beginning of the values
  /// \param[in] values_end InputIterator pointing to the end of the values
  ///  or null(0) values
  /// \return Status
  template <typename ValuesIter>
  Status AppendValues(ValuesIter values_begin, ValuesIter values_end) {
    int64_t length = static_cast<int64_t>(std::distance(values_begin, values_end));
    ARROW_RETURN_NOT_OK(Reserve(length));
    auto iter = values_begin;
    internal::GenerateBitsUnrolled(raw_data_, length_, length,
                                   [&iter]() -> bool { return *(iter++); });

    // this updates length_
    UnsafeSetNotNull(length);
    return Status::OK();
  }

  /// \brief Append a sequence of elements in one shot, with a specified nullmap
  /// \param[in] values_begin InputIterator to the beginning of the values
  /// \param[in] values_end InputIterator pointing to the end of the values
  /// \param[in] valid_begin InputIterator with elements indication valid(1)
  ///  or null(0) values
  /// \return Status
  template <typename ValuesIter, typename ValidIter>
  typename std::enable_if<!std::is_pointer<ValidIter>::value, Status>::type AppendValues(
      ValuesIter values_begin, ValuesIter values_end, ValidIter valid_begin) {
    static_assert(!is_null_pointer<ValidIter>::value,
                  "Don't pass a NULLPTR directly as valid_begin, use the 2-argument "
                  "version instead");
    int64_t length = static_cast<int64_t>(std::distance(values_begin, values_end));
    ARROW_RETURN_NOT_OK(Reserve(length));

    auto iter = values_begin;
    internal::GenerateBitsUnrolled(raw_data_, length_, length,
                                   [&iter]() -> bool { return *(iter++); });

    // this updates length_
    ArrayBuilder::UnsafeAppendToBitmap(valid_begin, std::next(valid_begin, length));
    return Status::OK();
  }

  /// \brief Append a sequence of elements in one shot, with a specified nullmap
  /// \param[in] values_begin InputIterator to the beginning of the values
  /// \param[in] values_end InputIterator pointing to the end of the values
  /// \param[in] valid_begin uint8_t* indication valid(1) or null(0) values.
  ///  nullptr indicates all values are valid.
  /// \return Status
  template <typename ValuesIter, typename ValidIter>
  typename std::enable_if<std::is_pointer<ValidIter>::value, Status>::type AppendValues(
      ValuesIter values_begin, ValuesIter values_end, ValidIter valid_begin) {
    int64_t length = static_cast<int64_t>(std::distance(values_begin, values_end));
    ARROW_RETURN_NOT_OK(Reserve(length));

    auto iter = values_begin;
    internal::GenerateBitsUnrolled(raw_data_, length_, length,
                                   [&iter]() -> bool { return *(iter++); });

    // this updates the length_
    if (valid_begin == NULLPTR) {
      UnsafeSetNotNull(length);
    } else {
      UnsafeAppendToBitmap(valid_begin, std::next(valid_begin, length));
    }

    return Status::OK();
  }

  Status FinishInternal(std::shared_ptr<ArrayData>* out) override;
  void Reset() override;
  Status Resize(int64_t capacity) override;

 protected:
  std::shared_ptr<ResizableBuffer> data_;
  uint8_t* raw_data_;
};

// ----------------------------------------------------------------------
// List builder

/// \class ListBuilder
/// \brief Builder class for variable-length list array value types
///
/// To use this class, you must append values to the child array builder and use
/// the Append function to delimit each distinct list value (once the values
/// have been appended to the child array) or use the bulk API to append
/// a sequence of offests and null values.
///
/// A note on types.  Per arrow/type.h all types in the c++ implementation are
/// logical so even though this class always builds list array, this can
/// represent multiple different logical types.  If no logical type is provided
/// at construction time, the class defaults to List<T> where t is taken from the
/// value_builder/values that the object is constructed with.
class ARROW_EXPORT ListBuilder : public ArrayBuilder {
 public:
  /// Use this constructor to incrementally build the value array along with offsets and
  /// null bitmap.
  ListBuilder(MemoryPool* pool, std::shared_ptr<ArrayBuilder> const& value_builder,
              const std::shared_ptr<DataType>& type = NULLPTR);

  Status Resize(int64_t capacity) override;
  void Reset() override;
  Status FinishInternal(std::shared_ptr<ArrayData>* out) override;

  /// \brief Vector append
  ///
  /// If passed, valid_bytes is of equal length to values, and any zero byte
  /// will be considered as a null for that slot
  Status AppendValues(const int32_t* offsets, int64_t length,
                      const uint8_t* valid_bytes = NULLPTR);

  /// \brief Start a new variable-length list slot
  ///
  /// This function should be called before beginning to append elements to the
  /// value builder
  Status Append(bool is_valid = true);

  Status AppendNull() { return Append(false); }

  ArrayBuilder* value_builder() const;

 protected:
  TypedBufferBuilder<int32_t> offsets_builder_;
  std::shared_ptr<ArrayBuilder> value_builder_;
  std::shared_ptr<Array> values_;

  Status AppendNextOffset();
};

// ----------------------------------------------------------------------
// Binary and String

/// \class BinaryBuilder
/// \brief Builder class for variable-length binary data
class ARROW_EXPORT BinaryBuilder : public ArrayBuilder {
 public:
  explicit BinaryBuilder(MemoryPool* pool ARROW_MEMORY_POOL_DEFAULT);

  BinaryBuilder(const std::shared_ptr<DataType>& type, MemoryPool* pool);

  Status Append(const uint8_t* value, int32_t length);

  Status Append(const char* value, int32_t length) {
    return Append(reinterpret_cast<const uint8_t*>(value), length);
  }

  Status Append(const std::string& value) {
    return Append(value.c_str(), static_cast<int32_t>(value.size()));
  }

  Status AppendNull();

  void Reset() override;
  Status Resize(int64_t capacity) override;

  /// \brief Ensures there is enough allocated capacity to append the indicated
  /// number of bytes to the value data buffer without additional allocations
  Status ReserveData(int64_t elements);

  Status FinishInternal(std::shared_ptr<ArrayData>* out) override;

  /// \return size of values buffer so far
  int64_t value_data_length() const { return value_data_builder_.length(); }
  /// \return capacity of values buffer
  int64_t value_data_capacity() const { return value_data_builder_.capacity(); }

  /// Temporary access to a value.
  ///
  /// This pointer becomes invalid on the next modifying operation.
  const uint8_t* GetValue(int64_t i, int32_t* out_length) const;

 protected:
  TypedBufferBuilder<int32_t> offsets_builder_;
  TypedBufferBuilder<uint8_t> value_data_builder_;

  Status AppendNextOffset();
};

/// \class StringBuilder
/// \brief Builder class for UTF8 strings
class ARROW_EXPORT StringBuilder : public BinaryBuilder {
 public:
  using BinaryBuilder::BinaryBuilder;
  explicit StringBuilder(MemoryPool* pool ARROW_MEMORY_POOL_DEFAULT);

  using BinaryBuilder::Append;
  using BinaryBuilder::Reset;

  /// \brief Append a sequence of strings in one shot.
  ///
  /// \param[in] values a vector of strings
  /// \param[in] valid_bytes an optional sequence of bytes where non-zero
  /// indicates a valid (non-null) value
  /// \return Status
  Status AppendValues(const std::vector<std::string>& values,
                      const uint8_t* valid_bytes = NULLPTR);

  /// \brief Append a sequence of nul-terminated strings in one shot.
  ///        If one of the values is NULL, it is processed as a null
  ///        value even if the corresponding valid_bytes entry is 1.
  ///
  /// \param[in] values a contiguous C array of nul-terminated char *
  /// \param[in] length the number of values to append
  /// \param[in] valid_bytes an optional sequence of bytes where non-zero
  /// indicates a valid (non-null) value
  /// \return Status
  Status AppendValues(const char** values, int64_t length,
                      const uint8_t* valid_bytes = NULLPTR);
};

// ----------------------------------------------------------------------
// FixedSizeBinaryBuilder

class ARROW_EXPORT FixedSizeBinaryBuilder : public ArrayBuilder {
 public:
  FixedSizeBinaryBuilder(const std::shared_ptr<DataType>& type,
                         MemoryPool* pool ARROW_MEMORY_POOL_DEFAULT);

  Status Append(const uint8_t* value) {
    ARROW_RETURN_NOT_OK(Reserve(1));
    UnsafeAppendToBitmap(true);
    return byte_builder_.Append(value, byte_width_);
  }
  Status Append(const char* value) {
    return Append(reinterpret_cast<const uint8_t*>(value));
  }

  template <size_t NBYTES>
  Status Append(const std::array<uint8_t, NBYTES>& value) {
    ARROW_RETURN_NOT_OK(Reserve(1));
    UnsafeAppendToBitmap(true);
    return byte_builder_.Append(value);
  }

  Status AppendValues(const uint8_t* data, int64_t length,
                      const uint8_t* valid_bytes = NULLPTR);
  Status Append(const std::string& value);
  Status AppendNull();

  void Reset() override;
  Status Resize(int64_t capacity) override;
  Status FinishInternal(std::shared_ptr<ArrayData>* out) override;

  /// \return size of values buffer so far
  int64_t value_data_length() const { return byte_builder_.length(); }

  int32_t byte_width() const { return byte_width_; }

  /// Temporary access to a value.
  ///
  /// This pointer becomes invalid on the next modifying operation.
  const uint8_t* GetValue(int64_t i) const;

 protected:
  int32_t byte_width_;
  BufferBuilder byte_builder_;
};

class ARROW_EXPORT Decimal128Builder : public FixedSizeBinaryBuilder {
 public:
  explicit Decimal128Builder(const std::shared_ptr<DataType>& type,
                             MemoryPool* pool ARROW_MEMORY_POOL_DEFAULT);

  using FixedSizeBinaryBuilder::Append;
  using FixedSizeBinaryBuilder::AppendValues;
  using FixedSizeBinaryBuilder::Reset;

  Status Append(const Decimal128& val);

  Status FinishInternal(std::shared_ptr<ArrayData>* out) override;
};

using DecimalBuilder = Decimal128Builder;

// ----------------------------------------------------------------------
// Struct

// ---------------------------------------------------------------------------------
// StructArray builder
/// Append, Resize and Reserve methods are acting on StructBuilder.
/// Please make sure all these methods of all child-builders' are consistently
/// called to maintain data-structure consistency.
class ARROW_EXPORT StructBuilder : public ArrayBuilder {
 public:
  StructBuilder(const std::shared_ptr<DataType>& type, MemoryPool* pool,
                std::vector<std::shared_ptr<ArrayBuilder>>&& field_builders);

  Status FinishInternal(std::shared_ptr<ArrayData>* out) override;

  /// Null bitmap is of equal length to every child field, and any zero byte
  /// will be considered as a null for that field, but users must using app-
  /// end methods or advance methods of the child builders' independently to
  /// insert data.
  Status AppendValues(int64_t length, const uint8_t* valid_bytes) {
    ARROW_RETURN_NOT_OK(Reserve(length));
    UnsafeAppendToBitmap(valid_bytes, length);
    return Status::OK();
  }

  /// Append an element to the Struct. All child-builders' Append method must
  /// be called independently to maintain data-structure consistency.
  Status Append(bool is_valid = true) {
    ARROW_RETURN_NOT_OK(Reserve(1));
    UnsafeAppendToBitmap(is_valid);
    return Status::OK();
  }

  Status AppendNull() { return Append(false); }

  void Reset() override;

  ArrayBuilder* field_builder(int i) const { return field_builders_[i].get(); }

  int num_fields() const { return static_cast<int>(field_builders_.size()); }

 protected:
  std::vector<std::shared_ptr<ArrayBuilder>> field_builders_;
};

// ----------------------------------------------------------------------
// Dictionary builder

namespace internal {

// TODO(ARROW-1176): Use Tensorflow's StringPiece instead of this here.
struct WrappedBinary {
  WrappedBinary(const uint8_t* ptr, int32_t length) : ptr_(ptr), length_(length) {}

  const uint8_t* ptr_;
  int32_t length_;
};

template <typename T>
struct DictionaryScalar {
  using type = typename T::c_type;
};

template <>
struct DictionaryScalar<BinaryType> {
  using type = WrappedBinary;
};

template <>
struct DictionaryScalar<StringType> {
  using type = WrappedBinary;
};

template <>
struct DictionaryScalar<FixedSizeBinaryType> {
  using type = const uint8_t*;
};

}  // namespace internal

/// \brief Array builder for created encoded DictionaryArray from dense array
///
/// Unlike other builders, dictionary builder does not completely reset the state
/// on Finish calls. The arrays built after the initial Finish call will reuse
/// the previously created encoding and build a delta dictionary when new terms
/// occur.
///
/// data
template <typename T>
class ARROW_EXPORT DictionaryBuilder : public ArrayBuilder {
 public:
  using Scalar = typename internal::DictionaryScalar<T>::type;

  DictionaryBuilder(const std::shared_ptr<DataType>& type, MemoryPool* pool);

  template <typename T1 = T>
  explicit DictionaryBuilder(
      typename std::enable_if<TypeTraits<T1>::is_parameter_free, MemoryPool*>::type pool)
      : DictionaryBuilder<T1>(TypeTraits<T1>::type_singleton(), pool) {}

  /// \brief Append a scalar value
  Status Append(const Scalar& value);

  /// \brief Append a scalar null value
  Status AppendNull();

  /// \brief Append a whole dense array to the builder
  Status AppendArray(const Array& array);

  void Reset() override;
  Status Resize(int64_t capacity) override;
  Status FinishInternal(std::shared_ptr<ArrayData>* out) override;

  /// is the dictionary builder in the delta building mode
  bool is_building_delta() { return entry_id_offset_ > 0; }

 protected:
  // Hash table implementation helpers
  Status DoubleTableSize();
  Scalar GetDictionaryValue(typename TypeTraits<T>::BuilderType& dictionary_builder,
                            int64_t index);
  int64_t HashValue(const Scalar& value);
  // Check whether the dictionary entry in *slot* is equal to the given *value*
  bool SlotDifferent(hash_slot_t slot, const Scalar& value);
  Status AppendDictionary(const Scalar& value);

  std::shared_ptr<Buffer> hash_table_;
  int32_t* hash_slots_;

  /// Size of the table. Must be a power of 2.
  int64_t hash_table_size_;

  // Offset for the dictionary entries in dict_builder_.
  // Increased on every Finish call by the number of current entries
  // in the dictionary.
  int64_t entry_id_offset_;

  // Store hash_table_size_ - 1, so that j & mod_bitmask_ is equivalent to j %
  // hash_table_size_, but uses far fewer CPU cycles
  int64_t mod_bitmask_;

  // This builder accumulates new dictionary entries since the last Finish call
  // (or since the beginning if Finish hasn't been called).
  // In other words, it contains the current delta dictionary.
  typename TypeTraits<T>::BuilderType dict_builder_;
  // This builder stores dictionary entries encountered before the last Finish call.
  typename TypeTraits<T>::BuilderType overflow_dict_builder_;

  AdaptiveIntBuilder values_builder_;
  int32_t byte_width_;

  /// Size at which we decide to resize
  int64_t hash_table_load_threshold_;
};

template <>
class ARROW_EXPORT DictionaryBuilder<NullType> : public ArrayBuilder {
 public:
  DictionaryBuilder(const std::shared_ptr<DataType>& type, MemoryPool* pool);
  explicit DictionaryBuilder(MemoryPool* pool);

  /// \brief Append a scalar null value
  Status AppendNull();

  /// \brief Append a whole dense array to the builder
  Status AppendArray(const Array& array);

  Status Resize(int64_t capacity) override;
  Status FinishInternal(std::shared_ptr<ArrayData>* out) override;

 protected:
  AdaptiveIntBuilder values_builder_;
};

class ARROW_EXPORT BinaryDictionaryBuilder : public DictionaryBuilder<BinaryType> {
 public:
  using DictionaryBuilder::Append;
  using DictionaryBuilder::DictionaryBuilder;

  Status Append(const uint8_t* value, int32_t length) {
    return Append(internal::WrappedBinary(value, length));
  }

  Status Append(const char* value, int32_t length) {
    return Append(
        internal::WrappedBinary(reinterpret_cast<const uint8_t*>(value), length));
  }

  Status Append(const std::string& value) {
    return Append(internal::WrappedBinary(reinterpret_cast<const uint8_t*>(value.c_str()),
                                          static_cast<int32_t>(value.size())));
  }
};

/// \brief Dictionary array builder with convenience methods for strings
class ARROW_EXPORT StringDictionaryBuilder : public DictionaryBuilder<StringType> {
 public:
  using DictionaryBuilder::Append;
  using DictionaryBuilder::DictionaryBuilder;

  Status Append(const uint8_t* value, int32_t length) {
    return Append(internal::WrappedBinary(value, length));
  }

  Status Append(const char* value, int32_t length) {
    return Append(
        internal::WrappedBinary(reinterpret_cast<const uint8_t*>(value), length));
  }

  Status Append(const std::string& value) {
    return Append(internal::WrappedBinary(reinterpret_cast<const uint8_t*>(value.c_str()),
                                          static_cast<int32_t>(value.size())));
  }
};

// ----------------------------------------------------------------------
// Helper functions

ARROW_EXPORT
Status MakeBuilder(MemoryPool* pool, const std::shared_ptr<DataType>& type,
                   std::unique_ptr<ArrayBuilder>* out);

}  // namespace arrow

#endif  // ARROW_BUILDER_H_
