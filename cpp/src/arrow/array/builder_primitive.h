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

#pragma once

#include <algorithm>
#include <memory>
#include <vector>

#include "arrow/array/builder_base.h"
#include "arrow/type.h"

namespace arrow {

class ARROW_EXPORT NullBuilder : public ArrayBuilder {
 public:
  explicit NullBuilder(MemoryPool* pool ARROW_MEMORY_POOL_DEFAULT)
      : ArrayBuilder(null(), pool) {}

  Status AppendNull() {
    ++null_count_;
    ++length_;
    return Status::OK();
  }

  Status Append(std::nullptr_t value) { return AppendNull(); }

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

  /// \brief Append a single null element
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
    static_assert(!internal::is_null_pointer<ValidIter>::value,
                  "Don't pass a NULLPTR directly as valid_begin, use the 2-argument "
                  "version instead");
    int64_t length = static_cast<int64_t>(std::distance(values_begin, values_end));
    ARROW_RETURN_NOT_OK(Reserve(length));

    std::copy(values_begin, values_end, raw_data_ + length_);

    // this updates the length_
    UnsafeAppendToBitmap(valid_begin, std::next(valid_begin, length));
    return Status::OK();
  }

  // Same as above, with a pointer type ValidIter
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

  using ArrayBuilder::UnsafeAppendNull;
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

class ARROW_EXPORT BooleanBuilder : public ArrayBuilder {
 public:
  using value_type = bool;
  explicit BooleanBuilder(MemoryPool* pool ARROW_MEMORY_POOL_DEFAULT);

  explicit BooleanBuilder(const std::shared_ptr<DataType>& type, MemoryPool* pool);

  using ArrayBuilder::Advance;
  using ArrayBuilder::UnsafeAppendNull;

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
    UnsafeAppend(val);
    return Status::OK();
  }

  Status Append(const uint8_t val) { return Append(val != 0); }

  /// Scalar append, without checking for capacity
  void UnsafeAppend(const bool val) {
    BitUtil::SetBit(null_bitmap_data_, length_);
    if (val) {
      BitUtil::SetBit(raw_data_, length_);
    } else {
      BitUtil::ClearBit(raw_data_, length_);
    }
    ++length_;
  }

  void UnsafeAppend(const uint8_t val) { UnsafeAppend(val != 0); }

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
    static_assert(!internal::is_null_pointer<ValidIter>::value,
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

  // Same as above, for a pointer type ValidIter
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

}  // namespace arrow
