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
#include "arrow/array/data.h"
#include "arrow/result.h"
#include "arrow/type.h"
#include "arrow/type_traits.h"

namespace arrow {

class ARROW_EXPORT NullBuilder : public ArrayBuilder {
 public:
  explicit NullBuilder(MemoryPool* pool = default_memory_pool()) : ArrayBuilder(pool) {}
  explicit NullBuilder(const std::shared_ptr<DataType>& type,
                       MemoryPool* pool = default_memory_pool())
      : NullBuilder(pool) {}

  /// \brief Append the specified number of null elements
  Status AppendNulls(int64_t length) final {
    if (length < 0) return Status::Invalid("length must be positive");
    null_count_ += length;
    length_ += length;
    return Status::OK();
  }

  /// \brief Append a single null element
  Status AppendNull() final { return AppendNulls(1); }

  Status AppendEmptyValues(int64_t length) final { return AppendNulls(length); }

  Status AppendEmptyValue() final { return AppendEmptyValues(1); }

  Status Append(std::nullptr_t) { return AppendNull(); }

  Status FinishInternal(std::shared_ptr<ArrayData>* out) override;

  /// \cond FALSE
  using ArrayBuilder::Finish;
  /// \endcond

  std::shared_ptr<DataType> type() const override { return null(); }

  Status Finish(std::shared_ptr<NullArray>* out) { return FinishTyped(out); }
};

/// Base class for all Builders that emit an Array of a scalar numerical type.
template <typename T>
class NumericBuilder : public ArrayBuilder {
 public:
  using TypeClass = T;
  using value_type = typename T::c_type;
  using ArrayType = typename TypeTraits<T>::ArrayType;

  template <typename T1 = T>
  explicit NumericBuilder(
      enable_if_parameter_free<T1, MemoryPool*> pool = default_memory_pool())
      : ArrayBuilder(pool), type_(TypeTraits<T>::type_singleton()), data_builder_(pool) {}

  NumericBuilder(const std::shared_ptr<DataType>& type, MemoryPool* pool)
      : ArrayBuilder(pool), type_(type), data_builder_(pool) {}

  /// Append a single scalar and increase the size if necessary.
  Status Append(const value_type val) {
    ARROW_RETURN_NOT_OK(ArrayBuilder::Reserve(1));
    UnsafeAppend(val);
    return Status::OK();
  }

  /// Write nulls as uint8_t* (0 value indicates null) into pre-allocated memory
  /// The memory at the corresponding data slot is set to 0 to prevent
  /// uninitialized memory access
  Status AppendNulls(int64_t length) final {
    ARROW_RETURN_NOT_OK(Reserve(length));
    data_builder_.UnsafeAppend(length, value_type{});  // zero
    UnsafeSetNull(length);
    return Status::OK();
  }

  /// \brief Append a single null element
  Status AppendNull() final {
    ARROW_RETURN_NOT_OK(Reserve(1));
    data_builder_.UnsafeAppend(value_type{});  // zero
    UnsafeAppendToBitmap(false);
    return Status::OK();
  }

  /// \brief Append a empty element
  Status AppendEmptyValue() final {
    ARROW_RETURN_NOT_OK(Reserve(1));
    data_builder_.UnsafeAppend(value_type{});  // zero
    UnsafeAppendToBitmap(true);
    return Status::OK();
  }

  /// \brief Append several empty elements
  Status AppendEmptyValues(int64_t length) final {
    ARROW_RETURN_NOT_OK(Reserve(length));
    data_builder_.UnsafeAppend(length, value_type{});  // zero
    UnsafeSetNotNull(length);
    return Status::OK();
  }

  value_type GetValue(int64_t index) const { return data_builder_.data()[index]; }

  void Reset() override { data_builder_.Reset(); }

  Status Resize(int64_t capacity) override {
    ARROW_RETURN_NOT_OK(CheckCapacity(capacity));
    capacity = std::max(capacity, kMinBuilderCapacity);
    ARROW_RETURN_NOT_OK(data_builder_.Resize(capacity));
    return ArrayBuilder::Resize(capacity);
  }

  value_type operator[](int64_t index) const { return GetValue(index); }

  value_type& operator[](int64_t index) {
    return reinterpret_cast<value_type*>(data_builder_.mutable_data())[index];
  }

  /// \brief Append a sequence of elements in one shot
  /// \param[in] values a contiguous C array of values
  /// \param[in] length the number of values to append
  /// \param[in] valid_bytes an optional sequence of bytes where non-zero
  /// indicates a valid (non-null) value
  /// \return Status
  Status AppendValues(const value_type* values, int64_t length,
                      const uint8_t* valid_bytes = NULLPTR) {
    ARROW_RETURN_NOT_OK(Reserve(length));
    data_builder_.UnsafeAppend(values, length);
    // length_ is update by these
    ArrayBuilder::UnsafeAppendToBitmap(valid_bytes, length);
    return Status::OK();
  }

  /// \brief Append a sequence of elements in one shot
  /// \param[in] values a contiguous C array of values
  /// \param[in] length the number of values to append
  /// \param[in] is_valid an std::vector<bool> indicating valid (1) or null
  /// (0). Equal in length to values
  /// \return Status
  Status AppendValues(const value_type* values, int64_t length,
                      const std::vector<bool>& is_valid) {
    ARROW_RETURN_NOT_OK(Reserve(length));
    data_builder_.UnsafeAppend(values, length);
    // length_ is update by these
    ArrayBuilder::UnsafeAppendToBitmap(is_valid);
    return Status::OK();
  }

  /// \brief Append a sequence of elements in one shot
  /// \param[in] values a std::vector of values
  /// \param[in] is_valid an std::vector<bool> indicating valid (1) or null
  /// (0). Equal in length to values
  /// \return Status
  Status AppendValues(const std::vector<value_type>& values,
                      const std::vector<bool>& is_valid) {
    return AppendValues(values.data(), static_cast<int64_t>(values.size()), is_valid);
  }

  /// \brief Append a sequence of elements in one shot
  /// \param[in] values a std::vector of values
  /// \return Status
  Status AppendValues(const std::vector<value_type>& values) {
    return AppendValues(values.data(), static_cast<int64_t>(values.size()));
  }

  Status FinishInternal(std::shared_ptr<ArrayData>* out) override {
    ARROW_ASSIGN_OR_RAISE(auto null_bitmap,
                          null_bitmap_builder_.FinishWithLength(length_));
    ARROW_ASSIGN_OR_RAISE(auto data, data_builder_.FinishWithLength(length_));
    *out = ArrayData::Make(type(), length_, {null_bitmap, data}, null_count_);
    capacity_ = length_ = null_count_ = 0;
    return Status::OK();
  }

  /// \cond FALSE
  using ArrayBuilder::Finish;
  /// \endcond

  Status Finish(std::shared_ptr<ArrayType>* out) { return FinishTyped(out); }

  /// \brief Append a sequence of elements in one shot
  /// \param[in] values_begin InputIterator to the beginning of the values
  /// \param[in] values_end InputIterator pointing to the end of the values
  /// \return Status
  template <typename ValuesIter>
  Status AppendValues(ValuesIter values_begin, ValuesIter values_end) {
    int64_t length = static_cast<int64_t>(std::distance(values_begin, values_end));
    ARROW_RETURN_NOT_OK(Reserve(length));
    data_builder_.UnsafeAppend(values_begin, values_end);
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
  enable_if_t<!std::is_pointer<ValidIter>::value, Status> AppendValues(
      ValuesIter values_begin, ValuesIter values_end, ValidIter valid_begin) {
    static_assert(!internal::is_null_pointer<ValidIter>::value,
                  "Don't pass a NULLPTR directly as valid_begin, use the 2-argument "
                  "version instead");
    int64_t length = static_cast<int64_t>(std::distance(values_begin, values_end));
    ARROW_RETURN_NOT_OK(Reserve(length));
    data_builder_.UnsafeAppend(values_begin, values_end);
    null_bitmap_builder_.UnsafeAppend<true>(
        length, [&valid_begin]() -> bool { return *valid_begin++; });
    length_ = null_bitmap_builder_.length();
    null_count_ = null_bitmap_builder_.false_count();
    return Status::OK();
  }

  // Same as above, with a pointer type ValidIter
  template <typename ValuesIter, typename ValidIter>
  enable_if_t<std::is_pointer<ValidIter>::value, Status> AppendValues(
      ValuesIter values_begin, ValuesIter values_end, ValidIter valid_begin) {
    int64_t length = static_cast<int64_t>(std::distance(values_begin, values_end));
    ARROW_RETURN_NOT_OK(Reserve(length));
    data_builder_.UnsafeAppend(values_begin, values_end);
    // this updates the length_
    if (valid_begin == NULLPTR) {
      UnsafeSetNotNull(length);
    } else {
      null_bitmap_builder_.UnsafeAppend<true>(
          length, [&valid_begin]() -> bool { return *valid_begin++; });
      length_ = null_bitmap_builder_.length();
      null_count_ = null_bitmap_builder_.false_count();
    }

    return Status::OK();
  }

  /// Append a single scalar under the assumption that the underlying Buffer is
  /// large enough.
  ///
  /// This method does not capacity-check; make sure to call Reserve
  /// beforehand.
  void UnsafeAppend(const value_type val) {
    ArrayBuilder::UnsafeAppendToBitmap(true);
    data_builder_.UnsafeAppend(val);
  }

  void UnsafeAppendNull() {
    ArrayBuilder::UnsafeAppendToBitmap(false);
    data_builder_.UnsafeAppend(value_type{});  // zero
  }

  std::shared_ptr<DataType> type() const override { return type_; }

 protected:
  std::shared_ptr<DataType> type_;
  TypedBufferBuilder<value_type> data_builder_;
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

using HalfFloatBuilder = NumericBuilder<HalfFloatType>;
using FloatBuilder = NumericBuilder<FloatType>;
using DoubleBuilder = NumericBuilder<DoubleType>;

class ARROW_EXPORT BooleanBuilder : public ArrayBuilder {
 public:
  using TypeClass = BooleanType;
  using value_type = bool;

  explicit BooleanBuilder(MemoryPool* pool = default_memory_pool());

  BooleanBuilder(const std::shared_ptr<DataType>& type,
                 MemoryPool* pool = default_memory_pool());

  /// Write nulls as uint8_t* (0 value indicates null) into pre-allocated memory
  Status AppendNulls(int64_t length) final {
    ARROW_RETURN_NOT_OK(Reserve(length));
    data_builder_.UnsafeAppend(length, false);
    UnsafeSetNull(length);
    return Status::OK();
  }

  Status AppendNull() final {
    ARROW_RETURN_NOT_OK(Reserve(1));
    UnsafeAppendNull();
    return Status::OK();
  }

  Status AppendEmptyValue() final {
    ARROW_RETURN_NOT_OK(Reserve(1));
    data_builder_.UnsafeAppend(false);
    UnsafeSetNotNull(1);
    return Status::OK();
  }

  Status AppendEmptyValues(int64_t length) final {
    ARROW_RETURN_NOT_OK(Reserve(length));
    data_builder_.UnsafeAppend(length, false);
    UnsafeSetNotNull(length);
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
    data_builder_.UnsafeAppend(val);
    UnsafeAppendToBitmap(true);
  }

  void UnsafeAppendNull() {
    data_builder_.UnsafeAppend(false);
    UnsafeAppendToBitmap(false);
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
    data_builder_.UnsafeAppend<false>(
        length, [&values_begin]() -> bool { return *values_begin++; });
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
  enable_if_t<!std::is_pointer<ValidIter>::value, Status> AppendValues(
      ValuesIter values_begin, ValuesIter values_end, ValidIter valid_begin) {
    static_assert(!internal::is_null_pointer<ValidIter>::value,
                  "Don't pass a NULLPTR directly as valid_begin, use the 2-argument "
                  "version instead");
    int64_t length = static_cast<int64_t>(std::distance(values_begin, values_end));
    ARROW_RETURN_NOT_OK(Reserve(length));

    data_builder_.UnsafeAppend<false>(
        length, [&values_begin]() -> bool { return *values_begin++; });
    null_bitmap_builder_.UnsafeAppend<true>(
        length, [&valid_begin]() -> bool { return *valid_begin++; });
    length_ = null_bitmap_builder_.length();
    null_count_ = null_bitmap_builder_.false_count();
    return Status::OK();
  }

  // Same as above, for a pointer type ValidIter
  template <typename ValuesIter, typename ValidIter>
  enable_if_t<std::is_pointer<ValidIter>::value, Status> AppendValues(
      ValuesIter values_begin, ValuesIter values_end, ValidIter valid_begin) {
    int64_t length = static_cast<int64_t>(std::distance(values_begin, values_end));
    ARROW_RETURN_NOT_OK(Reserve(length));
    data_builder_.UnsafeAppend<false>(
        length, [&values_begin]() -> bool { return *values_begin++; });

    if (valid_begin == NULLPTR) {
      UnsafeSetNotNull(length);
    } else {
      null_bitmap_builder_.UnsafeAppend<true>(
          length, [&valid_begin]() -> bool { return *valid_begin++; });
    }
    length_ = null_bitmap_builder_.length();
    null_count_ = null_bitmap_builder_.false_count();
    return Status::OK();
  }

  Status AppendValues(int64_t length, bool value);

  Status FinishInternal(std::shared_ptr<ArrayData>* out) override;

  /// \cond FALSE
  using ArrayBuilder::Finish;
  /// \endcond

  Status Finish(std::shared_ptr<BooleanArray>* out) { return FinishTyped(out); }

  void Reset() override;
  Status Resize(int64_t capacity) override;

  std::shared_ptr<DataType> type() const override { return boolean(); }

 protected:
  TypedBufferBuilder<bool> data_builder_;
};

}  // namespace arrow
