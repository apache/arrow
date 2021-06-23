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

// Array accessor types for primitive/C-type-based arrays, such as numbers,
// boolean, and temporal types.

#pragma once

#include <cstdint>
#include <memory>

#include "arrow/array/array_base.h"
#include "arrow/array/data.h"
#include "arrow/stl_iterator.h"
#include "arrow/type.h"
#include "arrow/type_fwd.h"  // IWYU pragma: export
#include "arrow/type_traits.h"
#include "arrow/util/bit_util.h"
#include "arrow/util/macros.h"
#include "arrow/util/visibility.h"

namespace arrow {

/// Concrete Array class for numeric data.
template <typename TYPE>
class NumericArray : public PrimitiveArray {
 public:
  using TypeClass = TYPE;
  using value_type = typename TypeClass::c_type;
  using IteratorType = stl::ArrayIterator<NumericArray<TYPE>>;

  explicit NumericArray(const std::shared_ptr<ArrayData>& data) : PrimitiveArray(data) {}

  // Only enable this constructor without a type argument for types without additional
  // metadata
  template <typename T1 = TYPE>
  NumericArray(enable_if_parameter_free<T1, int64_t> length,
               const std::shared_ptr<Buffer>& data,
               const std::shared_ptr<Buffer>& null_bitmap = NULLPTR,
               int64_t null_count = kUnknownNullCount, int64_t offset = 0)
      : PrimitiveArray(TypeTraits<T1>::type_singleton(), length, data, null_bitmap,
                       null_count, offset) {}

  const value_type* raw_values() const {
    return reinterpret_cast<const value_type*>(raw_values_) + data_->offset;
  }

  value_type Value(int64_t i) const { return raw_values()[i]; }

  // For API compatibility with BinaryArray etc.
  value_type GetView(int64_t i) const { return Value(i); }

  IteratorType begin() const { return IteratorType(*this); }

  IteratorType end() const { return IteratorType(*this, length()); }

 protected:
  using PrimitiveArray::PrimitiveArray;
};

/// Concrete Array class for boolean data
class ARROW_EXPORT BooleanArray : public PrimitiveArray {
 public:
  using TypeClass = BooleanType;
  using IteratorType = stl::ArrayIterator<BooleanArray>;

  explicit BooleanArray(const std::shared_ptr<ArrayData>& data);

  BooleanArray(int64_t length, const std::shared_ptr<Buffer>& data,
               const std::shared_ptr<Buffer>& null_bitmap = NULLPTR,
               int64_t null_count = kUnknownNullCount, int64_t offset = 0);

  bool Value(int64_t i) const {
    return BitUtil::GetBit(reinterpret_cast<const uint8_t*>(raw_values_),
                           i + data_->offset);
  }

  bool GetView(int64_t i) const { return Value(i); }

  /// \brief Return the number of false (0) values among the valid
  /// values. Result is not cached.
  int64_t false_count() const;

  /// \brief Return the number of true (1) values among the valid
  /// values. Result is not cached.
  int64_t true_count() const;

  IteratorType begin() const { return IteratorType(*this); }

  IteratorType end() const { return IteratorType(*this, length()); }

 protected:
  using PrimitiveArray::PrimitiveArray;
};

/// DayTimeArray
/// ---------------------
/// \brief Array of Day and Millisecond values.
class ARROW_EXPORT DayTimeIntervalArray : public PrimitiveArray {
 public:
  using TypeClass = DayTimeIntervalType;

  explicit DayTimeIntervalArray(const std::shared_ptr<ArrayData>& data);

  DayTimeIntervalArray(const std::shared_ptr<DataType>& type, int64_t length,
                       const std::shared_ptr<Buffer>& data,
                       const std::shared_ptr<Buffer>& null_bitmap = NULLPTR,
                       int64_t null_count = kUnknownNullCount, int64_t offset = 0);

  TypeClass::DayMilliseconds GetValue(int64_t i) const;
  TypeClass::DayMilliseconds Value(int64_t i) const { return GetValue(i); }

  // For compatibility with Take kernel.
  TypeClass::DayMilliseconds GetView(int64_t i) const { return GetValue(i); }

  int32_t byte_width() const { return sizeof(TypeClass::DayMilliseconds); }

  const uint8_t* raw_values() const { return raw_values_ + data_->offset * byte_width(); }
};

}  // namespace arrow
