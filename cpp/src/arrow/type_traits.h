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

#ifndef ARROW_TYPE_TRAITS_H
#define ARROW_TYPE_TRAITS_H

#include <type_traits>

#include "arrow/type_fwd.h"
#include "arrow/util/bit-util.h"

namespace arrow {

template <typename T>
struct TypeTraits {};

template <>
struct TypeTraits<UInt8Type> {
  using ArrayType = UInt8Array;
  using BuilderType = UInt8Builder;
  static inline int bytes_required(int elements) { return elements; }
};

template <>
struct TypeTraits<Int8Type> {
  using ArrayType = Int8Array;
  using BuilderType = Int8Builder;
  static inline int bytes_required(int elements) { return elements; }
};

template <>
struct TypeTraits<UInt16Type> {
  using ArrayType = UInt16Array;
  using BuilderType = UInt16Builder;

  static inline int bytes_required(int elements) { return elements * sizeof(uint16_t); }
};

template <>
struct TypeTraits<Int16Type> {
  using ArrayType = Int16Array;
  using BuilderType = Int16Builder;

  static inline int bytes_required(int elements) { return elements * sizeof(int16_t); }
};

template <>
struct TypeTraits<UInt32Type> {
  using ArrayType = UInt32Array;
  using BuilderType = UInt32Builder;

  static inline int bytes_required(int elements) { return elements * sizeof(uint32_t); }
};

template <>
struct TypeTraits<Int32Type> {
  using ArrayType = Int32Array;
  using BuilderType = Int32Builder;

  static inline int bytes_required(int elements) { return elements * sizeof(int32_t); }
};

template <>
struct TypeTraits<UInt64Type> {
  using ArrayType = UInt64Array;
  using BuilderType = UInt64Builder;

  static inline int bytes_required(int elements) { return elements * sizeof(uint64_t); }
};

template <>
struct TypeTraits<Int64Type> {
  using ArrayType = Int64Array;
  using BuilderType = Int64Builder;

  static inline int bytes_required(int elements) { return elements * sizeof(int64_t); }
};

template <>
struct TypeTraits<DateType> {
  using ArrayType = DateArray;
  // using BuilderType = DateBuilder;

  static inline int bytes_required(int elements) { return elements * sizeof(int64_t); }
};

template <>
struct TypeTraits<TimestampType> {
  using ArrayType = TimestampArray;
  // using BuilderType = TimestampBuilder;

  static inline int bytes_required(int elements) { return elements * sizeof(int64_t); }
};

template <>
struct TypeTraits<HalfFloatType> {
  using ArrayType = HalfFloatArray;
  using BuilderType = HalfFloatBuilder;

  static inline int bytes_required(int elements) { return elements * sizeof(uint16_t); }
};

template <>
struct TypeTraits<FloatType> {
  using ArrayType = FloatArray;
  using BuilderType = FloatBuilder;

  static inline int bytes_required(int elements) { return elements * sizeof(float); }
};

template <>
struct TypeTraits<DoubleType> {
  using ArrayType = DoubleArray;
  using BuilderType = DoubleBuilder;

  static inline int bytes_required(int elements) { return elements * sizeof(double); }
};

template <>
struct TypeTraits<BooleanType> {
  using ArrayType = BooleanArray;
  using BuilderType = BooleanBuilder;

  static inline int bytes_required(int elements) {
    return BitUtil::BytesForBits(elements);
  }
};

template <>
struct TypeTraits<StringType> {
  using ArrayType = StringArray;
  using BuilderType = StringBuilder;
};

template <>
struct TypeTraits<BinaryType> {
  using ArrayType = BinaryArray;
  using BuilderType = BinaryBuilder;
};

// Not all type classes have a c_type
template <typename T>
struct as_void {
  using type = void;
};

// The partial specialization will match if T has the ATTR_NAME member
#define GET_ATTR(ATTR_NAME, DEFAULT)                                             \
  template <typename T, typename Enable = void>                                  \
  struct GetAttr_##ATTR_NAME {                                                   \
    using type = DEFAULT;                                                        \
  };                                                                             \
                                                                                 \
  template <typename T>                                                          \
  struct GetAttr_##ATTR_NAME<T, typename as_void<typename T::ATTR_NAME>::type> { \
    using type = typename T::ATTR_NAME;                                          \
  };

GET_ATTR(c_type, void);
GET_ATTR(TypeClass, void);

#undef GET_ATTR

#define PRIMITIVE_TRAITS(T)                                                           \
  using TypeClass = typename std::conditional<std::is_base_of<DataType, T>::value, T, \
      typename GetAttr_TypeClass<T>::type>::type;                                     \
  using c_type = typename GetAttr_c_type<TypeClass>::type;

template <typename T>
struct IsUnsignedInt {
  PRIMITIVE_TRAITS(T);
  static constexpr bool value =
      std::is_integral<c_type>::value && std::is_unsigned<c_type>::value;
};

template <typename T>
struct IsSignedInt {
  PRIMITIVE_TRAITS(T);
  static constexpr bool value =
      std::is_integral<c_type>::value && std::is_signed<c_type>::value;
};

template <typename T>
struct IsInteger {
  PRIMITIVE_TRAITS(T);
  static constexpr bool value = std::is_integral<c_type>::value;
};

template <typename T>
struct IsFloatingPoint {
  PRIMITIVE_TRAITS(T);
  static constexpr bool value = std::is_floating_point<c_type>::value;
};

template <typename T>
struct IsNumeric {
  PRIMITIVE_TRAITS(T);
  static constexpr bool value = std::is_arithmetic<c_type>::value;
};

}  // namespace arrow

#endif  // ARROW_TYPE_TRAITS_H
