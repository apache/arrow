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
  static inline int bytes_required(int elements) { return elements; }
};

template <>
struct TypeTraits<Int8Type> {
  using ArrayType = Int8Array;
  static inline int bytes_required(int elements) { return elements; }
};

template <>
struct TypeTraits<UInt16Type> {
  using ArrayType = UInt16Array;

  static inline int bytes_required(int elements) { return elements * sizeof(uint16_t); }
};

template <>
struct TypeTraits<Int16Type> {
  using ArrayType = Int16Array;

  static inline int bytes_required(int elements) { return elements * sizeof(int16_t); }
};

template <>
struct TypeTraits<UInt32Type> {
  using ArrayType = UInt32Array;

  static inline int bytes_required(int elements) { return elements * sizeof(uint32_t); }
};

template <>
struct TypeTraits<Int32Type> {
  using ArrayType = Int32Array;

  static inline int bytes_required(int elements) { return elements * sizeof(int32_t); }
};

template <>
struct TypeTraits<UInt64Type> {
  using ArrayType = UInt64Array;

  static inline int bytes_required(int elements) { return elements * sizeof(uint64_t); }
};

template <>
struct TypeTraits<Int64Type> {
  using ArrayType = Int64Array;

  static inline int bytes_required(int elements) { return elements * sizeof(int64_t); }
};

template <>
struct TypeTraits<TimestampType> {
  using ArrayType = TimestampArray;

  static inline int bytes_required(int elements) { return elements * sizeof(int64_t); }
};
template <>

struct TypeTraits<FloatType> {
  using ArrayType = FloatArray;

  static inline int bytes_required(int elements) { return elements * sizeof(float); }
};

template <>
struct TypeTraits<DoubleType> {
  using ArrayType = DoubleArray;

  static inline int bytes_required(int elements) { return elements * sizeof(double); }
};

template <>
struct TypeTraits<BooleanType> {
  typedef BooleanArray ArrayType;

  static inline int bytes_required(int elements) {
    return BitUtil::BytesForBits(elements);
  }
};

// Not all type classes have a c_type
template <typename T>
struct as_void {
  using type = void;
};

template <typename T, typename Enable = void>
struct GetCType {
  using type = void;
};

// The partial specialization will match if T has the c_type member
template <typename T>
struct GetCType<T, typename as_void<typename T::c_type>::type> {
  using type = typename T::c_type;
};

#define PRIMITIVE_TRAITS(T)                                                           \
  using TypeClass = typename std::conditional<std::is_base_of<DataType, T>::value, T, \
      typename T::TypeClass>::type;                                                   \
  using c_type = typename GetCType<TypeClass>::type;

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
struct IsFloatingPoint {
  PRIMITIVE_TRAITS(T);
  static constexpr bool value = std::is_floating_point<c_type>::value;
};

}  // namespace arrow

#endif  // ARROW_TYPE_TRAITS_H
