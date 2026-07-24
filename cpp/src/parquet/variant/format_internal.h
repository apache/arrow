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

#include <cstdint>
#include <string_view>

#include "arrow/type_fwd.h"
#include "arrow/util/utf8.h"
#include "parquet/exception.h"
#include "parquet/variant/format.h"

namespace parquet::variant {

class VariantPrimitiveView;
class VariantShortStringView;
class VariantObjectView;
class VariantArrayView;

namespace internal {

inline constexpr uint8_t kVariantVersion = 1;
inline constexpr uint8_t kMetadataVersionMask = 0x0F;
inline constexpr uint8_t kMetadataSortedStringsMask = 0x10;
inline constexpr uint8_t kMetadataReservedMask = 0x20;

template <VariantBasicType type>
struct VariantBasicTypeTraits {};

template <>
struct VariantBasicTypeTraits<VariantBasicType::kPrimitive> {
  using ViewType = VariantPrimitiveView;
};

template <>
struct VariantBasicTypeTraits<VariantBasicType::kShortString> {
  using ViewType = VariantShortStringView;
};

template <>
struct VariantBasicTypeTraits<VariantBasicType::kObject> {
  using ViewType = VariantObjectView;
};

template <>
struct VariantBasicTypeTraits<VariantBasicType::kArray> {
  using ViewType = VariantArrayView;
};

inline bool IsKnownVariantPrimitive(VariantPrimitiveType type) {
  return type <= VariantPrimitiveType::kUuid;
}

inline bool IsDecimalVariantPrimitive(VariantPrimitiveType type) {
  return type == VariantPrimitiveType::kDecimal4 ||
         type == VariantPrimitiveType::kDecimal8 ||
         type == VariantPrimitiveType::kDecimal16;
}

inline void ValidateDecimalScale(uint8_t scale) {
  if (scale > 38) {
    throw ParquetInvalidOrCorruptedFileException("Invalid Variant decimal scale ", scale,
                                                 " exceeds 38");
  }
}

inline void ValidateUtf8(std::string_view value, std::string_view context) {
  ::arrow::util::InitializeUTF8();
  if (!::arrow::util::ValidateUTF8(reinterpret_cast<const uint8_t*>(value.data()),
                                   value.size())) {
    throw ParquetInvalidOrCorruptedFileException("Invalid Variant encoding: ", context,
                                                 " is not valid UTF-8");
  }
}

template <VariantPrimitiveType type>
concept HeaderOnlyVariantPrimitive =
    type == VariantPrimitiveType::kNull || type == VariantPrimitiveType::kBooleanTrue ||
    type == VariantPrimitiveType::kBooleanFalse;

template <VariantPrimitiveType type>
struct VariantFixedPrimitiveTraits {};

#define VARIANT_FIXED_PRIMITIVE_TRAITS_DEF(TYPE, C_TYPE)              \
  template <>                                                         \
  struct VariantFixedPrimitiveTraits<VariantPrimitiveType::k##TYPE> { \
    using CType = C_TYPE;                                             \
  };

VARIANT_FIXED_PRIMITIVE_TRAITS_DEF(Int8, int8_t)
VARIANT_FIXED_PRIMITIVE_TRAITS_DEF(Int16, int16_t)
VARIANT_FIXED_PRIMITIVE_TRAITS_DEF(Int32, int32_t)
VARIANT_FIXED_PRIMITIVE_TRAITS_DEF(Int64, int64_t)
VARIANT_FIXED_PRIMITIVE_TRAITS_DEF(Float, float)
VARIANT_FIXED_PRIMITIVE_TRAITS_DEF(Double, double)
VARIANT_FIXED_PRIMITIVE_TRAITS_DEF(Date, int32_t)
VARIANT_FIXED_PRIMITIVE_TRAITS_DEF(TimeNTZMicros, int64_t)
VARIANT_FIXED_PRIMITIVE_TRAITS_DEF(TimestampMicros, int64_t)
VARIANT_FIXED_PRIMITIVE_TRAITS_DEF(TimestampNTZMicros, int64_t)
VARIANT_FIXED_PRIMITIVE_TRAITS_DEF(TimestampNanos, int64_t)
VARIANT_FIXED_PRIMITIVE_TRAITS_DEF(TimestampNTZNanos, int64_t)

#undef VARIANT_FIXED_PRIMITIVE_TRAITS_DEF

template <VariantPrimitiveType type>
concept FixedVariantPrimitive =
    requires { typename VariantFixedPrimitiveTraits<type>::CType; };

template <VariantPrimitiveType type>
struct VariantDecimalPrimitiveTraits {};

#define VARIANT_DECIMAL_PRIMITIVE_TRAITS_DEF(TYPE, C_TYPE)              \
  template <>                                                           \
  struct VariantDecimalPrimitiveTraits<VariantPrimitiveType::k##TYPE> { \
    using CType = C_TYPE;                                               \
  };

VARIANT_DECIMAL_PRIMITIVE_TRAITS_DEF(Decimal4, ::arrow::Decimal32)
VARIANT_DECIMAL_PRIMITIVE_TRAITS_DEF(Decimal8, ::arrow::Decimal64)
VARIANT_DECIMAL_PRIMITIVE_TRAITS_DEF(Decimal16, ::arrow::Decimal128)

#undef VARIANT_DECIMAL_PRIMITIVE_TRAITS_DEF

template <VariantPrimitiveType type>
concept DecimalVariantPrimitive =
    requires { typename VariantDecimalPrimitiveTraits<type>::CType; };

template <VariantPrimitiveType type>
concept LengthPrefixedVariantPrimitive =
    type == VariantPrimitiveType::kBinary || type == VariantPrimitiveType::kString;

template <VariantPrimitiveType type>
concept UuidVariantPrimitive = type == VariantPrimitiveType::kUuid;

}  // namespace internal

}  // namespace parquet::variant
