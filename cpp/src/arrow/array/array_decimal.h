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
#include <memory>
#include <string>

#include "arrow/array/array_binary.h"
#include "arrow/array/data.h"
#include "arrow/type.h"
#include "arrow/util/visibility.h"

namespace arrow {


template<uint32_t width>
struct DecimalArrayHelper;

#define DECL_DECIMAL_TYPES_HELPER(width)                 \
template<>                                               \
struct DecimalArrayHelper<width> {                       \
  static constexpr Type::type id = Type::DECIMAL##width; \
  using type = Decimal##width##Type;                     \
  using value_type = Decimal##width;                     \
};

DECL_DECIMAL_TYPES_HELPER(128)
DECL_DECIMAL_TYPES_HELPER(256)

#undef DECL_DECIMAL_TYPES_HELPER

/// Template Array class for decimal data
template<uint32_t width>
class BaseDecimalArray : public FixedSizeBinaryArray {
 public:
  using TypeClass = typename DecimalArrayHelper<width>::type;

  using FixedSizeBinaryArray::FixedSizeBinaryArray;

  /// \brief Construct DecimalArray from ArrayData instance
  explicit BaseDecimalArray(const std::shared_ptr<ArrayData>& data);

  std::string FormatValue(int64_t i) const;
};

#define DECIMAL_ARRAY_DECL(width)                                           \
class ARROW_EXPORT Decimal##width##Array : public BaseDecimalArray<width> { \
  using BaseDecimalArray<width>::BaseDecimalArray;                          \
};

DECIMAL_ARRAY_DECL(128)
DECIMAL_ARRAY_DECL(256)

#undef DECIMAL_ARRAY_DECL

// Backward compatibility
using DecimalArray = Decimal128Array;


}  // namespace arrow
