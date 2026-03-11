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
#include <type_traits>
#include <utility>

#include "arrow/util/macros.h"
#include "parquet/exception.h"
#include "parquet/types.h"

namespace parquet {

#define TYPE_VISIT_INLINE(TYPE_VALUE)                                                 \
  case Type::TYPE_VALUE: {                                                            \
    const PhysicalType<Type::TYPE_VALUE>* concrete_ptr = NULLPTR;                     \
    return std::forward<VISITOR>(visitor)(concrete_ptr, std::forward<ARGS>(args)...); \
  }

/// \brief Call `visitor` with a nullptr of the corresponding concrete type class
/// \tparam ARGS Additional arguments, if any, will be passed to the visitor after
/// the type argument
///
/// The intent is for this to be called on a generic lambda
/// that may internally use `if constexpr` or similar constructs.
template <typename VISITOR, typename... ARGS>
auto VisitType(Type::type type, VISITOR&& visitor, ARGS&&... args)
    -> decltype(std::forward<VISITOR>(visitor)(std::declval<PhysicalType<Type::INT32>*>(),
                                               args...)) {
  switch (type) {
    TYPE_VISIT_INLINE(INT32)
    TYPE_VISIT_INLINE(INT64)
    TYPE_VISIT_INLINE(INT96)
    TYPE_VISIT_INLINE(FLOAT)
    TYPE_VISIT_INLINE(DOUBLE)
    TYPE_VISIT_INLINE(FIXED_LEN_BYTE_ARRAY)
    TYPE_VISIT_INLINE(BYTE_ARRAY)
    TYPE_VISIT_INLINE(BOOLEAN)
    default:
      throw ParquetException("Invalid Type::type");
  }
}

#undef TYPE_VISIT_INLINE

}  // namespace parquet
