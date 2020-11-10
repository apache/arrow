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
#include <limits>
#include <type_traits>

#include "arrow/status.h"
#include "arrow/util/macros.h"
#include "arrow/util/visibility.h"

// "safe-math.h" includes <intsafe.h> from the Windows headers.
#include "arrow/util/windows_compatibility.h"
#include "arrow/vendored/portable-snippets/safe-math.h"
// clang-format off (avoid include reordering)
#include "arrow/util/windows_fixup.h"
// clang-format on

namespace arrow {
namespace internal {

// Define functions AddWithOverflow, SubtractWithOverflow, MultiplyWithOverflow
// with the signature `bool(T u, T v, T* out)` where T is an integer type.
// On overflow, these functions return true.  Otherwise, false is returned
// and `out` is updated with the result of the operation.

#define OP_WITH_OVERFLOW(_func_name, _psnip_op, _type, _psnip_type) \
  static inline bool _func_name(_type u, _type v, _type* out) {     \
    return !psnip_safe_##_psnip_type##_##_psnip_op(out, u, v);      \
  }

#define OPS_WITH_OVERFLOW(_func_name, _psnip_op)            \
  OP_WITH_OVERFLOW(_func_name, _psnip_op, int8_t, int8)     \
  OP_WITH_OVERFLOW(_func_name, _psnip_op, int16_t, int16)   \
  OP_WITH_OVERFLOW(_func_name, _psnip_op, int32_t, int32)   \
  OP_WITH_OVERFLOW(_func_name, _psnip_op, int64_t, int64)   \
  OP_WITH_OVERFLOW(_func_name, _psnip_op, uint8_t, uint8)   \
  OP_WITH_OVERFLOW(_func_name, _psnip_op, uint16_t, uint16) \
  OP_WITH_OVERFLOW(_func_name, _psnip_op, uint32_t, uint32) \
  OP_WITH_OVERFLOW(_func_name, _psnip_op, uint64_t, uint64)

OPS_WITH_OVERFLOW(AddWithOverflow, add)
OPS_WITH_OVERFLOW(SubtractWithOverflow, sub)
OPS_WITH_OVERFLOW(MultiplyWithOverflow, mul)
OPS_WITH_OVERFLOW(DivideWithOverflow, div)

#undef OP_WITH_OVERFLOW
#undef OPS_WITH_OVERFLOW

/// Signed addition with well-defined behaviour on overflow (as unsigned)
template <typename SignedInt>
SignedInt SafeSignedAdd(SignedInt u, SignedInt v) {
  using UnsignedInt = typename std::make_unsigned<SignedInt>::type;
  return static_cast<SignedInt>(static_cast<UnsignedInt>(u) +
                                static_cast<UnsignedInt>(v));
}

/// Signed subtraction with well-defined behaviour on overflow (as unsigned)
template <typename SignedInt>
SignedInt SafeSignedSubtract(SignedInt u, SignedInt v) {
  using UnsignedInt = typename std::make_unsigned<SignedInt>::type;
  return static_cast<SignedInt>(static_cast<UnsignedInt>(u) -
                                static_cast<UnsignedInt>(v));
}

/// Signed multiply with well-defined behaviour on overflow (as unsigned)
template <typename SignedInt>
SignedInt SafeSignedMultiply(SignedInt u, SignedInt v) {
  using UnsignedInt = typename std::make_unsigned<SignedInt>::type;
  return static_cast<SignedInt>(static_cast<UnsignedInt>(u) *
                                static_cast<UnsignedInt>(v));
}

/// Signed left shift with well-defined behaviour on negative numbers or overflow
template <typename SignedInt, typename Shift>
SignedInt SafeLeftShift(SignedInt u, Shift shift) {
  using UnsignedInt = typename std::make_unsigned<SignedInt>::type;
  return static_cast<SignedInt>(static_cast<UnsignedInt>(u) << shift);
}

/// Upcast an integer to the largest possible width (currently 64 bits)

template <typename Integer>
typename std::enable_if<
    std::is_integral<Integer>::value && std::is_signed<Integer>::value, int64_t>::type
UpcastInt(Integer v) {
  return v;
}

template <typename Integer>
typename std::enable_if<
    std::is_integral<Integer>::value && std::is_unsigned<Integer>::value, uint64_t>::type
UpcastInt(Integer v) {
  return v;
}

static inline Status CheckSliceParams(int64_t object_length, int64_t slice_offset,
                                      int64_t slice_length, const char* object_name) {
  if (ARROW_PREDICT_FALSE(slice_offset < 0)) {
    return Status::Invalid("Negative ", object_name, " slice offset");
  }
  if (ARROW_PREDICT_FALSE(slice_length < 0)) {
    return Status::Invalid("Negative ", object_name, " slice length");
  }
  int64_t offset_plus_length;
  if (ARROW_PREDICT_FALSE(
          internal::AddWithOverflow(slice_offset, slice_length, &offset_plus_length))) {
    return Status::Invalid(object_name, " slice would overflow");
  }
  if (ARROW_PREDICT_FALSE(slice_offset + slice_length > object_length)) {
    return Status::Invalid(object_name, " slice would exceed ", object_name, " length");
  }
  return Status::OK();
}

}  // namespace internal
}  // namespace arrow
