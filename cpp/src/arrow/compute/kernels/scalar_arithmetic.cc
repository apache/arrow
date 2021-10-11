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

#include <algorithm>
#include <cmath>
#include <limits>
#include <utility>
#include <vector>

#include "arrow/compare.h"
#include "arrow/compute/api_scalar.h"
#include "arrow/compute/kernels/common.h"
#include "arrow/compute/kernels/util_internal.h"
#include "arrow/type.h"
#include "arrow/type_traits.h"
#include "arrow/util/decimal.h"
#include "arrow/util/int_util_internal.h"
#include "arrow/util/macros.h"

namespace arrow {

using internal::AddWithOverflow;
using internal::DivideWithOverflow;
using internal::MultiplyWithOverflow;
using internal::NegateWithOverflow;
using internal::SubtractWithOverflow;

namespace compute {
namespace internal {

using applicator::ScalarBinaryEqualTypes;
using applicator::ScalarBinaryNotNullEqualTypes;
using applicator::ScalarUnary;
using applicator::ScalarUnaryNotNull;
using applicator::ScalarUnaryNotNullStateful;

namespace {

// N.B. take care not to conflict with type_traits.h as that can cause surprises in a
// unity build

template <typename T>
using is_unsigned_integer = std::integral_constant<bool, std::is_integral<T>::value &&
                                                             std::is_unsigned<T>::value>;

template <typename T>
using is_signed_integer =
    std::integral_constant<bool, std::is_integral<T>::value && std::is_signed<T>::value>;

template <typename T, typename R = T>
using enable_if_signed_c_integer = enable_if_t<is_signed_integer<T>::value, R>;

template <typename T, typename R = T>
using enable_if_unsigned_c_integer = enable_if_t<is_unsigned_integer<T>::value, R>;

template <typename T, typename R = T>
using enable_if_c_integer =
    enable_if_t<is_signed_integer<T>::value || is_unsigned_integer<T>::value, R>;

template <typename T, typename R = T>
using enable_if_floating_point = enable_if_t<std::is_floating_point<T>::value, R>;

template <typename T, typename R = T>
using enable_if_decimal_value =
    enable_if_t<std::is_same<Decimal128, T>::value || std::is_same<Decimal256, T>::value,
                R>;

struct AbsoluteValue {
  template <typename T, typename Arg>
  static constexpr enable_if_floating_point<Arg, T> Call(KernelContext*, Arg arg,
                                                         Status*) {
    return std::fabs(arg);
  }

  template <typename T, typename Arg>
  static constexpr enable_if_unsigned_c_integer<Arg, T> Call(KernelContext*, Arg arg,
                                                             Status*) {
    return arg;
  }

  template <typename T, typename Arg>
  static constexpr enable_if_signed_c_integer<Arg, T> Call(KernelContext*, Arg arg,
                                                           Status* st) {
    return (arg < 0) ? arrow::internal::SafeSignedNegate(arg) : arg;
  }
};

struct AbsoluteValueChecked {
  template <typename T, typename Arg>
  static enable_if_signed_c_integer<Arg, T> Call(KernelContext*, Arg arg, Status* st) {
    static_assert(std::is_same<T, Arg>::value, "");
    if (arg == std::numeric_limits<Arg>::min()) {
      *st = Status::Invalid("overflow");
      return arg;
    }
    return std::abs(arg);
  }

  template <typename T, typename Arg>
  static enable_if_unsigned_c_integer<Arg, T> Call(KernelContext* ctx, Arg arg,
                                                   Status* st) {
    static_assert(std::is_same<T, Arg>::value, "");
    return arg;
  }

  template <typename T, typename Arg>
  static constexpr enable_if_floating_point<Arg, T> Call(KernelContext*, Arg arg,
                                                         Status* st) {
    static_assert(std::is_same<T, Arg>::value, "");
    return std::fabs(arg);
  }
};

struct Add {
  template <typename T, typename Arg0, typename Arg1>
  static constexpr enable_if_floating_point<T> Call(KernelContext*, Arg0 left, Arg1 right,
                                                    Status*) {
    return left + right;
  }

  template <typename T, typename Arg0, typename Arg1>
  static constexpr enable_if_unsigned_c_integer<T> Call(KernelContext*, Arg0 left,
                                                        Arg1 right, Status*) {
    return left + right;
  }

  template <typename T, typename Arg0, typename Arg1>
  static constexpr enable_if_signed_c_integer<T> Call(KernelContext*, Arg0 left,
                                                      Arg1 right, Status*) {
    return arrow::internal::SafeSignedAdd(left, right);
  }

  template <typename T, typename Arg0, typename Arg1>
  static enable_if_decimal_value<T> Call(KernelContext*, Arg0 left, Arg1 right, Status*) {
    return left + right;
  }
};

struct AddChecked {
  template <typename T, typename Arg0, typename Arg1>
  static enable_if_c_integer<T> Call(KernelContext*, Arg0 left, Arg1 right, Status* st) {
    static_assert(std::is_same<T, Arg0>::value && std::is_same<T, Arg1>::value, "");
    T result = 0;
    if (ARROW_PREDICT_FALSE(AddWithOverflow(left, right, &result))) {
      *st = Status::Invalid("overflow");
    }
    return result;
  }

  template <typename T, typename Arg0, typename Arg1>
  static enable_if_floating_point<T> Call(KernelContext*, Arg0 left, Arg1 right,
                                          Status*) {
    static_assert(std::is_same<T, Arg0>::value && std::is_same<T, Arg1>::value, "");
    return left + right;
  }

  template <typename T, typename Arg0, typename Arg1>
  static enable_if_decimal_value<T> Call(KernelContext*, Arg0 left, Arg1 right, Status*) {
    return left + right;
  }
};

struct Subtract {
  template <typename T, typename Arg0, typename Arg1>
  static constexpr enable_if_floating_point<T> Call(KernelContext*, Arg0 left, Arg1 right,
                                                    Status*) {
    static_assert(std::is_same<T, Arg0>::value && std::is_same<T, Arg1>::value, "");
    return left - right;
  }

  template <typename T, typename Arg0, typename Arg1>
  static constexpr enable_if_unsigned_c_integer<T> Call(KernelContext*, Arg0 left,
                                                        Arg1 right, Status*) {
    static_assert(std::is_same<T, Arg0>::value && std::is_same<T, Arg1>::value, "");
    return left - right;
  }

  template <typename T, typename Arg0, typename Arg1>
  static constexpr enable_if_signed_c_integer<T> Call(KernelContext*, Arg0 left,
                                                      Arg1 right, Status*) {
    static_assert(std::is_same<T, Arg0>::value && std::is_same<T, Arg1>::value, "");
    return arrow::internal::SafeSignedSubtract(left, right);
  }

  template <typename T, typename Arg0, typename Arg1>
  static enable_if_decimal_value<T> Call(KernelContext*, Arg0 left, Arg1 right, Status*) {
    return left + (-right);
  }
};

struct SubtractChecked {
  template <typename T, typename Arg0, typename Arg1>
  static enable_if_c_integer<T> Call(KernelContext*, Arg0 left, Arg1 right, Status* st) {
    static_assert(std::is_same<T, Arg0>::value && std::is_same<T, Arg1>::value, "");
    T result = 0;
    if (ARROW_PREDICT_FALSE(SubtractWithOverflow(left, right, &result))) {
      *st = Status::Invalid("overflow");
    }
    return result;
  }

  template <typename T, typename Arg0, typename Arg1>
  static enable_if_floating_point<T> Call(KernelContext*, Arg0 left, Arg1 right,
                                          Status*) {
    static_assert(std::is_same<T, Arg0>::value && std::is_same<T, Arg1>::value, "");
    return left - right;
  }

  template <typename T, typename Arg0, typename Arg1>
  static enable_if_decimal_value<T> Call(KernelContext*, Arg0 left, Arg1 right, Status*) {
    return left + (-right);
  }
};

struct Multiply {
  static_assert(std::is_same<decltype(int8_t() * int8_t()), int32_t>::value, "");
  static_assert(std::is_same<decltype(uint8_t() * uint8_t()), int32_t>::value, "");
  static_assert(std::is_same<decltype(int16_t() * int16_t()), int32_t>::value, "");
  static_assert(std::is_same<decltype(uint16_t() * uint16_t()), int32_t>::value, "");
  static_assert(std::is_same<decltype(int32_t() * int32_t()), int32_t>::value, "");
  static_assert(std::is_same<decltype(uint32_t() * uint32_t()), uint32_t>::value, "");
  static_assert(std::is_same<decltype(int64_t() * int64_t()), int64_t>::value, "");
  static_assert(std::is_same<decltype(uint64_t() * uint64_t()), uint64_t>::value, "");

  template <typename T, typename Arg0, typename Arg1>
  static constexpr enable_if_floating_point<T> Call(KernelContext*, T left, T right,
                                                    Status*) {
    return left * right;
  }

  template <typename T, typename Arg0, typename Arg1>
  static constexpr enable_if_t<
      is_unsigned_integer<T>::value && !std::is_same<T, uint16_t>::value, T>
  Call(KernelContext*, T left, T right, Status*) {
    return left * right;
  }

  template <typename T, typename Arg0, typename Arg1>
  static constexpr enable_if_t<
      is_signed_integer<T>::value && !std::is_same<T, int16_t>::value, T>
  Call(KernelContext*, T left, T right, Status*) {
    return to_unsigned(left) * to_unsigned(right);
  }

  // Multiplication of 16 bit integer types implicitly promotes to signed 32 bit
  // integer. However, some inputs may nevertheless overflow (which triggers undefined
  // behaviour). Therefore we first cast to 32 bit unsigned integers where overflow is
  // well defined.
  template <typename T, typename Arg0, typename Arg1>
  static constexpr enable_if_same<T, int16_t, T> Call(KernelContext*, int16_t left,
                                                      int16_t right, Status*) {
    return static_cast<uint32_t>(left) * static_cast<uint32_t>(right);
  }
  template <typename T, typename Arg0, typename Arg1>
  static constexpr enable_if_same<T, uint16_t, T> Call(KernelContext*, uint16_t left,
                                                       uint16_t right, Status*) {
    return static_cast<uint32_t>(left) * static_cast<uint32_t>(right);
  }

  template <typename T, typename Arg0, typename Arg1>
  static enable_if_decimal_value<T> Call(KernelContext*, Arg0 left, Arg1 right, Status*) {
    return left * right;
  }
};

struct MultiplyChecked {
  template <typename T, typename Arg0, typename Arg1>
  static enable_if_c_integer<T> Call(KernelContext*, Arg0 left, Arg1 right, Status* st) {
    static_assert(std::is_same<T, Arg0>::value && std::is_same<T, Arg1>::value, "");
    T result = 0;
    if (ARROW_PREDICT_FALSE(MultiplyWithOverflow(left, right, &result))) {
      *st = Status::Invalid("overflow");
    }
    return result;
  }

  template <typename T, typename Arg0, typename Arg1>
  static enable_if_floating_point<T> Call(KernelContext*, Arg0 left, Arg1 right,
                                          Status*) {
    static_assert(std::is_same<T, Arg0>::value && std::is_same<T, Arg1>::value, "");
    return left * right;
  }

  template <typename T, typename Arg0, typename Arg1>
  static enable_if_decimal_value<T> Call(KernelContext*, Arg0 left, Arg1 right, Status*) {
    return left * right;
  }
};

struct Divide {
  template <typename T, typename Arg0, typename Arg1>
  static enable_if_floating_point<T> Call(KernelContext*, Arg0 left, Arg1 right,
                                          Status*) {
    return left / right;
  }

  template <typename T, typename Arg0, typename Arg1>
  static enable_if_c_integer<T> Call(KernelContext*, Arg0 left, Arg1 right, Status* st) {
    T result;
    if (ARROW_PREDICT_FALSE(DivideWithOverflow(left, right, &result))) {
      if (right == 0) {
        *st = Status::Invalid("divide by zero");
      } else {
        result = 0;
      }
    }
    return result;
  }

  template <typename T, typename Arg0, typename Arg1>
  static enable_if_decimal_value<T> Call(KernelContext*, Arg0 left, Arg1 right,
                                         Status* st) {
    if (right == Arg1()) {
      *st = Status::Invalid("Divide by zero");
      return T();
    } else {
      return left / right;
    }
  }
};

struct DivideChecked {
  template <typename T, typename Arg0, typename Arg1>
  static enable_if_c_integer<T> Call(KernelContext*, Arg0 left, Arg1 right, Status* st) {
    static_assert(std::is_same<T, Arg0>::value && std::is_same<T, Arg1>::value, "");
    T result;
    if (ARROW_PREDICT_FALSE(DivideWithOverflow(left, right, &result))) {
      if (right == 0) {
        *st = Status::Invalid("divide by zero");
      } else {
        *st = Status::Invalid("overflow");
      }
    }
    return result;
  }

  template <typename T, typename Arg0, typename Arg1>
  static enable_if_floating_point<T> Call(KernelContext*, Arg0 left, Arg1 right,
                                          Status* st) {
    static_assert(std::is_same<T, Arg0>::value && std::is_same<T, Arg1>::value, "");
    if (ARROW_PREDICT_FALSE(right == 0)) {
      *st = Status::Invalid("divide by zero");
      return 0;
    }
    return left / right;
  }

  template <typename T, typename Arg0, typename Arg1>
  static enable_if_decimal_value<T> Call(KernelContext* ctx, Arg0 left, Arg1 right,
                                         Status* st) {
    return Divide::Call<T>(ctx, left, right, st);
  }
};

struct Negate {
  template <typename T, typename Arg>
  static constexpr enable_if_floating_point<T> Call(KernelContext*, Arg arg, Status*) {
    return -arg;
  }

  template <typename T, typename Arg>
  static constexpr enable_if_unsigned_c_integer<T> Call(KernelContext*, Arg arg,
                                                        Status*) {
    return ~arg + 1;
  }

  template <typename T, typename Arg>
  static constexpr enable_if_signed_c_integer<T> Call(KernelContext*, Arg arg, Status*) {
    return arrow::internal::SafeSignedNegate(arg);
  }
};

struct NegateChecked {
  template <typename T, typename Arg>
  static enable_if_signed_c_integer<Arg, T> Call(KernelContext*, Arg arg, Status* st) {
    static_assert(std::is_same<T, Arg>::value, "");
    T result = 0;
    if (ARROW_PREDICT_FALSE(NegateWithOverflow(arg, &result))) {
      *st = Status::Invalid("overflow");
    }
    return result;
  }

  template <typename T, typename Arg>
  static enable_if_unsigned_c_integer<Arg, T> Call(KernelContext* ctx, Arg arg,
                                                   Status* st) {
    static_assert(std::is_same<T, Arg>::value, "");
    DCHECK(false) << "This is included only for the purposes of instantiability from the "
                     "arithmetic kernel generator";
    return 0;
  }

  template <typename T, typename Arg>
  static constexpr enable_if_floating_point<Arg, T> Call(KernelContext*, Arg arg,
                                                         Status* st) {
    static_assert(std::is_same<T, Arg>::value, "");
    return -arg;
  }
};

struct Power {
  ARROW_NOINLINE
  static uint64_t IntegerPower(uint64_t base, uint64_t exp) {
    // right to left O(logn) power
    uint64_t pow = 1;
    while (exp) {
      pow *= (exp & 1) ? base : 1;
      base *= base;
      exp >>= 1;
    }
    return pow;
  }

  template <typename T, typename Arg0, typename Arg1>
  static enable_if_c_integer<T> Call(KernelContext*, T base, T exp, Status* st) {
    if (exp < 0) {
      *st = Status::Invalid("integers to negative integer powers are not allowed");
      return 0;
    }
    return static_cast<T>(IntegerPower(base, exp));
  }

  template <typename T, typename Arg0, typename Arg1>
  static enable_if_floating_point<T> Call(KernelContext*, T base, T exp, Status*) {
    return std::pow(base, exp);
  }
};

struct PowerChecked {
  template <typename T, typename Arg0, typename Arg1>
  static enable_if_c_integer<T> Call(KernelContext*, Arg0 base, Arg1 exp, Status* st) {
    if (exp < 0) {
      *st = Status::Invalid("integers to negative integer powers are not allowed");
      return 0;
    } else if (exp == 0) {
      return 1;
    }
    // left to right O(logn) power with overflow checks
    bool overflow = false;
    uint64_t bitmask =
        1ULL << (63 - BitUtil::CountLeadingZeros(static_cast<uint64_t>(exp)));
    T pow = 1;
    while (bitmask) {
      overflow |= MultiplyWithOverflow(pow, pow, &pow);
      if (exp & bitmask) {
        overflow |= MultiplyWithOverflow(pow, base, &pow);
      }
      bitmask >>= 1;
    }
    if (overflow) {
      *st = Status::Invalid("overflow");
    }
    return pow;
  }

  template <typename T, typename Arg0, typename Arg1>
  static enable_if_floating_point<T> Call(KernelContext*, Arg0 base, Arg1 exp, Status*) {
    static_assert(std::is_same<T, Arg0>::value && std::is_same<T, Arg1>::value, "");
    return std::pow(base, exp);
  }
};

struct Sign {
  template <typename T, typename Arg>
  static constexpr enable_if_floating_point<Arg, T> Call(KernelContext*, Arg arg,
                                                         Status*) {
    return std::isnan(arg) ? arg : ((arg == 0) ? 0 : (std::signbit(arg) ? -1 : 1));
  }

  template <typename T, typename Arg>
  static constexpr enable_if_unsigned_c_integer<Arg, T> Call(KernelContext*, Arg arg,
                                                             Status*) {
    return (arg > 0) ? 1 : 0;
  }

  template <typename T, typename Arg>
  static constexpr enable_if_signed_c_integer<Arg, T> Call(KernelContext*, Arg arg,
                                                           Status*) {
    return (arg > 0) ? 1 : ((arg == 0) ? 0 : -1);
  }
};

// Bitwise operations

struct BitWiseNot {
  template <typename T, typename Arg>
  static T Call(KernelContext*, Arg arg, Status*) {
    return ~arg;
  }
};

struct BitWiseAnd {
  template <typename T, typename Arg0, typename Arg1>
  static T Call(KernelContext*, Arg0 lhs, Arg1 rhs, Status*) {
    return lhs & rhs;
  }
};

struct BitWiseOr {
  template <typename T, typename Arg0, typename Arg1>
  static T Call(KernelContext*, Arg0 lhs, Arg1 rhs, Status*) {
    return lhs | rhs;
  }
};

struct BitWiseXor {
  template <typename T, typename Arg0, typename Arg1>
  static T Call(KernelContext*, Arg0 lhs, Arg1 rhs, Status*) {
    return lhs ^ rhs;
  }
};

struct ShiftLeft {
  template <typename T, typename Arg0, typename Arg1>
  static T Call(KernelContext*, Arg0 lhs, Arg1 rhs, Status*) {
    using Unsigned = typename std::make_unsigned<Arg0>::type;
    static_assert(std::is_same<T, Arg0>::value, "");
    if (ARROW_PREDICT_FALSE(rhs < 0 || rhs >= std::numeric_limits<Arg0>::digits)) {
      return lhs;
    }
    return static_cast<T>(static_cast<Unsigned>(lhs) << static_cast<Unsigned>(rhs));
  }
};

// See SEI CERT C Coding Standard rule INT34-C
struct ShiftLeftChecked {
  template <typename T, typename Arg0, typename Arg1>
  static enable_if_unsigned_c_integer<T> Call(KernelContext*, Arg0 lhs, Arg1 rhs,
                                              Status* st) {
    static_assert(std::is_same<T, Arg0>::value, "");
    if (ARROW_PREDICT_FALSE(rhs < 0 || rhs >= std::numeric_limits<Arg0>::digits)) {
      *st = Status::Invalid("shift amount must be >= 0 and less than precision of type");
      return lhs;
    }
    return lhs << rhs;
  }

  template <typename T, typename Arg0, typename Arg1>
  static enable_if_signed_c_integer<T> Call(KernelContext*, Arg0 lhs, Arg1 rhs,
                                            Status* st) {
    using Unsigned = typename std::make_unsigned<Arg0>::type;
    static_assert(std::is_same<T, Arg0>::value, "");
    if (ARROW_PREDICT_FALSE(rhs < 0 || rhs >= std::numeric_limits<Arg0>::digits)) {
      *st = Status::Invalid("shift amount must be >= 0 and less than precision of type");
      return lhs;
    }
    // In C/C++ left shift of a negative number is undefined (C++11 standard 5.8.2)
    // Mimic Java/etc. and treat left shift as based on two's complement representation
    // Assumes two's complement machine
    return static_cast<T>(static_cast<Unsigned>(lhs) << static_cast<Unsigned>(rhs));
  }
};

struct ShiftRight {
  template <typename T, typename Arg0, typename Arg1>
  static T Call(KernelContext*, Arg0 lhs, Arg1 rhs, Status*) {
    static_assert(std::is_same<T, Arg0>::value, "");
    // Logical right shift when Arg0 is unsigned
    // Arithmetic otherwise (this is implementation-defined but GCC and MSVC document this
    // as arithmetic right shift)
    // https://gcc.gnu.org/onlinedocs/gcc/Integers-implementation.html#Integers-implementation
    // https://docs.microsoft.com/en-us/cpp/cpp/left-shift-and-right-shift-operators-input-and-output?view=msvc-160
    // Clang doesn't document their behavior.
    if (ARROW_PREDICT_FALSE(rhs < 0 || rhs >= std::numeric_limits<Arg0>::digits)) {
      return lhs;
    }
    return lhs >> rhs;
  }
};

struct ShiftRightChecked {
  template <typename T, typename Arg0, typename Arg1>
  static T Call(KernelContext*, Arg0 lhs, Arg1 rhs, Status* st) {
    static_assert(std::is_same<T, Arg0>::value, "");
    if (ARROW_PREDICT_FALSE(rhs < 0 || rhs >= std::numeric_limits<Arg0>::digits)) {
      *st = Status::Invalid("shift amount must be >= 0 and less than precision of type");
      return lhs;
    }
    return lhs >> rhs;
  }
};

struct Sin {
  template <typename T, typename Arg0>
  static enable_if_floating_point<Arg0, T> Call(KernelContext*, Arg0 val, Status*) {
    static_assert(std::is_same<T, Arg0>::value, "");
    return std::sin(val);
  }
};

struct SinChecked {
  template <typename T, typename Arg0>
  static enable_if_floating_point<Arg0, T> Call(KernelContext*, Arg0 val, Status* st) {
    static_assert(std::is_same<T, Arg0>::value, "");
    if (ARROW_PREDICT_FALSE(std::isinf(val))) {
      *st = Status::Invalid("domain error");
      return val;
    }
    return std::sin(val);
  }
};

struct Cos {
  template <typename T, typename Arg0>
  static enable_if_floating_point<Arg0, T> Call(KernelContext*, Arg0 val, Status*) {
    static_assert(std::is_same<T, Arg0>::value, "");
    return std::cos(val);
  }
};

struct CosChecked {
  template <typename T, typename Arg0>
  static enable_if_floating_point<Arg0, T> Call(KernelContext*, Arg0 val, Status* st) {
    static_assert(std::is_same<T, Arg0>::value, "");
    if (ARROW_PREDICT_FALSE(std::isinf(val))) {
      *st = Status::Invalid("domain error");
      return val;
    }
    return std::cos(val);
  }
};

struct Tan {
  template <typename T, typename Arg0>
  static enable_if_floating_point<Arg0, T> Call(KernelContext*, Arg0 val, Status*) {
    static_assert(std::is_same<T, Arg0>::value, "");
    return std::tan(val);
  }
};

struct TanChecked {
  template <typename T, typename Arg0>
  static enable_if_floating_point<Arg0, T> Call(KernelContext*, Arg0 val, Status* st) {
    static_assert(std::is_same<T, Arg0>::value, "");
    if (ARROW_PREDICT_FALSE(std::isinf(val))) {
      *st = Status::Invalid("domain error");
      return val;
    }
    // Cannot raise range errors (overflow) since PI/2 is not exactly representable
    return std::tan(val);
  }
};

struct Asin {
  template <typename T, typename Arg0>
  static enable_if_floating_point<Arg0, T> Call(KernelContext*, Arg0 val, Status*) {
    static_assert(std::is_same<T, Arg0>::value, "");
    if (ARROW_PREDICT_FALSE(val < -1.0 || val > 1.0)) {
      return std::numeric_limits<T>::quiet_NaN();
    }
    return std::asin(val);
  }
};

struct AsinChecked {
  template <typename T, typename Arg0>
  static enable_if_floating_point<Arg0, T> Call(KernelContext*, Arg0 val, Status* st) {
    static_assert(std::is_same<T, Arg0>::value, "");
    if (ARROW_PREDICT_FALSE(val < -1.0 || val > 1.0)) {
      *st = Status::Invalid("domain error");
      return val;
    }
    return std::asin(val);
  }
};

struct Acos {
  template <typename T, typename Arg0>
  static enable_if_floating_point<Arg0, T> Call(KernelContext*, Arg0 val, Status*) {
    static_assert(std::is_same<T, Arg0>::value, "");
    if (ARROW_PREDICT_FALSE((val < -1.0 || val > 1.0))) {
      return std::numeric_limits<T>::quiet_NaN();
    }
    return std::acos(val);
  }
};

struct AcosChecked {
  template <typename T, typename Arg0>
  static enable_if_floating_point<Arg0, T> Call(KernelContext*, Arg0 val, Status* st) {
    static_assert(std::is_same<T, Arg0>::value, "");
    if (ARROW_PREDICT_FALSE((val < -1.0 || val > 1.0))) {
      *st = Status::Invalid("domain error");
      return val;
    }
    return std::acos(val);
  }
};

struct Atan {
  template <typename T, typename Arg0>
  static enable_if_floating_point<Arg0, T> Call(KernelContext*, Arg0 val, Status*) {
    static_assert(std::is_same<T, Arg0>::value, "");
    return std::atan(val);
  }
};

struct Atan2 {
  template <typename T, typename Arg0, typename Arg1>
  static enable_if_floating_point<Arg0, T> Call(KernelContext*, Arg0 y, Arg1 x, Status*) {
    static_assert(std::is_same<T, Arg0>::value, "");
    static_assert(std::is_same<Arg0, Arg1>::value, "");
    return std::atan2(y, x);
  }
};

struct LogNatural {
  template <typename T, typename Arg>
  static enable_if_floating_point<Arg, T> Call(KernelContext*, Arg arg, Status*) {
    static_assert(std::is_same<T, Arg>::value, "");
    if (arg == 0.0) {
      return -std::numeric_limits<T>::infinity();
    } else if (arg < 0.0) {
      return std::numeric_limits<T>::quiet_NaN();
    }
    return std::log(arg);
  }
};

struct LogNaturalChecked {
  template <typename T, typename Arg>
  static enable_if_floating_point<Arg, T> Call(KernelContext*, Arg arg, Status* st) {
    static_assert(std::is_same<T, Arg>::value, "");
    if (arg == 0.0) {
      *st = Status::Invalid("logarithm of zero");
      return arg;
    } else if (arg < 0.0) {
      *st = Status::Invalid("logarithm of negative number");
      return arg;
    }
    return std::log(arg);
  }
};

struct Log10 {
  template <typename T, typename Arg>
  static enable_if_floating_point<Arg, T> Call(KernelContext*, Arg arg, Status*) {
    static_assert(std::is_same<T, Arg>::value, "");
    if (arg == 0.0) {
      return -std::numeric_limits<T>::infinity();
    } else if (arg < 0.0) {
      return std::numeric_limits<T>::quiet_NaN();
    }
    return std::log10(arg);
  }
};

struct Log10Checked {
  template <typename T, typename Arg>
  static enable_if_floating_point<Arg, T> Call(KernelContext*, Arg arg, Status* st) {
    static_assert(std::is_same<T, Arg>::value, "");
    if (arg == 0) {
      *st = Status::Invalid("logarithm of zero");
      return arg;
    } else if (arg < 0) {
      *st = Status::Invalid("logarithm of negative number");
      return arg;
    }
    return std::log10(arg);
  }
};

struct Log2 {
  template <typename T, typename Arg>
  static enable_if_floating_point<Arg, T> Call(KernelContext*, Arg arg, Status*) {
    static_assert(std::is_same<T, Arg>::value, "");
    if (arg == 0.0) {
      return -std::numeric_limits<T>::infinity();
    } else if (arg < 0.0) {
      return std::numeric_limits<T>::quiet_NaN();
    }
    return std::log2(arg);
  }
};

struct Log2Checked {
  template <typename T, typename Arg>
  static enable_if_floating_point<Arg, T> Call(KernelContext*, Arg arg, Status* st) {
    static_assert(std::is_same<T, Arg>::value, "");
    if (arg == 0.0) {
      *st = Status::Invalid("logarithm of zero");
      return arg;
    } else if (arg < 0.0) {
      *st = Status::Invalid("logarithm of negative number");
      return arg;
    }
    return std::log2(arg);
  }
};

struct Log1p {
  template <typename T, typename Arg>
  static enable_if_floating_point<Arg, T> Call(KernelContext*, Arg arg, Status*) {
    static_assert(std::is_same<T, Arg>::value, "");
    if (arg == -1) {
      return -std::numeric_limits<T>::infinity();
    } else if (arg < -1) {
      return std::numeric_limits<T>::quiet_NaN();
    }
    return std::log1p(arg);
  }
};

struct Log1pChecked {
  template <typename T, typename Arg>
  static enable_if_floating_point<Arg, T> Call(KernelContext*, Arg arg, Status* st) {
    static_assert(std::is_same<T, Arg>::value, "");
    if (arg == -1) {
      *st = Status::Invalid("logarithm of zero");
      return arg;
    } else if (arg < -1) {
      *st = Status::Invalid("logarithm of negative number");
      return arg;
    }
    return std::log1p(arg);
  }
};

struct Logb {
  template <typename T, typename Arg0, typename Arg1>
  static enable_if_floating_point<T> Call(KernelContext*, Arg0 x, Arg1 base, Status*) {
    static_assert(std::is_same<T, Arg0>::value, "");
    static_assert(std::is_same<Arg0, Arg1>::value, "");
    if (x == 0.0) {
      if (base == 0.0 || base < 0.0) {
        return std::numeric_limits<T>::quiet_NaN();
      } else {
        return -std::numeric_limits<T>::infinity();
      }
    } else if (x < 0.0) {
      return std::numeric_limits<T>::quiet_NaN();
    }
    return std::log(x) / std::log(base);
  }
};

struct LogbChecked {
  template <typename T, typename Arg0, typename Arg1>
  static enable_if_floating_point<T> Call(KernelContext*, Arg0 x, Arg1 base, Status* st) {
    static_assert(std::is_same<T, Arg0>::value, "");
    static_assert(std::is_same<Arg0, Arg1>::value, "");
    if (x == 0.0 || base == 0.0) {
      *st = Status::Invalid("logarithm of zero");
      return x;
    } else if (x < 0.0 || base < 0.0) {
      *st = Status::Invalid("logarithm of negative number");
      return x;
    }
    return std::log(x) / std::log(base);
  }
};

struct RoundUtil {
  // Calculate powers of ten with arbitrary integer exponent
  template <typename T = double>
  static enable_if_floating_point<T> Pow10(int64_t power) {
    static constexpr T lut[] = {1e0F, 1e1F, 1e2F,  1e3F,  1e4F,  1e5F,  1e6F,  1e7F,
                                1e8F, 1e9F, 1e10F, 1e11F, 1e12F, 1e13F, 1e14F, 1e15F};
    int64_t lut_size = (sizeof(lut) / sizeof(*lut));
    int64_t abs_power = std::abs(power);
    auto pow10 = lut[std::min(abs_power, lut_size - 1)];
    while (abs_power-- >= lut_size) {
      pow10 *= 1e1F;
    }
    return (power >= 0) ? pow10 : (1 / pow10);
  }
};

// Specializations of rounding implementations for round kernels
template <typename Type, RoundMode>
struct RoundImpl;

template <typename Type>
struct RoundImpl<Type, RoundMode::DOWN> {
  template <typename T = Type>
  static constexpr enable_if_floating_point<T> Round(const T val) {
    return std::floor(val);
  }

  template <typename T = Type>
  static enable_if_decimal_value<T, void> Round(T* val, const T& remainder,
                                                const T& pow10, const int32_t scale) {
    (*val) -= remainder;
    if (remainder.Sign() < 0) {
      (*val) -= pow10;
    }
  }
};

template <typename Type>
struct RoundImpl<Type, RoundMode::UP> {
  template <typename T = Type>
  static constexpr enable_if_floating_point<T> Round(const T val) {
    return std::ceil(val);
  }

  template <typename T = Type>
  static enable_if_decimal_value<T, void> Round(T* val, const T& remainder,
                                                const T& pow10, const int32_t scale) {
    (*val) -= remainder;
    if (remainder.Sign() > 0 && remainder != 0) {
      (*val) += pow10;
    }
  }
};

template <typename Type>
struct RoundImpl<Type, RoundMode::TOWARDS_ZERO> {
  template <typename T = Type>
  static constexpr enable_if_floating_point<T> Round(const T val) {
    return std::trunc(val);
  }

  template <typename T = Type>
  static enable_if_decimal_value<T, void> Round(T* val, const T& remainder,
                                                const T& pow10, const int32_t scale) {
    (*val) -= remainder;
  }
};

template <typename Type>
struct RoundImpl<Type, RoundMode::TOWARDS_INFINITY> {
  template <typename T = Type>
  static constexpr enable_if_floating_point<T> Round(const T val) {
    return std::signbit(val) ? std::floor(val) : std::ceil(val);
  }

  template <typename T = Type>
  static enable_if_decimal_value<T, void> Round(T* val, const T& remainder,
                                                const T& pow10, const int32_t scale) {
    (*val) -= remainder;
    if (remainder.Sign() < 0) {
      (*val) -= pow10;
    } else if (remainder.Sign() > 0 && remainder != 0) {
      (*val) += pow10;
    }
  }
};

// NOTE: RoundImpl variants for the HALF_* rounding modes are only
// invoked when the fractional part is equal to 0.5 (std::round is invoked
// otherwise).

template <typename Type>
struct RoundImpl<Type, RoundMode::HALF_DOWN> {
  template <typename T = Type>
  static constexpr enable_if_floating_point<T> Round(const T val) {
    return RoundImpl<T, RoundMode::DOWN>::Round(val);
  }

  template <typename T = Type>
  static enable_if_decimal_value<T, void> Round(T* val, const T& remainder,
                                                const T& pow10, const int32_t scale) {
    RoundImpl<T, RoundMode::DOWN>::Round(val, remainder, pow10, scale);
  }
};

template <typename Type>
struct RoundImpl<Type, RoundMode::HALF_UP> {
  template <typename T = Type>
  static constexpr enable_if_floating_point<T> Round(const T val) {
    return RoundImpl<T, RoundMode::UP>::Round(val);
  }

  template <typename T = Type>
  static enable_if_decimal_value<T, void> Round(T* val, const T& remainder,
                                                const T& pow10, const int32_t scale) {
    RoundImpl<T, RoundMode::UP>::Round(val, remainder, pow10, scale);
  }
};

template <typename Type>
struct RoundImpl<Type, RoundMode::HALF_TOWARDS_ZERO> {
  template <typename T = Type>
  static constexpr enable_if_floating_point<T> Round(const T val) {
    return RoundImpl<T, RoundMode::TOWARDS_ZERO>::Round(val);
  }

  template <typename T = Type>
  static enable_if_decimal_value<T, void> Round(T* val, const T& remainder,
                                                const T& pow10, const int32_t scale) {
    RoundImpl<T, RoundMode::TOWARDS_ZERO>::Round(val, remainder, pow10, scale);
  }
};

template <typename Type>
struct RoundImpl<Type, RoundMode::HALF_TOWARDS_INFINITY> {
  template <typename T = Type>
  static constexpr enable_if_floating_point<T> Round(const T val) {
    return RoundImpl<T, RoundMode::TOWARDS_INFINITY>::Round(val);
  }

  template <typename T = Type>
  static enable_if_decimal_value<T, void> Round(T* val, const T& remainder,
                                                const T& pow10, const int32_t scale) {
    RoundImpl<T, RoundMode::TOWARDS_INFINITY>::Round(val, remainder, pow10, scale);
  }
};

template <typename Type>
struct RoundImpl<Type, RoundMode::HALF_TO_EVEN> {
  template <typename T = Type>
  static constexpr enable_if_floating_point<T> Round(const T val) {
    return std::round(val * T(0.5)) * 2;
  }

  template <typename T = Type>
  static enable_if_decimal_value<T, void> Round(T* val, const T& remainder,
                                                const T& pow10, const int32_t scale) {
    auto scaled = val->ReduceScaleBy(scale, /*round=*/false);
    if (scaled.low_bits() % 2 != 0) {
      scaled += remainder.Sign() >= 0 ? 1 : -1;
    }
    *val = scaled.IncreaseScaleBy(scale);
  }
};

template <typename Type>
struct RoundImpl<Type, RoundMode::HALF_TO_ODD> {
  template <typename T = Type>
  static constexpr enable_if_floating_point<T> Round(const T val) {
    return std::floor(val * T(0.5)) + std::ceil(val * T(0.5));
  }

  template <typename T = Type>
  static enable_if_decimal_value<T, void> Round(T* val, const T& remainder,
                                                const T& pow10, const int32_t scale) {
    auto scaled = val->ReduceScaleBy(scale, /*round=*/false);
    if (scaled.low_bits() % 2 == 0) {
      scaled += remainder.Sign() ? 1 : -1;
    }
    *val = scaled.IncreaseScaleBy(scale);
  }
};

// Specializations of kernel state for round kernels
template <typename>
struct RoundOptionsWrapper;

template <>
struct RoundOptionsWrapper<RoundOptions> : public OptionsWrapper<RoundOptions> {
  using OptionsType = RoundOptions;
  using State = RoundOptionsWrapper<OptionsType>;
  double pow10;

  explicit RoundOptionsWrapper(OptionsType options) : OptionsWrapper(std::move(options)) {
    // Only positive exponents for powers of 10 are used because combining
    // multiply and division operations produced more stable rounding than
    // using multiply-only.  Refer to NumPy's round implementation:
    // https://github.com/numpy/numpy/blob/7b2f20b406d27364c812f7a81a9c901afbd3600c/numpy/core/src/multiarray/calculation.c#L589
    pow10 = RoundUtil::Pow10(std::abs(options.ndigits));
  }

  static Result<std::unique_ptr<KernelState>> Init(KernelContext* ctx,
                                                   const KernelInitArgs& args) {
    if (auto options = static_cast<const OptionsType*>(args.options)) {
      return ::arrow::internal::make_unique<State>(*options);
    }
    return Status::Invalid(
        "Attempted to initialize KernelState from null FunctionOptions");
  }
};

template <>
struct RoundOptionsWrapper<RoundToMultipleOptions>
    : public OptionsWrapper<RoundToMultipleOptions> {
  using OptionsType = RoundToMultipleOptions;

  static Result<std::unique_ptr<KernelState>> Init(KernelContext* ctx,
                                                   const KernelInitArgs& args) {
    ARROW_ASSIGN_OR_RAISE(auto state, OptionsWrapper<OptionsType>::Init(ctx, args));
    auto options = Get(*state);
    const auto& type = *args.inputs[0].type;
    if (!options.multiple || !options.multiple->is_valid) {
      return Status::Invalid("Rounding multiple must be non-null and valid");
    }
    if (is_floating(type.id())) {
      switch (options.multiple->type->id()) {
        case Type::FLOAT: {
          if (UnboxScalar<FloatType>::Unbox(*options.multiple) < 0) {
            return Status::Invalid("Rounding multiple must be positive");
          }
          break;
        }
        case Type::DOUBLE: {
          if (UnboxScalar<DoubleType>::Unbox(*options.multiple) < 0) {
            return Status::Invalid("Rounding multiple must be positive");
          }
          break;
        }
        case Type::HALF_FLOAT:
          return Status::NotImplemented("Half-float values are not supported");
        default:
          return Status::Invalid("Rounding multiple must be a ", type, " scalar, not ",
                                 *options.multiple->type);
      }
    } else {
      DCHECK(is_decimal(type.id()));
      if (!type.Equals(*options.multiple->type)) {
        return Status::Invalid("Rounding multiple must be a ", type, " scalar, not ",
                               *options.multiple->type);
      }
      switch (options.multiple->type->id()) {
        case Type::DECIMAL128: {
          if (UnboxScalar<Decimal128Type>::Unbox(*options.multiple) <= 0) {
            return Status::Invalid("Rounding multiple must be positive");
          }
          break;
        }
        case Type::DECIMAL256: {
          if (UnboxScalar<Decimal256Type>::Unbox(*options.multiple) <= 0) {
            return Status::Invalid("Rounding multiple must be positive");
          }
          break;
        }
        default:
          // This shouldn't happen
          return Status::Invalid("Rounding multiple must be a ", type, " scalar, not ",
                                 *options.multiple->type);
      }
    }
    return std::move(state);
  }
};

template <typename ArrowType, RoundMode RndMode, typename Enable = void>
struct Round {
  using CType = typename TypeTraits<ArrowType>::CType;
  using State = RoundOptionsWrapper<RoundOptions>;

  CType pow10;
  int64_t ndigits;

  explicit Round(const State& state, const DataType& out_ty)
      : pow10(static_cast<CType>(state.pow10)), ndigits(state.options.ndigits) {}

  template <typename T = ArrowType, typename CType = typename TypeTraits<T>::CType>
  enable_if_floating_point<CType> Call(KernelContext* ctx, CType arg, Status* st) const {
    // Do not process Inf or NaN because they will trigger the overflow error at end of
    // function.
    if (!std::isfinite(arg)) {
      return arg;
    }
    auto round_val = ndigits >= 0 ? (arg * pow10) : (arg / pow10);
    auto frac = round_val - std::floor(round_val);
    if (frac != T(0)) {
      // Use std::round() if in tie-breaking mode and scaled value is not 0.5.
      if ((RndMode >= RoundMode::HALF_DOWN) && (frac != T(0.5))) {
        round_val = std::round(round_val);
      } else {
        round_val = RoundImpl<CType, RndMode>::Round(round_val);
      }
      // Equality check is ommitted so that the common case of 10^0 (integer rounding)
      // uses multiply-only
      round_val = ndigits > 0 ? (round_val / pow10) : (round_val * pow10);
      if (!std::isfinite(round_val)) {
        *st = Status::Invalid("overflow occurred during rounding");
        return arg;
      }
    } else {
      // If scaled value is an integer, then no rounding is needed.
      round_val = arg;
    }
    return round_val;
  }
};

template <typename ArrowType, RoundMode kRoundMode>
struct Round<ArrowType, kRoundMode, enable_if_decimal<ArrowType>> {
  using CType = typename TypeTraits<ArrowType>::CType;
  using State = RoundOptionsWrapper<RoundOptions>;

  const ArrowType& ty;
  int64_t ndigits;
  int32_t pow;
  // pow10 is "1" for the given decimal scale. Similarly half_pow10 is "0.5".
  CType pow10, half_pow10, neg_half_pow10;

  explicit Round(const State& state, const DataType& out_ty)
      : Round(state.options.ndigits, out_ty) {}

  explicit Round(int64_t ndigits, const DataType& out_ty)
      : ty(checked_cast<const ArrowType&>(out_ty)),
        ndigits(ndigits),
        pow(static_cast<int32_t>(ty.scale() - ndigits)) {
    if (pow >= ty.precision() || pow < 0) {
      pow10 = half_pow10 = neg_half_pow10 = 0;
    } else {
      pow10 = CType::GetScaleMultiplier(pow);
      half_pow10 = CType::GetHalfScaleMultiplier(pow);
      neg_half_pow10 = -half_pow10;
    }
  }

  template <typename T = ArrowType, typename CType = typename TypeTraits<T>::CType>
  enable_if_decimal_value<CType> Call(KernelContext* ctx, CType arg, Status* st) const {
    if (pow >= ty.precision()) {
      *st = Status::Invalid("Rounding to ", ndigits,
                            " digits will not fit in precision of ", ty);
      return arg;
    } else if (pow < 0) {
      // no-op, copy output to input
      return arg;
    }

    std::pair<CType, CType> pair;
    *st = arg.Divide(pow10).Value(&pair);
    if (!st->ok()) return arg;
    // The remainder is effectively the scaled fractional part after division.
    const auto& remainder = pair.second;
    if (remainder == 0) return arg;
    if (kRoundMode >= RoundMode::HALF_DOWN) {
      if (remainder == half_pow10 || remainder == neg_half_pow10) {
        // On the halfway point, use tiebreaker
        RoundImpl<CType, kRoundMode>::Round(&arg, remainder, pow10, pow);
      } else if (remainder.Sign() >= 0) {
        // Positive, round up/down
        arg -= remainder;
        if (remainder > half_pow10) {
          arg += pow10;
        }
      } else {
        // Negative, round up/down
        arg -= remainder;
        if (remainder < neg_half_pow10) {
          arg -= pow10;
        }
      }
    } else {
      RoundImpl<CType, kRoundMode>::Round(&arg, remainder, pow10, pow);
    }
    if (!arg.FitsInPrecision(ty.precision())) {
      *st = Status::Invalid("Rounded value ", arg.ToString(ty.scale()),
                            " does not fit in precision of ", ty);
      return 0;
    }
    return arg;
  }
};

template <typename DecimalType, RoundMode kMode, int32_t kDigits>
Status FixedRoundDecimalExec(KernelContext* ctx, const ExecBatch& batch, Datum* out) {
  using Op = Round<DecimalType, kMode>;
  return ScalarUnaryNotNullStateful<DecimalType, DecimalType, Op>(
             Op(kDigits, *out->type()))
      .Exec(ctx, batch, out);
}

template <typename ArrowType, RoundMode kRoundMode, typename Enable = void>
struct RoundToMultiple {
  using CType = typename TypeTraits<ArrowType>::CType;
  using State = RoundOptionsWrapper<RoundToMultipleOptions>;

  CType multiple;

  explicit RoundToMultiple(const State& state, const DataType& out_ty) {
    const auto& options = state.options;
    DCHECK(options.multiple);
    DCHECK(options.multiple->is_valid);
    DCHECK(is_floating(options.multiple->type->id()));
    switch (options.multiple->type->id()) {
      case Type::FLOAT:
        multiple = static_cast<CType>(UnboxScalar<FloatType>::Unbox(*options.multiple));
        break;
      case Type::DOUBLE:
        multiple = static_cast<CType>(UnboxScalar<DoubleType>::Unbox(*options.multiple));
        break;
      default:
        DCHECK(false);
    }
  }

  template <typename T = ArrowType, typename CType = typename TypeTraits<T>::CType>
  enable_if_floating_point<CType> Call(KernelContext* ctx, CType arg, Status* st) const {
    // Do not process Inf or NaN because they will trigger the overflow error at end of
    // function.
    if (!std::isfinite(arg)) {
      return arg;
    }
    auto round_val = arg / multiple;
    auto frac = round_val - std::floor(round_val);
    if (frac != T(0)) {
      // Use std::round() if in tie-breaking mode and scaled value is not 0.5.
      if ((kRoundMode >= RoundMode::HALF_DOWN) && (frac != T(0.5))) {
        round_val = std::round(round_val);
      } else {
        round_val = RoundImpl<CType, kRoundMode>::Round(round_val);
      }
      round_val *= multiple;
      if (!std::isfinite(round_val)) {
        *st = Status::Invalid("overflow occurred during rounding");
        return arg;
      }
    } else {
      // If scaled value is an integer, then no rounding is needed.
      round_val = arg;
    }
    return round_val;
  }
};

template <typename ArrowType, RoundMode kRoundMode>
struct RoundToMultiple<ArrowType, kRoundMode, enable_if_decimal<ArrowType>> {
  using CType = typename TypeTraits<ArrowType>::CType;
  using State = RoundOptionsWrapper<RoundToMultipleOptions>;

  const ArrowType& ty;
  CType multiple, half_multiple, neg_half_multiple;
  bool has_halfway_point;

  explicit RoundToMultiple(const State& state, const DataType& out_ty)
      : ty(checked_cast<const ArrowType&>(out_ty)) {
    const auto& options = state.options;
    DCHECK(options.multiple);
    DCHECK(options.multiple->is_valid);
    DCHECK(options.multiple->type->Equals(out_ty));
    multiple = UnboxScalar<ArrowType>::Unbox(*options.multiple);
    half_multiple = multiple;
    half_multiple /= 2;
    neg_half_multiple = -half_multiple;
    has_halfway_point = multiple.low_bits() % 2 == 0;
  }

  template <typename T = ArrowType, typename CType = typename TypeTraits<T>::CType>
  enable_if_decimal_value<CType> Call(KernelContext* ctx, CType arg, Status* st) const {
    std::pair<CType, CType> pair;
    *st = arg.Divide(multiple).Value(&pair);
    if (!st->ok()) return arg;
    const auto& remainder = pair.second;
    if (remainder == 0) return arg;
    if (kRoundMode >= RoundMode::HALF_DOWN) {
      if (has_halfway_point &&
          (remainder == half_multiple || remainder == neg_half_multiple)) {
        // On the halfway point, use tiebreaker
        // Manually implement rounding since we're not actually rounding a
        // decimal value, but rather manipulating the multiple
        switch (kRoundMode) {
          case RoundMode::HALF_DOWN:
            if (remainder.Sign() < 0) pair.first -= 1;
            break;
          case RoundMode::HALF_UP:
            if (remainder.Sign() >= 0) pair.first += 1;
            break;
          case RoundMode::HALF_TOWARDS_ZERO:
            // Do nothing
            break;
          case RoundMode::HALF_TOWARDS_INFINITY:
            if (remainder.Sign() >= 0) {
              pair.first += 1;
            } else {
              pair.first -= 1;
            }
            break;
          case RoundMode::HALF_TO_EVEN:
            if (pair.first.low_bits() % 2 != 0) {
              pair.first += remainder.Sign() >= 0 ? 1 : -1;
            }
            break;
          case RoundMode::HALF_TO_ODD:
            if (pair.first.low_bits() % 2 == 0) {
              pair.first += remainder.Sign() >= 0 ? 1 : -1;
            }
            break;
          default:
            DCHECK(false);
        }
      } else if (remainder.Sign() >= 0) {
        // Positive, round up/down
        if (remainder > half_multiple) {
          pair.first += 1;
        }
      } else {
        // Negative, round up/down
        if (remainder < neg_half_multiple) {
          pair.first -= 1;
        }
      }
    } else {
      // Manually implement rounding since we're not actually rounding a
      // decimal value, but rather manipulating the multiple
      switch (kRoundMode) {
        case RoundMode::DOWN:
          if (remainder.Sign() < 0) pair.first -= 1;
          break;
        case RoundMode::UP:
          if (remainder.Sign() >= 0) pair.first += 1;
          break;
        case RoundMode::TOWARDS_ZERO:
          // Do nothing
          break;
        case RoundMode::TOWARDS_INFINITY:
          if (remainder.Sign() >= 0) {
            pair.first += 1;
          } else {
            pair.first -= 1;
          }
          break;
        default:
          DCHECK(false);
      }
    }
    CType round_val = pair.first * multiple;
    if (!round_val.FitsInPrecision(ty.precision())) {
      *st = Status::Invalid("Rounded value ", round_val.ToString(ty.scale()),
                            " does not fit in precision of ", ty);
      return 0;
    }
    return round_val;
  }
};

struct Floor {
  template <typename T, typename Arg>
  static constexpr enable_if_floating_point<Arg, T> Call(KernelContext*, Arg arg,
                                                         Status*) {
    static_assert(std::is_same<T, Arg>::value, "");
    return RoundImpl<T, RoundMode::DOWN>::Round(arg);
  }
};

struct Ceil {
  template <typename T, typename Arg>
  static constexpr enable_if_floating_point<Arg, T> Call(KernelContext*, Arg arg,
                                                         Status*) {
    static_assert(std::is_same<T, Arg>::value, "");
    return RoundImpl<T, RoundMode::UP>::Round(arg);
  }
};

struct Trunc {
  template <typename T, typename Arg>
  static constexpr enable_if_floating_point<Arg, T> Call(KernelContext*, Arg arg,
                                                         Status*) {
    static_assert(std::is_same<T, Arg>::value, "");
    return RoundImpl<T, RoundMode::TOWARDS_ZERO>::Round(arg);
  }
};

// Generate a kernel given an arithmetic functor
template <template <typename... Args> class KernelGenerator, typename Op>
ArrayKernelExec ArithmeticExecFromOp(detail::GetTypeId get_id) {
  switch (get_id.id) {
    case Type::INT8:
      return KernelGenerator<Int8Type, Int8Type, Op>::Exec;
    case Type::UINT8:
      return KernelGenerator<UInt8Type, UInt8Type, Op>::Exec;
    case Type::INT16:
      return KernelGenerator<Int16Type, Int16Type, Op>::Exec;
    case Type::UINT16:
      return KernelGenerator<UInt16Type, UInt16Type, Op>::Exec;
    case Type::INT32:
      return KernelGenerator<Int32Type, Int32Type, Op>::Exec;
    case Type::UINT32:
      return KernelGenerator<UInt32Type, UInt32Type, Op>::Exec;
    case Type::INT64:
    case Type::TIMESTAMP:
      return KernelGenerator<Int64Type, Int64Type, Op>::Exec;
    case Type::UINT64:
      return KernelGenerator<UInt64Type, UInt64Type, Op>::Exec;
    case Type::FLOAT:
      return KernelGenerator<FloatType, FloatType, Op>::Exec;
    case Type::DOUBLE:
      return KernelGenerator<DoubleType, DoubleType, Op>::Exec;
    default:
      DCHECK(false);
      return ExecFail;
  }
}

// Generate a kernel given a bitwise arithmetic functor. Assumes the
// functor treats all integer types of equal width identically
template <template <typename... Args> class KernelGenerator, typename Op>
ArrayKernelExec TypeAgnosticBitWiseExecFromOp(detail::GetTypeId get_id) {
  switch (get_id.id) {
    case Type::INT8:
    case Type::UINT8:
      return KernelGenerator<UInt8Type, UInt8Type, Op>::Exec;
    case Type::INT16:
    case Type::UINT16:
      return KernelGenerator<UInt16Type, UInt16Type, Op>::Exec;
    case Type::INT32:
    case Type::UINT32:
      return KernelGenerator<UInt32Type, UInt32Type, Op>::Exec;
    case Type::INT64:
    case Type::UINT64:
      return KernelGenerator<UInt64Type, UInt64Type, Op>::Exec;
    default:
      DCHECK(false);
      return ExecFail;
  }
}

template <template <typename... Args> class KernelGenerator, typename Op>
ArrayKernelExec ShiftExecFromOp(detail::GetTypeId get_id) {
  switch (get_id.id) {
    case Type::INT8:
      return KernelGenerator<Int8Type, Int8Type, Op>::Exec;
    case Type::UINT8:
      return KernelGenerator<UInt8Type, UInt8Type, Op>::Exec;
    case Type::INT16:
      return KernelGenerator<Int16Type, Int16Type, Op>::Exec;
    case Type::UINT16:
      return KernelGenerator<UInt16Type, UInt16Type, Op>::Exec;
    case Type::INT32:
      return KernelGenerator<Int32Type, Int32Type, Op>::Exec;
    case Type::UINT32:
      return KernelGenerator<UInt32Type, UInt32Type, Op>::Exec;
    case Type::INT64:
      return KernelGenerator<Int64Type, Int64Type, Op>::Exec;
    case Type::UINT64:
      return KernelGenerator<UInt64Type, UInt64Type, Op>::Exec;
    default:
      DCHECK(false);
      return ExecFail;
  }
}

template <template <typename... Args> class KernelGenerator, typename Op>
ArrayKernelExec GenerateArithmeticFloatingPoint(detail::GetTypeId get_id) {
  switch (get_id.id) {
    case Type::FLOAT:
      return KernelGenerator<FloatType, FloatType, Op>::Exec;
    case Type::DOUBLE:
      return KernelGenerator<DoubleType, DoubleType, Op>::Exec;
    default:
      DCHECK(false);
      return ExecFail;
  }
}

// resolve decimal binary operation output type per *casted* args
template <typename OutputGetter>
Result<ValueDescr> ResolveDecimalBinaryOperationOutput(
    const std::vector<ValueDescr>& args, OutputGetter&& getter) {
  // casted args should be same size decimals
  auto left_type = checked_cast<const DecimalType*>(args[0].type.get());
  auto right_type = checked_cast<const DecimalType*>(args[1].type.get());
  DCHECK_EQ(left_type->id(), right_type->id());

  int32_t precision, scale;
  std::tie(precision, scale) = getter(left_type->precision(), left_type->scale(),
                                      right_type->precision(), right_type->scale());
  ARROW_ASSIGN_OR_RAISE(auto type, DecimalType::Make(left_type->id(), precision, scale));
  return ValueDescr(std::move(type), GetBroadcastShape(args));
}

Result<ValueDescr> ResolveDecimalAdditionOrSubtractionOutput(
    KernelContext*, const std::vector<ValueDescr>& args) {
  return ResolveDecimalBinaryOperationOutput(
      args, [](int32_t p1, int32_t s1, int32_t p2, int32_t s2) {
        DCHECK_EQ(s1, s2);
        const int32_t scale = s1;
        const int32_t precision = std::max(p1 - s1, p2 - s2) + scale + 1;
        return std::make_pair(precision, scale);
      });
}

Result<ValueDescr> ResolveDecimalMultiplicationOutput(
    KernelContext*, const std::vector<ValueDescr>& args) {
  return ResolveDecimalBinaryOperationOutput(
      args, [](int32_t p1, int32_t s1, int32_t p2, int32_t s2) {
        const int32_t scale = s1 + s2;
        const int32_t precision = p1 + p2 + 1;
        return std::make_pair(precision, scale);
      });
}

Result<ValueDescr> ResolveDecimalDivisionOutput(KernelContext*,
                                                const std::vector<ValueDescr>& args) {
  return ResolveDecimalBinaryOperationOutput(
      args, [](int32_t p1, int32_t s1, int32_t p2, int32_t s2) {
        DCHECK_GE(s1, s2);
        const int32_t scale = s1 - s2;
        const int32_t precision = p1;
        return std::make_pair(precision, scale);
      });
}

template <typename Op>
void AddDecimalBinaryKernels(const std::string& name,
                             std::shared_ptr<ScalarFunction>* func) {
  OutputType out_type(null());
  const std::string op = name.substr(0, name.find("_"));
  if (op == "add" || op == "subtract") {
    out_type = OutputType(ResolveDecimalAdditionOrSubtractionOutput);
  } else if (op == "multiply") {
    out_type = OutputType(ResolveDecimalMultiplicationOutput);
  } else if (op == "divide") {
    out_type = OutputType(ResolveDecimalDivisionOutput);
  } else {
    DCHECK(false);
  }

  auto in_type128 = InputType(Type::DECIMAL128);
  auto in_type256 = InputType(Type::DECIMAL256);
  auto exec128 = ScalarBinaryNotNullEqualTypes<Decimal128Type, Decimal128Type, Op>::Exec;
  auto exec256 = ScalarBinaryNotNullEqualTypes<Decimal256Type, Decimal256Type, Op>::Exec;
  DCHECK_OK((*func)->AddKernel({in_type128, in_type128}, out_type, exec128));
  DCHECK_OK((*func)->AddKernel({in_type256, in_type256}, out_type, exec256));
}

// Generate a kernel given an arithmetic functor
template <template <typename...> class KernelGenerator, typename OutType, typename Op>
ArrayKernelExec GenerateArithmeticWithFixedIntOutType(detail::GetTypeId get_id) {
  switch (get_id.id) {
    case Type::INT8:
      return KernelGenerator<OutType, Int8Type, Op>::Exec;
    case Type::UINT8:
      return KernelGenerator<OutType, UInt8Type, Op>::Exec;
    case Type::INT16:
      return KernelGenerator<OutType, Int16Type, Op>::Exec;
    case Type::UINT16:
      return KernelGenerator<OutType, UInt16Type, Op>::Exec;
    case Type::INT32:
      return KernelGenerator<OutType, Int32Type, Op>::Exec;
    case Type::UINT32:
      return KernelGenerator<OutType, UInt32Type, Op>::Exec;
    case Type::INT64:
    case Type::TIMESTAMP:
      return KernelGenerator<OutType, Int64Type, Op>::Exec;
    case Type::UINT64:
      return KernelGenerator<OutType, UInt64Type, Op>::Exec;
    case Type::FLOAT:
      return KernelGenerator<FloatType, FloatType, Op>::Exec;
    case Type::DOUBLE:
      return KernelGenerator<DoubleType, DoubleType, Op>::Exec;
    default:
      DCHECK(false);
      return ExecFail;
  }
}

struct ArithmeticFunction : ScalarFunction {
  using ScalarFunction::ScalarFunction;

  Result<const Kernel*> DispatchBest(std::vector<ValueDescr>* values) const override {
    RETURN_NOT_OK(CheckArity(*values));

    RETURN_NOT_OK(CheckDecimals(values));

    using arrow::compute::detail::DispatchExactImpl;
    if (auto kernel = DispatchExactImpl(this, *values)) return kernel;

    EnsureDictionaryDecoded(values);

    // Only promote types for binary functions
    if (values->size() == 2) {
      ReplaceNullWithOtherType(values);

      if (auto type = CommonNumeric(*values)) {
        ReplaceTypes(type, values);
      }
    }

    if (auto kernel = DispatchExactImpl(this, *values)) return kernel;
    return arrow::compute::detail::NoMatchingKernel(this, *values);
  }

  Status CheckDecimals(std::vector<ValueDescr>* values) const {
    if (!HasDecimal(*values)) return Status::OK();

    if (values->size() == 2) {
      // "add_checked" -> "add"
      const auto func_name = name();
      const std::string op = func_name.substr(0, func_name.find("_"));
      if (op == "add" || op == "subtract") {
        return CastBinaryDecimalArgs(DecimalPromotion::kAdd, values);
      } else if (op == "multiply") {
        return CastBinaryDecimalArgs(DecimalPromotion::kMultiply, values);
      } else if (op == "divide") {
        return CastBinaryDecimalArgs(DecimalPromotion::kDivide, values);
      } else {
        return Status::Invalid("Invalid decimal function: ", func_name);
      }
    }
    return Status::OK();
  }
};

/// An ArithmeticFunction that promotes only integer arguments to double.
struct ArithmeticIntegerToFloatingPointFunction : public ArithmeticFunction {
  using ArithmeticFunction::ArithmeticFunction;

  Result<const Kernel*> DispatchBest(std::vector<ValueDescr>* values) const override {
    RETURN_NOT_OK(CheckArity(*values));
    RETURN_NOT_OK(CheckDecimals(values));

    using arrow::compute::detail::DispatchExactImpl;
    if (auto kernel = DispatchExactImpl(this, *values)) return kernel;

    EnsureDictionaryDecoded(values);

    if (values->size() == 2) {
      ReplaceNullWithOtherType(values);
    }

    for (auto& descr : *values) {
      if (is_integer(descr.type->id())) {
        descr.type = float64();
      }
    }
    if (auto type = CommonNumeric(*values)) {
      ReplaceTypes(type, values);
    }

    if (auto kernel = DispatchExactImpl(this, *values)) return kernel;
    return arrow::compute::detail::NoMatchingKernel(this, *values);
  }
};

/// An ArithmeticFunction that promotes integer arguments to double.
struct ArithmeticFloatingPointFunction : public ArithmeticFunction {
  using ArithmeticFunction::ArithmeticFunction;

  Result<const Kernel*> DispatchBest(std::vector<ValueDescr>* values) const override {
    RETURN_NOT_OK(CheckArity(*values));
    RETURN_NOT_OK(CheckDecimals(values));

    using arrow::compute::detail::DispatchExactImpl;
    if (auto kernel = DispatchExactImpl(this, *values)) return kernel;

    EnsureDictionaryDecoded(values);

    if (values->size() == 2) {
      ReplaceNullWithOtherType(values);
    }

    for (auto& descr : *values) {
      if (is_integer(descr.type->id())) {
        descr.type = float64();
      }
    }
    if (auto type = CommonNumeric(*values)) {
      ReplaceTypes(type, values);
    }

    if (auto kernel = DispatchExactImpl(this, *values)) return kernel;
    return arrow::compute::detail::NoMatchingKernel(this, *values);
  }
};

// A scalar kernel that ignores (assumed all-null) inputs and returns null.
Status NullToNullExec(KernelContext* ctx, const ExecBatch& batch, Datum* out) {
  return Status::OK();
}

void AddNullExec(ScalarFunction* func) {
  std::vector<InputType> input_types(func->arity().num_args, InputType(Type::NA));
  DCHECK_OK(func->AddKernel(std::move(input_types), OutputType(null()), NullToNullExec));
}

template <typename Op>
std::shared_ptr<ScalarFunction> MakeArithmeticFunction(std::string name,
                                                       const FunctionDoc* doc) {
  auto func = std::make_shared<ArithmeticFunction>(name, Arity::Binary(), doc);
  for (const auto& ty : NumericTypes()) {
    auto exec = ArithmeticExecFromOp<ScalarBinaryEqualTypes, Op>(ty);
    DCHECK_OK(func->AddKernel({ty, ty}, ty, exec));
  }
  AddNullExec(func.get());
  return func;
}

// Like MakeArithmeticFunction, but for arithmetic ops that need to run
// only on non-null output.
template <typename Op>
std::shared_ptr<ScalarFunction> MakeArithmeticFunctionNotNull(std::string name,
                                                              const FunctionDoc* doc) {
  auto func = std::make_shared<ArithmeticFunction>(name, Arity::Binary(), doc);
  for (const auto& ty : NumericTypes()) {
    auto exec = ArithmeticExecFromOp<ScalarBinaryNotNullEqualTypes, Op>(ty);
    DCHECK_OK(func->AddKernel({ty, ty}, ty, exec));
  }
  AddNullExec(func.get());
  return func;
}

template <typename Op>
std::shared_ptr<ScalarFunction> MakeUnaryArithmeticFunction(std::string name,
                                                            const FunctionDoc* doc) {
  auto func = std::make_shared<ArithmeticFunction>(name, Arity::Unary(), doc);
  for (const auto& ty : NumericTypes()) {
    auto exec = ArithmeticExecFromOp<ScalarUnary, Op>(ty);
    DCHECK_OK(func->AddKernel({ty}, ty, exec));
  }
  AddNullExec(func.get());
  return func;
}

// Like MakeUnaryArithmeticFunction, but for unary arithmetic ops with a fixed
// output type for integral inputs.
template <typename Op, typename IntOutType>
std::shared_ptr<ScalarFunction> MakeUnaryArithmeticFunctionWithFixedIntOutType(
    std::string name, const FunctionDoc* doc) {
  auto int_out_ty = TypeTraits<IntOutType>::type_singleton();
  auto func = std::make_shared<ArithmeticFunction>(name, Arity::Unary(), doc);
  for (const auto& ty : NumericTypes()) {
    auto out_ty = arrow::is_floating(ty->id()) ? ty : int_out_ty;
    auto exec = GenerateArithmeticWithFixedIntOutType<ScalarUnary, IntOutType, Op>(ty);
    DCHECK_OK(func->AddKernel({ty}, out_ty, exec));
  }
  AddNullExec(func.get());
  return func;
}

// Like MakeUnaryArithmeticFunction, but for arithmetic ops that need to run
// only on non-null output.
template <typename Op>
std::shared_ptr<ScalarFunction> MakeUnaryArithmeticFunctionNotNull(
    std::string name, const FunctionDoc* doc) {
  auto func = std::make_shared<ArithmeticFunction>(name, Arity::Unary(), doc);
  for (const auto& ty : NumericTypes()) {
    auto exec = ArithmeticExecFromOp<ScalarUnaryNotNull, Op>(ty);
    DCHECK_OK(func->AddKernel({ty}, ty, exec));
  }
  AddNullExec(func.get());
  return func;
}

// Exec the round kernel for the given types
template <typename Type, typename OptionsType,
          template <typename, RoundMode, typename...> class OpImpl>
Status ExecRound(KernelContext* ctx, const ExecBatch& batch, Datum* out) {
  using State = RoundOptionsWrapper<OptionsType>;
  const auto& state = static_cast<const State&>(*ctx->state());
  switch (state.options.round_mode) {
    case RoundMode::DOWN: {
      using Op = OpImpl<Type, RoundMode::DOWN>;
      return ScalarUnaryNotNullStateful<Type, Type, Op>(Op(state, *out->type()))
          .Exec(ctx, batch, out);
    }
    case RoundMode::UP: {
      using Op = OpImpl<Type, RoundMode::UP>;
      return ScalarUnaryNotNullStateful<Type, Type, Op>(Op(state, *out->type()))
          .Exec(ctx, batch, out);
    }
    case RoundMode::TOWARDS_ZERO: {
      using Op = OpImpl<Type, RoundMode::TOWARDS_ZERO>;
      return ScalarUnaryNotNullStateful<Type, Type, Op>(Op(state, *out->type()))
          .Exec(ctx, batch, out);
    }
    case RoundMode::TOWARDS_INFINITY: {
      using Op = OpImpl<Type, RoundMode::TOWARDS_INFINITY>;
      return ScalarUnaryNotNullStateful<Type, Type, Op>(Op(state, *out->type()))
          .Exec(ctx, batch, out);
    }
    case RoundMode::HALF_DOWN: {
      using Op = OpImpl<Type, RoundMode::HALF_DOWN>;
      return ScalarUnaryNotNullStateful<Type, Type, Op>(Op(state, *out->type()))
          .Exec(ctx, batch, out);
    }
    case RoundMode::HALF_UP: {
      using Op = OpImpl<Type, RoundMode::HALF_UP>;
      return ScalarUnaryNotNullStateful<Type, Type, Op>(Op(state, *out->type()))
          .Exec(ctx, batch, out);
    }
    case RoundMode::HALF_TOWARDS_ZERO: {
      using Op = OpImpl<Type, RoundMode::HALF_TOWARDS_ZERO>;
      return ScalarUnaryNotNullStateful<Type, Type, Op>(Op(state, *out->type()))
          .Exec(ctx, batch, out);
    }
    case RoundMode::HALF_TOWARDS_INFINITY: {
      using Op = OpImpl<Type, RoundMode::HALF_TOWARDS_INFINITY>;
      return ScalarUnaryNotNullStateful<Type, Type, Op>(Op(state, *out->type()))
          .Exec(ctx, batch, out);
    }
    case RoundMode::HALF_TO_EVEN: {
      using Op = OpImpl<Type, RoundMode::HALF_TO_EVEN>;
      return ScalarUnaryNotNullStateful<Type, Type, Op>(Op(state, *out->type()))
          .Exec(ctx, batch, out);
    }
    case RoundMode::HALF_TO_ODD: {
      using Op = OpImpl<Type, RoundMode::HALF_TO_ODD>;
      return ScalarUnaryNotNullStateful<Type, Type, Op>(Op(state, *out->type()))
          .Exec(ctx, batch, out);
    }
  }
  DCHECK(false);
  return Status::NotImplemented(
      "Internal implementation error: round mode not implemented: ",
      state.options.ToString());
}

// Like MakeUnaryArithmeticFunction, but for unary rounding functions that control
// kernel dispatch based on RoundMode, only on non-null output.
template <template <typename, RoundMode, typename...> class Op, typename OptionsType>
std::shared_ptr<ScalarFunction> MakeUnaryRoundFunction(std::string name,
                                                       const FunctionDoc* doc) {
  using State = RoundOptionsWrapper<OptionsType>;
  static const OptionsType kDefaultOptions = OptionsType::Defaults();
  auto func = std::make_shared<ArithmeticIntegerToFloatingPointFunction>(
      name, Arity::Unary(), doc, &kDefaultOptions);
  for (const auto& ty : {float32(), float64(), decimal128(1, 0), decimal256(1, 0)}) {
    auto type_id = ty->id();
    auto exec = [type_id](KernelContext* ctx, const ExecBatch& batch, Datum* out) {
      switch (type_id) {
        case Type::FLOAT:
          return ExecRound<FloatType, OptionsType, Op>(ctx, batch, out);
        case Type::DOUBLE:
          return ExecRound<DoubleType, OptionsType, Op>(ctx, batch, out);
        case Type::DECIMAL128:
          return ExecRound<Decimal128Type, OptionsType, Op>(ctx, batch, out);
        case Type::DECIMAL256:
          return ExecRound<Decimal256Type, OptionsType, Op>(ctx, batch, out);
        default: {
          DCHECK(false);
          return ExecFail(ctx, batch, out);
        }
      }
    };
    DCHECK_OK(func->AddKernel(
        {InputType(type_id)},
        is_decimal(type_id) ? OutputType(FirstType) : OutputType(ty), exec, State::Init));
  }
  AddNullExec(func.get());
  return func;
}

// Like MakeUnaryArithmeticFunction, but for signed arithmetic ops that need to run
// only on non-null output.
template <typename Op>
std::shared_ptr<ScalarFunction> MakeUnarySignedArithmeticFunctionNotNull(
    std::string name, const FunctionDoc* doc) {
  auto func = std::make_shared<ArithmeticFunction>(name, Arity::Unary(), doc);
  for (const auto& ty : NumericTypes()) {
    if (!arrow::is_unsigned_integer(ty->id())) {
      auto exec = ArithmeticExecFromOp<ScalarUnaryNotNull, Op>(ty);
      DCHECK_OK(func->AddKernel({ty}, ty, exec));
    }
  }
  AddNullExec(func.get());
  return func;
}

template <typename Op>
std::shared_ptr<ScalarFunction> MakeBitWiseFunctionNotNull(std::string name,
                                                           const FunctionDoc* doc) {
  auto func = std::make_shared<ArithmeticFunction>(name, Arity::Binary(), doc);
  for (const auto& ty : IntTypes()) {
    auto exec = TypeAgnosticBitWiseExecFromOp<ScalarBinaryNotNullEqualTypes, Op>(ty);
    DCHECK_OK(func->AddKernel({ty, ty}, ty, exec));
  }
  AddNullExec(func.get());
  return func;
}

template <typename Op>
std::shared_ptr<ScalarFunction> MakeShiftFunctionNotNull(std::string name,
                                                         const FunctionDoc* doc) {
  auto func = std::make_shared<ArithmeticFunction>(name, Arity::Binary(), doc);
  for (const auto& ty : IntTypes()) {
    auto exec = ShiftExecFromOp<ScalarBinaryNotNullEqualTypes, Op>(ty);
    DCHECK_OK(func->AddKernel({ty, ty}, ty, exec));
  }
  AddNullExec(func.get());
  return func;
}

template <typename Op, typename FunctionImpl = ArithmeticFloatingPointFunction>
std::shared_ptr<ScalarFunction> MakeUnaryArithmeticFunctionFloatingPoint(
    std::string name, const FunctionDoc* doc) {
  auto func = std::make_shared<FunctionImpl>(name, Arity::Unary(), doc);
  for (const auto& ty : FloatingPointTypes()) {
    auto exec = GenerateArithmeticFloatingPoint<ScalarUnary, Op>(ty);
    DCHECK_OK(func->AddKernel({ty}, ty, exec));
  }
  AddNullExec(func.get());
  return func;
}

template <typename Op>
std::shared_ptr<ScalarFunction> MakeUnaryArithmeticFunctionFloatingPointNotNull(
    std::string name, const FunctionDoc* doc) {
  auto func =
      std::make_shared<ArithmeticFloatingPointFunction>(name, Arity::Unary(), doc);
  for (const auto& ty : FloatingPointTypes()) {
    auto exec = GenerateArithmeticFloatingPoint<ScalarUnaryNotNull, Op>(ty);
    DCHECK_OK(func->AddKernel({ty}, ty, exec));
  }
  AddNullExec(func.get());
  return func;
}

template <typename Op>
std::shared_ptr<ScalarFunction> MakeArithmeticFunctionFloatingPoint(
    std::string name, const FunctionDoc* doc) {
  auto func =
      std::make_shared<ArithmeticFloatingPointFunction>(name, Arity::Binary(), doc);
  for (const auto& ty : FloatingPointTypes()) {
    auto exec = GenerateArithmeticFloatingPoint<ScalarBinaryEqualTypes, Op>(ty);
    DCHECK_OK(func->AddKernel({ty, ty}, ty, exec));
  }
  AddNullExec(func.get());
  return func;
}

template <typename Op>
std::shared_ptr<ScalarFunction> MakeArithmeticFunctionFloatingPointNotNull(
    std::string name, const FunctionDoc* doc) {
  auto func =
      std::make_shared<ArithmeticFloatingPointFunction>(name, Arity::Binary(), doc);
  for (const auto& ty : FloatingPointTypes()) {
    auto output = is_integer(ty->id()) ? float64() : ty;
    auto exec = GenerateArithmeticFloatingPoint<ScalarBinaryNotNullEqualTypes, Op>(ty);
    DCHECK_OK(func->AddKernel({ty, ty}, output, exec));
  }
  AddNullExec(func.get());
  return func;
}

const FunctionDoc absolute_value_doc{
    "Calculate the absolute value of the argument element-wise",
    ("Results will wrap around on integer overflow.\n"
     "Use function \"abs_checked\" if you want overflow\n"
     "to return an error."),
    {"x"}};

const FunctionDoc absolute_value_checked_doc{
    "Calculate the absolute value of the argument element-wise",
    ("This function returns an error on overflow.  For a variant that\n"
     "doesn't fail on overflow, use function \"abs\"."),
    {"x"}};

const FunctionDoc add_doc{"Add the arguments element-wise",
                          ("Results will wrap around on integer overflow.\n"
                           "Use function \"add_checked\" if you want overflow\n"
                           "to return an error."),
                          {"x", "y"}};

const FunctionDoc add_checked_doc{
    "Add the arguments element-wise",
    ("This function returns an error on overflow.  For a variant that\n"
     "doesn't fail on overflow, use function \"add\"."),
    {"x", "y"}};

const FunctionDoc sub_doc{"Subtract the arguments element-wise",
                          ("Results will wrap around on integer overflow.\n"
                           "Use function \"subtract_checked\" if you want overflow\n"
                           "to return an error."),
                          {"x", "y"}};

const FunctionDoc sub_checked_doc{
    "Subtract the arguments element-wise",
    ("This function returns an error on overflow.  For a variant that\n"
     "doesn't fail on overflow, use function \"subtract\"."),
    {"x", "y"}};

const FunctionDoc mul_doc{"Multiply the arguments element-wise",
                          ("Results will wrap around on integer overflow.\n"
                           "Use function \"multiply_checked\" if you want overflow\n"
                           "to return an error."),
                          {"x", "y"}};

const FunctionDoc mul_checked_doc{
    "Multiply the arguments element-wise",
    ("This function returns an error on overflow.  For a variant that\n"
     "doesn't fail on overflow, use function \"multiply\"."),
    {"x", "y"}};

const FunctionDoc div_doc{
    "Divide the arguments element-wise",
    ("Integer division by zero returns an error. However, integer overflow\n"
     "wraps around, and floating-point division by zero returns an infinite.\n"
     "Use function \"divide_checked\" if you want to get an error\n"
     "in all the aforementioned cases."),
    {"dividend", "divisor"}};

const FunctionDoc div_checked_doc{
    "Divide the arguments element-wise",
    ("An error is returned when trying to divide by zero, or when\n"
     "integer overflow is encountered."),
    {"dividend", "divisor"}};

const FunctionDoc negate_doc{"Negate the argument element-wise",
                             ("Results will wrap around on integer overflow.\n"
                              "Use function \"negate_checked\" if you want overflow\n"
                              "to return an error."),
                             {"x"}};

const FunctionDoc negate_checked_doc{
    "Negate the arguments element-wise",
    ("This function returns an error on overflow.  For a variant that\n"
     "doesn't fail on overflow, use function \"negate\"."),
    {"x"}};

const FunctionDoc pow_doc{
    "Raise arguments to power element-wise",
    ("Integer to negative integer power returns an error. However, integer overflow\n"
     "wraps around. If either base or exponent is null the result will be null."),
    {"base", "exponent"}};

const FunctionDoc pow_checked_doc{
    "Raise arguments to power element-wise",
    ("An error is returned when integer to negative integer power is encountered,\n"
     "or integer overflow is encountered."),
    {"base", "exponent"}};

const FunctionDoc sign_doc{
    "Get the signedness of the arguments element-wise",
    ("Output is any of (-1,1) for nonzero inputs and 0 for zero input.\n"
     "NaN values return NaN.  Integral values return signedness as Int8 and\n"
     "floating-point values return it with the same type as the input values."),
    {"x"}};

const FunctionDoc bit_wise_not_doc{
    "Bit-wise negate the arguments element-wise", "Null values return null.", {"x"}};

const FunctionDoc bit_wise_and_doc{
    "Bit-wise AND the arguments element-wise", "Null values return null.", {"x", "y"}};

const FunctionDoc bit_wise_or_doc{
    "Bit-wise OR the arguments element-wise", "Null values return null.", {"x", "y"}};

const FunctionDoc bit_wise_xor_doc{
    "Bit-wise XOR the arguments element-wise", "Null values return null.", {"x", "y"}};

const FunctionDoc shift_left_doc{
    "Left shift `x` by `y`",
    ("This function will return `x` if `y` (the amount to shift by) is: "
     "(1) negative or (2) greater than or equal to the precision of `x`.\n"
     "The shift operates as if on the two's complement representation of the number. "
     "In other words, this is equivalent to multiplying `x` by 2 to the power `y`, "
     "even if overflow occurs.\n"
     "Use function \"shift_left_checked\" if you want an invalid shift amount to "
     "return an error."),
    {"x", "y"}};

const FunctionDoc shift_left_checked_doc{
    "Left shift `x` by `y` with invalid shift check",
    ("This function will raise an error if `y` (the amount to shift by) is: "
     "(1) negative or (2) greater than or equal to the precision of `x`. "
     "The shift operates as if on the two's complement representation of the number. "
     "In other words, this is equivalent to multiplying `x` by 2 to the power `y`, "
     "even if overflow occurs.\n"
     "See \"shift_left\" for a variant that doesn't fail for an invalid shift amount."),
    {"x", "y"}};

const FunctionDoc shift_right_doc{
    "Right shift `x` by `y`",
    ("Perform a logical shift for unsigned `x` and an arithmetic shift for signed `x`.\n"
     "This function will return `x` if `y` (the amount to shift by) is: "
     "(1) negative or (2) greater than or equal to the precision of `x`.\n"
     "Use function \"shift_right_checked\" if you want an invalid shift amount to return "
     "an error."),
    {"x", "y"}};

const FunctionDoc shift_right_checked_doc{
    "Right shift `x` by `y` with invalid shift check",
    ("Perform a logical shift for unsigned `x` and an arithmetic shift for signed `x`.\n"
     "This function will raise an error if `y` (the amount to shift by) is: "
     "(1) negative or (2) greater than or equal to the precision of `x`.\n"
     "See \"shift_right\" for a variant that doesn't fail for an invalid shift amount"),
    {"x", "y"}};

const FunctionDoc sin_doc{"Compute the sine of the elements argument-wise",
                          ("Integer arguments return double values. "
                           "This function returns NaN on values outside its domain. "
                           "To raise an error instead, see \"sin_checked\"."),
                          {"x"}};

const FunctionDoc sin_checked_doc{
    "Compute the sine of the elements argument-wise",
    ("Integer arguments return double values. "
     "This function raises an error on values outside its domain. "
     "To return NaN instead, see \"sin\"."),
    {"x"}};

const FunctionDoc cos_doc{"Compute the cosine of the elements argument-wise",
                          ("Integer arguments return double values. "
                           "This function returns NaN on values outside its domain. "
                           "To raise an error instead, see \"cos_checked\"."),
                          {"x"}};

const FunctionDoc cos_checked_doc{
    "Compute the cosine of the elements argument-wise",
    ("Integer arguments return double values. "
     "This function raises an error on values outside its domain. "
     "To return NaN instead, see \"cos\"."),
    {"x"}};

const FunctionDoc tan_doc{"Compute the tangent of the elements argument-wise",
                          ("Integer arguments return double values. "
                           "This function returns NaN on values outside its domain. "
                           "To raise an error instead, see \"tan_checked\"."),
                          {"x"}};

const FunctionDoc tan_checked_doc{
    "Compute the tangent of the elements argument-wise",
    ("Integer arguments return double values. "
     "This function raises an error on values outside its domain. "
     "To return NaN instead, see \"tan\"."),
    {"x"}};

const FunctionDoc asin_doc{"Compute the inverse sine of the elements argument-wise",
                           ("Integer arguments return double values. "
                            "This function returns NaN on values outside its domain. "
                            "To raise an error instead, see \"asin_checked\"."),
                           {"x"}};

const FunctionDoc asin_checked_doc{
    "Compute the inverse sine of the elements argument-wise",
    ("Integer arguments return double values. "
     "This function raises an error on values outside its domain. "
     "To return NaN instead, see \"asin\"."),
    {"x"}};

const FunctionDoc acos_doc{"Compute the inverse cosine of the elements argument-wise",
                           ("Integer arguments return double values. "
                            "This function returns NaN on values outside its domain. "
                            "To raise an error instead, see \"acos_checked\"."),
                           {"x"}};

const FunctionDoc acos_checked_doc{
    "Compute the inverse cosine of the elements argument-wise",
    ("Integer arguments return double values. "
     "This function raises an error on values outside its domain. "
     "To return NaN instead, see \"acos\"."),
    {"x"}};

const FunctionDoc atan_doc{"Compute the principal value of the inverse tangent",
                           "Integer arguments return double values.",
                           {"x"}};

const FunctionDoc atan2_doc{
    "Compute the inverse tangent using argument signs to determine the quadrant",
    "Integer arguments return double values.",
    {"y", "x"}};

const FunctionDoc ln_doc{
    "Compute natural log of arguments element-wise",
    ("Non-positive values return -inf or NaN. Null values return null.\n"
     "Use function \"ln_checked\" if you want non-positive values to raise an error."),
    {"x"}};

const FunctionDoc ln_checked_doc{
    "Compute natural log of arguments element-wise",
    ("Non-positive values return -inf or NaN. Null values return null.\n"
     "Use function \"ln\" if you want non-positive values to return "
     "-inf or NaN."),
    {"x"}};

const FunctionDoc log10_doc{
    "Compute log base 10 of arguments element-wise",
    ("Non-positive values return -inf or NaN. Null values return null.\n"
     "Use function \"log10_checked\" if you want non-positive values to raise an error."),
    {"x"}};

const FunctionDoc log10_checked_doc{
    "Compute log base 10 of arguments element-wise",
    ("Non-positive values return -inf or NaN. Null values return null.\n"
     "Use function \"log10\" if you want non-positive values to return "
     "-inf or NaN."),
    {"x"}};

const FunctionDoc log2_doc{
    "Compute log base 2 of arguments element-wise",
    ("Non-positive values return -inf or NaN. Null values return null.\n"
     "Use function \"log2_checked\" if you want non-positive values to raise an error."),
    {"x"}};

const FunctionDoc log2_checked_doc{
    "Compute log base 2 of arguments element-wise",
    ("Non-positive values return -inf or NaN. Null values return null.\n"
     "Use function \"log2\" if you want non-positive values to return "
     "-inf or NaN."),
    {"x"}};

const FunctionDoc log1p_doc{
    "Compute natural log of (1+x) element-wise",
    ("Values <= -1 return -inf or NaN. Null values return null.\n"
     "This function may be more precise than log(1 + x) for x close to zero."
     "Use function \"log1p_checked\" if you want non-positive values to raise an error."),
    {"x"}};

const FunctionDoc log1p_checked_doc{
    "Compute natural log of (1+x) element-wise",
    ("Values <= -1 return -inf or NaN. Null values return null.\n"
     "This function may be more precise than log(1 + x) for x close to zero."
     "Use function \"log1p\" if you want non-positive values to return "
     "-inf or NaN."),
    {"x"}};

const FunctionDoc logb_doc{
    "Compute log of x to base b of arguments element-wise",
    ("Values <= 0 return -inf or NaN. Null values return null.\n"
     "Use function \"logb_checked\" if you want non-positive values to raise an error."),
    {"x", "b"}};

const FunctionDoc logb_checked_doc{
    "Compute log of x to base b of arguments element-wise",
    ("Values <= 0 return -inf or NaN. Null values return null.\n"
     "Use function \"logb\" if you want non-positive values to return "
     "-inf or NaN."),
    {"x", "b"}};

const FunctionDoc floor_doc{
    "Round down to the nearest integer",
    ("Calculate the nearest integer less than or equal in magnitude to the "
     "argument element-wise"),
    {"x"}};

const FunctionDoc ceil_doc{
    "Round up to the nearest integer",
    ("Calculate the nearest integer greater than or equal in magnitude to the "
     "argument element-wise"),
    {"x"}};

const FunctionDoc trunc_doc{
    "Get the integral part without fractional digits",
    ("Calculate the nearest integer not greater in magnitude than to the "
     "argument element-wise."),
    {"x"}};

const FunctionDoc round_doc{
    "Round to a given precision",
    ("Options are used to control the number of digits and rounding mode.\n"
     "Default behavior is to round to the nearest integer and use half-to-even "
     "rule to break ties."),
    {"x"},
    "RoundOptions"};

const FunctionDoc round_to_multiple_doc{
    "Round to a given multiple",
    ("Options are used to control the rounding multiple and rounding mode.\n"
     "Default behavior is to round to the nearest integer and use half-to-even "
     "rule to break ties."),
    {"x"},
    "RoundToMultipleOptions"};
}  // namespace

void RegisterScalarArithmetic(FunctionRegistry* registry) {
  // ----------------------------------------------------------------------
  auto absolute_value =
      MakeUnaryArithmeticFunction<AbsoluteValue>("abs", &absolute_value_doc);
  DCHECK_OK(registry->AddFunction(std::move(absolute_value)));

  // ----------------------------------------------------------------------
  auto absolute_value_checked = MakeUnaryArithmeticFunctionNotNull<AbsoluteValueChecked>(
      "abs_checked", &absolute_value_checked_doc);
  DCHECK_OK(registry->AddFunction(std::move(absolute_value_checked)));

  // ----------------------------------------------------------------------
  auto add = MakeArithmeticFunction<Add>("add", &add_doc);
  AddDecimalBinaryKernels<Add>("add", &add);
  DCHECK_OK(registry->AddFunction(std::move(add)));

  // ----------------------------------------------------------------------
  auto add_checked =
      MakeArithmeticFunctionNotNull<AddChecked>("add_checked", &add_checked_doc);
  AddDecimalBinaryKernels<AddChecked>("add_checked", &add_checked);
  DCHECK_OK(registry->AddFunction(std::move(add_checked)));

  // ----------------------------------------------------------------------
  auto subtract = MakeArithmeticFunction<Subtract>("subtract", &sub_doc);
  AddDecimalBinaryKernels<Subtract>("subtract", &subtract);

  // Add subtract(timestamp, timestamp) -> duration
  for (auto unit : TimeUnit::values()) {
    InputType in_type(match::TimestampTypeUnit(unit));
    auto exec = ArithmeticExecFromOp<ScalarBinaryEqualTypes, Subtract>(Type::TIMESTAMP);
    DCHECK_OK(subtract->AddKernel({in_type, in_type}, duration(unit), std::move(exec)));
  }

  DCHECK_OK(registry->AddFunction(std::move(subtract)));

  // ----------------------------------------------------------------------
  auto subtract_checked = MakeArithmeticFunctionNotNull<SubtractChecked>(
      "subtract_checked", &sub_checked_doc);
  AddDecimalBinaryKernels<SubtractChecked>("subtract_checked", &subtract_checked);
  DCHECK_OK(registry->AddFunction(std::move(subtract_checked)));

  // ----------------------------------------------------------------------
  auto multiply = MakeArithmeticFunction<Multiply>("multiply", &mul_doc);
  AddDecimalBinaryKernels<Multiply>("multiply", &multiply);
  DCHECK_OK(registry->AddFunction(std::move(multiply)));

  // ----------------------------------------------------------------------
  auto multiply_checked = MakeArithmeticFunctionNotNull<MultiplyChecked>(
      "multiply_checked", &mul_checked_doc);
  AddDecimalBinaryKernels<MultiplyChecked>("multiply_checked", &multiply_checked);
  DCHECK_OK(registry->AddFunction(std::move(multiply_checked)));

  // ----------------------------------------------------------------------
  auto divide = MakeArithmeticFunctionNotNull<Divide>("divide", &div_doc);
  AddDecimalBinaryKernels<Divide>("divide", &divide);
  DCHECK_OK(registry->AddFunction(std::move(divide)));

  // ----------------------------------------------------------------------
  auto divide_checked =
      MakeArithmeticFunctionNotNull<DivideChecked>("divide_checked", &div_checked_doc);
  AddDecimalBinaryKernels<DivideChecked>("divide_checked", &divide_checked);
  DCHECK_OK(registry->AddFunction(std::move(divide_checked)));

  // ----------------------------------------------------------------------
  auto negate = MakeUnaryArithmeticFunction<Negate>("negate", &negate_doc);
  DCHECK_OK(registry->AddFunction(std::move(negate)));

  // ----------------------------------------------------------------------
  auto negate_checked = MakeUnarySignedArithmeticFunctionNotNull<NegateChecked>(
      "negate_checked", &negate_checked_doc);
  DCHECK_OK(registry->AddFunction(std::move(negate_checked)));

  // ----------------------------------------------------------------------
  auto power = MakeArithmeticFunction<Power>("power", &pow_doc);
  DCHECK_OK(registry->AddFunction(std::move(power)));

  // ----------------------------------------------------------------------
  auto power_checked =
      MakeArithmeticFunctionNotNull<PowerChecked>("power_checked", &pow_checked_doc);
  DCHECK_OK(registry->AddFunction(std::move(power_checked)));

  // ----------------------------------------------------------------------
  auto sign =
      MakeUnaryArithmeticFunctionWithFixedIntOutType<Sign, Int8Type>("sign", &sign_doc);
  DCHECK_OK(registry->AddFunction(std::move(sign)));

  // ----------------------------------------------------------------------
  // Bitwise functions
  {
    auto bit_wise_not = std::make_shared<ArithmeticFunction>(
        "bit_wise_not", Arity::Unary(), &bit_wise_not_doc);
    for (const auto& ty : IntTypes()) {
      auto exec = TypeAgnosticBitWiseExecFromOp<ScalarUnaryNotNull, BitWiseNot>(ty);
      DCHECK_OK(bit_wise_not->AddKernel({ty}, ty, exec));
    }
    AddNullExec(bit_wise_not.get());
    DCHECK_OK(registry->AddFunction(std::move(bit_wise_not)));
  }

  auto bit_wise_and =
      MakeBitWiseFunctionNotNull<BitWiseAnd>("bit_wise_and", &bit_wise_and_doc);
  DCHECK_OK(registry->AddFunction(std::move(bit_wise_and)));

  auto bit_wise_or =
      MakeBitWiseFunctionNotNull<BitWiseOr>("bit_wise_or", &bit_wise_or_doc);
  DCHECK_OK(registry->AddFunction(std::move(bit_wise_or)));

  auto bit_wise_xor =
      MakeBitWiseFunctionNotNull<BitWiseXor>("bit_wise_xor", &bit_wise_xor_doc);
  DCHECK_OK(registry->AddFunction(std::move(bit_wise_xor)));

  auto shift_left = MakeShiftFunctionNotNull<ShiftLeft>("shift_left", &shift_left_doc);
  DCHECK_OK(registry->AddFunction(std::move(shift_left)));

  auto shift_left_checked = MakeShiftFunctionNotNull<ShiftLeftChecked>(
      "shift_left_checked", &shift_left_checked_doc);
  DCHECK_OK(registry->AddFunction(std::move(shift_left_checked)));

  auto shift_right =
      MakeShiftFunctionNotNull<ShiftRight>("shift_right", &shift_right_doc);
  DCHECK_OK(registry->AddFunction(std::move(shift_right)));

  auto shift_right_checked = MakeShiftFunctionNotNull<ShiftRightChecked>(
      "shift_right_checked", &shift_right_checked_doc);
  DCHECK_OK(registry->AddFunction(std::move(shift_right_checked)));

  // ----------------------------------------------------------------------
  // Trig functions
  auto sin = MakeUnaryArithmeticFunctionFloatingPoint<Sin>("sin", &sin_doc);
  DCHECK_OK(registry->AddFunction(std::move(sin)));

  auto sin_checked = MakeUnaryArithmeticFunctionFloatingPointNotNull<SinChecked>(
      "sin_checked", &sin_checked_doc);
  DCHECK_OK(registry->AddFunction(std::move(sin_checked)));

  auto cos = MakeUnaryArithmeticFunctionFloatingPoint<Cos>("cos", &cos_doc);
  DCHECK_OK(registry->AddFunction(std::move(cos)));

  auto cos_checked = MakeUnaryArithmeticFunctionFloatingPointNotNull<CosChecked>(
      "cos_checked", &cos_checked_doc);
  DCHECK_OK(registry->AddFunction(std::move(cos_checked)));

  auto tan = MakeUnaryArithmeticFunctionFloatingPoint<Tan>("tan", &tan_doc);
  DCHECK_OK(registry->AddFunction(std::move(tan)));

  auto tan_checked = MakeUnaryArithmeticFunctionFloatingPointNotNull<TanChecked>(
      "tan_checked", &tan_checked_doc);
  DCHECK_OK(registry->AddFunction(std::move(tan_checked)));

  auto asin = MakeUnaryArithmeticFunctionFloatingPoint<Asin>("asin", &asin_doc);
  DCHECK_OK(registry->AddFunction(std::move(asin)));

  auto asin_checked = MakeUnaryArithmeticFunctionFloatingPointNotNull<AsinChecked>(
      "asin_checked", &asin_checked_doc);
  DCHECK_OK(registry->AddFunction(std::move(asin_checked)));

  auto acos = MakeUnaryArithmeticFunctionFloatingPoint<Acos>("acos", &acos_doc);
  DCHECK_OK(registry->AddFunction(std::move(acos)));

  auto acos_checked = MakeUnaryArithmeticFunctionFloatingPointNotNull<AcosChecked>(
      "acos_checked", &acos_checked_doc);
  DCHECK_OK(registry->AddFunction(std::move(acos_checked)));

  auto atan = MakeUnaryArithmeticFunctionFloatingPoint<Atan>("atan", &atan_doc);
  DCHECK_OK(registry->AddFunction(std::move(atan)));

  auto atan2 = MakeArithmeticFunctionFloatingPoint<Atan2>("atan2", &atan2_doc);
  DCHECK_OK(registry->AddFunction(std::move(atan2)));

  // ----------------------------------------------------------------------
  // Logarithms
  auto ln = MakeUnaryArithmeticFunctionFloatingPoint<LogNatural>("ln", &ln_doc);
  DCHECK_OK(registry->AddFunction(std::move(ln)));

  auto ln_checked = MakeUnaryArithmeticFunctionFloatingPointNotNull<LogNaturalChecked>(
      "ln_checked", &ln_checked_doc);
  DCHECK_OK(registry->AddFunction(std::move(ln_checked)));

  auto log10 = MakeUnaryArithmeticFunctionFloatingPoint<Log10>("log10", &log10_doc);
  DCHECK_OK(registry->AddFunction(std::move(log10)));

  auto log10_checked = MakeUnaryArithmeticFunctionFloatingPointNotNull<Log10Checked>(
      "log10_checked", &log10_checked_doc);
  DCHECK_OK(registry->AddFunction(std::move(log10_checked)));

  auto log2 = MakeUnaryArithmeticFunctionFloatingPoint<Log2>("log2", &log2_doc);
  DCHECK_OK(registry->AddFunction(std::move(log2)));

  auto log2_checked = MakeUnaryArithmeticFunctionFloatingPointNotNull<Log2Checked>(
      "log2_checked", &log2_checked_doc);
  DCHECK_OK(registry->AddFunction(std::move(log2_checked)));

  auto log1p = MakeUnaryArithmeticFunctionFloatingPoint<Log1p>("log1p", &log1p_doc);
  DCHECK_OK(registry->AddFunction(std::move(log1p)));

  auto log1p_checked = MakeUnaryArithmeticFunctionFloatingPointNotNull<Log1pChecked>(
      "log1p_checked", &log1p_checked_doc);
  DCHECK_OK(registry->AddFunction(std::move(log1p_checked)));

  auto logb = MakeArithmeticFunctionFloatingPoint<Logb>("logb", &logb_doc);
  DCHECK_OK(registry->AddFunction(std::move(logb)));

  auto logb_checked = MakeArithmeticFunctionFloatingPointNotNull<LogbChecked>(
      "logb_checked", &logb_checked_doc);
  DCHECK_OK(registry->AddFunction(std::move(logb_checked)));

  // ----------------------------------------------------------------------
  // Rounding functions
  auto floor =
      MakeUnaryArithmeticFunctionFloatingPoint<Floor,
                                               ArithmeticIntegerToFloatingPointFunction>(
          "floor", &floor_doc);
  DCHECK_OK(floor->AddKernel(
      {InputType(Type::DECIMAL128)}, OutputType(FirstType),
      FixedRoundDecimalExec<Decimal128Type, RoundMode::DOWN, /*ndigits=*/0>));
  DCHECK_OK(floor->AddKernel(
      {InputType(Type::DECIMAL256)}, OutputType(FirstType),
      FixedRoundDecimalExec<Decimal256Type, RoundMode::DOWN, /*ndigits=*/0>));
  DCHECK_OK(registry->AddFunction(std::move(floor)));

  auto ceil =
      MakeUnaryArithmeticFunctionFloatingPoint<Ceil,
                                               ArithmeticIntegerToFloatingPointFunction>(
          "ceil", &ceil_doc);
  DCHECK_OK(ceil->AddKernel(
      {InputType(Type::DECIMAL128)}, OutputType(FirstType),
      FixedRoundDecimalExec<Decimal128Type, RoundMode::UP, /*ndigits=*/0>));
  DCHECK_OK(ceil->AddKernel(
      {InputType(Type::DECIMAL256)}, OutputType(FirstType),
      FixedRoundDecimalExec<Decimal256Type, RoundMode::UP, /*ndigits=*/0>));
  DCHECK_OK(registry->AddFunction(std::move(ceil)));

  auto trunc =
      MakeUnaryArithmeticFunctionFloatingPoint<Trunc,
                                               ArithmeticIntegerToFloatingPointFunction>(
          "trunc", &trunc_doc);
  DCHECK_OK(trunc->AddKernel(
      {InputType(Type::DECIMAL128)}, OutputType(FirstType),
      FixedRoundDecimalExec<Decimal128Type, RoundMode::TOWARDS_ZERO, /*ndigits=*/0>));
  DCHECK_OK(trunc->AddKernel(
      {InputType(Type::DECIMAL256)}, OutputType(FirstType),
      FixedRoundDecimalExec<Decimal256Type, RoundMode::TOWARDS_ZERO, /*ndigits=*/0>));
  DCHECK_OK(registry->AddFunction(std::move(trunc)));

  auto round = MakeUnaryRoundFunction<Round, RoundOptions>("round", &round_doc);
  DCHECK_OK(registry->AddFunction(std::move(round)));

  auto round_to_multiple =
      MakeUnaryRoundFunction<RoundToMultiple, RoundToMultipleOptions>(
          "round_to_multiple", &round_to_multiple_doc);
  DCHECK_OK(registry->AddFunction(std::move(round_to_multiple)));
}

}  // namespace internal
}  // namespace compute
}  // namespace arrow
