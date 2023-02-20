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

#include "arrow/compute/api_scalar.h"
#include "arrow/compute/kernels/common_internal.h"
#include "arrow/compute/kernels/util_internal.h"
#include "arrow/type.h"
#include "arrow/type_traits.h"
#include "arrow/util/decimal.h"
#include "arrow/util/int_util_overflow.h"
#include "arrow/util/macros.h"

namespace arrow {

using internal::AddWithOverflow;
using internal::DivideWithOverflow;
using internal::MultiplyWithOverflow;
using internal::NegateWithOverflow;
using internal::SubtractWithOverflow;

namespace compute {
namespace internal {

struct Add {
  template <typename T, typename Arg0, typename Arg1>
  static constexpr enable_if_floating_value<T> Call(KernelContext*, Arg0 left, Arg1 right,
                                                    Status*) {
    return left + right;
  }

  template <typename T, typename Arg0, typename Arg1>
  static constexpr enable_if_unsigned_integer_value<T> Call(KernelContext*, Arg0 left,
                                                            Arg1 right, Status*) {
    return left + right;
  }

  template <typename T, typename Arg0, typename Arg1>
  static constexpr enable_if_signed_integer_value<T> Call(KernelContext*, Arg0 left,
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
  static enable_if_integer_value<T> Call(KernelContext*, Arg0 left, Arg1 right,
                                         Status* st) {
    static_assert(std::is_same<T, Arg0>::value && std::is_same<T, Arg1>::value, "");
    T result = 0;
    if (ARROW_PREDICT_FALSE(AddWithOverflow(left, right, &result))) {
      *st = Status::Invalid("overflow");
    }
    return result;
  }

  template <typename T, typename Arg0, typename Arg1>
  static enable_if_floating_value<T> Call(KernelContext*, Arg0 left, Arg1 right,
                                          Status*) {
    static_assert(std::is_same<T, Arg0>::value && std::is_same<T, Arg1>::value, "");
    return left + right;
  }

  template <typename T, typename Arg0, typename Arg1>
  static enable_if_decimal_value<T> Call(KernelContext*, Arg0 left, Arg1 right, Status*) {
    return left + right;
  }
};

template <int64_t multiple>
struct AddTimeDuration {
  template <typename T, typename Arg0, typename Arg1>
  static T Call(KernelContext*, Arg0 left, Arg1 right, Status* st) {
    T result =
        arrow::internal::SafeSignedAdd(static_cast<T>(left), static_cast<T>(right));
    if (result < 0 || multiple <= result) {
      *st = Status::Invalid(result, " is not within the acceptable range of ", "[0, ",
                            multiple, ") s");
    }
    return result;
  }
};

template <int64_t multiple>
struct AddTimeDurationChecked {
  template <typename T, typename Arg0, typename Arg1>
  static T Call(KernelContext*, Arg0 left, Arg1 right, Status* st) {
    T result = 0;
    if (ARROW_PREDICT_FALSE(
            AddWithOverflow(static_cast<T>(left), static_cast<T>(right), &result))) {
      *st = Status::Invalid("overflow");
    }
    if (result < 0 || multiple <= result) {
      *st = Status::Invalid(result, " is not within the acceptable range of ", "[0, ",
                            multiple, ") s");
    }
    return result;
  }
};

struct AbsoluteValue {
  template <typename T, typename Arg>
  static constexpr enable_if_floating_value<Arg, T> Call(KernelContext*, Arg arg,
                                                         Status*) {
    return std::fabs(arg);
  }

  template <typename T, typename Arg>
  static constexpr enable_if_unsigned_integer_value<Arg, T> Call(KernelContext*, Arg arg,
                                                                 Status*) {
    return arg;
  }

  template <typename T, typename Arg>
  static constexpr enable_if_signed_integer_value<Arg, T> Call(KernelContext*, Arg arg,
                                                               Status* st) {
    return (arg < 0) ? arrow::internal::SafeSignedNegate(arg) : arg;
  }

  template <typename T, typename Arg>
  static constexpr enable_if_decimal_value<Arg, T> Call(KernelContext*, Arg arg,
                                                        Status*) {
    return arg.Abs();
  }
};

struct AbsoluteValueChecked {
  template <typename T, typename Arg>
  static enable_if_signed_integer_value<Arg, T> Call(KernelContext*, Arg arg,
                                                     Status* st) {
    static_assert(std::is_same<T, Arg>::value, "");
    if (arg == std::numeric_limits<Arg>::min()) {
      *st = Status::Invalid("overflow");
      return arg;
    }
    return std::abs(arg);
  }

  template <typename T, typename Arg>
  static enable_if_unsigned_integer_value<Arg, T> Call(KernelContext* ctx, Arg arg,
                                                       Status* st) {
    static_assert(std::is_same<T, Arg>::value, "");
    return arg;
  }

  template <typename T, typename Arg>
  static constexpr enable_if_floating_value<Arg, T> Call(KernelContext*, Arg arg,
                                                         Status* st) {
    static_assert(std::is_same<T, Arg>::value, "");
    return std::fabs(arg);
  }

  template <typename T, typename Arg>
  static constexpr enable_if_decimal_value<Arg, T> Call(KernelContext*, Arg arg,
                                                        Status*) {
    return arg.Abs();
  }
};

struct Subtract {
  template <typename T, typename Arg0, typename Arg1>
  static constexpr enable_if_floating_value<T> Call(KernelContext*, Arg0 left, Arg1 right,
                                                    Status*) {
    static_assert(std::is_same<T, Arg0>::value && std::is_same<T, Arg1>::value, "");
    return left - right;
  }

  template <typename T, typename Arg0, typename Arg1>
  static constexpr enable_if_unsigned_integer_value<T> Call(KernelContext*, Arg0 left,
                                                            Arg1 right, Status*) {
    static_assert(std::is_same<T, Arg0>::value && std::is_same<T, Arg1>::value, "");
    return left - right;
  }

  template <typename T, typename Arg0, typename Arg1>
  static constexpr enable_if_signed_integer_value<T> Call(KernelContext*, Arg0 left,
                                                          Arg1 right, Status*) {
    return arrow::internal::SafeSignedSubtract(left, right);
  }

  template <typename T, typename Arg0, typename Arg1>
  static enable_if_decimal_value<T> Call(KernelContext*, Arg0 left, Arg1 right, Status*) {
    return left + (-right);
  }
};

struct SubtractChecked {
  template <typename T, typename Arg0, typename Arg1>
  static enable_if_integer_value<T> Call(KernelContext*, Arg0 left, Arg1 right,
                                         Status* st) {
    T result = 0;
    if (ARROW_PREDICT_FALSE(SubtractWithOverflow(left, right, &result))) {
      *st = Status::Invalid("overflow");
    }
    return result;
  }

  template <typename T, typename Arg0, typename Arg1>
  static enable_if_floating_value<T> Call(KernelContext*, Arg0 left, Arg1 right,
                                          Status*) {
    static_assert(std::is_same<T, Arg0>::value && std::is_same<T, Arg1>::value, "");
    return left - right;
  }

  template <typename T, typename Arg0, typename Arg1>
  static enable_if_decimal_value<T> Call(KernelContext*, Arg0 left, Arg1 right, Status*) {
    return left + (-right);
  }
};

struct SubtractDate32 {
  static constexpr int64_t kSecondsInDay = 86400;

  template <typename T, typename Arg0, typename Arg1>
  static constexpr T Call(KernelContext*, Arg0 left, Arg1 right, Status*) {
    return arrow::internal::SafeSignedSubtract(left, right) * kSecondsInDay;
  }
};

struct SubtractCheckedDate32 {
  static constexpr int64_t kSecondsInDay = 86400;

  template <typename T, typename Arg0, typename Arg1>
  static T Call(KernelContext*, Arg0 left, Arg1 right, Status* st) {
    T result = 0;
    if (ARROW_PREDICT_FALSE(SubtractWithOverflow(left, right, &result))) {
      *st = Status::Invalid("overflow");
    }
    if (ARROW_PREDICT_FALSE(MultiplyWithOverflow(result, kSecondsInDay, &result))) {
      *st = Status::Invalid("overflow");
    }
    return result;
  }
};

template <int64_t multiple>
struct SubtractTimeDuration {
  template <typename T, typename Arg0, typename Arg1>
  static T Call(KernelContext*, Arg0 left, Arg1 right, Status* st) {
    T result = arrow::internal::SafeSignedSubtract(left, static_cast<T>(right));
    if (result < 0 || multiple <= result) {
      *st = Status::Invalid(result, " is not within the acceptable range of ", "[0, ",
                            multiple, ") s");
    }
    return result;
  }
};

template <int64_t multiple>
struct SubtractTimeDurationChecked {
  template <typename T, typename Arg0, typename Arg1>
  static T Call(KernelContext*, Arg0 left, Arg1 right, Status* st) {
    T result = 0;
    if (ARROW_PREDICT_FALSE(SubtractWithOverflow(left, static_cast<T>(right), &result))) {
      *st = Status::Invalid("overflow");
    }
    if (result < 0 || multiple <= result) {
      *st = Status::Invalid(result, " is not within the acceptable range of ", "[0, ",
                            multiple, ") s");
    }
    return result;
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
  static constexpr enable_if_floating_value<T> Call(KernelContext*, T left, T right,
                                                    Status*) {
    return left * right;
  }

  template <typename T, typename Arg0, typename Arg1>
  static constexpr enable_if_t<
      is_unsigned_integer_value<T>::value && !std::is_same<T, uint16_t>::value, T>
  Call(KernelContext*, T left, T right, Status*) {
    return left * right;
  }

  template <typename T, typename Arg0, typename Arg1>
  static constexpr enable_if_t<
      is_signed_integer_value<T>::value && !std::is_same<T, int16_t>::value, T>
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
  static enable_if_integer_value<T> Call(KernelContext*, Arg0 left, Arg1 right,
                                         Status* st) {
    static_assert(std::is_same<T, Arg0>::value && std::is_same<T, Arg1>::value, "");
    T result = 0;
    if (ARROW_PREDICT_FALSE(MultiplyWithOverflow(left, right, &result))) {
      *st = Status::Invalid("overflow");
    }
    return result;
  }

  template <typename T, typename Arg0, typename Arg1>
  static enable_if_floating_value<T> Call(KernelContext*, Arg0 left, Arg1 right,
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
  static enable_if_floating_value<T> Call(KernelContext*, Arg0 left, Arg1 right,
                                          Status*) {
    return left / right;
  }

  template <typename T, typename Arg0, typename Arg1>
  static enable_if_integer_value<T> Call(KernelContext*, Arg0 left, Arg1 right,
                                         Status* st) {
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
  static enable_if_integer_value<T> Call(KernelContext*, Arg0 left, Arg1 right,
                                         Status* st) {
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
  static enable_if_floating_value<T> Call(KernelContext*, Arg0 left, Arg1 right,
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
  static constexpr enable_if_floating_value<T> Call(KernelContext*, Arg arg, Status*) {
    return -arg;
  }

  template <typename T, typename Arg>
  static constexpr enable_if_unsigned_integer_value<T> Call(KernelContext*, Arg arg,
                                                            Status*) {
    return ~arg + 1;
  }

  template <typename T, typename Arg>
  static constexpr enable_if_signed_integer_value<T> Call(KernelContext*, Arg arg,
                                                          Status*) {
    return arrow::internal::SafeSignedNegate(arg);
  }

  template <typename T, typename Arg>
  static constexpr enable_if_decimal_value<Arg, T> Call(KernelContext*, Arg arg,
                                                        Status*) {
    return arg.Negate();
  }
};

struct NegateChecked {
  template <typename T, typename Arg>
  static enable_if_signed_integer_value<Arg, T> Call(KernelContext*, Arg arg,
                                                     Status* st) {
    static_assert(std::is_same<T, Arg>::value, "");
    T result = 0;
    if (ARROW_PREDICT_FALSE(NegateWithOverflow(arg, &result))) {
      *st = Status::Invalid("overflow");
    }
    return result;
  }

  template <typename T, typename Arg>
  static enable_if_unsigned_integer_value<Arg, T> Call(KernelContext* ctx, Arg arg,
                                                       Status* st) {
    static_assert(std::is_same<T, Arg>::value, "");
    DCHECK(false) << "This is included only for the purposes of instantiability from the "
                     "arithmetic kernel generator";
    return 0;
  }

  template <typename T, typename Arg>
  static constexpr enable_if_floating_value<Arg, T> Call(KernelContext*, Arg arg,
                                                         Status* st) {
    static_assert(std::is_same<T, Arg>::value, "");
    return -arg;
  }

  template <typename T, typename Arg>
  static constexpr enable_if_decimal_value<Arg, T> Call(KernelContext*, Arg arg,
                                                        Status*) {
    return arg.Negate();
  }
};

struct Exp {
  template <typename T, typename Arg>
  static T Call(KernelContext*, Arg exp, Status*) {
    static_assert(std::is_same<T, Arg>::value, "");
    return std::exp(exp);
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
  static enable_if_integer_value<T> Call(KernelContext*, T base, T exp, Status* st) {
    if (exp < 0) {
      *st = Status::Invalid("integers to negative integer powers are not allowed");
      return 0;
    }
    return static_cast<T>(IntegerPower(base, exp));
  }

  template <typename T, typename Arg0, typename Arg1>
  static enable_if_floating_value<T> Call(KernelContext*, T base, T exp, Status*) {
    return std::pow(base, exp);
  }
};

struct PowerChecked {
  template <typename T, typename Arg0, typename Arg1>
  static enable_if_integer_value<T> Call(KernelContext*, Arg0 base, Arg1 exp,
                                         Status* st) {
    if (exp < 0) {
      *st = Status::Invalid("integers to negative integer powers are not allowed");
      return 0;
    } else if (exp == 0) {
      return 1;
    }
    // left to right O(logn) power with overflow checks
    bool overflow = false;
    uint64_t bitmask =
        1ULL << (63 - bit_util::CountLeadingZeros(static_cast<uint64_t>(exp)));
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
  static enable_if_floating_value<T> Call(KernelContext*, Arg0 base, Arg1 exp, Status*) {
    static_assert(std::is_same<T, Arg0>::value && std::is_same<T, Arg1>::value, "");
    return std::pow(base, exp);
  }
};

struct SquareRoot {
  template <typename T, typename Arg>
  static enable_if_floating_value<Arg, T> Call(KernelContext*, Arg arg, Status*) {
    static_assert(std::is_same<T, Arg>::value, "");
    if (arg < 0.0) {
      return std::numeric_limits<T>::quiet_NaN();
    }
    return std::sqrt(arg);
  }
};

struct SquareRootChecked {
  template <typename T, typename Arg>
  static enable_if_floating_value<Arg, T> Call(KernelContext*, Arg arg, Status* st) {
    static_assert(std::is_same<T, Arg>::value, "");
    if (arg < 0.0) {
      *st = Status::Invalid("square root of negative number");
      return arg;
    }
    return std::sqrt(arg);
  }
};

struct Sign {
  template <typename T, typename Arg>
  static constexpr enable_if_floating_value<Arg, T> Call(KernelContext*, Arg arg,
                                                         Status*) {
    return std::isnan(arg) ? arg : ((arg == 0) ? 0 : (std::signbit(arg) ? -1 : 1));
  }

  template <typename T, typename Arg>
  static constexpr enable_if_unsigned_integer_value<Arg, T> Call(KernelContext*, Arg arg,
                                                                 Status*) {
    return (arg > 0) ? 1 : 0;
  }

  template <typename T, typename Arg>
  static constexpr enable_if_signed_integer_value<Arg, T> Call(KernelContext*, Arg arg,
                                                               Status*) {
    return (arg > 0) ? 1 : ((arg == 0) ? 0 : -1);
  }

  template <typename T, typename Arg>
  static constexpr enable_if_decimal_value<Arg, T> Call(KernelContext*, Arg arg,
                                                        Status*) {
    return (arg == 0) ? 0 : arg.Sign();
  }
};

}  // namespace internal
}  // namespace compute
}  // namespace arrow
