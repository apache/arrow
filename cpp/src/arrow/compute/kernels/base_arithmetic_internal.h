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
#include "arrow/compute/kernels/common.h"
#include "arrow/compute/kernels/util_internal.h"
#include "arrow/type.h"
#include "arrow/type_traits.h"
#include "arrow/util/decimal.h"
#include "arrow/util/int_util_internal.h"
#include "arrow/util/macros.h"

namespace arrow {

using internal::AddWithOverflow;

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

}  // namespace internal
}  // namespace compute
}  // namespace arrow
