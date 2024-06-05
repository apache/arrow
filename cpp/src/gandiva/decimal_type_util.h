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

// Adapted from Apache Impala

#pragma once

#include <algorithm>
#include <memory>

#include "gandiva/arrow.h"
#include "gandiva/visibility.h"

namespace gandiva {

/// @brief Handles conversion of scale/precision for operations on decimal types.
/// TODO : do validations for all of these.
class GANDIVA_EXPORT DecimalTypeUtil {
 public:
  enum Op {
    kOpAdd,
    kOpSubtract,
    kOpMultiply,
    kOpDivide,
    kOpMod,
  };

  /// The maximum precision representable by a 4-byte decimal
  static constexpr int32_t kMaxDecimal32Precision = 9;

  /// The maximum precision representable by a 8-byte decimal
  static constexpr int32_t kMaxDecimal64Precision = 18;

  /// The minimum precision representable by a 16-byte decimal
  static constexpr int32_t kMinPrecision = 1;

  /// The maximum precision representable by a 16-byte decimal
  static constexpr int32_t kMaxPrecision = 38;

  // The maximum scale representable.
  static constexpr int32_t kMaxScale = kMaxPrecision;

  // When operating on decimal inputs, the integer part of the output can exceed the
  // max precision. In such cases, the scale can be reduced, up to a minimum of
  // kMinAdjustedScale.
  // * There is no strong reason for 6, but both SQLServer and Impala use 6 too.
  static constexpr int32_t kMinAdjustedScale = 6;

  // The same function with kMinAdjustedScale, just for compatibility with
  // compute module's decimal promotion rules.
  static constexpr int32_t kMinComputeAdjustedScale = 4;

  // For specified operation and input scale/precision, determine the output
  // scale/precision.
  //
  // The 'use_compute_rules' is for compatibility with compute module's
  // decimal promotion rules:
  // https://arrow.apache.org/docs/cpp/compute.html#arithmetic-functions
  static Status GetResultType(Op op, const Decimal128TypeVector& in_types,
                              Decimal128TypePtr* out_type,
                              bool use_compute_rules = false);

  static Decimal128TypePtr MakeType(int32_t precision, int32_t scale) {
    return std::dynamic_pointer_cast<arrow::Decimal128Type>(
        arrow::decimal(precision, scale));
  }

 private:
  // Reduce the scale if possible so that precision stays <= kMaxPrecision
  static Decimal128TypePtr MakeAdjustedType(int32_t precision, int32_t scale) {
    if (precision > kMaxPrecision) {
      int32_t min_scale = std::min(scale, kMinAdjustedScale);
      int32_t delta = precision - kMaxPrecision;
      precision = kMaxPrecision;
      scale = std::max(scale - delta, min_scale);
    }
    return MakeType(precision, scale);
  }
};

}  // namespace gandiva
