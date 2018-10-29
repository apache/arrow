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

#ifndef GANDIVA_DECIMAL_TYPE_SQL_H
#define GANDIVA_DECIMAL_TYPE_SQL_H

#include <algorithm>
#include <memory>

#include "arrow/type.h"
#include "gandiva/arrow.h"

namespace gandiva {

/// @brief Handles conversion of scale/precision for operations on decimal types.
/// TODO : do validations for all of these.
class DecimalTypeSql {
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

  /// The maximum precision representable by a 16-byte decimal
  static constexpr int32_t kMaxPrecision = 38;

  // The maximum scale representable.
  static constexpr int32_t kMaxScale = kMaxPrecision;

  // When operating on decimal inputs, the integer part of the output can exceed the
  // max precision. In such cases, the scale can be reduced, upto a minimum of
  // kMinAdjustedScale.
  // * There is no strong reason for 6, but both SQLServer and Impala use 6 too.
  static constexpr int32_t kMinAdjustedScale = 6;

  // For specified operation and input scale/precision, determine the output
  // scale/precision.
  static Status GetResultType(Op op, const Decimal128TypeVector& in_types,
                              Decimal128TypePtr* out_type);

 private:
  static Decimal128TypePtr MakeType(int32_t precision, int32_t scale);
  static Decimal128TypePtr MakeAdjustedType(int32_t precision, int32_t scale);
};

inline Decimal128TypePtr DecimalTypeSql::MakeType(int32_t precision, int32_t scale) {
  return std::make_shared<arrow::Decimal128Type>(precision, scale);
}

// Reduce the scale if possible so that precision stays <= kMaxPrecision
inline Decimal128TypePtr DecimalTypeSql::MakeAdjustedType(int32_t precision,
                                                          int32_t scale) {
  if (precision > kMaxPrecision) {
    int32_t min_scale = std::min(scale, kMinAdjustedScale);
    int32_t delta = precision - kMaxPrecision;
    precision = kMaxPrecision;
    scale = std::max(scale - delta, min_scale);
  }
  return MakeType(precision, scale);
}

// Implementation of decimal rules.
inline Status DecimalTypeSql::GetResultType(Op op, const Decimal128TypeVector& in_types,
                                            Decimal128TypePtr* out_type) {
  // TODO : validations
  *out_type = NULL;
  auto t1 = in_types[0];
  auto t2 = in_types[1];

  int32_t s1 = t1->scale();
  int32_t s2 = t2->scale();
  int32_t p1 = t1->precision();
  int32_t p2 = t2->precision();
  int32_t result_scale;
  int32_t result_precision;

  switch (op) {
    case kOpAdd:
    case kOpSubtract:
      result_scale = std::max(s1, s2);
      result_precision = std::max(p1 - s1, p2 - s2) + result_scale + 1;
      break;

    case kOpMultiply:
      result_scale = s1 + s2;
      result_precision = p1 + p2 + 1;
      break;

    case kOpDivide:
      result_scale = std::max(kMinAdjustedScale, s1 + p2 + 1);
      result_precision = p1 - s1 + s2 + result_scale;
      break;

    case kOpMod:
      result_scale = std::max(s1, s2);
      result_precision = std::min(p1 - s1, p2 - s2) + result_scale;
      break;
  }
  *out_type = MakeAdjustedType(result_precision, result_scale);
  return Status::OK();
}

}  // namespace gandiva

#endif  // GANDIVA_DECIMAL_TYPE_SQL_H
