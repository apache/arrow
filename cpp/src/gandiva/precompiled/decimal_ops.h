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
#include <string>
#include "gandiva/basic_decimal_scalar.h"

namespace gandiva {
namespace decimalops {

/// Return the sum of 'x' and 'y'.
/// out_precision and out_scale are passed along for efficiency, they must match
/// the rules in DecimalTypeSql::GetResultType.
arrow::BasicDecimal128 Add(const BasicDecimalScalar128& x, const BasicDecimalScalar128& y,
                           int32_t out_precision, int32_t out_scale);

/// Subtract 'y' from 'x', and return the result.
arrow::BasicDecimal128 Subtract(const BasicDecimalScalar128& x,
                                const BasicDecimalScalar128& y, int32_t out_precision,
                                int32_t out_scale);

/// Multiply 'x' from 'y', and return the result.
arrow::BasicDecimal128 Multiply(const BasicDecimalScalar128& x,
                                const BasicDecimalScalar128& y, int32_t out_precision,
                                int32_t out_scale, bool* overflow);

/// Divide 'x' by 'y', and return the result.
arrow::BasicDecimal128 Divide(int64_t context, const BasicDecimalScalar128& x,
                              const BasicDecimalScalar128& y, int32_t out_precision,
                              int32_t out_scale, bool* overflow);

/// Divide 'x' by 'y', and return the remainder.
arrow::BasicDecimal128 Mod(int64_t context, const BasicDecimalScalar128& x,
                           const BasicDecimalScalar128& y, int32_t out_precision,
                           int32_t out_scale, bool* overflow);

}  // namespace decimalops
}  // namespace gandiva
