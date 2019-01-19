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
// under the License

#pragma once

#include <cstdint>
#include <iostream>
#include <string>
#include "arrow/util/decimal.h"
#include "gandiva/decimal_basic_scalar.h"

namespace gandiva {

using Decimal128 = arrow::Decimal128;

/// Represents a 128-bit decimal value along with its precision and scale.
class DecimalScalar128 : public DecimalBasicScalar128 {
 public:
  DecimalScalar128(int64_t high_bits, uint64_t low_bits, int32_t precision, int32_t scale)
      : DecimalBasicScalar128(high_bits, low_bits, precision, scale) {}

  DecimalScalar128(std::string value, int32_t precision, int32_t scale)
      : DecimalBasicScalar128(Decimal128(value), precision, scale) {}

  DecimalScalar128(const Decimal128& value, int32_t precision, int32_t scale)
      : DecimalBasicScalar128(value, precision, scale) {}

  DecimalScalar128(int32_t precision, int32_t scale)
      : DecimalBasicScalar128(precision, scale) {}

  // constexpr DecimalScalar128(const DecimalBasic128 &value) noexcept :
  // DecimalBasic128(value) {}

  const arrow::Decimal128& value() const {
    return static_cast<const arrow::Decimal128&>(DecimalBasicScalar128::value());
  }

  inline std::string ToString() const {
    return value().ToString(0) + "," + std::to_string(precision()) + "," +
           std::to_string(scale());
  }

  friend std::ostream& operator<<(std::ostream& os, const DecimalScalar128& dec) {
    os << dec.ToString();
    return os;
  }
};

}  // namespace gandiva
