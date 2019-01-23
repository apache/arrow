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
#include "gandiva/basic_decimal_scalar.h"

namespace gandiva {

using Decimal128 = arrow::Decimal128;

/// Represents a 128-bit decimal value along with its precision and scale.
///
/// BasicDecimalScalar128 can be safely compiled to IR without references to libstdc++.
/// This class has additional functionality on top of BasicDecimalScalar128 to deal with
/// strings and streams.
class DecimalScalar128 : public BasicDecimalScalar128 {
 public:
  using BasicDecimalScalar128::BasicDecimalScalar128;

  DecimalScalar128(const std::string& value, int32_t precision, int32_t scale)
      : BasicDecimalScalar128(Decimal128(value), precision, scale) {}

  inline std::string ToString() const {
    Decimal128 dvalue(value());
    return dvalue.ToString(0) + "," + std::to_string(precision()) + "," +
           std::to_string(scale());
  }

  friend std::ostream& operator<<(std::ostream& os, const DecimalScalar128& dec) {
    os << dec.ToString();
    return os;
  }
};

}  // namespace gandiva
