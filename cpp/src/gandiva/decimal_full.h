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

#ifndef DECIMAL_FULL_H
#define DECIMAL_FULL_H

#include <cstdint>
#include <iostream>
#include <string>
#include "arrow/util/decimal.h"

namespace gandiva {

using Decimal128 = arrow::Decimal128;

/// Represents a 128-bit decimal value along with its precision and scale.
class Decimal128Full {
 public:
  Decimal128Full(int64_t high_bits, uint64_t low_bits, int32_t precision, int32_t scale)
      : value_(high_bits, low_bits), precision_(precision), scale_(scale) {}

  Decimal128Full(std::string value, int32_t precision, int32_t scale)
      : value_(value), precision_(precision), scale_(scale) {}

  Decimal128Full(const Decimal128& value, int32_t precision, int32_t scale)
      : value_(value), precision_(precision), scale_(scale) {}

  Decimal128Full(int32_t precision, int32_t scale)
      : value_(0), precision_(precision), scale_(scale) {}

  uint32_t scale() const { return scale_; }

  uint32_t precision() const { return precision_; }

  const arrow::Decimal128& value() const { return value_; }

  inline std::string ToString() const {
    return value_.ToString(0) + "," + std::to_string(precision_) + "," +
           std::to_string(scale_);
  }

  friend std::ostream& operator<<(std::ostream& os, const Decimal128Full& dec) {
    os << dec.ToString();
    return os;
  }

 private:
  Decimal128 value_;

  int32_t precision_;
  int32_t scale_;
};

inline bool operator==(const Decimal128Full& left, const Decimal128Full& right) {
  return left.value() == right.value() && left.precision() == right.precision() &&
         left.scale() == right.scale();
}

}  // namespace gandiva

#endif  // DECIMAL_FULL_H
