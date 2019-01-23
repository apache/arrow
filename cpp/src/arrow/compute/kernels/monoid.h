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

#ifndef ARROW_COMPUTE_KERNELS_MONOID_H
#define ARROW_COMPUTE_KERNELS_MONOID_H

#include <algorithm>
#include <functional>
#include <limits>

namespace arrow {
namespace compute {

template <typename T, T BinaryOp(const T&, const T&), T Identity()>
class Monoid {
 public:
  using ValueType = T;
  using ThisType = Monoid<T, BinaryOp, Identity>;

  static constexpr ValueType identity() { return Identity(); }

  Monoid() {}
  explicit Monoid(ValueType value) : value_(value) {}

  ThisType operator+(const ThisType& rhs) const {
    return BinaryOp(this->value_, rhs.value_);
  }

  ThisType& operator+=(const ThisType& rhs) {
    this->value_ = BinaryOp(this->value_, rhs.value_);
    return *this;
  }

  ValueType value() const { return value_; }

 private:
  ValueType value_ = identity();
};

template <typename T>
constexpr T zero() {
  return static_cast<T>(0);
}

template <typename T>
constexpr T add(const T& lhs, const T& rhs) {
  return static_cast<T>(lhs + rhs);
}

template <typename T>
using SumMonoid = Monoid<T, add<T>, zero<T>>;

template <typename T>
constexpr T min(const T& lhs, const T& rhs) {
  return std::min(lhs, rhs);
}

template <typename T>
using MinMonoid = Monoid<T, min<T>, std::numeric_limits<T>::max>;

template <typename T>
constexpr T max(const T& lhs, const T& rhs) {
  return std::max(lhs, rhs);
}

template <typename T>
using MaxMonoid = Monoid<T, max<T>, std::numeric_limits<T>::lowest>;

}  // namespace compute
}  // namespace arrow

#endif  // ARROW_COMPUTE_KERNELS_MONOID_H
