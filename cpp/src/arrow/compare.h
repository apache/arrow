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

// Functions for comparing Arrow data structures

#pragma once

#include <cmath>
#include <cstdint>
#include <iosfwd>
#include <limits>
#include <type_traits>

#include "arrow/type_fwd.h"
#include "arrow/util/macros.h"
#include "arrow/util/visibility.h"

namespace arrow {

class Array;
class DataType;
class Tensor;
class SparseTensor;
struct Scalar;
template <typename>
struct NumericScalar;
template <typename>
class Result;

static constexpr double kDefaultAbsoluteTolerance = 1E-5;
static constexpr double kDefaultOrderFloatingPointAbsoluteTolerance = 0;
static constexpr uint64_t kDefaultOrderIntegerAbsoluteTolerance = 0;

/// A container of options for equality comparisons
class EqualOptions {
 public:
  /// Whether or not NaNs are considered equal.
  bool nans_equal() const { return nans_equal_; }

  /// Return a new EqualOptions object with the "nans_equal" property changed.
  EqualOptions nans_equal(bool v) const {
    auto res = EqualOptions(*this);
    res.nans_equal_ = v;
    return res;
  }

  /// Whether or not zeros with differing signs are considered equal.
  bool signed_zeros_equal() const { return signed_zeros_equal_; }

  /// Return a new EqualOptions object with the "signed_zeros_equal" property changed.
  EqualOptions signed_zeros_equal(bool v) const {
    auto res = EqualOptions(*this);
    res.signed_zeros_equal_ = v;
    return res;
  }

  /// The absolute tolerance for approximate comparisons of floating-point values.
  double atol() const { return atol_; }

  /// Return a new EqualOptions object with the "atol" property changed.
  EqualOptions atol(double v) const {
    auto res = EqualOptions(*this);
    res.atol_ = v;
    return res;
  }

  /// The ostream to which a diff will be formatted if arrays disagree.
  /// If this is null (the default) no diff will be formatted.
  std::ostream* diff_sink() const { return diff_sink_; }

  /// Return a new EqualOptions object with the "diff_sink" property changed.
  /// This option will be ignored if diff formatting of the types of compared arrays is
  /// not supported.
  EqualOptions diff_sink(std::ostream* diff_sink) const {
    auto res = EqualOptions(*this);
    res.diff_sink_ = diff_sink;
    return res;
  }

  static EqualOptions Defaults() { return {}; }

 protected:
  double atol_ = kDefaultAbsoluteTolerance;
  bool nans_equal_ = false;
  bool signed_zeros_equal_ = true;

  std::ostream* diff_sink_ = NULLPTR;
};

/// Returns true if the arrays are exactly equal
bool ARROW_EXPORT ArrayEquals(const Array& left, const Array& right,
                              const EqualOptions& = EqualOptions::Defaults());

/// Returns true if the arrays are approximately equal. For non-floating point
/// types, this is equivalent to ArrayEquals(left, right)
bool ARROW_EXPORT ArrayApproxEquals(const Array& left, const Array& right,
                                    const EqualOptions& = EqualOptions::Defaults());

/// Returns true if indicated equal-length segment of arrays are exactly equal
bool ARROW_EXPORT ArrayRangeEquals(const Array& left, const Array& right,
                                   int64_t start_idx, int64_t end_idx,
                                   int64_t other_start_idx,
                                   const EqualOptions& = EqualOptions::Defaults());

/// Returns true if indicated equal-length segment of arrays are approximately equal
bool ARROW_EXPORT ArrayRangeApproxEquals(const Array& left, const Array& right,
                                         int64_t start_idx, int64_t end_idx,
                                         int64_t other_start_idx,
                                         const EqualOptions& = EqualOptions::Defaults());

bool ARROW_EXPORT TensorEquals(const Tensor& left, const Tensor& right,
                               const EqualOptions& = EqualOptions::Defaults());

/// EXPERIMENTAL: Returns true if the given sparse tensors are exactly equal
bool ARROW_EXPORT SparseTensorEquals(const SparseTensor& left, const SparseTensor& right,
                                     const EqualOptions& = EqualOptions::Defaults());

/// Returns true if the type metadata are exactly equal
/// \param[in] left a DataType
/// \param[in] right a DataType
/// \param[in] check_metadata whether to compare KeyValueMetadata for child
/// fields
bool ARROW_EXPORT TypeEquals(const DataType& left, const DataType& right,
                             bool check_metadata = true);

/// Returns true if scalars are equal
/// \param[in] left a Scalar
/// \param[in] right a Scalar
/// \param[in] options comparison options
bool ARROW_EXPORT ScalarEquals(const Scalar& left, const Scalar& right,
                               const EqualOptions& options = EqualOptions::Defaults());

/// Returns true if scalars are approximately equal
/// \param[in] left a Scalar
/// \param[in] right a Scalar
/// \param[in] options comparison options
bool ARROW_EXPORT
ScalarApproxEquals(const Scalar& left, const Scalar& right,
                   const EqualOptions& options = EqualOptions::Defaults());

class OrderOptions {
 public:
  // Whether NaNs are considered least in order
  bool nans_least() const { return nans_least_; }

  OrderOptions nans_least(bool v) const {
    auto res = OrderOptions(*this);
    res.nans_least_ = v;
    return res;
  }

  /// The absolute tolerance for approximate comparisons of floating-point values.
  double atolf() const { return atolf_; }

  /// Return a new EqualOptions object with the "atol" property changed.
  OrderOptions atolf(double v) const {
    auto res = OrderOptions(*this);
    res.atolf_ = v;
    return res;
  }

  /// The absolute tolerance for approximate comparisons of integer values.
  double atold() const { return atold_; }

  /// Return a new EqualOptions object with the "atol" property changed.
  OrderOptions atold(double v) const {
    auto res = OrderOptions(*this);
    res.atold_ = v;
    return res;
  }

  static OrderOptions Defaults() { return {}; }

 protected:
  double atolf_ = kDefaultOrderFloatingPointAbsoluteTolerance;
  uint64_t atold_ = kDefaultOrderIntegerAbsoluteTolerance;
  bool nans_least_ = false;
};

namespace internal {

template <typename T>
T TypeAdjustedTolerance(double tolerance) {
  T max = std::numeric_limits<T>::max();
  return tolerance >= max ? max : static_cast<T>(tolerance);
}

template <typename T>
T TypeAdjustedTolerance(uint64_t tolerance) {
  T max = std::numeric_limits<T>::max();
  return tolerance >= max ? max : static_cast<T>(tolerance);
}

template <typename T, template <typename> typename Op,
          typename = std::enable_if<std::is_base_of<Scalar, T>::value>>
bool NumericScalarCompare(const T& left, const T& right, const OrderOptions& options) {
  if (left.type != right.type) {
    return false;  // arbitrary - unequal types are unordered
  }
  auto ty = left.type;
  bool fp_type = ty == float16() || ty == float32() || ty == float64();
  bool left_nan = !left.is_valid || (fp_type && std::isnan(left.value));
  bool right_nan = !right.is_valid || (fp_type && std::isnan(right.value));
  using V = decltype(left.value);
  Op<V> cmp;
  if (left_nan) {
    if (right_nan) {
      return false;  // arbitrary - NaNs are unordered
    } else {
      return options.nans_least() ? cmp(true) : cmp(false);
    }
  } else {
    if (right_nan) {
      return options.nans_least() ? cmp(false) : cmp(true);
    } else {
      V tolerance = fp_type ? TypeAdjustedTolerance<V>(options.atolf())
                            : TypeAdjustedTolerance<V>(options.atold());
      return cmp(left.value, right.value, tolerance);
    }
  }
}

template <typename T>
struct LessThanOp {
  bool operator()(T left, T right, T tolerance) {
    return (left < right) || (left - right < tolerance);
  }
  bool operator()(bool left_least) { return left_least; }
};

template <typename T>
struct IsAtMostOp {
  bool operator()(T left, T right, T tolerance) {
    return (left <= right) || (left - right <= tolerance);
  }
  bool operator()(bool left_least) { return left_least; }
};

template <typename T>
struct MoreThanOp {
  bool operator()(T left, T right, T tolerance) {
    return (left > right) || (right - left < tolerance);
  }
  bool operator()(bool left_least) { return !left_least; }
};

template <typename T>
struct IsAtLeastOp {
  bool operator()(T left, T right, T tolerance) {
    return (left >= right) || (right - left <= tolerance);
  }
  bool operator()(bool left_least) { return !left_least; }
};

}  // namespace internal

/// Returns true if left numeric scalar is less than right numeric scalar
/// \param[in] left a NumericScalar
/// \param[in] right a NumericScalar
/// \param[in] options comparison options
template <typename T>
bool ARROW_EXPORT ScalarLessThan(const NumericScalar<T>& left,
                                 const NumericScalar<T>& right,
                                 const OrderOptions& options = OrderOptions::Defaults()) {
  return internal::NumericScalarCompare<NumericScalar<T>, internal::LessThanOp>(
      left, right, options);
}

/// Returns true if left numeric scalar is at most right numeric scalar
/// \param[in] left a NumericScalar
/// \param[in] right a NumericScalar
/// \param[in] options comparison options
template <typename T>
bool ARROW_EXPORT ScalarIsAtMost(const NumericScalar<T>& left,
                                 const NumericScalar<T>& right,
                                 const OrderOptions& options = OrderOptions::Defaults()) {
  return internal::NumericScalarCompare<NumericScalar<T>, internal::IsAtMostOp>(
      left, right, options);
}

/// Returns true if left numeric scalar is more than right numeric scalar
/// \param[in] left a NumericScalar
/// \param[in] right a NumericScalar
/// \param[in] options comparison options
template <typename T>
bool ARROW_EXPORT ScalarMoreThan(const NumericScalar<T>& left,
                                 const NumericScalar<T>& right,
                                 const OrderOptions& options = OrderOptions::Defaults()) {
  return internal::NumericScalarCompare<NumericScalar<T>, internal::MoreThanOp>(
      left, right, options);
}

/// Returns true if left numeric scalar is at least right numeric scalar
/// \param[in] left a NumericScalar
/// \param[in] right a NumericScalar
/// \param[in] options comparison options
template <typename T>
bool ARROW_EXPORT
ScalarIsAtLeast(const NumericScalar<T>& left, const NumericScalar<T>& right,
                const OrderOptions& options = OrderOptions::Defaults()) {
  return internal::NumericScalarCompare<NumericScalar<T>, internal::IsAtLeastOp>(
      left, right, options);
}

/// Returns true if left scalar is less than right scalar
///
/// Returns an arbitrary result if unordered, or fails if incomparable.
///
/// \param[in] left a Scalar
/// \param[in] right a Scalar
/// \param[in] options comparison options
Result<bool> ARROW_EXPORT
ScalarLessThan(const Scalar& left, const Scalar& right,
               const OrderOptions& options = OrderOptions::Defaults());

/// Returns true if left scalar is at most right scalar
///
/// Returns an arbitrary result if unordered, or fails if incomparable.
///
/// \param[in] left a Scalar
/// \param[in] right a Scalar
/// \param[in] options comparison options
Result<bool> ARROW_EXPORT
ScalarIsAtMost(const Scalar& left, const Scalar& right,
               const OrderOptions& options = OrderOptions::Defaults());

/// Returns true if left scalar is more than right scalar
///
/// Returns an arbitrary result if unordered, or fails if incomparable.
///
/// \param[in] left a Scalar
/// \param[in] right a Scalar
/// \param[in] options comparison options
Result<bool> ARROW_EXPORT
ScalarMoreThan(const Scalar& left, const Scalar& right,
               const OrderOptions& options = OrderOptions::Defaults());

/// Returns true if left scalar is at least right scalar
///
/// Returns an arbitrary result if unordered, or fails if incomparable.
///
/// \param[in] left a Scalar
/// \param[in] right a Scalar
/// \param[in] options comparison options
Result<bool> ARROW_EXPORT
ScalarIsAtLeast(const Scalar& left, const Scalar& right,
                const OrderOptions& options = OrderOptions::Defaults());

}  // namespace arrow
