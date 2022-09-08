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

#include <cstdint>
#include <iosfwd>

#include "arrow/util/macros.h"
#include "arrow/util/visibility.h"

namespace arrow {

class Array;
class DataType;
class Tensor;
class SparseTensor;
struct Scalar;

static constexpr double kDefaultAbsoluteTolerance = 1E-5;

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

}  // namespace arrow
