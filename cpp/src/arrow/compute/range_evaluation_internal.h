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

#include <memory>

#include "arrow/result.h"
#include "arrow/type_fwd.h"
#include "arrow/util/visibility.h"

namespace arrow::compute::internal {

/// \brief The values the argument of a unary call is known to take, for example
///   from the statistics of a Parquet row group.
struct ArgumentRange {
  /// The smallest and largest value the argument may take, in the argument's
  /// type and neither null nor NaN. A null pointer leaves that side unbounded.
  std::shared_ptr<Scalar> lower, upper;
  /// Whether the argument may be equal to the respective bound
  bool lower_inclusive = true, upper_inclusive = true;
  /// Whether the argument may be null, in addition to the values in the range
  bool nullable = true;
};

/// \brief What a call evaluates to for every argument in an ArgumentRange
enum class RangeEvaluation {
  /// Nothing is known: the call must be evaluated
  kUnknown,
  /// false for every argument, null or not
  kFalse,
  /// null for every argument, null or not
  kNull,
  /// false for arguments which are not null and null for those which are
  kFalseUnlessNull,
};

/// \brief Interface for kernel states which can tell what their (unary) kernel
///   evaluates to when its argument lies in a range.
///
/// A KernelState may implement this in addition to KernelState. Expression
/// simplification (SimplifyWithGuarantee) discovers it with dynamic_cast and
/// replaces calls for which the result is known, which is how filters are
/// pruned against statistics without the expression machinery knowing the
/// semantics of the function.
///
/// Implementations must be safe to call concurrently: bound expressions and
/// their kernel states are shared between threads.
class ARROW_EXPORT RangeEvaluator {
 public:
  virtual ~RangeEvaluator() = default;

  virtual Result<RangeEvaluation> EvaluateOverRange(const ArgumentRange& range) const = 0;
};

}  // namespace arrow::compute::internal
