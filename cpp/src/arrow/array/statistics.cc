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

#include "arrow/array/statistics.h"

#include <cmath>
#include <type_traits>

#include "arrow/compare.h"
namespace arrow {
using ValueType = ArrayStatistics::ValueType;
namespace {
bool DoubleEquals(const double& left, const double& right, const bool is_approximate,
                  const EqualOptions& options) {
  if (left == right) {
    return options.signed_zeros_equal() || (std::signbit(left) == std::signbit(right));
  } else if (options.nans_equal() && (std::isnan(left) || std::isnan(right))) {
    return true;
  } else if (is_approximate) {
    return std::fabs(left - right) <= options.atol();
  } else {
    return false;
  }
}

bool ValueTypeEquals(const std::optional<ValueType>& left,
                     const std::optional<ValueType>& right, const EqualOptions& options,
                     const bool is_approximate) {
  if (!left.has_value() && !right.has_value()) {
    return true;
  } else if (left->index() != right->index()) {
    return false;
  } else {
    auto CheckVisitor = [&](const auto& v1, const auto& v2) {
      using type_1 = std::decay_t<decltype(v1)>;
      using type_2 = std::decay_t<decltype(v2)>;
      if constexpr (std::conjunction_v<std::is_same<type_1, double>,
                                       std::is_same<type_2, double>>) {
        return DoubleEquals(v1, v2, is_approximate, options);
      } else if constexpr (std::is_same_v<type_1, type_2>) {
        return v1 == v2;
      }
      // It is unreachable
      return false;
    };
    return std::visit(CheckVisitor, left.value(), right.value());
  }
}
bool ArrayStatisticsEquals(const ArrayStatistics& left, const ArrayStatistics& right,
                           const EqualOptions& equal_options, bool is_approximate) {
  return left.null_count == right.null_count &&
         left.distinct_count == right.distinct_count &&
         left.is_min_exact == right.is_min_exact &&
         left.is_max_exact == right.is_max_exact &&
         ValueTypeEquals(left.min, right.min, equal_options, is_approximate) &&
         ValueTypeEquals(left.max, right.max, equal_options, is_approximate);
}
}  // namespace
bool ArrayStatistics::Equals(const ArrayStatistics& other,
                             const EqualOptions& options) const {
  return ArrayStatisticsEquals(*this, other, options, false);
}
bool ArrayStatistics::ApproximateEquals(const ArrayStatistics& other,
                                        const EqualOptions& options) const {
  return ArrayStatisticsEquals(*this, other, options, true);
}
}  // namespace arrow
