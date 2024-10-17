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
#include <optional>
#include <string>
#include <variant>

#include "arrow/util/visibility.h"

namespace arrow {

/// \brief Statistics for an Array
///
/// Apache Arrow format doesn't have statistics but data source such
/// as Apache Parquet may have statistics. Statistics associated with
/// data source can be read unified API via this class.
struct ARROW_EXPORT ArrayStatistics {
  using ValueType = std::variant<bool, int64_t, uint64_t, double, std::string>;

  /// \brief The number of null values, may not be set
  std::optional<int64_t> null_count = std::nullopt;

  /// \brief The number of distinct values, may not be set
  std::optional<int64_t> distinct_count = std::nullopt;

  /// \brief The minimum value, may not be set
  std::optional<ValueType> min = std::nullopt;

  /// \brief Whether the minimum value is exact or not
  bool is_min_exact = false;

  /// \brief The maximum value, may not be set
  std::optional<ValueType> max = std::nullopt;

  /// \brief Whether the maximum value is exact or not
  bool is_max_exact = false;

  /// \brief Check two statistics for equality
  bool Equals(const ArrayStatistics& other) const {
    return null_count == other.null_count && distinct_count == other.distinct_count &&
           min == other.min && is_min_exact == other.is_min_exact && max == other.max &&
           is_max_exact == other.is_max_exact;
  }

  /// \brief Check two statistics for equality
  bool operator==(const ArrayStatistics& other) const { return Equals(other); }

  /// \brief Check two statistics for not equality
  bool operator!=(const ArrayStatistics& other) const { return !Equals(other); }
};

}  // namespace arrow
