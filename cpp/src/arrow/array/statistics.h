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

#include <optional>
#include <variant>

#include "arrow/util/visibility.h"

namespace arrow {

/// \brief Statistics for an Array
///
/// Apache Arrow format doesn't have statistics but data source such
/// as Apache Parquet may have statistics. Statistics associate with
/// data source can be read unified API via this class.
struct ARROW_EXPORT ArrayStatistics {
 public:
  using ElementBufferType = std::variant<bool, int8_t, uint8_t, int16_t, uint16_t,
                                         int32_t, uint32_t, int64_t, uint64_t>;

  ArrayStatistics() = default;
  ~ArrayStatistics() = default;

  /// \brief The number of null values, may not be set
  std::optional<int64_t> null_count = std::nullopt;

  /// \brief The number of distinct values, may not be set
  std::optional<int64_t> distinct_count = std::nullopt;

  /// \brief The minimum value buffer, may not be set
  std::optional<ElementBufferType> min_buffer = std::nullopt;

  /// \brief Whether the minimum value is exact or not, may not be set
  std::optional<bool> is_min_exact = std::nullopt;

  /// \brief The maximum value buffer, may not be set
  std::optional<ElementBufferType> max_buffer = std::nullopt;

  /// \brief Whether the maximum value is exact or not, may not be set
  std::optional<bool> is_max_exact = std::nullopt;

  /// \brief Check two Statistics for equality
  bool Equals(const ArrayStatistics& other) const {
    return null_count == other.null_count && distinct_count == other.distinct_count &&
           min_buffer == other.min_buffer && is_min_exact == other.is_min_exact &&
           max_buffer == other.max_buffer && is_max_exact == other.is_max_exact;
  }
};

/// \brief A typed implementation of ArrayStatistics
template <typename TypeClass>
class TypedArrayStatistics : public ArrayStatistics {
 public:
  using ElementType = typename TypeClass::c_type;

  /// \brief The current minimum value, may not be set
  std::optional<ElementType> min() const {
    if (min_buffer && std::holds_alternative<ElementType>(*min_buffer)) {
      return std::get<ElementType>(*min_buffer);
    } else {
      return std::nullopt;
    }
  }

  /// \brief The current maximum value, may not be set
  std::optional<ElementType> max() const {
    if (max_buffer && std::holds_alternative<ElementType>(*max_buffer)) {
      return std::get<ElementType>(*max_buffer);
    } else {
      return std::nullopt;
    }
  }
};

}  // namespace arrow
