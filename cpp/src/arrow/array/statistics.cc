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

#include <memory>
#include <type_traits>
#include <variant>

#include "arrow/scalar.h"
#include "arrow/type.h"

namespace arrow {

const std::shared_ptr<DataType>& ArrayStatistics::ValueToArrowType(
    const std::optional<ArrayStatistics::ValueType>& value,
    const std::shared_ptr<DataType>& array_type) {
  if (!value.has_value()) {
    return null();
  }

  struct Visitor {
    const std::shared_ptr<DataType>& array_type;

    const std::shared_ptr<DataType>& operator()(const bool&) { return boolean(); }
    const std::shared_ptr<DataType>& operator()(const int64_t&) { return int64(); }
    const std::shared_ptr<DataType>& operator()(const uint64_t&) { return uint64(); }
    const std::shared_ptr<DataType>& operator()(const double&) { return float64(); }
    const std::shared_ptr<DataType>& operator()(const std::shared_ptr<Scalar>& value) {
      return value->type;
    }
    const std::shared_ptr<DataType>& operator()(const std::string&) {
      switch (array_type->id()) {
        case Type::STRING:
        case Type::BINARY:
        case Type::FIXED_SIZE_BINARY:
        case Type::LARGE_STRING:
        case Type::LARGE_BINARY:
          return array_type;
        default:
          return utf8();
      }
    }
  } visitor{array_type};
  return std::visit(visitor, value.value());
}
namespace {
bool ValueTypeEquality(const std::optional<ArrayStatistics::ValueType>& first,
                       const std::optional<ArrayStatistics::ValueType>& second) {
  if (first == second) {
    return true;
  }
  if (!first || !second) {
    return false;
  }

  return std::visit(
      [](auto& v1, auto& v2) {
        if constexpr (std::is_same_v<std::decay_t<decltype(v1)>,
                                     std::shared_ptr<Scalar>> &&
                      std::is_same_v<std::decay_t<decltype(v2)>,
                                     std::shared_ptr<Scalar>>) {
          if (!v1 || !v2) {
            // both null case is handled in std::optional and return true
            return false;
          }
          return v1->Equals(*v2);
        }
        return false;
      },
      first.value(), second.value());
}
}  // namespace
bool ArrayStatistics::Equals(const ArrayStatistics& other) const {
  return null_count == other.null_count && distinct_count == other.distinct_count &&
         is_min_exact == other.is_min_exact && is_max_exact == other.is_max_exact &&
         ValueTypeEquality(this->max, other.max) &&
         ValueTypeEquality(this->min, other.min);
}

}  // namespace arrow
