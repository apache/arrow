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

// This empty .cc file is for embedding not inlined symbols in
// arrow::ArrayStatistics into libarrow.

#include "arrow/array/statistics.h"

#include "arrow/scalar.h"

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
}  // namespace arrow
