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

#include "arrow/array.h"
#include "arrow/flight/sql/odbc/flight_sql/accessors/types.h"
#include "arrow/flight/sql/odbc/flight_sql/utils.h"

namespace driver {
namespace flight_sql {

using arrow::Array;

class FlightSqlResultSetColumn {
 private:
  std::shared_ptr<Array> original_array_;
  std::shared_ptr<Array> cached_casted_array_;
  std::unique_ptr<Accessor> cached_accessor_;

  std::unique_ptr<Accessor> CreateAccessor(CDataType target_type);

  Accessor* GetAccessorForTargetType(CDataType target_type);

 public:
  FlightSqlResultSetColumn() = default;
  explicit FlightSqlResultSetColumn(bool use_wide_char);

  ColumnBinding binding_;
  bool use_wide_char_;
  bool is_bound_;

  inline Accessor* GetAccessorForBinding() { return cached_accessor_.get(); }

  inline Accessor* GetAccessorForGetData(CDataType target_type) {
    if (target_type == odbcabstraction::CDataType_DEFAULT) {
      target_type = ConvertArrowTypeToC(original_array_->type_id(), use_wide_char_);
    }

    if (cached_accessor_ && cached_accessor_->target_type_ == target_type) {
      return cached_accessor_.get();
    }
    return GetAccessorForTargetType(target_type);
  }

  void SetBinding(const ColumnBinding& new_binding, arrow::Type::type arrow_type);

  void ResetBinding();

  inline void ResetAccessor(std::shared_ptr<Array> array) {
    original_array_ = std::move(array);
    if (cached_accessor_) {
      cached_accessor_ = CreateAccessor(cached_accessor_->target_type_);
    } else if (is_bound_) {
      cached_accessor_ = CreateAccessor(binding_.target_type);
    } else {
      cached_casted_array_.reset();
      cached_accessor_.reset();
    }
  }
};
}  // namespace flight_sql
}  // namespace driver
