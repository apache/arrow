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

#include "arrow/flight/sql/odbc/flight_sql/flight_sql_result_set_column.h"
#include <memory>
#include "arrow/flight/sql/odbc/flight_sql/accessors/types.h"
#include "arrow/flight/sql/odbc/flight_sql/flight_sql_result_set_accessors.h"
#include "arrow/flight/sql/odbc/flight_sql/utils.h"
#include "arrow/flight/sql/odbc/odbcabstraction/include/odbcabstraction/platform.h"
#include "arrow/flight/sql/odbc/odbcabstraction/include/odbcabstraction/types.h"

namespace driver {
namespace flight_sql {

namespace {
std::shared_ptr<Array> CastArray(const std::shared_ptr<arrow::Array>& original_array,
                                 CDataType target_type) {
  bool conversion = NeedArrayConversion(original_array->type()->id(), target_type);

  if (conversion) {
    auto converter = GetConverter(original_array->type_id(), target_type);
    return converter(original_array);
  } else {
    return original_array;
  }
}
}  // namespace

std::unique_ptr<Accessor> FlightSqlResultSetColumn::CreateAccessor(
    CDataType target_type) {
  cached_casted_array_ = CastArray(original_array_, target_type);

  return flight_sql::CreateAccessor(cached_casted_array_.get(), target_type);
}

Accessor* FlightSqlResultSetColumn::GetAccessorForTargetType(CDataType target_type) {
  // Cast the original array to a type matching the target_type.
  if (target_type == odbcabstraction::CDataType_DEFAULT) {
    target_type = ConvertArrowTypeToC(original_array_->type_id(), use_wide_char_);
  }

  cached_accessor_ = CreateAccessor(target_type);
  return cached_accessor_.get();
}

FlightSqlResultSetColumn::FlightSqlResultSetColumn(bool use_wide_char)
    : use_wide_char_(use_wide_char), is_bound_(false) {}

void FlightSqlResultSetColumn::SetBinding(const ColumnBinding& new_binding,
                                          arrow::Type::type arrow_type) {
  binding_ = new_binding;
  is_bound_ = true;

  if (binding_.target_type == odbcabstraction::CDataType_DEFAULT) {
    binding_.target_type = ConvertArrowTypeToC(arrow_type, use_wide_char_);
  }

  // Overwrite the binding if the caller is using SQL_C_NUMERIC and has used zero
  // precision if it is zero (this is precision unset and will always fail).
  if (binding_.precision == 0 &&
      binding_.target_type == odbcabstraction::CDataType_NUMERIC) {
    binding_.precision = arrow::Decimal128Type::kMaxPrecision;
  }

  // Rebuild the accessor and casted array if the target type changed.
  if (original_array_ &&
      (!cached_casted_array_ || cached_accessor_->target_type_ != binding_.target_type)) {
    cached_accessor_ = CreateAccessor(binding_.target_type);
  }
}

void FlightSqlResultSetColumn::ResetBinding() {
  is_bound_ = false;
  cached_casted_array_.reset();
  cached_accessor_.reset();
}

}  // namespace flight_sql
}  // namespace driver
