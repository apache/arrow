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

#include "arrow/flight/sql/odbc/flight_sql/accessors/decimal_array_accessor.h"

#include "arrow/array.h"
#include "arrow/scalar.h"

namespace driver {
namespace flight_sql {

using arrow::Decimal128;
using arrow::Decimal128Array;
using arrow::Decimal128Type;
using arrow::Status;

using odbcabstraction::DriverException;
using odbcabstraction::NUMERIC_STRUCT;
using odbcabstraction::RowStatus;

template <typename ARROW_ARRAY, CDataType TARGET_TYPE>
DecimalArrayFlightSqlAccessor<ARROW_ARRAY, TARGET_TYPE>::DecimalArrayFlightSqlAccessor(
    Array* array)
    : FlightSqlAccessor<ARROW_ARRAY, TARGET_TYPE,
                        DecimalArrayFlightSqlAccessor<ARROW_ARRAY, TARGET_TYPE>>(array),
      data_type_(static_cast<Decimal128Type*>(array->type().get())) {}

template <>
RowStatus
DecimalArrayFlightSqlAccessor<Decimal128Array, odbcabstraction::CDataType_NUMERIC>::
    MoveSingleCell_impl(ColumnBinding* binding, int64_t arrow_row, int64_t i,
                        int64_t& value_offset, bool update_value_offset,
                        odbcabstraction::Diagnostics& diagnostics) {
  auto result = &(static_cast<NUMERIC_STRUCT*>(binding->buffer)[i]);
  int32_t original_scale = data_type_->scale();

  const uint8_t* bytes = this->GetArray()->Value(arrow_row);
  Decimal128 value(bytes);
  if (original_scale != binding->scale) {
    const Status& status = value.Rescale(original_scale, binding->scale).Value(&value);
    ThrowIfNotOK(status);
  }
  if (!value.FitsInPrecision(binding->precision)) {
    throw DriverException("Decimal value doesn't fit in precision " +
                          std::to_string(binding->precision));
  }

  result->sign = value.IsNegative() ? 0 : 1;

  // Take the absolute value since the ODBC SQL_NUMERIC_STRUCT holds
  // a positive-only number.
  if (value.IsNegative()) {
    Decimal128 abs_value = Decimal128::Abs(value);
    abs_value.ToBytes(result->val);
  } else {
    value.ToBytes(result->val);
  }
  result->precision = static_cast<uint8_t>(binding->precision);
  result->scale = static_cast<int8_t>(binding->scale);

  result->precision = data_type_->precision();

  if (binding->strlen_buffer) {
    binding->strlen_buffer[i] = static_cast<ssize_t>(GetCellLength_impl(binding));
  }

  return odbcabstraction::RowStatus_SUCCESS;
}

template <typename ARROW_ARRAY, CDataType TARGET_TYPE>
size_t DecimalArrayFlightSqlAccessor<ARROW_ARRAY, TARGET_TYPE>::GetCellLength_impl(
    ColumnBinding* binding) const {
  return sizeof(NUMERIC_STRUCT);
}

template class DecimalArrayFlightSqlAccessor<Decimal128Array,
                                             odbcabstraction::CDataType_NUMERIC>;

}  // namespace flight_sql
}  // namespace driver
