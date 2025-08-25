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

#include "arrow/flight/sql/odbc/flight_sql/accessors/boolean_array_accessor.h"

namespace driver {
namespace flight_sql {

using arrow::BooleanArray;
using odbcabstraction::RowStatus;

template <CDataType TARGET_TYPE>
BooleanArrayFlightSqlAccessor<TARGET_TYPE>::BooleanArrayFlightSqlAccessor(Array* array)
    : FlightSqlAccessor<BooleanArray, TARGET_TYPE,
                        BooleanArrayFlightSqlAccessor<TARGET_TYPE>>(array) {}

template <CDataType TARGET_TYPE>
RowStatus BooleanArrayFlightSqlAccessor<TARGET_TYPE>::MoveSingleCell_impl(
    ColumnBinding* binding, int64_t arrow_row, int64_t i, int64_t& value_offset,
    bool update_value_offset, odbcabstraction::Diagnostics& diagnostics) {
  typedef unsigned char c_type;
  bool value = this->GetArray()->Value(arrow_row);

  auto* buffer = static_cast<c_type*>(binding->buffer);
  buffer[i] = value ? 1 : 0;

  if (binding->strlen_buffer) {
    binding->strlen_buffer[i] = static_cast<ssize_t>(GetCellLength_impl(binding));
  }

  return odbcabstraction::RowStatus_SUCCESS;
}

template <CDataType TARGET_TYPE>
size_t BooleanArrayFlightSqlAccessor<TARGET_TYPE>::GetCellLength_impl(
    ColumnBinding* binding) const {
  return sizeof(unsigned char);
}

template class BooleanArrayFlightSqlAccessor<odbcabstraction::CDataType_BIT>;

}  // namespace flight_sql
}  // namespace driver
