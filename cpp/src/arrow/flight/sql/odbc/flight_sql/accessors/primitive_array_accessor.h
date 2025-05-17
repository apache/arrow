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
#include "arrow/flight/sql/odbc/flight_sql/accessors/common.h"
#include "arrow/flight/sql/odbc/flight_sql/accessors/types.h"
#include "arrow/flight/sql/odbc/flight_sql/flight_sql_result_set.h"
#include "arrow/flight/sql/odbc/odbcabstraction/include/odbcabstraction/types.h"
#include "arrow/scalar.h"

namespace driver {
namespace flight_sql {

template <typename ARROW_ARRAY, CDataType TARGET_TYPE>
class PrimitiveArrayFlightSqlAccessor
    : public FlightSqlAccessor<
          ARROW_ARRAY, TARGET_TYPE,
          PrimitiveArrayFlightSqlAccessor<ARROW_ARRAY, TARGET_TYPE>> {
 public:
  explicit PrimitiveArrayFlightSqlAccessor(Array* array);

  size_t GetColumnarData_impl(ColumnBinding* binding, int64_t starting_row, int64_t cells,
                              int64_t& value_offset, bool update_value_offset,
                              odbcabstraction::Diagnostics& diagnostics,
                              uint16_t* row_status_array);

  size_t GetCellLength_impl(ColumnBinding* binding) const;
};

}  // namespace flight_sql
}  // namespace driver
