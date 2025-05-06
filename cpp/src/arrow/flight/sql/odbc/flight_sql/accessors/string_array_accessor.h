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

#include <locale>
#include "arrow/flight/sql/odbc/flight_sql/accessors/types.h"
#include "arrow/flight/sql/odbc/flight_sql/utils.h"
#include "arrow/flight/sql/odbc/odbcabstraction/include/odbcabstraction/encoding.h"
#include "arrow/flight/sql/odbc/odbcabstraction/include/odbcabstraction/types.h"
#include "arrow/type_fwd.h"

namespace driver {
namespace flight_sql {

using arrow::StringArray;
using odbcabstraction::CDataType;
using odbcabstraction::DriverException;
using odbcabstraction::RowStatus;

using odbcabstraction::GetSqlWCharSize;

template <CDataType TARGET_TYPE, typename CHAR_TYPE>
class StringArrayFlightSqlAccessor
    : public FlightSqlAccessor<StringArray, TARGET_TYPE,
                               StringArrayFlightSqlAccessor<TARGET_TYPE, CHAR_TYPE>> {
 public:
  explicit StringArrayFlightSqlAccessor(Array* array);

  RowStatus MoveSingleCell_impl(ColumnBinding* binding, int64_t arrow_row, int64_t i,
                                int64_t& value_offset, bool update_value_offset,
                                odbcabstraction::Diagnostics& diagnostics);

  size_t GetCellLength_impl(ColumnBinding* binding) const;

 private:
  std::vector<uint8_t> buffer_;
#if defined _WIN32 || defined _WIN64
  std::string clocale_str_;
#endif
  int64_t last_arrow_row_;
};

inline Accessor* CreateWCharStringArrayAccessor(arrow::Array* array) {
  switch (GetSqlWCharSize()) {
    case sizeof(char16_t):
      return new StringArrayFlightSqlAccessor<odbcabstraction::CDataType_WCHAR, char16_t>(
          array);
    case sizeof(char32_t):
      return new StringArrayFlightSqlAccessor<odbcabstraction::CDataType_WCHAR, char32_t>(
          array);
    default:
      assert(false);
      throw DriverException("Encoding is unsupported, SQLWCHAR size: " +
                            std::to_string(GetSqlWCharSize()));
  }
}

}  // namespace flight_sql
}  // namespace driver
