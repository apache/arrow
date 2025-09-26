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

#include "arrow/flight/sql/odbc/flight_sql/accessors/primitive_array_accessor.h"

namespace arrow::flight::sql::odbc {

using arrow::DoubleArray;
using arrow::FloatArray;
using arrow::Int16Array;
using arrow::Int32Array;
using arrow::Int64Array;
using arrow::Int8Array;
using arrow::UInt16Array;
using arrow::UInt32Array;
using arrow::UInt64Array;
using arrow::UInt8Array;

template <typename ARROW_ARRAY, CDataType TARGET_TYPE>
PrimitiveArrayFlightSqlAccessor<
    ARROW_ARRAY, TARGET_TYPE>::PrimitiveArrayFlightSqlAccessor(Array* array)
    : FlightSqlAccessor<ARROW_ARRAY, TARGET_TYPE,
                        PrimitiveArrayFlightSqlAccessor<ARROW_ARRAY, TARGET_TYPE>>(
          array) {}

template <typename ARROW_ARRAY, CDataType TARGET_TYPE>
size_t PrimitiveArrayFlightSqlAccessor<ARROW_ARRAY, TARGET_TYPE>::GetColumnarDataImpl(
    ColumnBinding* binding, int64_t starting_row, int64_t cells, int64_t& value_offset,
    bool update_value_offset, Diagnostics& diagnostics, uint16_t* row_status_array) {
  return CopyFromArrayValuesToBinding<ARROW_ARRAY>(this->GetArray(), binding,
                                                   starting_row, cells);
}

template <typename ARROW_ARRAY, CDataType TARGET_TYPE>
size_t PrimitiveArrayFlightSqlAccessor<ARROW_ARRAY, TARGET_TYPE>::GetCellLengthImpl(
    ColumnBinding* binding) const {
  return sizeof(typename ARROW_ARRAY::TypeClass::c_type);
}

template class PrimitiveArrayFlightSqlAccessor<Int64Array, CDataType_SBIGINT>;
template class PrimitiveArrayFlightSqlAccessor<Int32Array, CDataType_SLONG>;
template class PrimitiveArrayFlightSqlAccessor<Int16Array, CDataType_SSHORT>;
template class PrimitiveArrayFlightSqlAccessor<Int8Array, CDataType_STINYINT>;
template class PrimitiveArrayFlightSqlAccessor<UInt64Array, CDataType_UBIGINT>;
template class PrimitiveArrayFlightSqlAccessor<UInt32Array, CDataType_ULONG>;
template class PrimitiveArrayFlightSqlAccessor<UInt16Array, CDataType_USHORT>;
template class PrimitiveArrayFlightSqlAccessor<UInt8Array, CDataType_UTINYINT>;
template class PrimitiveArrayFlightSqlAccessor<DoubleArray, CDataType_DOUBLE>;
template class PrimitiveArrayFlightSqlAccessor<FloatArray, CDataType_FLOAT>;

}  // namespace arrow::flight::sql::odbc
