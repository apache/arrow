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

#include "arrow/flight/sql/odbc/odbc_impl/flight_sql_result_set_accessors.h"

#include <boost/functional/hash.hpp>

#include "arrow/flight/sql/odbc/odbc_impl/accessors/binary_array_accessor.h"
#include "arrow/flight/sql/odbc/odbc_impl/accessors/boolean_array_accessor.h"
#include "arrow/flight/sql/odbc/odbc_impl/accessors/date_array_accessor.h"
#include "arrow/flight/sql/odbc/odbc_impl/accessors/decimal_array_accessor.h"
#include "arrow/flight/sql/odbc/odbc_impl/accessors/primitive_array_accessor.h"
#include "arrow/flight/sql/odbc/odbc_impl/accessors/string_array_accessor.h"
#include "arrow/flight/sql/odbc/odbc_impl/accessors/time_array_accessor.h"
#include "arrow/flight/sql/odbc/odbc_impl/accessors/timestamp_array_accessor.h"
#include "arrow/flight/sql/odbc/odbc_impl/platform.h"

// Workaround for ODBC `BOOL` def conflict on Linux
#ifdef __linux__
#  ifdef BOOL
#    undef BOOL
#  endif  // BOOL
#endif    // __linux__

namespace arrow::flight::sql::odbc {

typedef std::pair<Type::type, CDataType> SourceAndTargetPair;
typedef std::function<Accessor*(Array*)> AccessorConstructor;

namespace {

const std::unordered_map<SourceAndTargetPair, AccessorConstructor,
                         boost::hash<SourceAndTargetPair>>
    ACCESSORS_CONSTRUCTORS = {
        {SourceAndTargetPair(Type::type::STRING, CDataType_CHAR),
         [](Array* array) {
           return new StringArrayFlightSqlAccessor<CDataType_CHAR, char>(array);
         }},
        {SourceAndTargetPair(Type::type::STRING, CDataType_WCHAR),
         CreateWCharStringArrayAccessor},
        {SourceAndTargetPair(Type::type::DOUBLE, CDataType_DOUBLE),
         [](Array* array) {
           return new PrimitiveArrayFlightSqlAccessor<DoubleArray, CDataType_DOUBLE>(
               array);
         }},
        {SourceAndTargetPair(Type::type::FLOAT, CDataType_FLOAT),
         [](Array* array) {
           return new PrimitiveArrayFlightSqlAccessor<FloatArray, CDataType_FLOAT>(array);
         }},
        {SourceAndTargetPair(Type::type::INT64, CDataType_SBIGINT),
         [](Array* array) {
           return new PrimitiveArrayFlightSqlAccessor<Int64Array, CDataType_SBIGINT>(
               array);
         }},
        {SourceAndTargetPair(Type::type::UINT64, CDataType_UBIGINT),
         [](Array* array) {
           return new PrimitiveArrayFlightSqlAccessor<UInt64Array, CDataType_UBIGINT>(
               array);
         }},
        {SourceAndTargetPair(Type::type::INT32, CDataType_SLONG),
         [](Array* array) {
           return new PrimitiveArrayFlightSqlAccessor<Int32Array, CDataType_SLONG>(array);
         }},
        {SourceAndTargetPair(Type::type::UINT32, CDataType_ULONG),
         [](Array* array) {
           return new PrimitiveArrayFlightSqlAccessor<UInt32Array, CDataType_ULONG>(
               array);
         }},
        {SourceAndTargetPair(Type::type::INT16, CDataType_SSHORT),
         [](Array* array) {
           return new PrimitiveArrayFlightSqlAccessor<Int16Array, CDataType_SSHORT>(
               array);
         }},
        {SourceAndTargetPair(Type::type::UINT16, CDataType_USHORT),
         [](Array* array) {
           return new PrimitiveArrayFlightSqlAccessor<UInt16Array, CDataType_USHORT>(
               array);
         }},
        {SourceAndTargetPair(Type::type::INT8, CDataType_STINYINT),
         [](Array* array) {
           return new PrimitiveArrayFlightSqlAccessor<Int8Array, CDataType_STINYINT>(
               array);
         }},
        {SourceAndTargetPair(Type::type::UINT8, CDataType_UTINYINT),
         [](Array* array) {
           return new PrimitiveArrayFlightSqlAccessor<UInt8Array, CDataType_UTINYINT>(
               array);
         }},
        {SourceAndTargetPair(Type::type::BOOL, CDataType_BIT),
         [](Array* array) {
           return new BooleanArrayFlightSqlAccessor<CDataType_BIT>(array);
         }},
        {SourceAndTargetPair(Type::type::BINARY, CDataType_BINARY),
         [](Array* array) {
           return new BinaryArrayFlightSqlAccessor<CDataType_BINARY>(array);
         }},
        {SourceAndTargetPair(Type::type::DATE32, CDataType_DATE),
         [](Array* array) {
           return new DateArrayFlightSqlAccessor<CDataType_DATE, Date32Array>(array);
         }},
        {SourceAndTargetPair(Type::type::DATE64, CDataType_DATE),
         [](Array* array) {
           return new DateArrayFlightSqlAccessor<CDataType_DATE, Date64Array>(array);
         }},
        {SourceAndTargetPair(Type::type::TIMESTAMP, CDataType_TIMESTAMP),
         [](Array* array) {
           auto time_type =
               arrow::internal::checked_pointer_cast<TimestampType>(array->type());
           auto time_unit = time_type->unit();
           Accessor* result;
           switch (time_unit) {
             case TimeUnit::SECOND:
               result = new TimestampArrayFlightSqlAccessor<CDataType_TIMESTAMP,
                                                            TimeUnit::SECOND>(array);
               break;
             case TimeUnit::MILLI:
               result = new TimestampArrayFlightSqlAccessor<CDataType_TIMESTAMP,
                                                            TimeUnit::MILLI>(array);
               break;
             case TimeUnit::MICRO:
               result = new TimestampArrayFlightSqlAccessor<CDataType_TIMESTAMP,
                                                            TimeUnit::MICRO>(array);
               break;
             case TimeUnit::NANO:
               result = new TimestampArrayFlightSqlAccessor<CDataType_TIMESTAMP,
                                                            TimeUnit::NANO>(array);
               break;
             default:
               assert(false);
               throw DriverException("Unrecognized time unit " +
                                     std::to_string(time_unit));
           }
           return result;
         }},
        {SourceAndTargetPair(Type::type::TIME32, CDataType_TIME),
         [](Array* array) { return CreateTimeAccessor(array, Type::type::TIME32); }},
        {SourceAndTargetPair(Type::type::TIME64, CDataType_TIME),
         [](Array* array) { return CreateTimeAccessor(array, Type::type::TIME64); }},
        {SourceAndTargetPair(Type::type::DECIMAL128, CDataType_NUMERIC),
         [](Array* array) {
           return new DecimalArrayFlightSqlAccessor<Decimal128Array, CDataType_NUMERIC>(
               array);
         }}};
}  // namespace

std::unique_ptr<Accessor> CreateAccessor(Array* source_array, CDataType target_type) {
  auto it = ACCESSORS_CONSTRUCTORS.find(
      SourceAndTargetPair(source_array->type_id(), target_type));
  if (it != ACCESSORS_CONSTRUCTORS.end()) {
    auto accessor = it->second(source_array);
    return std::unique_ptr<Accessor>(accessor);
  }

  std::stringstream ss;
  ss << "Unsupported type conversion! Tried to convert '"
     << source_array->type()->ToString() << "' to C type '" << target_type << "'";
  throw DriverException(ss.str());
}

}  // namespace arrow::flight::sql::odbc
