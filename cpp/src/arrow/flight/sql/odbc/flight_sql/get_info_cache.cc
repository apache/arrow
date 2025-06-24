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

#include "arrow/flight/sql/odbc/flight_sql/get_info_cache.h"

#include "arrow/flight/sql/odbc/odbcabstraction/include/odbcabstraction/platform.h"

#include <sql.h>
#include <sqlext.h>
#include "arrow/array.h"
#include "arrow/array/array_nested.h"
#include "arrow/flight/sql/api.h"
#include "arrow/flight/sql/odbc/odbcabstraction/include/odbcabstraction/exceptions.h"
#include "arrow/scalar.h"
#include "arrow/type_fwd.h"

#include "arrow/flight/sql/odbc/flight_sql/flight_sql_stream_chunk_buffer.h"
#include "arrow/flight/sql/odbc/flight_sql/scalar_function_reporter.h"
#include "arrow/flight/sql/odbc/flight_sql/utils.h"

// Aliases for entries in SqlInfoOptions::SqlInfo that are defined here
// due to causing compilation errors conflicting with ODBC definitions.
#define ARROW_SQL_IDENTIFIER_CASE 503
#define ARROW_SQL_IDENTIFIER_QUOTE_CHAR 504
#define ARROW_SQL_QUOTED_IDENTIFIER_CASE 505
#define ARROW_SQL_KEYWORDS 508
#define ARROW_SQL_NUMERIC_FUNCTIONS 509
#define ARROW_SQL_STRING_FUNCTIONS 510
#define ARROW_SQL_SYSTEM_FUNCTIONS 511
#define ARROW_SQL_SCHEMA_TERM 529
#define ARROW_SQL_PROCEDURE_TERM 530
#define ARROW_SQL_CATALOG_TERM 531
#define ARROW_SQL_MAX_COLUMNS_IN_GROUP_BY 544
#define ARROW_SQL_MAX_COLUMNS_IN_INDEX 545
#define ARROW_SQL_MAX_COLUMNS_IN_ORDER_BY 546
#define ARROW_SQL_MAX_COLUMNS_IN_SELECT 547
#define ARROW_SQL_MAX_COLUMNS_IN_TABLE 548
#define ARROW_SQL_MAX_ROW_SIZE 555
#define ARROW_SQL_MAX_TABLES_IN_SELECT 560

#define ARROW_CONVERT_BIGINT 0
#define ARROW_CONVERT_BINARY 1
#define ARROW_CONVERT_BIT 2
#define ARROW_CONVERT_CHAR 3
#define ARROW_CONVERT_DATE 4
#define ARROW_CONVERT_DECIMAL 5
#define ARROW_CONVERT_FLOAT 6
#define ARROW_CONVERT_INTEGER 7
#define ARROW_CONVERT_INTERVAL_DAY_TIME 8
#define ARROW_CONVERT_INTERVAL_YEAR_MONTH 9
#define ARROW_CONVERT_LONGVARBINARY 10
#define ARROW_CONVERT_LONGVARCHAR 11
#define ARROW_CONVERT_NUMERIC 12
#define ARROW_CONVERT_REAL 13
#define ARROW_CONVERT_SMALLINT 14
#define ARROW_CONVERT_TIME 15
#define ARROW_CONVERT_TIMESTAMP 16
#define ARROW_CONVERT_TINYINT 17
#define ARROW_CONVERT_VARBINARY 18
#define ARROW_CONVERT_VARCHAR 19

namespace {
// Return the corresponding field in SQLGetInfo's SQL_CONVERT_* field
// types for the given Arrow SqlConvert enum value.
//
// The caller is responsible for casting the result to a uint16. Note
// that -1 is returned if there's no corresponding entry.
int32_t GetInfoTypeForArrowConvertEntry(int32_t convert_entry) {
  switch (convert_entry) {
    case ARROW_CONVERT_BIGINT:
      return SQL_CONVERT_BIGINT;
    case ARROW_CONVERT_BINARY:
      return SQL_CONVERT_BINARY;
    case ARROW_CONVERT_BIT:
      return SQL_CONVERT_BIT;
    case ARROW_CONVERT_CHAR:
      return SQL_CONVERT_CHAR;
    case ARROW_CONVERT_DATE:
      return SQL_CONVERT_DATE;
    case ARROW_CONVERT_DECIMAL:
      return SQL_CONVERT_DECIMAL;
    case ARROW_CONVERT_FLOAT:
      return SQL_CONVERT_FLOAT;
    case ARROW_CONVERT_INTEGER:
      return SQL_CONVERT_INTEGER;
    case ARROW_CONVERT_INTERVAL_DAY_TIME:
      return SQL_CONVERT_INTERVAL_DAY_TIME;
    case ARROW_CONVERT_INTERVAL_YEAR_MONTH:
      return SQL_CONVERT_INTERVAL_YEAR_MONTH;
    case ARROW_CONVERT_LONGVARBINARY:
      return SQL_CONVERT_LONGVARBINARY;
    case ARROW_CONVERT_LONGVARCHAR:
      return SQL_CONVERT_LONGVARCHAR;
    case ARROW_CONVERT_NUMERIC:
      return SQL_CONVERT_NUMERIC;
    case ARROW_CONVERT_REAL:
      return SQL_CONVERT_REAL;
    case ARROW_CONVERT_SMALLINT:
      return SQL_CONVERT_SMALLINT;
    case ARROW_CONVERT_TIME:
      return SQL_CONVERT_TIME;
    case ARROW_CONVERT_TIMESTAMP:
      return SQL_CONVERT_TIMESTAMP;
    case ARROW_CONVERT_TINYINT:
      return SQL_CONVERT_TINYINT;
    case ARROW_CONVERT_VARBINARY:
      return SQL_CONVERT_VARBINARY;
    case ARROW_CONVERT_VARCHAR:
      return SQL_CONVERT_VARCHAR;
  }
  // Arbitrarily return a negative value
  return -1;
}

// Return the corresponding bitmask to OR in SQLGetInfo's SQL_CONVERT_* field
// value for the given Arrow SqlConvert enum value.
//
// This is _not_ a bit position, it is an integer with only a single bit set.
uint32_t GetCvtBitForArrowConvertEntry(int32_t convert_entry) {
  switch (convert_entry) {
    case ARROW_CONVERT_BIGINT:
      return SQL_CVT_BIGINT;
    case ARROW_CONVERT_BINARY:
      return SQL_CVT_BINARY;
    case ARROW_CONVERT_BIT:
      return SQL_CVT_BIT;
    case ARROW_CONVERT_CHAR:
      return SQL_CVT_CHAR | SQL_CVT_WCHAR;
    case ARROW_CONVERT_DATE:
      return SQL_CVT_DATE;
    case ARROW_CONVERT_DECIMAL:
      return SQL_CVT_DECIMAL;
    case ARROW_CONVERT_FLOAT:
      return SQL_CVT_FLOAT;
    case ARROW_CONVERT_INTEGER:
      return SQL_CVT_INTEGER;
    case ARROW_CONVERT_INTERVAL_DAY_TIME:
      return SQL_CVT_INTERVAL_DAY_TIME;
    case ARROW_CONVERT_INTERVAL_YEAR_MONTH:
      return SQL_CVT_INTERVAL_YEAR_MONTH;
    case ARROW_CONVERT_LONGVARBINARY:
      return SQL_CVT_LONGVARBINARY;
    case ARROW_CONVERT_LONGVARCHAR:
      return SQL_CVT_LONGVARCHAR | SQL_CVT_WLONGVARCHAR;
    case ARROW_CONVERT_NUMERIC:
      return SQL_CVT_NUMERIC;
    case ARROW_CONVERT_REAL:
      return SQL_CVT_REAL;
    case ARROW_CONVERT_SMALLINT:
      return SQL_CVT_SMALLINT;
    case ARROW_CONVERT_TIME:
      return SQL_CVT_TIME;
    case ARROW_CONVERT_TIMESTAMP:
      return SQL_CVT_TIMESTAMP;
    case ARROW_CONVERT_TINYINT:
      return SQL_CVT_TINYINT;
    case ARROW_CONVERT_VARBINARY:
      return SQL_CVT_VARBINARY;
    case ARROW_CONVERT_VARCHAR:
      return SQL_CVT_VARCHAR | SQL_CVT_WLONGVARCHAR;
  }
  // Note: GUID not supported by GetSqlInfo.
  // Return zero, which has no bits set.
  return 0;
}

inline int32_t ScalarToInt32(arrow::UnionScalar* scalar) {
  return reinterpret_cast<arrow::Int32Scalar*>(scalar->child_value().get())->value;
}

inline int64_t ScalarToInt64(arrow::UnionScalar* scalar) {
  return reinterpret_cast<arrow::Int64Scalar*>(scalar->child_value().get())->value;
}

inline std::string ScalarToBoolString(arrow::UnionScalar* scalar) {
  return reinterpret_cast<arrow::BooleanScalar*>(scalar->child_value().get())->value
             ? "Y"
             : "N";
}

inline void SetDefaultIfMissing(
    std::unordered_map<uint16_t, driver::odbcabstraction::Connection::Info>& cache,
    uint16_t info_type, driver::odbcabstraction::Connection::Info default_value) {
  // Note: emplace() only writes if the key isn't found.
  cache.emplace(info_type, std::move(default_value));
}

}  // namespace

namespace driver {
namespace flight_sql {
using arrow::flight::FlightCallOptions;
using arrow::flight::sql::FlightSqlClient;
using arrow::flight::sql::SqlInfoOptions;
using driver::odbcabstraction::Connection;
using driver::odbcabstraction::DriverException;

GetInfoCache::GetInfoCache(FlightCallOptions& call_options,
                           std::unique_ptr<FlightSqlClient>& client,
                           const std::string& driver_version)
    : call_options_(call_options), sql_client_(client), has_server_info_(false) {
  info_[SQL_DRIVER_NAME] = "Arrow Flight ODBC Driver";
  info_[SQL_DRIVER_VER] = ConvertToDBMSVer(driver_version);

  info_[SQL_GETDATA_EXTENSIONS] =
      static_cast<uint32_t>(SQL_GD_ANY_COLUMN | SQL_GD_ANY_ORDER);
  info_[SQL_CURSOR_SENSITIVITY] = static_cast<uint32_t>(SQL_UNSPECIFIED);

  // Properties which don't currently have SqlGetInfo fields but probably
  // should.
  info_[SQL_ACCESSIBLE_PROCEDURES] = "N";
  info_[SQL_COLLATION_SEQ] = "";
  info_[SQL_ALTER_DOMAIN] = static_cast<uint32_t>(0);
  info_[SQL_ALTER_TABLE] = static_cast<uint32_t>(0);
  info_[SQL_COLUMN_ALIAS] = "Y";
  info_[SQL_DATETIME_LITERALS] = static_cast<uint32_t>(
      SQL_DL_SQL92_DATE | SQL_DL_SQL92_TIME | SQL_DL_SQL92_TIMESTAMP);
  info_[SQL_CREATE_ASSERTION] = static_cast<uint32_t>(0);
  info_[SQL_CREATE_CHARACTER_SET] = static_cast<uint32_t>(0);
  info_[SQL_CREATE_COLLATION] = static_cast<uint32_t>(0);
  info_[SQL_CREATE_DOMAIN] = static_cast<uint32_t>(0);
  info_[SQL_INDEX_KEYWORDS] = static_cast<uint32_t>(SQL_IK_NONE);
  info_[SQL_TIMEDATE_ADD_INTERVALS] = static_cast<uint32_t>(
      SQL_FN_TSI_FRAC_SECOND | SQL_FN_TSI_SECOND | SQL_FN_TSI_MINUTE | SQL_FN_TSI_HOUR |
      SQL_FN_TSI_DAY | SQL_FN_TSI_WEEK | SQL_FN_TSI_MONTH | SQL_FN_TSI_QUARTER |
      SQL_FN_TSI_YEAR);
  info_[SQL_TIMEDATE_DIFF_INTERVALS] = static_cast<uint32_t>(
      SQL_FN_TSI_FRAC_SECOND | SQL_FN_TSI_SECOND | SQL_FN_TSI_MINUTE | SQL_FN_TSI_HOUR |
      SQL_FN_TSI_DAY | SQL_FN_TSI_WEEK | SQL_FN_TSI_MONTH | SQL_FN_TSI_QUARTER |
      SQL_FN_TSI_YEAR);
  info_[SQL_CURSOR_COMMIT_BEHAVIOR] = static_cast<uint16_t>(SQL_CB_CLOSE);
  info_[SQL_CURSOR_ROLLBACK_BEHAVIOR] = static_cast<uint16_t>(SQL_CB_CLOSE);
  info_[SQL_CREATE_TRANSLATION] = static_cast<uint32_t>(0);
  info_[SQL_DDL_INDEX] = static_cast<uint32_t>(0);
  info_[SQL_DROP_ASSERTION] = static_cast<uint32_t>(0);
  info_[SQL_DROP_CHARACTER_SET] = static_cast<uint32_t>(0);
  info_[SQL_DROP_COLLATION] = static_cast<uint32_t>(0);
  info_[SQL_DROP_DOMAIN] = static_cast<uint32_t>(0);
  info_[SQL_DROP_SCHEMA] = static_cast<uint32_t>(0);
  info_[SQL_DROP_TABLE] = static_cast<uint32_t>(0);
  info_[SQL_DROP_TRANSLATION] = static_cast<uint32_t>(0);
  info_[SQL_DROP_VIEW] = static_cast<uint32_t>(0);
  info_[SQL_MAX_IDENTIFIER_LEN] = static_cast<uint16_t>(65535);  // arbitrary

  // Assume all aggregate functions reported in ODBC are supported.
  info_[SQL_AGGREGATE_FUNCTIONS] =
      static_cast<uint32_t>(SQL_AF_ALL | SQL_AF_AVG | SQL_AF_COUNT | SQL_AF_DISTINCT |
                            SQL_AF_MAX | SQL_AF_MIN | SQL_AF_SUM);

  // Assume catalogs are not supported by default. ODBC checks if SQL_CATALOG_NAME is
  // "Y" or "N" to determine if catalogs are supported.
  info_[SQL_CATALOG_TERM] = "";
  info_[SQL_CATALOG_NAME] = "N";
  info_[SQL_CATALOG_NAME_SEPARATOR] = "";
  info_[SQL_CATALOG_LOCATION] = static_cast<uint16_t>(0);
}

void GetInfoCache::SetProperty(uint16_t property,
                               driver::odbcabstraction::Connection::Info value) {
  info_[property] = value;
}

Connection::Info GetInfoCache::GetInfo(uint16_t info_type) {
  auto it = info_.find(info_type);

  if (info_.end() == it) {
    if (LoadInfoFromServer()) {
      it = info_.find(info_type);
    }
    if (info_.end() == it) {
      throw DriverException("Unknown GetInfo type: " + std::to_string(info_type));
    }
  }
  return it->second;
}

bool GetInfoCache::LoadInfoFromServer() {
  if (sql_client_ && !has_server_info_.exchange(true)) {
    std::unique_lock<std::mutex> lock(mutex_);
    arrow::Result<std::shared_ptr<FlightInfo>> result =
        sql_client_->GetSqlInfo(call_options_, {});
    ThrowIfNotOK(result.status());
    FlightStreamChunkBuffer chunk_iter(*sql_client_, call_options_, result.ValueOrDie());

    FlightStreamChunk chunk;
    bool supports_correlation_name = false;
    bool requires_different_correlation_name = false;
    bool transactions_supported = false;
    bool transaction_ddl_commit = false;
    bool transaction_ddl_ignore = false;
    while (chunk_iter.GetNext(&chunk)) {
      auto name_array = chunk.data->GetColumnByName("info_name");
      auto value_array = chunk.data->GetColumnByName("value");

      arrow::UInt32Array* info_type_array =
          static_cast<arrow::UInt32Array*>(name_array.get());
      arrow::UnionArray* value_union_array =
          static_cast<arrow::UnionArray*>(value_array.get());
      for (int64_t i = 0; i < chunk.data->num_rows(); ++i) {
        if (!value_array->IsNull(i)) {
          auto info_type = static_cast<arrow::flight::sql::SqlInfoOptions::SqlInfo>(
              info_type_array->Value(i));
          auto result_scalar = value_union_array->GetScalar(i);
          ThrowIfNotOK(result_scalar.status());
          std::shared_ptr<arrow::Scalar> scalar_ptr = result_scalar.ValueOrDie();
          arrow::UnionScalar* scalar =
              reinterpret_cast<arrow::UnionScalar*>(scalar_ptr.get());
          switch (info_type) {
            // String properties
            case SqlInfoOptions::FLIGHT_SQL_SERVER_NAME: {
              std::string server_name(
                  reinterpret_cast<arrow::StringScalar*>(scalar->child_value().get())
                      ->view());

              // TODO: Consider creating different properties in GetSqlInfo.
              // TODO: Investigate if SQL_SERVER_NAME should just be the host
              // address as well. In JDBC, FLIGHT_SQL_SERVER_NAME is only used for
              // the DatabaseProductName.
              info_[SQL_SERVER_NAME] = server_name;
              info_[SQL_DBMS_NAME] = server_name;
              info_[SQL_DATABASE_NAME] =
                  server_name;  // This is usually the current catalog. May need to
                                // throw HYC00 instead.
              break;
            }
            case SqlInfoOptions::FLIGHT_SQL_SERVER_VERSION: {
              info_[SQL_DBMS_VER] = ConvertToDBMSVer(std::string(
                  reinterpret_cast<arrow::StringScalar*>(scalar->child_value().get())
                      ->view()));
              break;
            }
            case SqlInfoOptions::FLIGHT_SQL_SERVER_ARROW_VERSION: {
              // Unused.
              break;
            }
            case SqlInfoOptions::SQL_SEARCH_STRING_ESCAPE: {
              info_[SQL_SEARCH_PATTERN_ESCAPE] = std::string(
                  reinterpret_cast<arrow::StringScalar*>(scalar->child_value().get())
                      ->view());
              break;
            }
            case ARROW_SQL_IDENTIFIER_QUOTE_CHAR: {
              info_[SQL_IDENTIFIER_QUOTE_CHAR] = std::string(
                  reinterpret_cast<arrow::StringScalar*>(scalar->child_value().get())
                      ->view());
              break;
            }
            case SqlInfoOptions::SQL_EXTRA_NAME_CHARACTERS: {
              info_[SQL_SPECIAL_CHARACTERS] = std::string(
                  reinterpret_cast<arrow::StringScalar*>(scalar->child_value().get())
                      ->view());
              break;
            }
            case ARROW_SQL_SCHEMA_TERM: {
              info_[SQL_SCHEMA_TERM] = std::string(
                  reinterpret_cast<arrow::StringScalar*>(scalar->child_value().get())
                      ->view());
              break;
            }
            case ARROW_SQL_PROCEDURE_TERM: {
              info_[SQL_PROCEDURE_TERM] = std::string(
                  reinterpret_cast<arrow::StringScalar*>(scalar->child_value().get())
                      ->view());
              break;
            }
            case ARROW_SQL_CATALOG_TERM: {
              std::string catalog_term(std::string(
                  reinterpret_cast<arrow::StringScalar*>(scalar->child_value().get())
                      ->view()));
              if (catalog_term.empty()) {
                info_[SQL_CATALOG_NAME] = "N";
                info_[SQL_CATALOG_NAME_SEPARATOR] = "";
                info_[SQL_CATALOG_LOCATION] = static_cast<uint16_t>(0);
              } else {
                info_[SQL_CATALOG_NAME] = "Y";
                info_[SQL_CATALOG_NAME_SEPARATOR] = ".";
                info_[SQL_CATALOG_LOCATION] = static_cast<uint16_t>(SQL_CL_START);
              }
              info_[SQL_CATALOG_TERM] = std::string(
                  reinterpret_cast<arrow::StringScalar*>(scalar->child_value().get())
                      ->view());

              break;
            }

            // Bool properties
            case SqlInfoOptions::FLIGHT_SQL_SERVER_READ_ONLY: {
              info_[SQL_DATA_SOURCE_READ_ONLY] = ScalarToBoolString(scalar);

              // Assume all forms of insert are supported, however this should
              // come from a property.
              info_[SQL_INSERT_STATEMENT] = static_cast<uint32_t>(
                  SQL_IS_INSERT_LITERALS | SQL_IS_INSERT_SEARCHED | SQL_IS_SELECT_INTO);
              break;
            }
            case SqlInfoOptions::SQL_DDL_CATALOG:
              // Unused by ODBC.
              break;
            case SqlInfoOptions::SQL_DDL_SCHEMA: {
              bool supports_schema_ddl =
                  reinterpret_cast<arrow::BooleanScalar*>(scalar->child_value().get())
                      ->value;
              // Note: this is a bitmask and we can't describe cascade or restrict
              // flags.
              info_[SQL_DROP_SCHEMA] = static_cast<uint32_t>(SQL_DS_DROP_SCHEMA);

              // Note: this is a bitmask and we can't describe authorization or
              // collation
              info_[SQL_CREATE_SCHEMA] = static_cast<uint32_t>(SQL_CS_CREATE_SCHEMA);
              break;
            }
            case SqlInfoOptions::SQL_DDL_TABLE: {
              bool supports_table_ddl =
                  reinterpret_cast<arrow::BooleanScalar*>(scalar->child_value().get())
                      ->value;
              // This is a bitmask and we cannot describe all clauses.
              info_[SQL_CREATE_TABLE] = static_cast<uint32_t>(SQL_CT_CREATE_TABLE);
              info_[SQL_DROP_TABLE] = static_cast<uint32_t>(SQL_DT_DROP_TABLE);
              break;
            }
            case SqlInfoOptions::SQL_ALL_TABLES_ARE_SELECTABLE: {
              info_[SQL_ACCESSIBLE_TABLES] = ScalarToBoolString(scalar);
              break;
            }
            case SqlInfoOptions::SQL_SUPPORTS_COLUMN_ALIASING: {
              info_[SQL_COLUMN_ALIAS] = ScalarToBoolString(scalar);
              break;
            }
            case SqlInfoOptions::SQL_NULL_PLUS_NULL_IS_NULL: {
              info_[SQL_CONCAT_NULL_BEHAVIOR] = static_cast<uint16_t>(
                  reinterpret_cast<arrow::BooleanScalar*>(scalar->child_value().get())
                          ->value
                      ? SQL_CB_NULL
                      : SQL_CB_NON_NULL);
              break;
            }
            case SqlInfoOptions::SQL_SUPPORTS_TABLE_CORRELATION_NAMES: {
              // Simply cache SQL_SUPPORTS_TABLE_CORRELATION_NAMES and
              // SQL_SUPPORTS_DIFFERENT_TABLE_CORRELATION_NAMES since we need both
              // properties to determine the value for SQL_CORRELATION_NAME.
              supports_correlation_name =
                  reinterpret_cast<arrow::BooleanScalar*>(scalar->child_value().get())
                      ->value;
              break;
            }
            case SqlInfoOptions::SQL_SUPPORTS_DIFFERENT_TABLE_CORRELATION_NAMES: {
              // Simply cache SQL_SUPPORTS_TABLE_CORRELATION_NAMES and
              // SQL_SUPPORTS_DIFFERENT_TABLE_CORRELATION_NAMES since we need both
              // properties to determine the value for SQL_CORRELATION_NAME.
              requires_different_correlation_name =
                  reinterpret_cast<arrow::BooleanScalar*>(scalar->child_value().get())
                      ->value;
              break;
            }
            case SqlInfoOptions::SQL_SUPPORTS_EXPRESSIONS_IN_ORDER_BY: {
              info_[SQL_EXPRESSIONS_IN_ORDERBY] = ScalarToBoolString(scalar);
              break;
            }
            case SqlInfoOptions::SQL_SUPPORTS_ORDER_BY_UNRELATED: {
              // Note: this is the negation of the Flight SQL property.
              info_[SQL_ORDER_BY_COLUMNS_IN_SELECT] =
                  reinterpret_cast<arrow::BooleanScalar*>(scalar->child_value().get())
                          ->value
                      ? "N"
                      : "Y";
              break;
            }
            case SqlInfoOptions::SQL_SUPPORTS_LIKE_ESCAPE_CLAUSE: {
              info_[SQL_LIKE_ESCAPE_CLAUSE] = ScalarToBoolString(scalar);
              break;
            }
            case SqlInfoOptions::SQL_SUPPORTS_NON_NULLABLE_COLUMNS: {
              info_[SQL_NON_NULLABLE_COLUMNS] = static_cast<uint16_t>(
                  reinterpret_cast<arrow::BooleanScalar*>(scalar->child_value().get())
                          ->value
                      ? SQL_NNC_NON_NULL
                      : SQL_NNC_NULL);
              break;
            }
            case SqlInfoOptions::SQL_SUPPORTS_INTEGRITY_ENHANCEMENT_FACILITY: {
              info_[SQL_INTEGRITY] = ScalarToBoolString(scalar);
              break;
            }
            case SqlInfoOptions::SQL_CATALOG_AT_START: {
              info_[SQL_CATALOG_LOCATION] = static_cast<uint16_t>(
                  reinterpret_cast<arrow::BooleanScalar*>(scalar->child_value().get())
                          ->value
                      ? SQL_CL_START
                      : SQL_CL_END);
              break;
            }
            case SqlInfoOptions::SQL_SELECT_FOR_UPDATE_SUPPORTED:
              // Not used.
              break;
            case SqlInfoOptions::SQL_STORED_PROCEDURES_SUPPORTED: {
              info_[SQL_PROCEDURES] = ScalarToBoolString(scalar);
              break;
            }
            case SqlInfoOptions::SQL_MAX_ROW_SIZE_INCLUDES_BLOBS: {
              info_[SQL_MAX_ROW_SIZE_INCLUDES_LONG] = ScalarToBoolString(scalar);
              break;
            }
            case SqlInfoOptions::SQL_TRANSACTIONS_SUPPORTED: {
              transactions_supported =
                  reinterpret_cast<arrow::BooleanScalar*>(scalar->child_value().get())
                      ->value;
              break;
            }
            case SqlInfoOptions::SQL_DATA_DEFINITION_CAUSES_TRANSACTION_COMMIT: {
              transaction_ddl_commit =
                  reinterpret_cast<arrow::BooleanScalar*>(scalar->child_value().get())
                      ->value;
              break;
            }
            case SqlInfoOptions::SQL_DATA_DEFINITIONS_IN_TRANSACTIONS_IGNORED: {
              transaction_ddl_ignore =
                  reinterpret_cast<arrow::BooleanScalar*>(scalar->child_value().get())
                      ->value;
              break;
            }
            case SqlInfoOptions::SQL_BATCH_UPDATES_SUPPORTED: {
              info_[SQL_BATCH_SUPPORT] = static_cast<uint32_t>(
                  reinterpret_cast<arrow::BooleanScalar*>(scalar->child_value().get())
                          ->value
                      ? SQL_BS_ROW_COUNT_EXPLICIT
                      : 0);
              break;
            }
            case SqlInfoOptions::SQL_SAVEPOINTS_SUPPORTED:
              // Not used.
              break;
            case SqlInfoOptions::SQL_NAMED_PARAMETERS_SUPPORTED:
              // Not used.
              break;
            case SqlInfoOptions::SQL_LOCATORS_UPDATE_COPY:
              // Not used.
              break;
            case SqlInfoOptions::SQL_STORED_FUNCTIONS_USING_CALL_SYNTAX_SUPPORTED:
              // Not used.
              break;
            case SqlInfoOptions::SQL_CORRELATED_SUBQUERIES_SUPPORTED:
              // Not used. This is implied by SQL_SUPPORTED_SUBQUERIES.
              break;

            // Int64 properties
            case ARROW_SQL_IDENTIFIER_CASE: {
              // Missing from C++ enum. constant from Java.
              constexpr int64_t LOWER = 3;
              uint16_t value = 0;
              int64_t sensitivity = ScalarToInt64(scalar);
              switch (sensitivity) {
                case SqlInfoOptions::SQL_CASE_SENSITIVITY_UNKNOWN:
                  value = SQL_IC_SENSITIVE;
                  break;
                case SqlInfoOptions::SQL_CASE_SENSITIVITY_CASE_INSENSITIVE:
                  value = SQL_IC_MIXED;
                  break;
                case SqlInfoOptions::SQL_CASE_SENSITIVITY_UPPERCASE:
                  value = SQL_IC_UPPER;
                  break;
                case LOWER:
                  value = SQL_IC_LOWER;
                  break;
                default:
                  value = SQL_IC_SENSITIVE;
                  break;
              }
              info_[SQL_IDENTIFIER_CASE] = value;
              break;
            }
            case SqlInfoOptions::SQL_NULL_ORDERING: {
              uint16_t value = 0;
              int64_t scalar_value = ScalarToInt64(scalar);
              switch (scalar_value) {
                case SqlInfoOptions::SQL_NULLS_SORTED_AT_START:
                  value = SQL_NC_START;
                  break;
                case SqlInfoOptions::SQL_NULLS_SORTED_AT_END:
                  value = SQL_NC_END;
                  break;
                case SqlInfoOptions::SQL_NULLS_SORTED_HIGH:
                  value = SQL_NC_HIGH;
                  break;
                case SqlInfoOptions::SQL_NULLS_SORTED_LOW:
                default:
                  value = SQL_NC_LOW;
                  break;
              }
              info_[SQL_NULL_COLLATION] = value;
              break;
            }
            case ARROW_SQL_QUOTED_IDENTIFIER_CASE: {
              // Missing from C++ enum. constant from Java.
              constexpr int64_t LOWER = 3;
              uint16_t value = 0;
              int64_t sensitivity = ScalarToInt64(scalar);
              switch (sensitivity) {
                case SqlInfoOptions::SQL_CASE_SENSITIVITY_UNKNOWN:
                  value = SQL_IC_SENSITIVE;
                  break;
                case SqlInfoOptions::SQL_CASE_SENSITIVITY_CASE_INSENSITIVE:
                  value = SQL_IC_MIXED;
                  break;
                case SqlInfoOptions::SQL_CASE_SENSITIVITY_UPPERCASE:
                  value = SQL_IC_UPPER;
                  break;
                case LOWER:
                  value = SQL_IC_LOWER;
                  break;
                default:
                  value = SQL_IC_SENSITIVE;
                  break;
              }
              info_[SQL_QUOTED_IDENTIFIER_CASE] = value;
              break;
            }
            case SqlInfoOptions::SQL_MAX_BINARY_LITERAL_LENGTH: {
              info_[SQL_MAX_BINARY_LITERAL_LEN] =
                  static_cast<uint32_t>(ScalarToInt64(scalar));
              break;
            }
            case SqlInfoOptions::SQL_MAX_CHAR_LITERAL_LENGTH: {
              info_[SQL_MAX_CHAR_LITERAL_LEN] =
                  static_cast<uint32_t>(ScalarToInt64(scalar));
              break;
            }
            case SqlInfoOptions::SQL_MAX_COLUMN_NAME_LENGTH: {
              info_[SQL_MAX_COLUMN_NAME_LEN] =
                  static_cast<uint16_t>(ScalarToInt64(scalar));
              break;
            }
            case ARROW_SQL_MAX_COLUMNS_IN_GROUP_BY: {
              info_[SQL_MAX_COLUMNS_IN_GROUP_BY] =
                  static_cast<uint16_t>(ScalarToInt64(scalar));
              break;
            }
            case ARROW_SQL_MAX_COLUMNS_IN_INDEX: {
              info_[SQL_MAX_COLUMNS_IN_INDEX] =
                  static_cast<uint16_t>(ScalarToInt64(scalar));
              break;
            }
            case ARROW_SQL_MAX_COLUMNS_IN_ORDER_BY: {
              info_[SQL_MAX_COLUMNS_IN_ORDER_BY] =
                  static_cast<uint16_t>(ScalarToInt64(scalar));
              break;
            }
            case ARROW_SQL_MAX_COLUMNS_IN_SELECT: {
              info_[SQL_MAX_COLUMNS_IN_SELECT] =
                  static_cast<uint16_t>(ScalarToInt64(scalar));
              break;
            }
            case ARROW_SQL_MAX_COLUMNS_IN_TABLE: {
              info_[SQL_MAX_COLUMNS_IN_TABLE] =
                  static_cast<uint16_t>(ScalarToInt64(scalar));
              break;
            }
            case SqlInfoOptions::SQL_MAX_CONNECTIONS: {
              info_[SQL_MAX_DRIVER_CONNECTIONS] =
                  static_cast<uint16_t>(ScalarToInt64(scalar));
              break;
            }
            case SqlInfoOptions::SQL_MAX_CURSOR_NAME_LENGTH: {
              info_[SQL_MAX_CURSOR_NAME_LEN] =
                  static_cast<uint16_t>(ScalarToInt64(scalar));
              break;
            }
            case SqlInfoOptions::SQL_MAX_INDEX_LENGTH: {
              info_[SQL_MAX_INDEX_SIZE] = static_cast<uint32_t>(ScalarToInt64(scalar));
              break;
            }
            case SqlInfoOptions::SQL_SCHEMA_NAME_LENGTH: {
              info_[SQL_MAX_SCHEMA_NAME_LEN] =
                  static_cast<uint16_t>(ScalarToInt64(scalar));
              break;
            }
            case SqlInfoOptions::SQL_MAX_PROCEDURE_NAME_LENGTH: {
              info_[SQL_MAX_PROCEDURE_NAME_LEN] =
                  static_cast<uint16_t>(ScalarToInt64(scalar));
              break;
            }
            case SqlInfoOptions::SQL_MAX_CATALOG_NAME_LENGTH: {
              info_[SQL_MAX_CATALOG_NAME_LEN] =
                  static_cast<uint16_t>(ScalarToInt64(scalar));
              break;
            }
            case ARROW_SQL_MAX_ROW_SIZE: {
              info_[SQL_MAX_ROW_SIZE] = static_cast<uint32_t>(ScalarToInt64(scalar));
              break;
            }
            case SqlInfoOptions::SQL_MAX_STATEMENT_LENGTH: {
              info_[SQL_MAX_STATEMENT_LEN] = static_cast<uint32_t>(ScalarToInt64(scalar));
              break;
            }
            case SqlInfoOptions::SQL_MAX_STATEMENTS: {
              info_[SQL_MAX_CONCURRENT_ACTIVITIES] =
                  static_cast<uint16_t>(ScalarToInt64(scalar));
              break;
            }
            case SqlInfoOptions::SQL_MAX_TABLE_NAME_LENGTH: {
              info_[SQL_MAX_TABLE_NAME_LEN] =
                  static_cast<uint16_t>(ScalarToInt64(scalar));
              break;
            }
            case ARROW_SQL_MAX_TABLES_IN_SELECT: {
              info_[SQL_MAX_TABLES_IN_SELECT] =
                  static_cast<uint16_t>(ScalarToInt64(scalar));
              break;
            }
            case SqlInfoOptions::SQL_MAX_USERNAME_LENGTH: {
              info_[SQL_MAX_USER_NAME_LEN] = static_cast<uint16_t>(ScalarToInt64(scalar));
              break;
            }
            case SqlInfoOptions::SQL_DEFAULT_TRANSACTION_ISOLATION: {
              constexpr int32_t NONE = 0;
              constexpr int32_t READ_UNCOMMITTED = 1;
              constexpr int32_t READ_COMMITTED = 2;
              constexpr int32_t REPEATABLE_READ = 3;
              constexpr int32_t SERIALIZABLE = 4;
              int64_t scalar_value = static_cast<uint64_t>(ScalarToInt64(scalar));
              uint32_t result_val = 0;
              if ((scalar_value & (1 << READ_UNCOMMITTED)) != 0) {
                result_val = SQL_TXN_READ_UNCOMMITTED;
              } else if ((scalar_value & (1 << READ_COMMITTED)) != 0) {
                result_val = SQL_TXN_READ_COMMITTED;
              } else if ((scalar_value & (1 << REPEATABLE_READ)) != 0) {
                result_val = SQL_TXN_REPEATABLE_READ;
              } else if ((scalar_value & (1 << SERIALIZABLE)) != 0) {
                result_val = SQL_TXN_SERIALIZABLE;
              }
              info_[SQL_DEFAULT_TXN_ISOLATION] = result_val;
              break;
            }

            // Int32 properties
            case SqlInfoOptions::SQL_SUPPORTED_GROUP_BY: {
              // Note: SqlGroupBy enum is missing in C++. Using Java values.
              constexpr int32_t UNRELATED = 0;
              constexpr int32_t BEYOND_SELECT = 1;
              int32_t scalar_value = static_cast<int32_t>(ScalarToInt32(scalar));
              uint16_t result_val = SQL_GB_NOT_SUPPORTED;
              if ((scalar_value & (1 << UNRELATED)) != 0) {
                result_val = SQL_GB_NO_RELATION;
              } else if ((scalar_value & (1 << BEYOND_SELECT)) != 0) {
                result_val = SQL_GB_GROUP_BY_CONTAINS_SELECT;
              }
              // Note GROUP_BY_EQUALS_SELECT and COLLATE cannot be described.
              info_[SQL_GROUP_BY] = result_val;
              break;
            }
            case SqlInfoOptions::SQL_SUPPORTED_GRAMMAR: {
              // Note: SupportedSqlGrammar enum is missing in C++. Using Java
              // values.
              constexpr int32_t MINIMUM = 0;
              constexpr int32_t CORE = 1;
              constexpr int32_t EXTENDED = 2;
              int32_t scalar_value = static_cast<int32_t>(ScalarToInt32(scalar));
              uint32_t result_val = SQL_OIC_CORE;
              if ((scalar_value & (1 << MINIMUM)) != 0) {
                result_val = SQL_OIC_CORE;
              } else if ((scalar_value & (1 << CORE)) != 0) {
                result_val = SQL_OIC_LEVEL1;
              } else if ((scalar_value & (1 << EXTENDED)) != 0) {
                result_val = SQL_OIC_LEVEL2;
              }
              info_[SQL_ODBC_API_CONFORMANCE] = result_val;
              break;
            }
            case SqlInfoOptions::SQL_ANSI92_SUPPORTED_LEVEL: {
              // Note: SupportedAnsi92SqlGrammarLevel enum is missing in C++.
              // Using Java values.
              constexpr int32_t ENTRY = 0;
              constexpr int32_t INTERMEDIATE = 1;
              constexpr int32_t FULL = 2;
              int32_t scalar_value = static_cast<int32_t>(ScalarToInt32(scalar));
              uint32_t result_val = SQL_SC_SQL92_ENTRY;
              uint16_t odbc_sql_conformance = SQL_OSC_MINIMUM;
              if ((scalar_value & (1 << ENTRY)) != 0) {
                result_val = SQL_SC_SQL92_ENTRY;
              } else if ((scalar_value & (1 << INTERMEDIATE)) != 0) {
                result_val = SQL_SC_SQL92_INTERMEDIATE;
                odbc_sql_conformance = SQL_OSC_CORE;
              } else if ((scalar_value & (1 << FULL)) != 0) {
                result_val = SQL_SC_SQL92_FULL;
                odbc_sql_conformance = SQL_OSC_EXTENDED;
              }
              info_[SQL_SQL_CONFORMANCE] = result_val;
              info_[SQL_ODBC_SQL_CONFORMANCE] = odbc_sql_conformance;
              break;
            }
            case SqlInfoOptions::SQL_OUTER_JOINS_SUPPORT_LEVEL: {
              int32_t scalar_value = static_cast<int32_t>(ScalarToInt32(scalar));

              // If limited outer joins is supported, we can't tell which joins
              // are supported so just report none. If full outer joins is
              // supported, nested joins are supported and full outer joins are
              // supported, so all joins + nested are supported.
              constexpr int32_t UNSUPPORTED = 0;
              constexpr int32_t LIMITED = 1;
              constexpr int32_t FULL = 2;
              uint32_t result_val = 0;
              // Assume inner and cross joins are supported. Flight SQL can't
              // report this currently.
              uint32_t relational_operators = SQL_SRJO_CROSS_JOIN | SQL_SRJO_INNER_JOIN;
              if ((scalar_value & (1 << FULL)) != 0) {
                result_val = SQL_OJ_LEFT | SQL_OJ_RIGHT | SQL_OJ_FULL | SQL_OJ_NESTED;
                relational_operators |= SQL_SRJO_FULL_OUTER_JOIN |
                                        SQL_SRJO_LEFT_OUTER_JOIN |
                                        SQL_SRJO_RIGHT_OUTER_JOIN;
              } else if ((scalar_value & (1 << LIMITED)) != 0) {
                result_val = SQL_SC_SQL92_INTERMEDIATE;
              } else if ((scalar_value & (1 << UNSUPPORTED)) != 0) {
                result_val = 0;
              }
              info_[SQL_OJ_CAPABILITIES] = result_val;
              info_[SQL_OUTER_JOINS] = result_val != 0 ? "Y" : "N";
              info_[SQL_SQL92_RELATIONAL_JOIN_OPERATORS] = relational_operators;
              break;
            }
            case SqlInfoOptions::SQL_SCHEMAS_SUPPORTED_ACTIONS: {
              int32_t scalar_value = static_cast<int32_t>(ScalarToInt32(scalar));

              // Missing SqlSupportedElementActions enum in C++. Values taken from
              // java.
              constexpr int32_t PROCEDURE = 0;
              constexpr int32_t INDEX = 1;
              constexpr int32_t PRIVILEGE = 2;
              // Assume schemas are supported in DML and Table manipulation.
              uint32_t result_val = SQL_SU_DML_STATEMENTS | SQL_SU_TABLE_DEFINITION;
              if ((scalar_value & (1 << PROCEDURE)) != 0) {
                result_val |= SQL_SU_PROCEDURE_INVOCATION;
              }
              if ((scalar_value & (1 << INDEX)) != 0) {
                result_val |= SQL_SU_INDEX_DEFINITION;
              }
              if ((scalar_value & (1 << PRIVILEGE)) != 0) {
                result_val |= SQL_SU_PRIVILEGE_DEFINITION;
              }
              info_[SQL_SCHEMA_USAGE] = result_val;
              break;
            }
            case SqlInfoOptions::SQL_CATALOGS_SUPPORTED_ACTIONS: {
              int32_t scalar_value = static_cast<int32_t>(ScalarToInt32(scalar));

              // Missing SqlSupportedElementActions enum in C++. Values taken from
              // java.
              constexpr int32_t PROCEDURE = 0;
              constexpr int32_t INDEX = 1;
              constexpr int32_t PRIVILEGE = 2;
              // Assume catalogs are supported in DML and Table manipulation.
              uint32_t result_val = SQL_CU_DML_STATEMENTS | SQL_CU_TABLE_DEFINITION;
              if ((scalar_value & (1 << PROCEDURE)) != 0) {
                result_val |= SQL_CU_PROCEDURE_INVOCATION;
              }
              if ((scalar_value & (1 << INDEX)) != 0) {
                result_val |= SQL_CU_INDEX_DEFINITION;
              }
              if ((scalar_value & (1 << PRIVILEGE)) != 0) {
                result_val |= SQL_CU_PRIVILEGE_DEFINITION;
              }
              info_[SQL_CATALOG_USAGE] = result_val;
              break;
            }
            case SqlInfoOptions::SQL_SUPPORTED_POSITIONED_COMMANDS: {
              // Ignore, positioned updates/deletes unsupported.
              break;
            }
            case SqlInfoOptions::SQL_SUPPORTED_SUBQUERIES: {
              int32_t scalar_value = static_cast<int32_t>(ScalarToInt32(scalar));

              // Missing SqlSupportedElementActions enum in C++. Values taken from
              // java.
              constexpr int32_t COMPARISONS = 0;
              constexpr int32_t EXISTS = 1;
              constexpr int32_t INN = 2;
              constexpr int32_t QUANTIFIEDS = 3;
              uint32_t result_val = 0;
              if ((scalar_value & (1 << COMPARISONS)) != 0) {
                result_val |= SQL_SQ_COMPARISON;
              }
              if ((scalar_value & (1 << EXISTS)) != 0) {
                result_val |= SQL_SQ_EXISTS;
              }
              if ((scalar_value & (1 << INN)) != 0) {
                result_val |= SQL_SQ_IN;
              }
              if ((scalar_value & (1 << QUANTIFIEDS)) != 0) {
                result_val |= SQL_SQ_QUANTIFIED;
              }
              info_[SQL_SUBQUERIES] = result_val;
              break;
            }
            case SqlInfoOptions::SQL_SUPPORTED_UNIONS: {
              int32_t scalar_value = static_cast<int32_t>(ScalarToInt32(scalar));

              // Missing enum in C++. Values taken from java.
              constexpr int32_t UNION = 0;
              constexpr int32_t UNION_ALL = 1;
              uint32_t result_val = 0;
              if ((scalar_value & (1 << UNION)) != 0) {
                result_val |= SQL_U_UNION;
              }
              if ((scalar_value & (1 << UNION_ALL)) != 0) {
                result_val |= SQL_U_UNION_ALL;
              }
              info_[SQL_UNION] = result_val;
              break;
            }
            case SqlInfoOptions::SQL_SUPPORTED_TRANSACTIONS_ISOLATION_LEVELS: {
              int32_t scalar_value = static_cast<int32_t>(ScalarToInt32(scalar));

              // Missing enum in C++. Values taken from java.
              constexpr int32_t NONE = 0;
              constexpr int32_t READ_UNCOMMITTED = 1;
              constexpr int32_t READ_COMMITTED = 2;
              constexpr int32_t REPEATABLE_READ = 3;
              constexpr int32_t SERIALIZABLE = 4;
              uint32_t result_val = 0;
              if ((scalar_value & (1 << NONE)) != 0) {
                result_val = 0;
              }
              if ((scalar_value & (1 << READ_UNCOMMITTED)) != 0) {
                result_val |= SQL_TXN_READ_UNCOMMITTED;
              }
              if ((scalar_value & (1 << READ_COMMITTED)) != 0) {
                result_val |= SQL_TXN_READ_COMMITTED;
              }
              if ((scalar_value & (1 << REPEATABLE_READ)) != 0) {
                result_val |= SQL_TXN_REPEATABLE_READ;
              }
              if ((scalar_value & (1 << SERIALIZABLE)) != 0) {
                result_val |= SQL_TXN_SERIALIZABLE;
              }
              info_[SQL_TXN_ISOLATION_OPTION] = result_val;
              break;
            }
            case SqlInfoOptions::SQL_SUPPORTED_RESULT_SET_TYPES:
              // Ignored. Warpdrive supports forward-only only.
              break;
            case SqlInfoOptions::SQL_SUPPORTED_CONCURRENCIES_FOR_RESULT_SET_UNSPECIFIED:
              // Ignored. Warpdrive supports forward-only only.
              break;
            case SqlInfoOptions::SQL_SUPPORTED_CONCURRENCIES_FOR_RESULT_SET_FORWARD_ONLY:
              // Ignored. Warpdrive supports forward-only only.
              break;
            case SqlInfoOptions::
                SQL_SUPPORTED_CONCURRENCIES_FOR_RESULT_SET_SCROLL_SENSITIVE:
              // Ignored. Warpdrive supports forward-only only.
              break;
            case SqlInfoOptions::
                SQL_SUPPORTED_CONCURRENCIES_FOR_RESULT_SET_SCROLL_INSENSITIVE:
              // Ignored. Warpdrive supports forward-only only.
              break;

            // List<string> properties
            case ARROW_SQL_NUMERIC_FUNCTIONS: {
              std::shared_ptr<arrow::Array> list_value =
                  reinterpret_cast<arrow::BaseListScalar*>(scalar->child_value().get())
                      ->value;
              uint32_t result_val = 0;
              for (int64_t list_index = 0; list_index < list_value->length();
                   ++list_index) {
                if (!list_value->IsNull(list_index)) {
                  ReportNumericFunction(
                      reinterpret_cast<arrow::StringArray*>(list_value.get())
                          ->GetString(list_index),
                      result_val);
                }
              }
              info_[SQL_NUMERIC_FUNCTIONS] = result_val;
              break;
            }

            case ARROW_SQL_STRING_FUNCTIONS: {
              std::shared_ptr<arrow::Array> list_value =
                  reinterpret_cast<arrow::BaseListScalar*>(scalar->child_value().get())
                      ->value;
              uint32_t result_val = 0;
              for (int64_t list_index = 0; list_index < list_value->length();
                   ++list_index) {
                if (!list_value->IsNull(list_index)) {
                  ReportStringFunction(
                      reinterpret_cast<arrow::StringArray*>(list_value.get())
                          ->GetString(list_index),
                      result_val);
                }
              }
              info_[SQL_STRING_FUNCTIONS] = result_val;
              break;
            }
            case ARROW_SQL_SYSTEM_FUNCTIONS: {
              std::shared_ptr<arrow::Array> list_value =
                  reinterpret_cast<arrow::BaseListScalar*>(scalar->child_value().get())
                      ->value;
              uint32_t sys_result = 0;
              uint32_t convert_result = 0;
              for (int64_t list_index = 0; list_index < list_value->length();
                   ++list_index) {
                if (!list_value->IsNull(list_index)) {
                  ReportSystemFunction(
                      reinterpret_cast<arrow::StringArray*>(list_value.get())
                          ->GetString(list_index),
                      sys_result, convert_result);
                }
              }
              info_[SQL_CONVERT_FUNCTIONS] = convert_result;
              info_[SQL_SYSTEM_FUNCTIONS] = sys_result;
              break;
            }
            case SqlInfoOptions::SQL_DATETIME_FUNCTIONS: {
              std::shared_ptr<arrow::Array> list_value =
                  reinterpret_cast<arrow::BaseListScalar*>(scalar->child_value().get())
                      ->value;
              uint32_t result_val = 0;
              for (int64_t list_index = 0; list_index < list_value->length();
                   ++list_index) {
                if (!list_value->IsNull(list_index)) {
                  ReportDatetimeFunction(
                      reinterpret_cast<arrow::StringArray*>(list_value.get())
                          ->GetString(list_index),
                      result_val);
                }
              }
              info_[SQL_TIMEDATE_FUNCTIONS] = result_val;
              break;
            }

            case ARROW_SQL_KEYWORDS: {
              std::shared_ptr<arrow::Array> list_value =
                  reinterpret_cast<arrow::BaseListScalar*>(scalar->child_value().get())
                      ->value;
              std::string result_str;
              for (int64_t list_index = 0; list_index < list_value->length();
                   ++list_index) {
                if (!list_value->IsNull(list_index)) {
                  if (list_index != 0) {
                    result_str += ", ";
                  }

                  result_str += reinterpret_cast<arrow::StringArray*>(list_value.get())
                                    ->GetString(list_index);
                }
              }
              info_[SQL_KEYWORDS] = std::move(result_str);
              break;
            }

            // Map<int32, list<int32> properties
            case SqlInfoOptions::SQL_SUPPORTS_CONVERT: {
              arrow::MapScalar* map_scalar =
                  reinterpret_cast<arrow::MapScalar*>(scalar->child_value().get());
              auto data_array = map_scalar->value;
              arrow::StructArray* map_contents =
                  reinterpret_cast<arrow::StructArray*>(data_array.get());
              auto map_keys = map_contents->field(0);
              auto map_values = map_contents->field(1);
              for (int64_t map_index = 0; map_index < map_contents->length();
                   ++map_index) {
                if (!map_values->IsNull(map_index)) {
                  auto map_key_scalar_ptr = map_keys->GetScalar(map_index).ValueOrDie();
                  auto map_value_scalar_ptr =
                      map_values->GetScalar(map_index).ValueOrDie();
                  int32_t map_key_scalar =
                      reinterpret_cast<arrow::Int32Scalar*>(map_key_scalar_ptr.get())
                          ->value;
                  auto map_value_scalar =
                      reinterpret_cast<arrow::BaseListScalar*>(map_value_scalar_ptr.get())
                          ->value;

                  int32_t get_info_type = GetInfoTypeForArrowConvertEntry(map_key_scalar);
                  if (get_info_type < 0) {
                    continue;
                  }
                  uint32_t info_bitmask_value_to_write = 0;
                  for (int64_t map_value_array_index = 0;
                       map_value_array_index < map_value_scalar->length();
                       ++map_value_array_index) {
                    if (!map_value_scalar->IsNull(map_value_array_index)) {
                      auto list_entry_scalar =
                          map_value_scalar->GetScalar(map_value_array_index).ValueOrDie();
                      info_bitmask_value_to_write |= GetCvtBitForArrowConvertEntry(
                          reinterpret_cast<arrow::Int32Scalar*>(list_entry_scalar.get())
                              ->value);
                    }
                  }
                  info_[get_info_type] = info_bitmask_value_to_write;
                }
              }
              break;
            }

            default:
              // Ignore unrecognized.
              break;
          }
        }
      }

      if (transactions_supported) {
        if (transaction_ddl_commit) {
          info_[SQL_TXN_CAPABLE] = static_cast<uint16_t>(SQL_TC_DDL_COMMIT);
        } else if (transaction_ddl_ignore) {
          info_[SQL_TXN_CAPABLE] = static_cast<uint16_t>(SQL_TC_DDL_IGNORE);
        } else {
          // Ambiguous if this means transactions on DDL is supported or not.
          // Assume not
          info_[SQL_TXN_CAPABLE] = static_cast<uint16_t>(SQL_TC_DML);
        }
      } else {
        info_[SQL_TXN_CAPABLE] = static_cast<uint16_t>(SQL_TC_NONE);
      }

      if (supports_correlation_name) {
        if (requires_different_correlation_name) {
          info_[SQL_CORRELATION_NAME] = static_cast<uint16_t>(SQL_CN_DIFFERENT);
        } else {
          info_[SQL_CORRELATION_NAME] = static_cast<uint16_t>(SQL_CN_ANY);
        }
      } else {
        info_[SQL_CORRELATION_NAME] = static_cast<uint16_t>(SQL_CN_NONE);
      }
    }
    LoadDefaultsForMissingEntries();
    return true;
  }

  return false;
}

void GetInfoCache::LoadDefaultsForMissingEntries() {
  // For safety's sake, this function does not discriminate between driver and hard-coded
  // values.
  SetDefaultIfMissing(info_, SQL_ACCESSIBLE_PROCEDURES, "N");
  SetDefaultIfMissing(info_, SQL_ACCESSIBLE_TABLES, "Y");
  SetDefaultIfMissing(info_, SQL_ACTIVE_ENVIRONMENTS, static_cast<uint16_t>(0));
  SetDefaultIfMissing(
      info_, SQL_AGGREGATE_FUNCTIONS,
      static_cast<uint32_t>(SQL_AF_ALL | SQL_AF_AVG | SQL_AF_COUNT | SQL_AF_DISTINCT |
                            SQL_AF_MAX | SQL_AF_MIN | SQL_AF_SUM));
  SetDefaultIfMissing(info_, SQL_ALTER_DOMAIN, static_cast<uint32_t>(0));
  SetDefaultIfMissing(info_, SQL_ALTER_TABLE, static_cast<uint32_t>(0));
  SetDefaultIfMissing(info_, SQL_ASYNC_MODE, static_cast<uint32_t>(SQL_AM_NONE));
  SetDefaultIfMissing(info_, SQL_BATCH_ROW_COUNT, static_cast<uint32_t>(0));
  SetDefaultIfMissing(info_, SQL_BATCH_SUPPORT, static_cast<uint32_t>(0));
  SetDefaultIfMissing(info_, SQL_BOOKMARK_PERSISTENCE, static_cast<uint32_t>(0));
  SetDefaultIfMissing(info_, SQL_CATALOG_LOCATION, static_cast<uint16_t>(0));
  SetDefaultIfMissing(info_, SQL_CATALOG_NAME, "N");
  SetDefaultIfMissing(info_, SQL_CATALOG_NAME_SEPARATOR, "");
  SetDefaultIfMissing(info_, SQL_CATALOG_TERM, "");
  SetDefaultIfMissing(info_, SQL_CATALOG_USAGE, static_cast<uint32_t>(0));
  SetDefaultIfMissing(info_, SQL_COLLATION_SEQ, "");
  SetDefaultIfMissing(info_, SQL_COLUMN_ALIAS, "Y");
  SetDefaultIfMissing(info_, SQL_CONCAT_NULL_BEHAVIOR,
                      static_cast<uint16_t>(SQL_CB_NULL));
  SetDefaultIfMissing(info_, SQL_CONVERT_BIGINT, static_cast<uint32_t>(0));
  SetDefaultIfMissing(info_, SQL_CONVERT_BINARY, static_cast<uint32_t>(0));
  SetDefaultIfMissing(info_, SQL_CONVERT_BIT, static_cast<uint32_t>(0));
  SetDefaultIfMissing(info_, SQL_CONVERT_CHAR, static_cast<uint32_t>(0));
  SetDefaultIfMissing(info_, SQL_CONVERT_DATE, static_cast<uint32_t>(0));
  SetDefaultIfMissing(info_, SQL_CONVERT_DECIMAL, static_cast<uint32_t>(0));
  SetDefaultIfMissing(info_, SQL_CONVERT_DOUBLE, static_cast<uint32_t>(0));
  SetDefaultIfMissing(info_, SQL_CONVERT_FLOAT, static_cast<uint32_t>(0));
  SetDefaultIfMissing(info_, SQL_CONVERT_GUID, static_cast<uint32_t>(0));
  SetDefaultIfMissing(info_, SQL_CONVERT_INTEGER, static_cast<uint32_t>(0));
  SetDefaultIfMissing(info_, SQL_CONVERT_INTERVAL_YEAR_MONTH, static_cast<uint32_t>(0));
  SetDefaultIfMissing(info_, SQL_CONVERT_INTERVAL_DAY_TIME, static_cast<uint32_t>(0));
  SetDefaultIfMissing(info_, SQL_CONVERT_LONGVARBINARY, static_cast<uint32_t>(0));
  SetDefaultIfMissing(info_, SQL_CONVERT_LONGVARCHAR, static_cast<uint32_t>(0));
  SetDefaultIfMissing(info_, SQL_CONVERT_NUMERIC, static_cast<uint32_t>(0));
  SetDefaultIfMissing(info_, SQL_CONVERT_REAL, static_cast<uint32_t>(0));
  SetDefaultIfMissing(info_, SQL_CONVERT_SMALLINT, static_cast<uint32_t>(0));
  SetDefaultIfMissing(info_, SQL_CONVERT_TIME, static_cast<uint32_t>(0));
  SetDefaultIfMissing(info_, SQL_CONVERT_TIMESTAMP, static_cast<uint32_t>(0));
  SetDefaultIfMissing(info_, SQL_CONVERT_TINYINT, static_cast<uint32_t>(0));
  SetDefaultIfMissing(info_, SQL_CONVERT_VARBINARY, static_cast<uint32_t>(0));
  SetDefaultIfMissing(info_, SQL_CONVERT_VARCHAR, static_cast<uint32_t>(0));
  SetDefaultIfMissing(info_, SQL_CONVERT_WCHAR, static_cast<uint32_t>(0));
  SetDefaultIfMissing(info_, SQL_CONVERT_WVARCHAR, static_cast<uint32_t>(0));
  SetDefaultIfMissing(info_, SQL_CONVERT_WLONGVARCHAR, static_cast<uint32_t>(0));
  SetDefaultIfMissing(info_, SQL_CONVERT_WLONGVARCHAR,
                      static_cast<uint32_t>(SQL_FN_CVT_CAST));
  SetDefaultIfMissing(info_, SQL_CORRELATION_NAME, static_cast<uint32_t>(SQL_CN_NONE));
  SetDefaultIfMissing(info_, SQL_CREATE_ASSERTION, static_cast<uint32_t>(0));
  SetDefaultIfMissing(info_, SQL_CREATE_CHARACTER_SET, static_cast<uint32_t>(0));
  SetDefaultIfMissing(info_, SQL_CREATE_DOMAIN, static_cast<uint32_t>(0));
  SetDefaultIfMissing(info_, SQL_CREATE_SCHEMA, static_cast<uint32_t>(0));
  SetDefaultIfMissing(info_, SQL_CREATE_TABLE, static_cast<uint32_t>(0));
  SetDefaultIfMissing(info_, SQL_CREATE_TRANSLATION, static_cast<uint32_t>(0));
  SetDefaultIfMissing(info_, SQL_CREATE_VIEW, static_cast<uint32_t>(0));
  SetDefaultIfMissing(info_, SQL_CURSOR_COMMIT_BEHAVIOR,
                      static_cast<uint16_t>(SQL_CB_CLOSE));
  SetDefaultIfMissing(info_, SQL_CURSOR_ROLLBACK_BEHAVIOR,
                      static_cast<uint16_t>(SQL_CB_CLOSE));
  SetDefaultIfMissing(info_, SQL_CURSOR_SENSITIVITY,
                      static_cast<uint32_t>(SQL_UNSPECIFIED));
  SetDefaultIfMissing(info_, SQL_DATA_SOURCE_READ_ONLY, "N");
  SetDefaultIfMissing(info_, SQL_DBMS_NAME, "Arrow Flight SQL Server");
  SetDefaultIfMissing(info_, SQL_DBMS_VER, "00.01.0000");
  SetDefaultIfMissing(info_, SQL_DDL_INDEX, static_cast<uint32_t>(0));
  SetDefaultIfMissing(info_, SQL_DEFAULT_TXN_ISOLATION, static_cast<uint32_t>(0));
  SetDefaultIfMissing(info_, SQL_DESCRIBE_PARAMETER, "N");
  SetDefaultIfMissing(info_, SQL_DRIVER_NAME, "Arrow Flight SQL Driver");
  SetDefaultIfMissing(info_, SQL_DRIVER_ODBC_VER, "03.80");
  SetDefaultIfMissing(info_, SQL_DRIVER_VER, "00.09.0000");
  SetDefaultIfMissing(info_, SQL_DROP_ASSERTION, static_cast<uint32_t>(0));
  SetDefaultIfMissing(info_, SQL_DROP_CHARACTER_SET, static_cast<uint32_t>(0));
  SetDefaultIfMissing(info_, SQL_DROP_COLLATION, static_cast<uint32_t>(0));
  SetDefaultIfMissing(info_, SQL_DROP_DOMAIN, static_cast<uint32_t>(0));
  SetDefaultIfMissing(info_, SQL_DROP_SCHEMA, static_cast<uint32_t>(0));
  SetDefaultIfMissing(info_, SQL_DROP_TABLE, static_cast<uint32_t>(0));
  SetDefaultIfMissing(info_, SQL_DROP_TRANSLATION, static_cast<uint32_t>(0));
  SetDefaultIfMissing(info_, SQL_DROP_VIEW, static_cast<uint32_t>(0));
  SetDefaultIfMissing(info_, SQL_EXPRESSIONS_IN_ORDERBY, "N");
  SetDefaultIfMissing(info_, SQL_GETDATA_EXTENSIONS,
                      static_cast<uint32_t>(SQL_GD_ANY_COLUMN | SQL_GD_ANY_ORDER));
  SetDefaultIfMissing(info_, SQL_GROUP_BY,
                      static_cast<uint16_t>(SQL_GB_GROUP_BY_CONTAINS_SELECT));
  SetDefaultIfMissing(info_, SQL_IDENTIFIER_CASE, static_cast<uint16_t>(SQL_IC_MIXED));
  SetDefaultIfMissing(info_, SQL_IDENTIFIER_QUOTE_CHAR, "\"");
  SetDefaultIfMissing(info_, SQL_INDEX_KEYWORDS, static_cast<uint32_t>(SQL_IK_NONE));
  SetDefaultIfMissing(
      info_, SQL_INFO_SCHEMA_VIEWS,
      static_cast<uint32_t>(SQL_ISV_TABLES | SQL_ISV_COLUMNS | SQL_ISV_VIEWS));
  SetDefaultIfMissing(info_, SQL_INSERT_STATEMENT,
                      static_cast<uint32_t>(SQL_IS_INSERT_LITERALS |
                                            SQL_IS_INSERT_SEARCHED | SQL_IS_SELECT_INTO));
  SetDefaultIfMissing(info_, SQL_INTEGRITY, "N");
  SetDefaultIfMissing(info_, SQL_KEYWORDS, "");
  SetDefaultIfMissing(info_, SQL_LIKE_ESCAPE_CLAUSE, "Y");
  SetDefaultIfMissing(info_, SQL_MAX_ASYNC_CONCURRENT_STATEMENTS,
                      static_cast<uint32_t>(0));
  SetDefaultIfMissing(info_, SQL_MAX_BINARY_LITERAL_LEN, static_cast<uint32_t>(0));
  SetDefaultIfMissing(info_, SQL_MAX_CATALOG_NAME_LEN, static_cast<uint16_t>(0));
  SetDefaultIfMissing(info_, SQL_MAX_CHAR_LITERAL_LEN, static_cast<uint32_t>(0));
  SetDefaultIfMissing(info_, SQL_MAX_COLUMN_NAME_LEN, static_cast<uint16_t>(0));
  SetDefaultIfMissing(info_, SQL_MAX_COLUMNS_IN_GROUP_BY, static_cast<uint16_t>(0));
  SetDefaultIfMissing(info_, SQL_MAX_COLUMNS_IN_INDEX, static_cast<uint16_t>(0));
  SetDefaultIfMissing(info_, SQL_MAX_COLUMNS_IN_ORDER_BY, static_cast<uint16_t>(0));
  SetDefaultIfMissing(info_, SQL_MAX_COLUMNS_IN_SELECT, static_cast<uint16_t>(0));
  SetDefaultIfMissing(info_, SQL_MAX_COLUMNS_IN_TABLE, static_cast<uint16_t>(0));
  SetDefaultIfMissing(info_, SQL_MAX_CURSOR_NAME_LEN, static_cast<uint16_t>(0));
  SetDefaultIfMissing(info_, SQL_MAX_DRIVER_CONNECTIONS, static_cast<uint16_t>(0));
  SetDefaultIfMissing(info_, SQL_MAX_IDENTIFIER_LEN, static_cast<uint16_t>(65535));
  SetDefaultIfMissing(info_, SQL_MAX_INDEX_SIZE, static_cast<uint32_t>(0));
  SetDefaultIfMissing(info_, SQL_MAX_PROCEDURE_NAME_LEN, static_cast<uint16_t>(0));
  SetDefaultIfMissing(info_, SQL_MAX_ROW_SIZE, static_cast<uint32_t>(0));
  SetDefaultIfMissing(info_, SQL_MAX_ROW_SIZE_INCLUDES_LONG, "N");
  SetDefaultIfMissing(info_, SQL_MAX_SCHEMA_NAME_LEN, static_cast<uint16_t>(0));
  SetDefaultIfMissing(info_, SQL_MAX_STATEMENT_LEN, static_cast<uint32_t>(0));
  SetDefaultIfMissing(info_, SQL_MAX_TABLE_NAME_LEN, static_cast<uint16_t>(0));
  SetDefaultIfMissing(info_, SQL_MAX_TABLES_IN_SELECT, static_cast<uint16_t>(0));
  SetDefaultIfMissing(info_, SQL_MAX_USER_NAME_LEN, static_cast<uint16_t>(0));
  SetDefaultIfMissing(info_, SQL_NON_NULLABLE_COLUMNS,
                      static_cast<uint16_t>(SQL_NNC_NULL));
  SetDefaultIfMissing(info_, SQL_NULL_COLLATION, static_cast<uint16_t>(SQL_NC_END));
  SetDefaultIfMissing(info_, SQL_NUMERIC_FUNCTIONS, static_cast<uint32_t>(0));
  SetDefaultIfMissing(info_, SQL_OJ_CAPABILITIES,
                      static_cast<uint32_t>(SQL_OJ_LEFT | SQL_OJ_RIGHT | SQL_OJ_FULL));
  SetDefaultIfMissing(info_, SQL_ORDER_BY_COLUMNS_IN_SELECT, "Y");
  SetDefaultIfMissing(info_, SQL_PROCEDURE_TERM, "");
  SetDefaultIfMissing(info_, SQL_PROCEDURES, "N");
  SetDefaultIfMissing(info_, SQL_QUOTED_IDENTIFIER_CASE,
                      static_cast<uint16_t>(SQL_IC_SENSITIVE));
  SetDefaultIfMissing(info_, SQL_SCHEMA_TERM, "schema");
  SetDefaultIfMissing(info_, SQL_SCHEMA_USAGE,
                      static_cast<uint32_t>(SQL_SU_DML_STATEMENTS));
  SetDefaultIfMissing(info_, SQL_SEARCH_PATTERN_ESCAPE, "\\");
  SetDefaultIfMissing(
      info_, SQL_SERVER_NAME,
      "Arrow Flight SQL Server");  // This might actually need to be the hostname.
  SetDefaultIfMissing(info_, SQL_SQL_CONFORMANCE,
                      static_cast<uint32_t>(SQL_SC_SQL92_ENTRY));
  SetDefaultIfMissing(info_, SQL_SQL92_DATETIME_FUNCTIONS,
                      static_cast<uint32_t>(SQL_SDF_CURRENT_DATE | SQL_SDF_CURRENT_TIME |
                                            SQL_SDF_CURRENT_TIMESTAMP));
  SetDefaultIfMissing(info_, SQL_SQL92_FOREIGN_KEY_DELETE_RULE, static_cast<uint32_t>(0));
  SetDefaultIfMissing(info_, SQL_SQL92_FOREIGN_KEY_UPDATE_RULE, static_cast<uint32_t>(0));
  SetDefaultIfMissing(info_, SQL_SQL92_GRANT, static_cast<uint32_t>(0));
  SetDefaultIfMissing(info_, SQL_SQL92_NUMERIC_VALUE_FUNCTIONS, static_cast<uint32_t>(0));
  SetDefaultIfMissing(
      info_, SQL_SQL92_PREDICATES,
      static_cast<uint32_t>(SQL_SP_BETWEEN | SQL_SP_COMPARISON | SQL_SP_EXISTS |
                            SQL_SP_IN | SQL_SP_ISNOTNULL | SQL_SP_ISNULL | SQL_SP_LIKE));
  SetDefaultIfMissing(
      info_, SQL_SQL92_RELATIONAL_JOIN_OPERATORS,
      static_cast<uint32_t>(SQL_SRJO_INNER_JOIN | SQL_SRJO_CROSS_JOIN |
                            SQL_SRJO_LEFT_OUTER_JOIN | SQL_SRJO_FULL_OUTER_JOIN |
                            SQL_SRJO_RIGHT_OUTER_JOIN));
  SetDefaultIfMissing(info_, SQL_SQL92_REVOKE, static_cast<uint32_t>(0));
  SetDefaultIfMissing(info_, SQL_SQL92_ROW_VALUE_CONSTRUCTOR,
                      static_cast<uint32_t>(SQL_SRVC_VALUE_EXPRESSION | SQL_SRVC_NULL));
  SetDefaultIfMissing(
      info_, SQL_SQL92_STRING_FUNCTIONS,
      static_cast<uint32_t>(SQL_SSF_CONVERT | SQL_SSF_LOWER | SQL_SSF_UPPER |
                            SQL_SSF_SUBSTRING | SQL_SSF_TRIM_BOTH | SQL_SSF_TRIM_LEADING |
                            SQL_SSF_TRIM_TRAILING));
  SetDefaultIfMissing(info_, SQL_SQL92_VALUE_EXPRESSIONS,
                      static_cast<uint32_t>(SQL_SVE_CASE | SQL_SVE_CAST |
                                            SQL_SVE_COALESCE | SQL_SVE_NULLIF));
  SetDefaultIfMissing(info_, SQL_STANDARD_CLI_CONFORMANCE, static_cast<uint32_t>(0));
  SetDefaultIfMissing(
      info_, SQL_STRING_FUNCTIONS,
      static_cast<uint32_t>(SQL_FN_STR_CONCAT | SQL_FN_STR_LCASE | SQL_FN_STR_LENGTH |
                            SQL_FN_STR_LTRIM | SQL_FN_STR_RTRIM | SQL_FN_STR_SPACE |
                            SQL_FN_STR_SUBSTRING | SQL_FN_STR_UCASE));
  SetDefaultIfMissing(
      info_, SQL_SUBQUERIES,
      static_cast<uint32_t>(SQL_SQ_CORRELATED_SUBQUERIES | SQL_SQ_COMPARISON |
                            SQL_SQ_EXISTS | SQL_SQ_IN | SQL_SQ_QUANTIFIED));
  SetDefaultIfMissing(info_, SQL_SYSTEM_FUNCTIONS,
                      static_cast<uint32_t>(SQL_FN_SYS_IFNULL | SQL_FN_SYS_USERNAME));
  SetDefaultIfMissing(info_, SQL_TIMEDATE_ADD_INTERVALS,
                      static_cast<uint32_t>(
                          SQL_FN_TSI_FRAC_SECOND | SQL_FN_TSI_SECOND | SQL_FN_TSI_MINUTE |
                          SQL_FN_TSI_HOUR | SQL_FN_TSI_DAY | SQL_FN_TSI_WEEK |
                          SQL_FN_TSI_MONTH | SQL_FN_TSI_QUARTER | SQL_FN_TSI_YEAR));
  SetDefaultIfMissing(info_, SQL_TIMEDATE_DIFF_INTERVALS,
                      static_cast<uint32_t>(
                          SQL_FN_TSI_FRAC_SECOND | SQL_FN_TSI_SECOND | SQL_FN_TSI_MINUTE |
                          SQL_FN_TSI_HOUR | SQL_FN_TSI_DAY | SQL_FN_TSI_WEEK |
                          SQL_FN_TSI_MONTH | SQL_FN_TSI_QUARTER | SQL_FN_TSI_YEAR));
  SetDefaultIfMissing(info_, SQL_UNION,
                      static_cast<uint32_t>(SQL_U_UNION | SQL_U_UNION_ALL));
  SetDefaultIfMissing(info_, SQL_XOPEN_CLI_YEAR, "1995");
  SetDefaultIfMissing(info_, SQL_ODBC_SQL_CONFORMANCE,
                      static_cast<uint16_t>(SQL_OSC_MINIMUM));
  SetDefaultIfMissing(info_, SQL_ODBC_SAG_CLI_CONFORMANCE,
                      static_cast<uint16_t>(SQL_OSCC_COMPLIANT));
}

}  // namespace flight_sql
}  // namespace driver
