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

#include "arrow/flight/sql/example/sqlite_sql_info.h"

#include "arrow/flight/sql/FlightSql.pb.h"
#include "arrow/flight/sql/types.h"

namespace arrow {
namespace flight {
namespace sql {
namespace example {

namespace flight_sql_pb = arrow::flight::protocol::sql;

/// \brief Gets the mapping from SQL info ids to SqlInfoResult instances.
/// \return the cache.
SqlInfoResultMap GetSqlInfoResultMap() {
  return {
      {flight_sql_pb::SqlInfo::FLIGHT_SQL_SERVER_NAME,
       SqlInfoResult(std::string("db_name"))},
      {flight_sql_pb::SqlInfo::FLIGHT_SQL_SERVER_VERSION,
       SqlInfoResult(std::string("sqlite 3"))},
      {flight_sql_pb::SqlInfo::FLIGHT_SQL_SERVER_ARROW_VERSION,
       SqlInfoResult(std::string("7.0.0-SNAPSHOT" /* Only an example */))},
      {flight_sql_pb::SqlInfo::FLIGHT_SQL_SERVER_READ_ONLY, SqlInfoResult(false)},
      {flight_sql_pb::SqlInfo::SQL_DDL_CATALOG,
       SqlInfoResult(false /* SQLite 3 does not support catalogs */)},
      {flight_sql_pb::SqlInfo::SQL_DDL_SCHEMA,
       SqlInfoResult(false /* SQLite 3 does not support schemas */)},
      {flight_sql_pb::SqlInfo::SQL_DDL_TABLE, SqlInfoResult(true)},
      {flight_sql_pb::SqlInfo::SQL_IDENTIFIER_CASE,
       SqlInfoResult(int64_t(flight_sql_pb::SqlSupportedCaseSensitivity::
                                 SQL_CASE_SENSITIVITY_CASE_INSENSITIVE))},
      {flight_sql_pb::SqlInfo::SQL_IDENTIFIER_QUOTE_CHAR,
       SqlInfoResult(std::string("\""))},
      {flight_sql_pb::SqlInfo::SQL_QUOTED_IDENTIFIER_CASE,
       SqlInfoResult(int64_t(flight_sql_pb::SqlSupportedCaseSensitivity::
                                 SQL_CASE_SENSITIVITY_CASE_INSENSITIVE))},
      {flight_sql_pb::SqlInfo::SQL_ALL_TABLES_ARE_SELECTABLE, SqlInfoResult(true)},
      {flight_sql_pb::SqlInfo::SQL_NULL_ORDERING,
       SqlInfoResult(int64_t(flight_sql_pb::SqlNullOrdering::SQL_NULLS_SORTED_AT_START))},
      {flight_sql_pb::SqlInfo::SQL_KEYWORDS,
       SqlInfoResult(std::vector<std::string>({"ABORT",
                                               "ACTION",
                                               "ADD",
                                               "AFTER",
                                               "ALL",
                                               "ALTER",
                                               "ALWAYS",
                                               "ANALYZE",
                                               "AND",
                                               "AS",
                                               "ASC",
                                               "ATTACH",
                                               "AUTOINCREMENT",
                                               "BEFORE",
                                               "BEGIN",
                                               "BETWEEN",
                                               "BY",
                                               "CASCADE",
                                               "CASE",
                                               "CAST",
                                               "CHECK",
                                               "COLLATE",
                                               "COLUMN",
                                               "COMMIT",
                                               "CONFLICT",
                                               "CONSTRAINT",
                                               "CREATE",
                                               "CROSS",
                                               "CURRENT",
                                               "CURRENT_DATE",
                                               "CURRENT_TIME",
                                               "CURRENT_TIMESTAMP",
                                               "DATABASE",
                                               "DEFAULT",
                                               "DEFERRABLE",
                                               "DEFERRED",
                                               "DELETE",
                                               "DESC",
                                               "DETACH",
                                               "DISTINCT",
                                               "DO",
                                               "DROP",
                                               "EACH",
                                               "ELSE",
                                               "END",
                                               "ESCAPE",
                                               "EXCEPT",
                                               "EXCLUDE",
                                               "EXCLUSIVE",
                                               "EXISTS",
                                               "EXPLAIN",
                                               "FAIL",
                                               "FILTER",
                                               "FIRST",
                                               "FOLLOWING",
                                               "FOR",
                                               "FOREIGN",
                                               "FROM",
                                               "FULL",
                                               "GENERATED",
                                               "GLOB",
                                               "GROUP",
                                               "GROUPS",
                                               "HAVING",
                                               "IF",
                                               "IGNORE",
                                               "IMMEDIATE",
                                               "IN",
                                               "INDEX",
                                               "INDEXED",
                                               "INITIALLY",
                                               "INNER",
                                               "INSERT",
                                               "INSTEAD",
                                               "INTERSECT",
                                               "INTO",
                                               "IS",
                                               "ISNULL",
                                               "JOIN",
                                               "KEY",
                                               "LAST",
                                               "LEFT",
                                               "LIKE",
                                               "LIMIT",
                                               "MATCH",
                                               "MATERIALIZED",
                                               "NATURAL",
                                               "NO",
                                               "NOT",
                                               "NOTHING",
                                               "NOTNULL",
                                               "NULL",
                                               "NULLS",
                                               "OF",
                                               "OFFSET",
                                               "ON",
                                               "OR",
                                               "ORDER",
                                               "OTHERS",
                                               "OUTER",
                                               "OVER",
                                               "PARTITION",
                                               "PLAN",
                                               "PRAGMA",
                                               "PRECEDING",
                                               "PRIMARY",
                                               "QUERY",
                                               "RAISE",
                                               "RANGE",
                                               "RECURSIVE",
                                               "REFERENCES",
                                               "REGEXP",
                                               "REINDEX",
                                               "RELEASE",
                                               "RENAME",
                                               "REPLACE",
                                               "RESTRICT",
                                               "RETURNING",
                                               "RIGHT",
                                               "ROLLBACK",
                                               "ROW",
                                               "ROWS",
                                               "SAVEPOINT",
                                               "SELECT",
                                               "SET",
                                               "TABLE",
                                               "TEMP",
                                               "TEMPORARY",
                                               "THEN",
                                               "TIES",
                                               "TO",
                                               "TRANSACTION",
                                               "TRIGGER",
                                               "UNBOUNDED",
                                               "UNION",
                                               "UNIQUE",
                                               "UPDATE",
                                               "USING",
                                               "VACUUM",
                                               "VALUES",
                                               "VIEW",
                                               "VIRTUAL",
                                               "WHEN",
                                               "WHERE",
                                               "WINDOW",
                                               "WITH",
                                               "WITHOUT"}))},
      {flight_sql_pb::SqlInfo::SQL_NUMERIC_FUNCTIONS,
       SqlInfoResult(std::vector<std::string>(
           {"acos",    "acosh", "asin", "asinh",   "atan", "atan2", "atanh", "ceil",
            "ceiling", "cos",   "cosh", "degrees", "exp",  "floor", "ln",    "log",
            "log",     "log10", "log2", "mod",     "pi",   "pow",   "power", "radians",
            "sin",     "sinh",  "sqrt", "tan",     "tanh", "trunc"}))},
      {flight_sql_pb::SqlInfo::SQL_STRING_FUNCTIONS,
       SqlInfoResult(
           std::vector<std::string>({"SUBSTR", "TRIM", "LTRIM", "RTRIM", "LENGTH",
                                     "REPLACE", "UPPER", "LOWER", "INSTR"}))},
      {flight_sql_pb::SqlInfo::SQL_SUPPORTS_CONVERT,
       SqlInfoResult(std::unordered_map<int32_t, std::vector<int32_t>>(
           {{flight_sql_pb::SqlSupportsConvert::SQL_CONVERT_BIGINT,
             std::vector<int32_t>(
                 {flight_sql_pb::SqlSupportsConvert::SQL_CONVERT_INTEGER})}}))}};
}

}  // namespace example
}  // namespace sql
}  // namespace flight
}  // namespace arrow
