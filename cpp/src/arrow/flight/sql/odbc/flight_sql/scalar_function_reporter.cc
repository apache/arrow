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

#include "arrow/flight/sql/odbc/flight_sql/scalar_function_reporter.h"

#include "arrow/flight/sql/odbc/odbcabstraction/include/odbcabstraction/platform.h"

#include <sqlext.h>
#include <string>
#include <unordered_map>

namespace driver {
namespace flight_sql {

// The list of functions that can be converted from string to ODBC bitmasks is
// based on Calcite's SqlJdbcFunctionCall class.

namespace {
static const std::unordered_map<std::string, uint32_t> numeric_functions = {
    {"ABS", SQL_FN_NUM_ABS},         {"ACOS", SQL_FN_NUM_ACOS},
    {"ASIN", SQL_FN_NUM_ASIN},       {"ATAN", SQL_FN_NUM_ATAN},
    {"ATAN2", SQL_FN_NUM_ATAN2},     {"CEILING", SQL_FN_NUM_CEILING},
    {"COS", SQL_FN_NUM_ACOS},        {"COT", SQL_FN_NUM_COT},
    {"DEGREES", SQL_FN_NUM_DEGREES}, {"EXP", SQL_FN_NUM_EXP},
    {"FLOOR", SQL_FN_NUM_FLOOR},     {"LOG", SQL_FN_NUM_LOG},
    {"LOG10", SQL_FN_NUM_LOG10},     {"MOD", SQL_FN_NUM_MOD},
    {"PI", SQL_FN_NUM_PI},           {"POWER", SQL_FN_NUM_POWER},
    {"RADIANS", SQL_FN_NUM_RADIANS}, {"RAND", SQL_FN_NUM_RAND},
    {"ROUND", SQL_FN_NUM_ROUND},     {"SIGN", SQL_FN_NUM_SIGN},
    {"SIN", SQL_FN_NUM_SIN},         {"SQRT", SQL_FN_NUM_SQRT},
    {"TAN", SQL_FN_NUM_TAN},         {"TRUNCATE", SQL_FN_NUM_TRUNCATE}};

static const std::unordered_map<std::string, uint32_t> system_functions = {
    {"DATABASE", SQL_FN_SYS_DBNAME},
    {"IFNULL", SQL_FN_SYS_IFNULL},
    {"USER", SQL_FN_SYS_USERNAME}};

static const std::unordered_map<std::string, uint32_t> datetime_functions = {
    {"CURDATE", SQL_FN_TD_CURDATE},
    {"CURTIME", SQL_FN_TD_CURTIME},
    {"DAYNAME", SQL_FN_TD_DAYNAME},
    {"DAYOFMONTH", SQL_FN_TD_DAYOFMONTH},
    {"DAYOFWEEK", SQL_FN_TD_DAYOFWEEK},
    {"DAYOFYEAR", SQL_FN_TD_DAYOFYEAR},
    {"HOUR", SQL_FN_TD_HOUR},
    {"MINUTE", SQL_FN_TD_MINUTE},
    {"MONTH", SQL_FN_TD_MONTH},
    {"MONTHNAME", SQL_FN_TD_MONTHNAME},
    {"NOW", SQL_FN_TD_NOW},
    {"QUARTER", SQL_FN_TD_QUARTER},
    {"SECOND", SQL_FN_TD_SECOND},
    {"TIMESTAMPADD", SQL_FN_TD_TIMESTAMPADD},
    {"TIMESTAMPDIFF", SQL_FN_TD_TIMESTAMPDIFF},
    {"WEEK", SQL_FN_TD_WEEK},
    {"YEAR", SQL_FN_TD_YEAR},
    // Additional functions in ODBC but not Calcite:
    {"CURRENT_DATE", SQL_FN_TD_CURRENT_DATE},
    {"CURRENT_TIME", SQL_FN_TD_CURRENT_TIME},
    {"CURRENT_TIMESTAMP", SQL_FN_TD_CURRENT_TIMESTAMP},
    {"EXTRACT", SQL_FN_TD_EXTRACT}};

static const std::unordered_map<std::string, uint32_t> string_functions = {
    {"ASCII", SQL_FN_STR_ASCII},
    {"CHAR", SQL_FN_STR_CHAR},
    {"CONCAT", SQL_FN_STR_CONCAT},
    {"DIFFERENCE", SQL_FN_STR_DIFFERENCE},
    {"INSERT", SQL_FN_STR_INSERT},
    {"LCASE", SQL_FN_STR_LCASE},
    {"LEFT", SQL_FN_STR_LEFT},
    {"LENGTH", SQL_FN_STR_LENGTH},
    {"LOCATE", SQL_FN_STR_LOCATE},
    {"LTRIM", SQL_FN_STR_LTRIM},
    {"REPEAT", SQL_FN_STR_REPEAT},
    {"REPLACE", SQL_FN_STR_REPLACE},
    {"RIGHT", SQL_FN_STR_RIGHT},
    {"RTRIM", SQL_FN_STR_RTRIM},
    {"SOUNDEX", SQL_FN_STR_SOUNDEX},
    {"SPACE", SQL_FN_STR_SPACE},
    {"SUBSTRING", SQL_FN_STR_SUBSTRING},
    {"UCASE", SQL_FN_STR_UCASE},
    // Additional functions in ODBC but not Calcite:
    {"LOCATE_2", SQL_FN_STR_LOCATE_2},
    {"BIT_LENGTH", SQL_FN_STR_BIT_LENGTH},
    {"CHAR_LENGTH", SQL_FN_STR_CHAR_LENGTH},
    {"CHARACTER_LENGTH", SQL_FN_STR_CHARACTER_LENGTH},
    {"OCTET_LENGTH", SQL_FN_STR_OCTET_LENGTH},
    {"POSTION", SQL_FN_STR_POSITION},
    {"SOUNDEX", SQL_FN_STR_SOUNDEX}};
}  // namespace

void ReportSystemFunction(const std::string& function, uint32_t& current_sys_functions,
                          uint32_t& current_convert_functions) {
  const auto& result = system_functions.find(function);
  if (result != system_functions.end()) {
    current_sys_functions |= result->second;
  } else if (function == "CONVERT") {
    // CAST and CONVERT are system functions from FlightSql/Calcite, but are
    // CONVERT functions in ODBC. Assume that if CONVERT is reported as a system
    // function, then CAST and CONVERT are both supported.
    current_convert_functions |= SQL_FN_CVT_CONVERT | SQL_FN_CVT_CAST;
  }
}

void ReportNumericFunction(const std::string& function, uint32_t& current_functions) {
  const auto& result = numeric_functions.find(function);
  if (result != numeric_functions.end()) {
    current_functions |= result->second;
  }
}

void ReportStringFunction(const std::string& function, uint32_t& current_functions) {
  const auto& result = string_functions.find(function);
  if (result != string_functions.end()) {
    current_functions |= result->second;
  }
}

void ReportDatetimeFunction(const std::string& function, uint32_t& current_functions) {
  const auto& result = datetime_functions.find(function);
  if (result != datetime_functions.end()) {
    current_functions |= result->second;
  }
}

}  // namespace flight_sql
}  // namespace driver
