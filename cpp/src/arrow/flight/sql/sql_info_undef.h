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

/// \brief Undefine the ODBC macros in sql.h and sqlext.h to avoid conflicts with ODBC
/// library
///
/// This file is to resolve build issues from linking. Should not be used with sql.h or
/// sql_ext.h

#ifdef SQL_IDENTIFIER_CASE
#  undef SQL_IDENTIFIER_CASE
#endif  // SQL_IDENTIFIER_CASE

#ifdef SQL_IDENTIFIER_QUOTE_CHAR
#  undef SQL_IDENTIFIER_QUOTE_CHAR
#endif  // SQL_IDENTIFIER_QUOTE_CHAR

#ifdef SQL_QUOTED_IDENTIFIER_CASE
#  undef SQL_QUOTED_IDENTIFIER_CASE
#endif  // SQL_QUOTED_IDENTIFIER_CASE

#ifdef SQL_KEYWORDS
#  undef SQL_KEYWORDS
#endif  // SQL_KEYWORDS

#ifdef SQL_NUMERIC_FUNCTIONS
#  undef SQL_NUMERIC_FUNCTIONS
#endif  // SQL_NUMERIC_FUNCTIONS

#ifdef SQL_STRING_FUNCTIONS
#  undef SQL_STRING_FUNCTIONS
#endif  // SQL_STRING_FUNCTIONS

#ifdef SQL_SYSTEM_FUNCTIONS
#  undef SQL_SYSTEM_FUNCTIONS
#endif  // SQL_SYSTEM_FUNCTIONS

#ifdef SQL_SCHEMA_TERM
#  undef SQL_SCHEMA_TERM
#endif  // SQL_SCHEMA_TERM

#ifdef SQL_PROCEDURE_TERM
#  undef SQL_PROCEDURE_TERM
#endif  // SQL_PROCEDURE_TERM

#ifdef SQL_CATALOG_TERM
#  undef SQL_CATALOG_TERM
#endif  // SQL_CATALOG_TERM

#ifdef SQL_MAX_COLUMNS_IN_GROUP_BY
#  undef SQL_MAX_COLUMNS_IN_GROUP_BY
#endif  // SQL_MAX_COLUMNS_IN_GROUP_BY

#ifdef SQL_MAX_COLUMNS_IN_INDEX
#  undef SQL_MAX_COLUMNS_IN_INDEX
#endif  // SQL_MAX_COLUMNS_IN_INDEX

#ifdef SQL_MAX_COLUMNS_IN_ORDER_BY
#  undef SQL_MAX_COLUMNS_IN_ORDER_BY
#endif  // SQL_MAX_COLUMNS_IN_ORDER_BY

#ifdef SQL_MAX_COLUMNS_IN_SELECT
#  undef SQL_MAX_COLUMNS_IN_SELECT
#endif  // SQL_MAX_COLUMNS_IN_SELECT

#ifdef SQL_MAX_COLUMNS_IN_TABLE
#  undef SQL_MAX_COLUMNS_IN_TABLE
#endif  // SQL_MAX_COLUMNS_IN_TABLE

#ifdef SQL_MAX_ROW_SIZE
#  undef SQL_MAX_ROW_SIZE
#endif  // SQL_MAX_ROW_SIZE

#ifdef SQL_MAX_TABLES_IN_SELECT
#  undef SQL_MAX_TABLES_IN_SELECT
#endif  // SQL_MAX_TABLES_IN_SELECT

#ifdef SQL_CONVERT_BIGINT
#  undef SQL_CONVERT_BIGINT
#endif  // SQL_CONVERT_BIGINT

#ifdef SQL_CONVERT_BINARY
#  undef SQL_CONVERT_BINARY
#endif  // SQL_CONVERT_BINARY

#ifdef SQL_CONVERT_BIT
#  undef SQL_CONVERT_BIT
#endif  // SQL_CONVERT_BIT

#ifdef SQL_CONVERT_CHAR
#  undef SQL_CONVERT_CHAR
#endif  // SQL_CONVERT_CHAR

#ifdef SQL_CONVERT_DATE
#  undef SQL_CONVERT_DATE
#endif  // SQL_CONVERT_DATE

#ifdef SQL_CONVERT_DECIMAL
#  undef SQL_CONVERT_DECIMAL
#endif  // SQL_CONVERT_DECIMAL

#ifdef SQL_CONVERT_FLOAT
#  undef SQL_CONVERT_FLOAT
#endif  // SQL_CONVERT_FLOAT

#ifdef SQL_CONVERT_INTEGER
#  undef SQL_CONVERT_INTEGER
#endif  // SQL_CONVERT_INTEGER

#ifdef SQL_CONVERT_INTERVAL_DAY_TIME
#  undef SQL_CONVERT_INTERVAL_DAY_TIME
#endif  // SQL_CONVERT_INTERVAL_DAY_TIME

#ifdef SQL_CONVERT_INTERVAL_YEAR_MONTH
#  undef SQL_CONVERT_INTERVAL_YEAR_MONTH
#endif  // SQL_CONVERT_INTERVAL_YEAR_MONTH

#ifdef SQL_CONVERT_LONGVARBINARY
#  undef SQL_CONVERT_LONGVARBINARY
#endif  // SQL_CONVERT_LONGVARBINARY

#ifdef SQL_CONVERT_LONGVARCHAR
#  undef SQL_CONVERT_LONGVARCHAR
#endif  // SQL_CONVERT_LONGVARCHAR

#ifdef SQL_CONVERT_NUMERIC
#  undef SQL_CONVERT_NUMERIC
#endif  // SQL_CONVERT_NUMERIC

#ifdef SQL_CONVERT_REAL
#  undef SQL_CONVERT_REAL
#endif  // SQL_CONVERT_REAL

#ifdef SQL_CONVERT_SMALLINT
#  undef SQL_CONVERT_SMALLINT
#endif  // SQL_CONVERT_SMALLINT

#ifdef SQL_CONVERT_TIME
#  undef SQL_CONVERT_TIME
#endif  // SQL_CONVERT_TIME

#ifdef SQL_CONVERT_TIMESTAMP
#  undef SQL_CONVERT_TIMESTAMP
#endif  // SQL_CONVERT_TIMESTAMP

#ifdef SQL_CONVERT_TINYINT
#  undef SQL_CONVERT_TINYINT
#endif  // SQL_CONVERT_TINYINT

#ifdef SQL_CONVERT_VARBINARY
#  undef SQL_CONVERT_VARBINARY
#endif  // SQL_CONVERT_VARBINARY

#ifdef SQL_CONVERT_VARCHAR
#  undef SQL_CONVERT_VARCHAR
#endif  // SQL_CONVERT_VARCHAR
