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

#include <cstdint>
#include <iosfwd>
#include <optional>
#include <string>
#include <unordered_map>
#include <variant>
#include <vector>

#include "arrow/flight/sql/visibility.h"
#include "arrow/type_fwd.h"

namespace arrow {
namespace flight {
namespace sql {

/// \defgroup flight-sql-common-types Common protocol types for Flight SQL
///
/// @{

/// \brief Variant supporting all possible types on SQL info.
using SqlInfoResult =
    std::variant<std::string, bool, int64_t, int32_t, std::vector<std::string>,
                 std::unordered_map<int32_t, std::vector<int32_t>>>;

/// \brief Map SQL info identifier to its value.
using SqlInfoResultMap = std::unordered_map<int32_t, SqlInfoResult>;

/// \brief Options to be set in the SqlInfo.
struct ARROW_FLIGHT_SQL_EXPORT SqlInfoOptions {
  /// \brief Predefined info values for GetSqlInfo.
  enum SqlInfo {
    /// \name Server Information
    /// Values [0-500): Provides basic information about the Flight SQL Server.
    /// @{

    /// Retrieves a UTF-8 string with the name of the Flight SQL Server.
    FLIGHT_SQL_SERVER_NAME = 0,

    /// Retrieves a UTF-8 string with the native version of the Flight SQL
    /// Server.
    FLIGHT_SQL_SERVER_VERSION = 1,

    /// Retrieves a UTF-8 string with the Arrow format version of the Flight
    /// SQL Server.
    FLIGHT_SQL_SERVER_ARROW_VERSION = 2,

    /// Retrieves a boolean value indicating whether the Flight SQL Server is
    /// read only.
    ///
    /// Returns:
    /// - false: if read-write
    /// - true: if read only
    FLIGHT_SQL_SERVER_READ_ONLY = 3,

    /// Retrieves a boolean value indicating whether the Flight SQL Server
    /// supports executing SQL queries.
    ///
    /// Note that the absence of this info (as opposed to a false
    /// value) does not necessarily mean that SQL is not supported, as
    /// this property was not originally defined.
    FLIGHT_SQL_SERVER_SQL = 4,

    /// Retrieves a boolean value indicating whether the Flight SQL Server
    /// supports executing Substrait plans.
    FLIGHT_SQL_SERVER_SUBSTRAIT = 5,

    /// Retrieves a string value indicating the minimum supported
    /// Substrait version, or null if Substrait is not supported.
    FLIGHT_SQL_SERVER_SUBSTRAIT_MIN_VERSION = 6,

    /// Retrieves a string value indicating the maximum supported
    /// Substrait version, or null if Substrait is not supported.
    FLIGHT_SQL_SERVER_SUBSTRAIT_MAX_VERSION = 7,

    /// Retrieves an int32 indicating whether the Flight SQL Server
    /// supports the BeginTransaction, EndTransaction, BeginSavepoint,
    /// and EndSavepoint actions.
    ///
    /// Even if this is not supported, the database may still support
    /// explicit "BEGIN TRANSACTION"/"COMMIT" SQL statements (see
    /// SQL_TRANSACTIONS_SUPPORTED); this property is only about
    /// whether the server implements the Flight SQL API endpoints.
    ///
    /// The possible values are listed in `SqlSupportedTransaction`.
    FLIGHT_SQL_SERVER_TRANSACTION = 8,

    /// Retrieves a boolean value indicating whether the Flight SQL Server
    /// supports explicit query cancellation (the CancelQuery action).
    FLIGHT_SQL_SERVER_CANCEL = 9,

    /// Retrieves an int32 value indicating the timeout (in milliseconds) for
    /// prepared statement handles.
    ///
    /// If 0, there is no timeout.
    FLIGHT_SQL_SERVER_STATEMENT_TIMEOUT = 100,

    /// Retrieves an int32 value indicating the timeout (in milliseconds) for
    /// transactions, since transactions are not tied to a connection.
    ///
    /// If 0, there is no timeout.
    FLIGHT_SQL_SERVER_TRANSACTION_TIMEOUT = 101,

    /// @}

    /// \name SQL Syntax Information
    /// Values [500-1000): provides information about SQL syntax supported
    /// by the Flight SQL Server.
    /// @{

    /// Retrieves a boolean value indicating whether the Flight SQL
    /// Server supports CREATE and DROP of catalogs.
    ///
    /// Returns:
    /// - false: if it doesn't support CREATE and DROP of catalogs.
    /// - true: if it supports CREATE and DROP of catalogs.
    SQL_DDL_CATALOG = 500,

    /// Retrieves a boolean value indicating whether the Flight SQL
    /// Server supports CREATE and DROP of schemas.
    ///
    /// Returns:
    /// - false: if it doesn't support CREATE and DROP of schemas.
    /// - true: if it supports CREATE and DROP of schemas.
    SQL_DDL_SCHEMA = 501,

    /// Indicates whether the Flight SQL Server supports CREATE and DROP of
    /// tables.
    ///
    /// Returns:
    /// - false: if it doesn't support CREATE and DROP of tables.
    /// - true: if it supports CREATE and DROP of tables.
    SQL_DDL_TABLE = 502,

    /// Retrieves a int32 value representing the enum ordinal for the
    /// case sensitivity of catalog, table and schema names.
    ///
    /// The possible values are listed in
    /// `arrow.flight.protocol.sql.SqlSupportedCaseSensitivity`.
    SQL_IDENTIFIER_CASE = 503,

    /// Retrieves a UTF-8 string with the supported character(s) used
    /// to surround a delimited identifier.
    SQL_IDENTIFIER_QUOTE_CHAR = 504,

    /// Retrieves a int32 value representing the enum ordinal
    /// for the case sensitivity of quoted identifiers.
    ///
    /// The possible values are listed in
    /// `arrow.flight.protocol.sql.SqlSupportedCaseSensitivity`.
    SQL_QUOTED_IDENTIFIER_CASE = 505,

    /// Retrieves a boolean value indicating whether all tables are
    /// selectable.
    ///
    /// Returns:
    /// - false: if not all tables are selectable or if none are;
    /// - true: if all tables are selectable.
    SQL_ALL_TABLES_ARE_SELECTABLE = 506,

    /// Retrieves the null ordering used by the database as a int32
    /// ordinal value.
    ///
    /// Returns a int32 ordinal for the null ordering being used, as
    /// described in `arrow.flight.protocol.sql.SqlNullOrdering`.
    SQL_NULL_ORDERING = 507,

    /// Retrieves a UTF-8 string list with values of the supported keywords.
    SQL_KEYWORDS = 508,

    /// Retrieves a UTF-8 string list with values of the supported numeric functions.
    SQL_NUMERIC_FUNCTIONS = 509,

    /// Retrieves a UTF-8 string list with values of the supported string functions.
    SQL_STRING_FUNCTIONS = 510,

    /// Retrieves a UTF-8 string list with values of the supported system functions.
    SQL_SYSTEM_FUNCTIONS = 511,

    /// Retrieves a UTF-8 string list with values of the supported datetime functions.
    SQL_DATETIME_FUNCTIONS = 512,

    /// Retrieves the UTF-8 string that can be used to escape wildcard characters.
    /// This is the string that can be used to escape '_' or '%' in the catalog search
    /// parameters that are a pattern (and therefore use one of the wildcard characters).
    /// The '_' character represents any single character; the '%' character represents
    /// any
    /// sequence of zero or more characters.
    SQL_SEARCH_STRING_ESCAPE = 513,

    /// Retrieves a UTF-8 string with all the "extra" characters that can be used in
    /// unquoted identifier names (those beyond a-z, A-Z, 0-9 and _).
    SQL_EXTRA_NAME_CHARACTERS = 514,

    /// Retrieves a boolean value indicating whether column aliasing is supported.
    /// If so, the SQL AS clause can be used to provide names for computed columns or to
    /// provide alias names for columns as required.
    ///
    /// Returns:
    /// - false: if column aliasing is unsupported;
    /// - true: if column aliasing is supported.
    SQL_SUPPORTS_COLUMN_ALIASING = 515,

    /// Retrieves a boolean value indicating whether concatenations between null and
    /// non-null values being null are supported.
    ///
    /// - Returns:
    /// - false: if concatenations between null and non-null values being null are
    /// unsupported;
    /// - true: if concatenations between null and non-null values being null are
    /// supported.
    SQL_NULL_PLUS_NULL_IS_NULL = 516,

    /// Retrieves a map where the key is the type to convert from and the value is a list
    /// with the types to convert to, indicating the supported conversions. Each key and
    /// each item on the list value is a value to a predefined type on SqlSupportsConvert
    /// enum. The returned map will be:  map<int32, list<int32>>
    SQL_SUPPORTS_CONVERT = 517,

    /// Retrieves a boolean value indicating whether, when table correlation names are
    /// supported, they are restricted to being different from the names of the tables.
    ///
    /// Returns:
    /// - false: if table correlation names are unsupported;
    /// - true: if table correlation names are supported.
    SQL_SUPPORTS_TABLE_CORRELATION_NAMES = 518,

    /// Retrieves a boolean value indicating whether, when table correlation names are
    /// supported, they are restricted to being different from the names of the tables.
    ///
    /// Returns:
    /// - false: if different table correlation names are unsupported;
    /// - true: if different table correlation names are supported
    SQL_SUPPORTS_DIFFERENT_TABLE_CORRELATION_NAMES = 519,

    /// Retrieves a boolean value indicating whether expressions in ORDER BY lists are
    /// supported.
    ///
    /// Returns:
    /// - false: if expressions in ORDER BY are unsupported;
    /// - true: if expressions in ORDER BY are supported;
    SQL_SUPPORTS_EXPRESSIONS_IN_ORDER_BY = 520,

    /// Retrieves a boolean value indicating whether using a column that is not in the
    /// SELECT statement in a GROUP BY clause is supported.
    ///
    /// Returns:
    /// - false: if using a column that is not in the SELECT statement in a GROUP BY
    /// clause
    /// is unsupported;
    /// - true: if using a column that is not in the SELECT statement in a GROUP BY clause
    /// is supported.
    SQL_SUPPORTS_ORDER_BY_UNRELATED = 521,

    /// Retrieves the supported GROUP BY commands as an int32 bitmask.
    /// The returned bitmask should be parsed in order to retrieve the supported commands.
    ///
    /// - return 0 (0b0)   => [] (GROUP BY is unsupported);
    /// - return 1 (0b1)   => [SQL_GROUP_BY_UNRELATED];
    /// - return 2 (0b10)  => [SQL_GROUP_BY_BEYOND_SELECT];
    /// - return 3 (0b11)  => [SQL_GROUP_BY_UNRELATED, SQL_GROUP_BY_BEYOND_SELECT].
    ///
    /// Valid GROUP BY types are described under
    /// `arrow.flight.protocol.sql.SqlSupportedGroupBy`.
    SQL_SUPPORTED_GROUP_BY = 522,

    /// Retrieves a boolean value indicating whether specifying a LIKE escape clause is
    /// supported.
    ///
    /// Returns:
    /// - false: if specifying a LIKE escape clause is unsupported;
    /// - true: if specifying a LIKE escape clause is supported.
    SQL_SUPPORTS_LIKE_ESCAPE_CLAUSE = 523,

    /// Retrieves a boolean value indicating whether columns may be defined as
    /// non-nullable.
    ///
    /// Returns:
    /// - false: if columns cannot be defined as non-nullable;
    /// - true: if columns may be defined as non-nullable.
    SQL_SUPPORTS_NON_NULLABLE_COLUMNS = 524,

    /// Retrieves the supported SQL grammar level as per the ODBC
    /// specification.
    ///
    /// Returns an int32 bitmask value representing the supported SQL grammar
    /// level. The returned bitmask should be parsed in order to retrieve the
    /// supported grammar levels.
    ///
    /// For instance:
    /// - return 0 (0b0)   => [] (SQL grammar is unsupported);
    /// - return 1 (0b1)   => [SQL_MINIMUM_GRAMMAR];
    /// - return 2 (0b10)  => [SQL_CORE_GRAMMAR];
    /// - return 3 (0b11)  => [SQL_MINIMUM_GRAMMAR, SQL_CORE_GRAMMAR];
    /// - return 4 (0b100) => [SQL_EXTENDED_GRAMMAR];
    /// - return 5 (0b101) => [SQL_MINIMUM_GRAMMAR, SQL_EXTENDED_GRAMMAR];
    /// - return 6 (0b110) => [SQL_CORE_GRAMMAR, SQL_EXTENDED_GRAMMAR];
    /// - return 7 (0b111) => [SQL_MINIMUM_GRAMMAR, SQL_CORE_GRAMMAR,
    /// SQL_EXTENDED_GRAMMAR].
    ///
    /// Valid SQL grammar levels are described under
    /// `arrow.flight.protocol.sql.SupportedSqlGrammar`.
    SQL_SUPPORTED_GRAMMAR = 525,

    /// Retrieves the supported ANSI92 SQL grammar level as per the ODBC
    /// specification.
    ///
    /// Returns an int32 bitmask value representing the supported ANSI92 SQL
    /// grammar level. The returned bitmask should be parsed in order to
    /// retrieve the supported grammar levels.
    ///
    /// For instance:
    /// - return 0 (0b0)   => [] (ANSI92 SQL grammar is unsupported);
    /// - return 1 (0b1)   => [ANSI92_ENTRY_SQL];
    /// - return 2 (0b10)  => [ANSI92_INTERMEDIATE_SQL];
    /// - return 3 (0b11)  => [ANSI92_ENTRY_SQL, ANSI92_INTERMEDIATE_SQL];
    /// - return 4 (0b100) => [ANSI92_FULL_SQL];
    /// - return 5 (0b101) => [ANSI92_ENTRY_SQL, ANSI92_FULL_SQL];
    /// - return 6 (0b110) => [ANSI92_INTERMEDIATE_SQL, ANSI92_FULL_SQL];
    /// - return 7 (0b111) => [ANSI92_ENTRY_SQL, ANSI92_INTERMEDIATE_SQL,
    /// ANSI92_FULL_SQL].
    ///
    /// Valid ANSI92 SQL grammar levels are described under
    /// `arrow.flight.protocol.sql.SupportedAnsi92SqlGrammarLevel`.
    SQL_ANSI92_SUPPORTED_LEVEL = 526,

    /// Retrieves a boolean value indicating whether the SQL Integrity
    /// Enhancement Facility is supported.
    ///
    /// Returns:
    /// - false: if the SQL Integrity Enhancement Facility is supported;
    /// - true: if the SQL Integrity Enhancement Facility is supported.
    SQL_SUPPORTS_INTEGRITY_ENHANCEMENT_FACILITY = 527,

    /// Retrieves the support level for SQL OUTER JOINs as an int32 ordinal, as
    /// described in `arrow.flight.protocol.sql.SqlOuterJoinsSupportLevel`.
    SQL_OUTER_JOINS_SUPPORT_LEVEL = 528,

    /// Retrieves a UTF-8 string with the preferred term for "schema".
    SQL_SCHEMA_TERM = 529,

    /// Retrieves a UTF-8 string with the preferred term for "procedure".
    SQL_PROCEDURE_TERM = 530,

    /// Retrieves a UTF-8 string with the preferred term for "catalog".
    SQL_CATALOG_TERM = 531,

    /// Retrieves a boolean value indicating whether a catalog appears at the
    /// start of a fully qualified table name.
    ///
    /// - false: if a catalog does not appear at the start of a fully qualified table
    /// name;
    /// - true: if a catalog appears at the start of a fully qualified table name.
    SQL_CATALOG_AT_START = 532,

    /// Retrieves the supported actions for a SQL database schema as an int32
    /// bitmask value.
    ///
    /// Returns an int32 bitmask value representing the supported actions for a
    /// SQL schema. The returned bitmask should be parsed in order to retrieve
    /// the supported actions for a SQL schema.
    ///
    /// For instance:
    /// - return 0 (0b0)   => [] (no supported actions for SQL schema);
    /// - return 1 (0b1)   => [SQL_ELEMENT_IN_PROCEDURE_CALLS];
    /// - return 2 (0b10)  => [SQL_ELEMENT_IN_INDEX_DEFINITIONS];
    /// - return 3 (0b11)  => [SQL_ELEMENT_IN_PROCEDURE_CALLS,
    /// SQL_ELEMENT_IN_INDEX_DEFINITIONS];
    /// - return 4 (0b100) => [SQL_ELEMENT_IN_PRIVILEGE_DEFINITIONS];
    /// - return 5 (0b101) => [SQL_ELEMENT_IN_PROCEDURE_CALLS,
    /// SQL_ELEMENT_IN_PRIVILEGE_DEFINITIONS];
    /// - return 6 (0b110) => [SQL_ELEMENT_IN_INDEX_DEFINITIONS,
    /// SQL_ELEMENT_IN_PRIVILEGE_DEFINITIONS];
    /// - return 7 (0b111) => [SQL_ELEMENT_IN_PROCEDURE_CALLS,
    /// SQL_ELEMENT_IN_INDEX_DEFINITIONS, SQL_ELEMENT_IN_PRIVILEGE_DEFINITIONS].
    ///
    /// Valid actions for a SQL schema described under
    /// `arrow.flight.protocol.sql.SqlSupportedElementActions`.
    SQL_SCHEMAS_SUPPORTED_ACTIONS = 533,

    /// Retrieves the supported actions for a SQL catalog as an int32 bitmask
    /// value.
    ///
    /// Returns an int32 bitmask value representing the supported actions for a SQL
    /// catalog. The returned bitmask should be parsed in order to retrieve the supported
    /// actions for a SQL catalog.
    ///
    /// For instance:
    /// - return 0 (0b0)   => [] (no supported actions for SQL catalog);
    /// - return 1 (0b1)   => [SQL_ELEMENT_IN_PROCEDURE_CALLS];
    /// - return 2 (0b10)  => [SQL_ELEMENT_IN_INDEX_DEFINITIONS];
    /// - return 3 (0b11)  => [SQL_ELEMENT_IN_PROCEDURE_CALLS,
    /// SQL_ELEMENT_IN_INDEX_DEFINITIONS];
    /// - return 4 (0b100) => [SQL_ELEMENT_IN_PRIVILEGE_DEFINITIONS];
    /// - return 5 (0b101) => [SQL_ELEMENT_IN_PROCEDURE_CALLS,
    /// SQL_ELEMENT_IN_PRIVILEGE_DEFINITIONS];
    /// - return 6 (0b110) => [SQL_ELEMENT_IN_INDEX_DEFINITIONS,
    /// SQL_ELEMENT_IN_PRIVILEGE_DEFINITIONS];
    /// - return 7 (0b111) => [SQL_ELEMENT_IN_PROCEDURE_CALLS,
    /// SQL_ELEMENT_IN_INDEX_DEFINITIONS, SQL_ELEMENT_IN_PRIVILEGE_DEFINITIONS].
    ///
    /// Valid actions for a SQL catalog are described under
    /// `arrow.flight.protocol.sql.SqlSupportedElementActions`.
    SQL_CATALOGS_SUPPORTED_ACTIONS = 534,

    /// Retrieves the supported SQL positioned commands as an int32 bitmask
    /// value.
    ///
    /// Returns an int32 bitmask value representing the supported SQL positioned commands.
    /// The returned bitmask should be parsed in order to retrieve the supported SQL
    /// positioned commands.
    ///
    /// For instance:
    /// - return 0 (0b0)   => [] (no supported SQL positioned commands);
    /// - return 1 (0b1)   => [SQL_POSITIONED_DELETE];
    /// - return 2 (0b10)  => [SQL_POSITIONED_UPDATE];
    /// - return 3 (0b11)  => [SQL_POSITIONED_DELETE, SQL_POSITIONED_UPDATE].
    ///
    /// Valid SQL positioned commands are described under
    /// `arrow.flight.protocol.sql.SqlSupportedPositionedCommands`.
    SQL_SUPPORTED_POSITIONED_COMMANDS = 535,

    /// Retrieves a boolean value indicating whether SELECT FOR UPDATE
    /// statements are supported.
    ///
    /// Returns:
    /// - false: if SELECT FOR UPDATE statements are unsupported;
    /// - true: if SELECT FOR UPDATE statements are supported.
    SQL_SELECT_FOR_UPDATE_SUPPORTED = 536,

    /// Retrieves a boolean value indicating whether stored procedure calls
    /// that use the stored procedure escape syntax are supported.
    ///
    /// Returns:
    /// - false: if stored procedure calls that use the stored procedure escape syntax are
    /// unsupported;
    /// - true: if stored procedure calls that use the stored procedure escape syntax are
    /// supported.
    SQL_STORED_PROCEDURES_SUPPORTED = 537,

    /// Retrieves the types of supported SQL subqueries as an int32 bitmask
    /// value.
    ///
    /// Returns an int32 bitmask value representing the supported SQL
    /// subqueries. The returned bitmask should be parsed in order to retrieve
    /// the supported SQL subqueries.
    ///
    /// For instance:
    /// - return 0   (0b0)     => [] (no supported SQL subqueries);
    /// - return 1   (0b1)     => [SQL_SUBQUERIES_IN_COMPARISONS];
    /// - return 2   (0b10)    => [SQL_SUBQUERIES_IN_EXISTS];
    /// - return 3   (0b11)    => [SQL_SUBQUERIES_IN_COMPARISONS,
    /// SQL_SUBQUERIES_IN_EXISTS];
    /// - return 4   (0b100)   => [SQL_SUBQUERIES_IN_INS];
    /// - return 5   (0b101)   => [SQL_SUBQUERIES_IN_COMPARISONS, SQL_SUBQUERIES_IN_INS];
    /// - return 6   (0b110)   => [SQL_SUBQUERIES_IN_COMPARISONS,
    /// SQL_SUBQUERIES_IN_EXISTS];
    /// - return 7   (0b111)   => [SQL_SUBQUERIES_IN_COMPARISONS,
    /// SQL_SUBQUERIES_IN_EXISTS,
    /// SQL_SUBQUERIES_IN_INS];
    /// - return 8   (0b1000)  => [SQL_SUBQUERIES_IN_QUANTIFIEDS];
    /// - return 9   (0b1001)  => [SQL_SUBQUERIES_IN_COMPARISONS,
    /// SQL_SUBQUERIES_IN_QUANTIFIEDS];
    /// - return 10  (0b1010)  => [SQL_SUBQUERIES_IN_EXISTS,
    /// SQL_SUBQUERIES_IN_QUANTIFIEDS];
    /// - return 11  (0b1011)  => [SQL_SUBQUERIES_IN_COMPARISONS,
    /// SQL_SUBQUERIES_IN_EXISTS,
    /// SQL_SUBQUERIES_IN_QUANTIFIEDS];
    /// - return 12  (0b1100)  => [SQL_SUBQUERIES_IN_INS, SQL_SUBQUERIES_IN_QUANTIFIEDS];
    /// - return 13  (0b1101)  => [SQL_SUBQUERIES_IN_COMPARISONS, SQL_SUBQUERIES_IN_INS,
    /// SQL_SUBQUERIES_IN_QUANTIFIEDS];
    /// - return 14  (0b1110)  => [SQL_SUBQUERIES_IN_EXISTS, SQL_SUBQUERIES_IN_INS,
    /// SQL_SUBQUERIES_IN_QUANTIFIEDS];
    /// - return 15  (0b1111)  => [SQL_SUBQUERIES_IN_COMPARISONS,
    /// SQL_SUBQUERIES_IN_EXISTS,
    /// SQL_SUBQUERIES_IN_INS, SQL_SUBQUERIES_IN_QUANTIFIEDS];
    /// - ...
    ///
    /// Valid SQL subqueries are described under
    /// `arrow.flight.protocol.sql.SqlSupportedSubqueries`.
    SQL_SUPPORTED_SUBQUERIES = 538,

    /// Retrieves a boolean value indicating whether correlated subqueries are
    /// supported.
    ///
    /// Returns:
    /// - false: if correlated subqueries are unsupported;
    /// - true: if correlated subqueries are supported.
    SQL_CORRELATED_SUBQUERIES_SUPPORTED = 539,

    /// Retrieves the supported SQL UNION features as an int32 bitmask
    /// value.
    ///
    /// Returns an int32 bitmask value representing the supported SQL UNIONs.
    /// The returned bitmask should be parsed in order to retrieve the supported SQL
    /// UNIONs.
    ///
    /// For instance:
    /// - return 0 (0b0)   => [] (no supported SQL positioned commands);
    /// - return 1 (0b1)   => [SQL_UNION];
    /// - return 2 (0b10)  => [SQL_UNION_ALL];
    /// - return 3 (0b11)  => [SQL_UNION, SQL_UNION_ALL].
    ///
    /// Valid SQL union operators are described under
    /// `arrow.flight.protocol.sql.SqlSupportedUnions`.
    SQL_SUPPORTED_UNIONS = 540,

    /// Retrieves a int64 value representing the maximum number of hex
    /// characters allowed in an inline binary literal.
    SQL_MAX_BINARY_LITERAL_LENGTH = 541,

    /// Retrieves a int64 value representing the maximum number of characters
    /// allowed for a character literal.
    SQL_MAX_CHAR_LITERAL_LENGTH = 542,

    /// Retrieves a int64 value representing the maximum number of characters
    /// allowed for a column name.
    SQL_MAX_COLUMN_NAME_LENGTH = 543,

    /// Retrieves a int64 value representing the maximum number of columns
    /// allowed in a GROUP BY clause.
    SQL_MAX_COLUMNS_IN_GROUP_BY = 544,

    /// Retrieves a int64 value representing the maximum number of columns
    /// allowed in an index.
    SQL_MAX_COLUMNS_IN_INDEX = 545,

    /// Retrieves a int64 value representing the maximum number of columns
    /// allowed in an ORDER BY clause.
    SQL_MAX_COLUMNS_IN_ORDER_BY = 546,

    /// Retrieves a int64 value representing the maximum number of columns
    /// allowed in a SELECT list.
    SQL_MAX_COLUMNS_IN_SELECT = 547,

    /// Retrieves a int64 value representing the maximum number of columns
    /// allowed in a table.
    SQL_MAX_COLUMNS_IN_TABLE = 548,

    /// Retrieves a int64 value representing the maximum number of concurrent
    /// connections possible.
    SQL_MAX_CONNECTIONS = 549,

    /// Retrieves a int64 value the maximum number of characters allowed in a
    /// cursor name.
    SQL_MAX_CURSOR_NAME_LENGTH = 550,

    /// Retrieves a int64 value representing the maximum number of bytes
    /// allowed for an index, including all of the parts of the index.
    SQL_MAX_INDEX_LENGTH = 551,

    /// Retrieves a int64 value representing the maximum number of characters
    /// allowed in a procedure name.
    SQL_SCHEMA_NAME_LENGTH = 552,

    /// Retrieves a int64 value representing the maximum number of bytes
    /// allowed in a single row.
    SQL_MAX_PROCEDURE_NAME_LENGTH = 553,

    /// Retrieves a int64 value representing the maximum number of characters
    /// allowed in a catalog name.
    SQL_MAX_CATALOG_NAME_LENGTH = 554,

    /// Retrieves a int64 value representing the maximum number of bytes
    /// allowed in a single row.
    SQL_MAX_ROW_SIZE = 555,

    /// Retrieves a boolean indicating whether the return value for the JDBC
    /// method getMaxRowSize includes the SQL data types LONGVARCHAR and
    /// LONGVARBINARY.
    ///
    /// Returns:
    /// - false: if return value for the JDBC method getMaxRowSize does
    ///          not include the SQL data types LONGVARCHAR and LONGVARBINARY;
    /// - true: if return value for the JDBC method getMaxRowSize includes
    ///         the SQL data types LONGVARCHAR and LONGVARBINARY.
    SQL_MAX_ROW_SIZE_INCLUDES_BLOBS = 556,

    /// Retrieves a int32 value representing the maximum number of characters
    /// allowed for an SQL statement; a result of 0 (zero) means that there is
    /// no limit or the limit is not known.
    SQL_MAX_STATEMENT_LENGTH = 557,

    /// Retrieves a int32 value representing the maximum number of active
    /// statements that can be open at the same time.
    SQL_MAX_STATEMENTS = 558,

    /// Retrieves a int32 value representing the maximum number of characters
    /// allowed in a table name.
    SQL_MAX_TABLE_NAME_LENGTH = 559,

    /// Retrieves a int32 value representing the maximum number of tables
    /// allowed in a SELECT statement.
    SQL_MAX_TABLES_IN_SELECT = 560,

    /// Retrieves a int32 value representing the maximum number of characters
    /// allowed in a user name.
    SQL_MAX_USERNAME_LENGTH = 561,

    /// Retrieves this database's default transaction isolation level as
    /// described in `arrow.flight.protocol.sql.SqlTransactionIsolationLevel`.
    ///
    /// Returns a int32 ordinal for the SQL transaction isolation level.
    SQL_DEFAULT_TRANSACTION_ISOLATION = 562,

    /// Retrieves a boolean value indicating whether transactions are
    /// supported. If not, invoking the method commit is a noop, and the
    /// isolation level is
    /// `arrow.flight.protocol.sql.SqlTransactionIsolationLevel.TRANSACTION_NONE`.
    ///
    /// Returns:
    /// - false: if transactions are unsupported;
    /// - true: if transactions are supported.
    SQL_TRANSACTIONS_SUPPORTED = 563,

    /// Retrieves the supported transactions isolation levels, if transactions
    /// are supported.
    ///
    /// Returns an int32 bitmask value representing the supported transactions
    /// isolation levels. The returned bitmask should be parsed in order to
    /// retrieve the supported transactions isolation levels.
    ///
    /// For instance:
    /// - return 0   (0b0)     => [] (no supported SQL transactions isolation levels);
    /// - return 1   (0b1)     => [SQL_TRANSACTION_NONE];
    /// - return 2   (0b10)    => [SQL_TRANSACTION_READ_UNCOMMITTED];
    /// - return 3   (0b11)    => [SQL_TRANSACTION_NONE,
    /// SQL_TRANSACTION_READ_UNCOMMITTED];
    /// - return 4   (0b100)   => [SQL_TRANSACTION_REPEATABLE_READ];
    /// - return 5   (0b101)   => [SQL_TRANSACTION_NONE, SQL_TRANSACTION_REPEATABLE_READ];
    /// - return 6   (0b110)   => [SQL_TRANSACTION_READ_UNCOMMITTED,
    /// SQL_TRANSACTION_REPEATABLE_READ];
    /// - return 7   (0b111)   => [SQL_TRANSACTION_NONE, SQL_TRANSACTION_READ_UNCOMMITTED,
    /// SQL_TRANSACTION_REPEATABLE_READ];
    /// - return 8   (0b1000)  => [SQL_TRANSACTION_REPEATABLE_READ];
    /// - return 9   (0b1001)  => [SQL_TRANSACTION_NONE, SQL_TRANSACTION_REPEATABLE_READ];
    /// - return 10  (0b1010)  => [SQL_TRANSACTION_READ_UNCOMMITTED,
    /// SQL_TRANSACTION_REPEATABLE_READ];
    /// - return 11  (0b1011)  => [SQL_TRANSACTION_NONE, SQL_TRANSACTION_READ_UNCOMMITTED,
    /// SQL_TRANSACTION_REPEATABLE_READ];
    /// - return 12  (0b1100)  => [SQL_TRANSACTION_REPEATABLE_READ,
    /// SQL_TRANSACTION_REPEATABLE_READ];
    /// - return 13  (0b1101)  => [SQL_TRANSACTION_NONE, SQL_TRANSACTION_REPEATABLE_READ,
    /// SQL_TRANSACTION_REPEATABLE_READ];
    /// - return 14  (0b1110)  => [SQL_TRANSACTION_READ_UNCOMMITTED,
    /// SQL_TRANSACTION_REPEATABLE_READ, SQL_TRANSACTION_REPEATABLE_READ];
    /// - return 15  (0b1111)  => [SQL_TRANSACTION_NONE, SQL_TRANSACTION_READ_UNCOMMITTED,
    /// SQL_TRANSACTION_REPEATABLE_READ, SQL_TRANSACTION_REPEATABLE_READ];
    /// - return 16  (0b10000) => [SQL_TRANSACTION_SERIALIZABLE];
    /// - ...
    ///
    /// Valid SQL positioned commands are described under
    /// `arrow.flight.protocol.sql.SqlTransactionIsolationLevel`.
    SQL_SUPPORTED_TRANSACTIONS_ISOLATION_LEVELS = 564,

    /// Retrieves a boolean value indicating whether a data definition
    /// statement within a transaction forces the transaction to commit.
    ///
    /// Returns:
    /// - false: if a data definition statement within a transaction does not force the
    /// transaction to commit;
    /// - true: if a data definition statement within a transaction forces the transaction
    /// to commit.
    SQL_DATA_DEFINITION_CAUSES_TRANSACTION_COMMIT = 565,

    /// Retrieves a boolean value indicating whether a data definition
    /// statement within a transaction is ignored.
    ///
    /// Returns:
    /// - false: if a data definition statement within a transaction is taken into
    /// account;
    /// - true: a data definition statement within a transaction is ignored.
    SQL_DATA_DEFINITIONS_IN_TRANSACTIONS_IGNORED = 566,

    /// Retrieves an int32 bitmask value representing the supported result set
    /// types. The returned bitmask should be parsed in order to retrieve the
    /// supported result set types.
    ///
    /// For instance:
    /// - return 0   (0b0)     => [] (no supported result set types);
    /// - return 1   (0b1)     => [SQL_RESULT_SET_TYPE_UNSPECIFIED];
    /// - return 2   (0b10)    => [SQL_RESULT_SET_TYPE_FORWARD_ONLY];
    /// - return 3   (0b11)    => [SQL_RESULT_SET_TYPE_UNSPECIFIED,
    /// SQL_RESULT_SET_TYPE_FORWARD_ONLY];
    /// - return 4   (0b100)   => [SQL_RESULT_SET_TYPE_SCROLL_INSENSITIVE];
    /// - return 5   (0b101)   => [SQL_RESULT_SET_TYPE_UNSPECIFIED,
    /// SQL_RESULT_SET_TYPE_SCROLL_INSENSITIVE];
    /// - return 6   (0b110)   => [SQL_RESULT_SET_TYPE_FORWARD_ONLY,
    /// SQL_RESULT_SET_TYPE_SCROLL_INSENSITIVE];
    /// - return 7   (0b111)   => [SQL_RESULT_SET_TYPE_UNSPECIFIED,
    /// SQL_RESULT_SET_TYPE_FORWARD_ONLY, SQL_RESULT_SET_TYPE_SCROLL_INSENSITIVE];
    /// - return 8   (0b1000)  => [SQL_RESULT_SET_TYPE_SCROLL_SENSITIVE];
    /// - ...
    ///
    /// Valid result set types are described under
    /// `arrow.flight.protocol.sql.SqlSupportedResultSetType`.
    SQL_SUPPORTED_RESULT_SET_TYPES = 567,

    /// Returns an int32 bitmask value representing the concurrency types
    /// supported by the server for
    /// `SqlSupportedResultSetType.SQL_RESULT_SET_TYPE_UNSPECIFIED`.
    ///
    /// For instance:
    /// - return 0 (0b0)   => [] (no supported concurrency types for this result set type)
    /// - return 1 (0b1)   => [SQL_RESULT_SET_CONCURRENCY_UNSPECIFIED]
    /// - return 2 (0b10)  => [SQL_RESULT_SET_CONCURRENCY_READ_ONLY]
    /// - return 3 (0b11)  => [SQL_RESULT_SET_CONCURRENCY_UNSPECIFIED,
    /// SQL_RESULT_SET_CONCURRENCY_READ_ONLY]
    /// - return 4 (0b100) => [SQL_RESULT_SET_CONCURRENCY_UPDATABLE]
    /// - return 5 (0b101) => [SQL_RESULT_SET_CONCURRENCY_UNSPECIFIED,
    /// SQL_RESULT_SET_CONCURRENCY_UPDATABLE]
    /// - return 6 (0b110)  => [SQL_RESULT_SET_CONCURRENCY_READ_ONLY,
    /// SQL_RESULT_SET_CONCURRENCY_UPDATABLE]
    /// - return 7 (0b111)  => [SQL_RESULT_SET_CONCURRENCY_UNSPECIFIED,
    /// SQL_RESULT_SET_CONCURRENCY_READ_ONLY, SQL_RESULT_SET_CONCURRENCY_UPDATABLE]
    ///
    /// Valid result set types are described under
    /// `arrow.flight.protocol.sql.SqlSupportedResultSetConcurrency`.
    SQL_SUPPORTED_CONCURRENCIES_FOR_RESULT_SET_UNSPECIFIED = 568,

    /// Returns an int32 bitmask value representing the concurrency types
    /// supported by the server for
    /// `SqlSupportedResultSetType.SQL_RESULT_SET_TYPE_FORWARD_ONLY`.
    ///
    /// For instance:
    /// - return 0 (0b0)   => [] (no supported concurrency types for this result set type)
    /// - return 1 (0b1)   => [SQL_RESULT_SET_CONCURRENCY_UNSPECIFIED]
    /// - return 2 (0b10)  => [SQL_RESULT_SET_CONCURRENCY_READ_ONLY]
    /// - return 3 (0b11)  => [SQL_RESULT_SET_CONCURRENCY_UNSPECIFIED,
    /// SQL_RESULT_SET_CONCURRENCY_READ_ONLY]
    /// - return 4 (0b100) => [SQL_RESULT_SET_CONCURRENCY_UPDATABLE]
    /// - return 5 (0b101) => [SQL_RESULT_SET_CONCURRENCY_UNSPECIFIED,
    /// SQL_RESULT_SET_CONCURRENCY_UPDATABLE]
    /// - return 6 (0b110)  => [SQL_RESULT_SET_CONCURRENCY_READ_ONLY,
    /// SQL_RESULT_SET_CONCURRENCY_UPDATABLE]
    /// - return 7 (0b111)  => [SQL_RESULT_SET_CONCURRENCY_UNSPECIFIED,
    /// SQL_RESULT_SET_CONCURRENCY_READ_ONLY, SQL_RESULT_SET_CONCURRENCY_UPDATABLE]
    ///
    /// Valid result set types are described under
    /// `arrow.flight.protocol.sql.SqlSupportedResultSetConcurrency`.
    SQL_SUPPORTED_CONCURRENCIES_FOR_RESULT_SET_FORWARD_ONLY = 569,

    /// Returns an int32 bitmask value representing the concurrency types
    /// supported by the server for
    /// `SqlSupportedResultSetType.SQL_RESULT_SET_TYPE_SCROLL_SENSITIVE`.
    ///
    /// For instance:
    /// - return 0 (0b0)   => [] (no supported concurrency types for this result set type)
    /// - return 1 (0b1)   => [SQL_RESULT_SET_CONCURRENCY_UNSPECIFIED]
    /// - return 2 (0b10)  => [SQL_RESULT_SET_CONCURRENCY_READ_ONLY]
    /// - return 3 (0b11)  => [SQL_RESULT_SET_CONCURRENCY_UNSPECIFIED,
    /// SQL_RESULT_SET_CONCURRENCY_READ_ONLY]
    /// - return 4 (0b100) => [SQL_RESULT_SET_CONCURRENCY_UPDATABLE]
    /// - return 5 (0b101) => [SQL_RESULT_SET_CONCURRENCY_UNSPECIFIED,
    /// SQL_RESULT_SET_CONCURRENCY_UPDATABLE]
    /// - return 6 (0b110)  => [SQL_RESULT_SET_CONCURRENCY_READ_ONLY,
    /// SQL_RESULT_SET_CONCURRENCY_UPDATABLE]
    /// - return 7 (0b111)  => [SQL_RESULT_SET_CONCURRENCY_UNSPECIFIED,
    /// SQL_RESULT_SET_CONCURRENCY_READ_ONLY, SQL_RESULT_SET_CONCURRENCY_UPDATABLE]
    ///
    /// Valid result set types are described under
    /// `arrow.flight.protocol.sql.SqlSupportedResultSetConcurrency`.
    SQL_SUPPORTED_CONCURRENCIES_FOR_RESULT_SET_SCROLL_SENSITIVE = 570,

    /// Returns an int32 bitmask value representing concurrency types supported
    /// by the server for
    /// `SqlSupportedResultSetType.SQL_RESULT_SET_TYPE_SCROLL_INSENSITIVE`.
    ///
    /// For instance:
    /// - return 0 (0b0)   => [] (no supported concurrency types for this result set type)
    /// - return 1 (0b1)   => [SQL_RESULT_SET_CONCURRENCY_UNSPECIFIED]
    /// - return 2 (0b10)  => [SQL_RESULT_SET_CONCURRENCY_READ_ONLY]
    /// - return 3 (0b11)  => [SQL_RESULT_SET_CONCURRENCY_UNSPECIFIED,
    /// SQL_RESULT_SET_CONCURRENCY_READ_ONLY]
    /// - return 4 (0b100) => [SQL_RESULT_SET_CONCURRENCY_UPDATABLE]
    /// - return 5 (0b101) => [SQL_RESULT_SET_CONCURRENCY_UNSPECIFIED,
    /// SQL_RESULT_SET_CONCURRENCY_UPDATABLE]
    /// - return 6 (0b110)  => [SQL_RESULT_SET_CONCURRENCY_READ_ONLY,
    /// SQL_RESULT_SET_CONCURRENCY_UPDATABLE]
    /// - return 7 (0b111)  => [SQL_RESULT_SET_CONCURRENCY_UNSPECIFIED,
    /// SQL_RESULT_SET_CONCURRENCY_READ_ONLY, SQL_RESULT_SET_CONCURRENCY_UPDATABLE]
    ///
    /// Valid result set types are described under
    /// `arrow.flight.protocol.sql.SqlSupportedResultSetConcurrency`.
    SQL_SUPPORTED_CONCURRENCIES_FOR_RESULT_SET_SCROLL_INSENSITIVE = 571,

    /// Retrieves a boolean value indicating whether this database supports batch updates.
    ///
    /// - false: if this database does not support batch updates;
    /// - true: if this database supports batch updates.
    SQL_BATCH_UPDATES_SUPPORTED = 572,

    /// Retrieves a boolean value indicating whether this database supports savepoints.
    ///
    /// Returns:
    /// - false: if this database does not support savepoints;
    /// - true: if this database supports savepoints.
    SQL_SAVEPOINTS_SUPPORTED = 573,

    /// Retrieves a boolean value indicating whether named parameters are supported in
    /// callable statements.
    ///
    /// Returns:
    /// - false: if named parameters in callable statements are unsupported;
    /// - true: if named parameters in callable statements are supported.
    SQL_NAMED_PARAMETERS_SUPPORTED = 574,

    /// Retrieves a boolean value indicating whether updates made to a LOB are made on a
    /// copy or directly to the LOB.
    ///
    /// Returns:
    /// - false: if updates made to a LOB are made directly to the LOB;
    /// - true: if updates made to a LOB are made on a copy.
    SQL_LOCATORS_UPDATE_COPY = 575,

    /// Retrieves a boolean value indicating whether invoking user-defined or vendor
    /// functions using the stored procedure escape syntax is supported.
    ///
    /// Returns:
    /// - false: if invoking user-defined or vendor functions using the stored procedure
    /// escape syntax is unsupported;
    /// - true: if invoking user-defined or vendor functions using the stored procedure
    /// escape syntax is supported.
    SQL_STORED_FUNCTIONS_USING_CALL_SYNTAX_SUPPORTED = 576,

    /// @}
  };

  /// The level of support for Flight SQL transaction RPCs.
  enum SqlSupportedTransaction {
    /// Unknown/not indicated/no support
    SQL_SUPPORTED_TRANSACTION_NONE = 0,
    /// Transactions, but not savepoints.
    SQL_SUPPORTED_TRANSACTION_TRANSACTION = 1,
    /// Transactions and savepoints.
    SQL_SUPPORTED_TRANSACTION_SAVEPOINT = 2,
  };

  /// Indicate whether something (e.g. an identifier) is case-sensitive.
  enum SqlSupportedCaseSensitivity {
    SQL_CASE_SENSITIVITY_UNKNOWN = 0,
    SQL_CASE_SENSITIVITY_CASE_INSENSITIVE = 1,
    SQL_CASE_SENSITIVITY_UPPERCASE = 2,
  };

  /// Indicate how nulls are sorted.
  enum SqlNullOrdering {
    SQL_NULLS_SORTED_HIGH = 0,
    SQL_NULLS_SORTED_LOW = 1,
    SQL_NULLS_SORTED_AT_START = 2,
    SQL_NULLS_SORTED_AT_END = 3,
  };

  /// Type identifiers used to indicate support for converting between types.
  enum SqlSupportsConvert {
    SQL_CONVERT_BIGINT = 0,
    SQL_CONVERT_BINARY = 1,
    SQL_CONVERT_BIT = 2,
    SQL_CONVERT_CHAR = 3,
    SQL_CONVERT_DATE = 4,
    SQL_CONVERT_DECIMAL = 5,
    SQL_CONVERT_FLOAT = 6,
    SQL_CONVERT_INTEGER = 7,
    SQL_CONVERT_INTERVAL_DAY_TIME = 8,
    SQL_CONVERT_INTERVAL_YEAR_MONTH = 9,
    SQL_CONVERT_LONGVARBINARY = 10,
    SQL_CONVERT_LONGVARCHAR = 11,
    SQL_CONVERT_NUMERIC = 12,
    SQL_CONVERT_REAL = 13,
    SQL_CONVERT_SMALLINT = 14,
    SQL_CONVERT_TIME = 15,
    SQL_CONVERT_TIMESTAMP = 16,
    SQL_CONVERT_TINYINT = 17,
    SQL_CONVERT_VARBINARY = 18,
    SQL_CONVERT_VARCHAR = 19,
  };
};

/// \brief A SQL %table reference, optionally containing table's catalog and db_schema.
struct ARROW_FLIGHT_SQL_EXPORT TableRef {
  /// \brief The table's catalog.
  std::optional<std::string> catalog;
  /// \brief The table's database schema.
  std::optional<std::string> db_schema;
  /// \brief The table name.
  std::string table;
};

/// \brief A Substrait plan to be executed, along with associated metadata.
struct ARROW_FLIGHT_SQL_EXPORT SubstraitPlan {
  /// \brief The serialized plan.
  std::string plan;
  /// \brief The Substrait release, e.g. "0.12.0".
  std::string version;
};

/// \brief The result of cancelling a query.
enum class CancelResult : int8_t {
  kUnspecified,
  kCancelled,
  kCancelling,
  kNotCancellable,
};

ARROW_FLIGHT_SQL_EXPORT
std::ostream& operator<<(std::ostream& os, CancelResult result);

/// @}

}  // namespace sql
}  // namespace flight
}  // namespace arrow
