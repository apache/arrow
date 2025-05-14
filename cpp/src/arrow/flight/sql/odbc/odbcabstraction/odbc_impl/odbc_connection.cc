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

#include "arrow/flight/sql/odbc/odbcabstraction/include/odbcabstraction/odbc_impl/odbc_connection.h"

#include "arrow/flight/sql/odbc/odbcabstraction/include/odbcabstraction/exceptions.h"
#include "arrow/flight/sql/odbc/odbcabstraction/include/odbcabstraction/odbc_impl/attribute_utils.h"
#include "arrow/flight/sql/odbc/odbcabstraction/include/odbcabstraction/odbc_impl/odbc_descriptor.h"
#include "arrow/flight/sql/odbc/odbcabstraction/include/odbcabstraction/odbc_impl/odbc_environment.h"
#include "arrow/flight/sql/odbc/odbcabstraction/include/odbcabstraction/odbc_impl/odbc_statement.h"
#include "arrow/flight/sql/odbc/odbcabstraction/include/odbcabstraction/spi/connection.h"
#include "arrow/flight/sql/odbc/odbcabstraction/include/odbcabstraction/spi/statement.h"

#include <odbcinst.h>
#include <sql.h>
#include <sqlext.h>
#include <boost/algorithm/string.hpp>
#include <boost/xpressive/xpressive.hpp>
#include <iterator>
#include <memory>
#include <utility>

using ODBC::ODBCConnection;
using ODBC::ODBCDescriptor;
using ODBC::ODBCStatement;

using driver::odbcabstraction::Connection;
using driver::odbcabstraction::Diagnostics;
using driver::odbcabstraction::DriverException;
using driver::odbcabstraction::Statement;

namespace {
// Key-value pairs separated by semi-colon.
// Note that the value can be wrapped in curly braces to escape other significant
// characters such as semi-colons and equals signs. NOTE: This can be optimized to be
// built statically.
const boost::xpressive::sregex CONNECTION_STR_REGEX(
    boost::xpressive::sregex::compile("([^=;]+)=({.+}|[^=;]+|[^;])"));

// Load properties from the given DSN. The properties loaded do _not_ overwrite existing
// entries in the properties.
void loadPropertiesFromDSN(const std::string& dsn,
                           Connection::ConnPropertyMap& properties) {
  const size_t BUFFER_SIZE = 1024 * 10;
  std::vector<char> outputBuffer;
  outputBuffer.resize(BUFFER_SIZE, '\0');
  SQLSetConfigMode(ODBC_BOTH_DSN);

  SQLGetPrivateProfileString(dsn.c_str(), NULL, "", &outputBuffer[0], BUFFER_SIZE,
                             "odbc.ini");

  // The output buffer holds the list of keys in a series of NUL-terminated strings.
  // The series is terminated with an empty string (eg a NUL-terminator terminating the
  // last key followed by a NUL terminator after).
  std::vector<std::string_view> keys;
  size_t pos = 0;
  while (pos < BUFFER_SIZE) {
    std::string key(&outputBuffer[pos]);
    if (key.empty()) {
      break;
    }
    size_t len = key.size();

    // Skip over Driver or DSN keys.
    if (!boost::iequals(key, "DSN") && !boost::iequals(key, "Driver")) {
      keys.emplace_back(std::move(key));
    }
    pos += len + 1;
  }

  for (auto& key : keys) {
    outputBuffer.clear();
    outputBuffer.resize(BUFFER_SIZE, '\0');

    std::string key_str = std::string(key);
    SQLGetPrivateProfileString(dsn.c_str(), key_str.c_str(), "", &outputBuffer[0],
                               BUFFER_SIZE, "odbc.ini");

    std::string value = std::string(&outputBuffer[0]);
    auto propIter = properties.find(key);
    if (propIter == properties.end()) {
      properties.emplace(std::make_pair(std::move(key), std::move(value)));
    }
  }
}

}  // namespace

// Public
// =========================================================================================
ODBCConnection::ODBCConnection(ODBCEnvironment& environment,
                               std::shared_ptr<Connection> spiConnection)
    : m_environment(environment),
      m_spiConnection(std::move(spiConnection)),
      m_is2xConnection(environment.getODBCVersion() == SQL_OV_ODBC2),
      m_isConnected(false) {}

Diagnostics& ODBCConnection::GetDiagnostics_Impl() {
  return m_spiConnection->GetDiagnostics();
}

bool ODBCConnection::isConnected() const { return m_isConnected; }

const std::string& ODBCConnection::GetDSN() const { return m_dsn; }

void ODBCConnection::connect(std::string dsn,
                             const Connection::ConnPropertyMap& properties,
                             std::vector<std::string_view>& missing_properties) {
  if (m_isConnected) {
    throw DriverException("Already connected.", "HY010");
  }

  m_dsn = std::move(dsn);
  m_spiConnection->Connect(properties, missing_properties);
  m_isConnected = true;
  std::shared_ptr<Statement> spiStatement = m_spiConnection->CreateStatement();
  m_attributeTrackingStatement = std::make_shared<ODBCStatement>(*this, spiStatement);
}

void ODBCConnection::GetInfo(SQLUSMALLINT infoType, SQLPOINTER value,
                             SQLSMALLINT bufferLength, SQLSMALLINT* outputLength,
                             bool isUnicode) {
  switch (infoType) {
    case SQL_ACTIVE_ENVIRONMENTS:
      GetAttribute(static_cast<SQLUSMALLINT>(0), value, bufferLength, outputLength);
      break;
#ifdef SQL_ASYNC_DBC_FUNCTIONS
    case SQL_ASYNC_DBC_FUNCTIONS:
      GetAttribute(static_cast<SQLUINTEGER>(SQL_ASYNC_DBC_NOT_CAPABLE), value,
                   bufferLength, outputLength);
      break;
#endif
    case SQL_ASYNC_MODE:
      GetAttribute(static_cast<SQLUINTEGER>(SQL_AM_NONE), value, bufferLength,
                   outputLength);
      break;
#ifdef SQL_ASYNC_NOTIFICATION
    case SQL_ASYNC_NOTIFICATION:
      GetAttribute(static_cast<SQLUINTEGER>(SQL_ASYNC_NOTIFICATION_NOT_CAPABLE), value,
                   bufferLength, outputLength);
      break;
#endif
    case SQL_BATCH_ROW_COUNT:
      GetAttribute(static_cast<SQLUINTEGER>(0), value, bufferLength, outputLength);
      break;
    case SQL_BATCH_SUPPORT:
      GetAttribute(static_cast<SQLUINTEGER>(0), value, bufferLength, outputLength);
      break;
    case SQL_DATA_SOURCE_NAME:
      GetStringAttribute(isUnicode, m_dsn, true, value, bufferLength, outputLength,
                         GetDiagnostics());
      break;
    case SQL_DRIVER_ODBC_VER:
      GetStringAttribute(isUnicode, "03.80", true, value, bufferLength, outputLength,
                         GetDiagnostics());
      break;
    case SQL_DYNAMIC_CURSOR_ATTRIBUTES1:
      GetAttribute(static_cast<SQLUINTEGER>(0), value, bufferLength, outputLength);
      break;
    case SQL_DYNAMIC_CURSOR_ATTRIBUTES2:
      GetAttribute(static_cast<SQLUINTEGER>(0), value, bufferLength, outputLength);
      break;
    case SQL_FORWARD_ONLY_CURSOR_ATTRIBUTES1:
      GetAttribute(static_cast<SQLUINTEGER>(SQL_CA1_NEXT), value, bufferLength,
                   outputLength);
      break;
    case SQL_FORWARD_ONLY_CURSOR_ATTRIBUTES2:
      GetAttribute(static_cast<SQLUINTEGER>(SQL_CA2_READ_ONLY_CONCURRENCY), value,
                   bufferLength, outputLength);
      break;
    case SQL_FILE_USAGE:
      GetAttribute(static_cast<SQLUSMALLINT>(SQL_FILE_NOT_SUPPORTED), value, bufferLength,
                   outputLength);
      break;
    case SQL_KEYSET_CURSOR_ATTRIBUTES1:
      GetAttribute(static_cast<SQLUINTEGER>(0), value, bufferLength, outputLength);
      break;
    case SQL_KEYSET_CURSOR_ATTRIBUTES2:
      GetAttribute(static_cast<SQLUINTEGER>(0), value, bufferLength, outputLength);
      break;
    case SQL_MAX_ASYNC_CONCURRENT_STATEMENTS:
      GetAttribute(static_cast<SQLUINTEGER>(0), value, bufferLength, outputLength);
      break;
    case SQL_ODBC_INTERFACE_CONFORMANCE:
      GetAttribute(static_cast<SQLUINTEGER>(SQL_OIC_CORE), value, bufferLength,
                   outputLength);
      break;
    // case SQL_ODBC_STANDARD_CLI_CONFORMANCE: - mentioned in SQLGetInfo spec with no
    // description and there is no constant for this.
    case SQL_PARAM_ARRAY_ROW_COUNTS:
      GetAttribute(static_cast<SQLUINTEGER>(SQL_PARC_NO_BATCH), value, bufferLength,
                   outputLength);
      break;
    case SQL_PARAM_ARRAY_SELECTS:
      GetAttribute(static_cast<SQLUINTEGER>(SQL_PAS_NO_SELECT), value, bufferLength,
                   outputLength);
      break;
    case SQL_ROW_UPDATES:
      GetStringAttribute(isUnicode, "N", true, value, bufferLength, outputLength,
                         GetDiagnostics());
      break;
    case SQL_SCROLL_OPTIONS:
      GetAttribute(static_cast<SQLUINTEGER>(SQL_SO_FORWARD_ONLY), value, bufferLength,
                   outputLength);
      break;
    case SQL_STATIC_CURSOR_ATTRIBUTES1:
      GetAttribute(static_cast<SQLUINTEGER>(0), value, bufferLength, outputLength);
      break;
    case SQL_STATIC_CURSOR_ATTRIBUTES2:
      GetAttribute(static_cast<SQLUINTEGER>(0), value, bufferLength, outputLength);
      break;
    case SQL_BOOKMARK_PERSISTENCE:
      GetAttribute(static_cast<SQLUINTEGER>(0), value, bufferLength, outputLength);
      break;
    case SQL_DESCRIBE_PARAMETER:
      GetStringAttribute(isUnicode, "N", true, value, bufferLength, outputLength,
                         GetDiagnostics());
      break;
    case SQL_MULT_RESULT_SETS:
      GetStringAttribute(isUnicode, "N", true, value, bufferLength, outputLength,
                         GetDiagnostics());
      break;
    case SQL_MULTIPLE_ACTIVE_TXN:
      GetStringAttribute(isUnicode, "N", true, value, bufferLength, outputLength,
                         GetDiagnostics());
      break;
    case SQL_NEED_LONG_DATA_LEN:
      GetStringAttribute(isUnicode, "N", true, value, bufferLength, outputLength,
                         GetDiagnostics());
      break;
    case SQL_TXN_CAPABLE:
      GetAttribute(static_cast<SQLUSMALLINT>(SQL_TC_NONE), value, bufferLength,
                   outputLength);
      break;
    case SQL_TXN_ISOLATION_OPTION:
      GetAttribute(static_cast<SQLUINTEGER>(0), value, bufferLength, outputLength);
      break;
    case SQL_TABLE_TERM:
      GetStringAttribute(isUnicode, "table", true, value, bufferLength, outputLength,
                         GetDiagnostics());
      break;
    // Deprecated ODBC 2.x fields required for backwards compatibility.
    case SQL_ODBC_API_CONFORMANCE:
      GetAttribute(static_cast<SQLUSMALLINT>(SQL_OAC_LEVEL1), value, bufferLength,
                   outputLength);
      break;
    case SQL_FETCH_DIRECTION:
      GetAttribute(static_cast<SQLINTEGER>(SQL_FETCH_NEXT), value, bufferLength,
                   outputLength);
      break;
    case SQL_LOCK_TYPES:
      GetAttribute(static_cast<SQLINTEGER>(0), value, bufferLength, outputLength);
      break;
    case SQL_POS_OPERATIONS:
      GetAttribute(static_cast<SQLINTEGER>(0), value, bufferLength, outputLength);
      break;
    case SQL_POSITIONED_STATEMENTS:
      GetAttribute(static_cast<SQLINTEGER>(0), value, bufferLength, outputLength);
      break;
    case SQL_SCROLL_CONCURRENCY:
      GetAttribute(static_cast<SQLINTEGER>(0), value, bufferLength, outputLength);
      break;
    case SQL_STATIC_SENSITIVITY:
      GetAttribute(static_cast<SQLINTEGER>(0), value, bufferLength, outputLength);
      break;

    // Driver-level string properties.
    case SQL_USER_NAME:
    case SQL_COLUMN_ALIAS:
    case SQL_DBMS_NAME:
    case SQL_DBMS_VER:
    case SQL_DRIVER_NAME:  // TODO: This should be the driver's filename and shouldn't
                           // come from the SPI.
    case SQL_DRIVER_VER:
    case SQL_SEARCH_PATTERN_ESCAPE:
    case SQL_SERVER_NAME:
    case SQL_DATA_SOURCE_READ_ONLY:
    case SQL_ACCESSIBLE_TABLES:
    case SQL_ACCESSIBLE_PROCEDURES:
    case SQL_CATALOG_TERM:
    case SQL_COLLATION_SEQ:
    case SQL_SCHEMA_TERM:
    case SQL_CATALOG_NAME:
    case SQL_CATALOG_NAME_SEPARATOR:
    case SQL_EXPRESSIONS_IN_ORDERBY:
    case SQL_IDENTIFIER_QUOTE_CHAR:
    case SQL_INTEGRITY:
    case SQL_KEYWORDS:
    case SQL_LIKE_ESCAPE_CLAUSE:
    case SQL_MAX_ROW_SIZE_INCLUDES_LONG:
    case SQL_ORDER_BY_COLUMNS_IN_SELECT:
    case SQL_OUTER_JOINS:  // Not documented in SQLGetInfo, but other drivers return Y/N
                           // strings
    case SQL_PROCEDURE_TERM:
    case SQL_PROCEDURES:
    case SQL_SPECIAL_CHARACTERS:
    case SQL_XOPEN_CLI_YEAR: {
      const auto& info = m_spiConnection->GetInfo(infoType);
      const std::string& infoValue = boost::get<std::string>(info);
      GetStringAttribute(isUnicode, infoValue, true, value, bufferLength, outputLength,
                         GetDiagnostics());
      break;
    }

    // Driver-level 32-bit integer properties.
    case SQL_GETDATA_EXTENSIONS:
    case SQL_INFO_SCHEMA_VIEWS:
    case SQL_CURSOR_SENSITIVITY:
    case SQL_DEFAULT_TXN_ISOLATION:
    case SQL_AGGREGATE_FUNCTIONS:
    case SQL_ALTER_DOMAIN:
      //    case SQL_ALTER_SCHEMA:
    case SQL_ALTER_TABLE:
    case SQL_DATETIME_LITERALS:
    case SQL_CATALOG_USAGE:
    case SQL_CREATE_ASSERTION:
    case SQL_CREATE_CHARACTER_SET:
    case SQL_CREATE_COLLATION:
    case SQL_CREATE_DOMAIN:
    case SQL_CREATE_SCHEMA:
    case SQL_CREATE_TABLE:
    case SQL_CREATE_TRANSLATION:
    case SQL_CREATE_VIEW:
    case SQL_INDEX_KEYWORDS:
    case SQL_INSERT_STATEMENT:
    case SQL_OJ_CAPABILITIES:
    case SQL_SCHEMA_USAGE:
    case SQL_SQL_CONFORMANCE:
    case SQL_SUBQUERIES:
    case SQL_UNION:
    case SQL_MAX_BINARY_LITERAL_LEN:
    case SQL_MAX_CHAR_LITERAL_LEN:
    case SQL_MAX_ROW_SIZE:
    case SQL_MAX_STATEMENT_LEN:
    case SQL_CONVERT_FUNCTIONS:
    case SQL_NUMERIC_FUNCTIONS:
    case SQL_STRING_FUNCTIONS:
    case SQL_SYSTEM_FUNCTIONS:
    case SQL_TIMEDATE_ADD_INTERVALS:
    case SQL_TIMEDATE_DIFF_INTERVALS:
    case SQL_TIMEDATE_FUNCTIONS:
    case SQL_CONVERT_BIGINT:
    case SQL_CONVERT_BINARY:
    case SQL_CONVERT_BIT:
    case SQL_CONVERT_CHAR:
    case SQL_CONVERT_DATE:
    case SQL_CONVERT_DECIMAL:
    case SQL_CONVERT_DOUBLE:
    case SQL_CONVERT_FLOAT:
    case SQL_CONVERT_GUID:
    case SQL_CONVERT_INTEGER:
    case SQL_CONVERT_INTERVAL_DAY_TIME:
    case SQL_CONVERT_INTERVAL_YEAR_MONTH:
    case SQL_CONVERT_LONGVARBINARY:
    case SQL_CONVERT_LONGVARCHAR:
    case SQL_CONVERT_NUMERIC:
    case SQL_CONVERT_REAL:
    case SQL_CONVERT_SMALLINT:
    case SQL_CONVERT_TIME:
    case SQL_CONVERT_TIMESTAMP:
    case SQL_CONVERT_TINYINT:
    case SQL_CONVERT_VARBINARY:
    case SQL_CONVERT_VARCHAR:
    case SQL_CONVERT_WCHAR:
    case SQL_CONVERT_WVARCHAR:
    case SQL_CONVERT_WLONGVARCHAR:
    case SQL_DDL_INDEX:
    case SQL_DROP_ASSERTION:
    case SQL_DROP_CHARACTER_SET:
    case SQL_DROP_COLLATION:
    case SQL_DROP_DOMAIN:
    case SQL_DROP_SCHEMA:
    case SQL_DROP_TABLE:
    case SQL_DROP_TRANSLATION:
    case SQL_DROP_VIEW:
    case SQL_MAX_INDEX_SIZE:
    case SQL_SQL92_DATETIME_FUNCTIONS:
    case SQL_SQL92_FOREIGN_KEY_DELETE_RULE:
    case SQL_SQL92_FOREIGN_KEY_UPDATE_RULE:
    case SQL_SQL92_GRANT:
    case SQL_SQL92_NUMERIC_VALUE_FUNCTIONS:
    case SQL_SQL92_PREDICATES:
    case SQL_SQL92_RELATIONAL_JOIN_OPERATORS:
    case SQL_SQL92_REVOKE:
    case SQL_SQL92_ROW_VALUE_CONSTRUCTOR:
    case SQL_SQL92_STRING_FUNCTIONS:
    case SQL_SQL92_VALUE_EXPRESSIONS:
    case SQL_STANDARD_CLI_CONFORMANCE: {
      const auto& info = m_spiConnection->GetInfo(infoType);
      uint32_t infoValue = boost::get<uint32_t>(info);
      GetAttribute(infoValue, value, bufferLength, outputLength);
      break;
    }

    // Driver-level 16-bit integer properties.
    case SQL_MAX_CONCURRENT_ACTIVITIES:
    case SQL_MAX_DRIVER_CONNECTIONS:
    case SQL_CONCAT_NULL_BEHAVIOR:
    case SQL_CURSOR_COMMIT_BEHAVIOR:
    case SQL_CURSOR_ROLLBACK_BEHAVIOR:
    case SQL_NULL_COLLATION:
    case SQL_CATALOG_LOCATION:
    case SQL_CORRELATION_NAME:
    case SQL_GROUP_BY:
    case SQL_IDENTIFIER_CASE:
    case SQL_NON_NULLABLE_COLUMNS:
    case SQL_QUOTED_IDENTIFIER_CASE:
    case SQL_MAX_CATALOG_NAME_LEN:
    case SQL_MAX_COLUMN_NAME_LEN:
    case SQL_MAX_COLUMNS_IN_GROUP_BY:
    case SQL_MAX_COLUMNS_IN_INDEX:
    case SQL_MAX_COLUMNS_IN_ORDER_BY:
    case SQL_MAX_COLUMNS_IN_SELECT:
    case SQL_MAX_COLUMNS_IN_TABLE:
    case SQL_MAX_CURSOR_NAME_LEN:
    case SQL_MAX_IDENTIFIER_LEN:
    case SQL_MAX_SCHEMA_NAME_LEN:
    case SQL_MAX_TABLE_NAME_LEN:
    case SQL_MAX_TABLES_IN_SELECT:
    case SQL_MAX_PROCEDURE_NAME_LEN:
    case SQL_MAX_USER_NAME_LEN:
    case SQL_ODBC_SQL_CONFORMANCE:
    case SQL_ODBC_SAG_CLI_CONFORMANCE: {
      const auto& info = m_spiConnection->GetInfo(infoType);
      uint16_t infoValue = boost::get<uint16_t>(info);
      GetAttribute(infoValue, value, bufferLength, outputLength);
      break;
    }

    // Special case - SQL_DATABASE_NAME is an alias for SQL_ATTR_CURRENT_CATALOG.
    case SQL_DATABASE_NAME: {
      const auto& attr = m_spiConnection->GetAttribute(Connection::CURRENT_CATALOG);
      if (!attr) {
        throw DriverException("Optional feature not supported.", "HYC00");
      }
      const std::string& infoValue = boost::get<std::string>(*attr);
      GetStringAttribute(isUnicode, infoValue, true, value, bufferLength, outputLength,
                         GetDiagnostics());
      break;
    }
    default:
      throw DriverException("Unknown SQLGetInfo type: " + std::to_string(infoType));
  }
}

void ODBCConnection::SetConnectAttr(SQLINTEGER attribute, SQLPOINTER value,
                                    SQLINTEGER stringLength, bool isUnicode) {
  uint32_t attributeToWrite = 0;
  bool successfully_written = false;
  switch (attribute) {
    // Internal connection attributes
#ifdef SQL_ATR_ASYNC_DBC_EVENT
    case SQL_ATTR_ASYNC_DBC_EVENT:
      throw DriverException("Optional feature not supported.", "HYC00");
#endif
#ifdef SQL_ATTR_ASYNC_DBC_FUNCTIONS_ENABLE
    case SQL_ATTR_ASYNC_DBC_FUNCTIONS_ENABLE:
      throw DriverException("Optional feature not supported.", "HYC00");
#endif
#ifdef SQL_ATTR_ASYNC_PCALLBACK
    case SQL_ATTR_ASYNC_DBC_PCALLBACK:
      throw DriverException("Optional feature not supported.", "HYC00");
#endif
#ifdef SQL_ATTR_ASYNC_DBC_PCONTEXT
    case SQL_ATTR_ASYNC_DBC_PCONTEXT:
      throw DriverException("Optional feature not supported.", "HYC00");
#endif
    case SQL_ATTR_AUTO_IPD:
      throw DriverException("Cannot set read-only attribute", "HY092");
    case SQL_ATTR_AUTOCOMMIT:
      CheckIfAttributeIsSetToOnlyValidValue(value,
                                            static_cast<SQLUINTEGER>(SQL_AUTOCOMMIT_ON));
      return;
    case SQL_ATTR_CONNECTION_DEAD:
      throw DriverException("Cannot set read-only attribute", "HY092");
#ifdef SQL_ATTR_DBC_INFO_TOKEN
    case SQL_ATTR_DBC_INFO_TOKEN:
      throw DriverException("Optional feature not supported.", "HYC00");
#endif
    case SQL_ATTR_ENLIST_IN_DTC:
      throw DriverException("Optional feature not supported.", "HYC00");
    case SQL_ATTR_ODBC_CURSORS:  // DM-only.
      throw DriverException("Invalid attribute", "HY092");
    case SQL_ATTR_QUIET_MODE:
      throw DriverException("Cannot set read-only attribute", "HY092");
    case SQL_ATTR_TRACE:  // DM-only
      throw DriverException("Cannot set read-only attribute", "HY092");
    case SQL_ATTR_TRACEFILE:
      throw DriverException("Optional feature not supported.", "HYC00");
    case SQL_ATTR_TRANSLATE_LIB:
      throw DriverException("Optional feature not supported.", "HYC00");
    case SQL_ATTR_TRANSLATE_OPTION:
      throw DriverException("Optional feature not supported.", "HYC00");
    case SQL_ATTR_TXN_ISOLATION:
      throw DriverException("Optional feature not supported.", "HYC00");

    // ODBCAbstraction-level attributes
    case SQL_ATTR_CURRENT_CATALOG: {
      std::string catalog;
      if (isUnicode) {
        SetAttributeUTF8(value, stringLength, catalog);
      } else {
        SetAttributeSQLWCHAR(value, stringLength, catalog);
      }
      if (!m_spiConnection->SetAttribute(Connection::CURRENT_CATALOG, catalog)) {
        throw DriverException("Option value changed.", "01S02");
      }
      return;
    }

    // Statement attributes that can be set through the connection.
    // Only applies to SQL_ATTR_METADATA_ID, SQL_ATTR_ASYNC_ENABLE, and ODBC 2.x statement
    // attributes. SQL_ATTR_ROW_NUMBER is excluded because it is read-only. Note that
    // SQLGetConnectAttr cannot retrieve these attributes.
    case SQL_ATTR_ASYNC_ENABLE:
    case SQL_ATTR_METADATA_ID:
    case SQL_ATTR_CONCURRENCY:
    case SQL_ATTR_CURSOR_TYPE:
    case SQL_ATTR_KEYSET_SIZE:
    case SQL_ATTR_MAX_LENGTH:
    case SQL_ATTR_MAX_ROWS:
    case SQL_ATTR_NOSCAN:
    case SQL_ATTR_QUERY_TIMEOUT:
    case SQL_ATTR_RETRIEVE_DATA:
    case SQL_ATTR_ROW_BIND_TYPE:
    case SQL_ATTR_SIMULATE_CURSOR:
    case SQL_ATTR_USE_BOOKMARKS:
      m_attributeTrackingStatement->SetStmtAttr(attribute, value, stringLength,
                                                isUnicode);
      return;

    case SQL_ATTR_ACCESS_MODE:
      SetAttribute(value, attributeToWrite);
      successfully_written =
          m_spiConnection->SetAttribute(Connection::ACCESS_MODE, attributeToWrite);
      break;
    case SQL_ATTR_CONNECTION_TIMEOUT:
      SetAttribute(value, attributeToWrite);
      successfully_written =
          m_spiConnection->SetAttribute(Connection::CONNECTION_TIMEOUT, attributeToWrite);
      break;
    case SQL_ATTR_LOGIN_TIMEOUT:
      SetAttribute(value, attributeToWrite);
      successfully_written =
          m_spiConnection->SetAttribute(Connection::LOGIN_TIMEOUT, attributeToWrite);
      break;
    case SQL_ATTR_PACKET_SIZE:
      SetAttribute(value, attributeToWrite);
      successfully_written =
          m_spiConnection->SetAttribute(Connection::PACKET_SIZE, attributeToWrite);
      break;
    default:
      throw DriverException("Invalid attribute: " + std::to_string(attribute), "HY092");
  }

  if (!successfully_written) {
    GetDiagnostics().AddWarning("Option value changed.", "01S02",
                                driver::odbcabstraction::ODBCErrorCodes_GENERAL_WARNING);
  }
}

void ODBCConnection::GetConnectAttr(SQLINTEGER attribute, SQLPOINTER value,
                                    SQLINTEGER bufferLength, SQLINTEGER* outputLength,
                                    bool isUnicode) {
  using driver::odbcabstraction::Connection;
  boost::optional<Connection::Attribute> spiAttribute;

  switch (attribute) {
    // Internal connection attributes
#ifdef SQL_ATR_ASYNC_DBC_EVENT
    case SQL_ATTR_ASYNC_DBC_EVENT:
      GetAttribute(static_cast<SQLPOINTER>(NULL), value, bufferLength, outputLength);
      return;
#endif
#ifdef SQL_ATTR_ASYNC_DBC_FUNCTIONS_ENABLE
    case SQL_ATTR_ASYNC_DBC_FUNCTIONS_ENABLE:
      GetAttribute(static_cast<SQLUINTEGER>(SQL_ASYNC_DBC_ENABLE_OFF), value,
                   bufferLength, outputLength);
      return;
#endif
#ifdef SQL_ATTR_ASYNC_PCALLBACK
    case SQL_ATTR_ASYNC_DBC_PCALLBACK:
      GetAttribute(static_cast<SQLPOINTER>(NULL), value, bufferLength, outputLength);
      return;
#endif
#ifdef SQL_ATTR_ASYNC_DBC_PCONTEXT
    case SQL_ATTR_ASYNC_DBC_PCONTEXT:
      GetAttribute(static_cast<SQLPOINTER>(NULL), value, bufferLength, outputLength);
      return;
#endif
    case SQL_ATTR_ASYNC_ENABLE:
      GetAttribute(static_cast<SQLULEN>(SQL_ASYNC_ENABLE_OFF), value, bufferLength,
                   outputLength);
      return;
    case SQL_ATTR_AUTO_IPD:
      GetAttribute(static_cast<SQLUINTEGER>(SQL_FALSE), value, bufferLength,
                   outputLength);
      return;
    case SQL_ATTR_AUTOCOMMIT:
      GetAttribute(static_cast<SQLUINTEGER>(SQL_AUTOCOMMIT_ON), value, bufferLength,
                   outputLength);
      return;
#ifdef SQL_ATTR_DBC_INFO_TOKEN
    case SQL_ATTR_DBC_INFO_TOKEN:
      throw DriverException("Cannot read set-only attribute", "HY092");
#endif
    case SQL_ATTR_ENLIST_IN_DTC:
      GetAttribute(static_cast<SQLPOINTER>(NULL), value, bufferLength, outputLength);
      return;
    case SQL_ATTR_ODBC_CURSORS:  // DM-only.
      throw DriverException("Invalid attribute", "HY092");
    case SQL_ATTR_QUIET_MODE:
      GetAttribute(static_cast<SQLPOINTER>(NULL), value, bufferLength, outputLength);
      return;
    case SQL_ATTR_TRACE:  // DM-only
      throw DriverException("Invalid attribute", "HY092");
    case SQL_ATTR_TRACEFILE:
      throw DriverException("Optional feature not supported.", "HYC00");
    case SQL_ATTR_TRANSLATE_LIB:
      throw DriverException("Optional feature not supported.", "HYC00");
    case SQL_ATTR_TRANSLATE_OPTION:
      throw DriverException("Optional feature not supported.", "HYC00");
    case SQL_ATTR_TXN_ISOLATION:
      throw DriverException("Optional feature not supported.", "HCY00");

    // ODBCAbstraction-level connection attributes.
    case SQL_ATTR_CURRENT_CATALOG: {
      const auto& catalog = m_spiConnection->GetAttribute(Connection::CURRENT_CATALOG);
      if (!catalog) {
        throw DriverException("Optional feature not supported.", "HYC00");
      }
      const std::string& infoValue = boost::get<std::string>(*catalog);
      GetStringAttribute(isUnicode, infoValue, true, value, bufferLength, outputLength,
                         GetDiagnostics());
      return;
    }

    // These all are uint32_t attributes.
    case SQL_ATTR_ACCESS_MODE:
      spiAttribute = m_spiConnection->GetAttribute(Connection::ACCESS_MODE);
      break;
    case SQL_ATTR_CONNECTION_DEAD:
      spiAttribute = m_spiConnection->GetAttribute(Connection::CONNECTION_DEAD);
      break;
    case SQL_ATTR_CONNECTION_TIMEOUT:
      spiAttribute = m_spiConnection->GetAttribute(Connection::CONNECTION_TIMEOUT);
      break;
    case SQL_ATTR_LOGIN_TIMEOUT:
      spiAttribute = m_spiConnection->GetAttribute(Connection::LOGIN_TIMEOUT);
      break;
    case SQL_ATTR_PACKET_SIZE:
      spiAttribute = m_spiConnection->GetAttribute(Connection::PACKET_SIZE);
      break;
    default:
      throw DriverException("Invalid attribute", "HY092");
  }

  if (!spiAttribute) {
    throw DriverException("Invalid attribute", "HY092");
  }

  GetAttribute(static_cast<SQLUINTEGER>(boost::get<uint32_t>(*spiAttribute)), value,
               bufferLength, outputLength);
}

void ODBCConnection::disconnect() {
  if (m_isConnected) {
    // Ensure that all statements (and corresponding SPI statements) get cleaned
    // up before terminating the SPI connection in case they need to be de-allocated in
    // the reverse of the allocation order.
    m_statements.clear();
    m_spiConnection->Close();
    m_isConnected = false;
  }
}

void ODBCConnection::releaseConnection() {
  disconnect();
  m_environment.DropConnection(this);
}

std::shared_ptr<ODBCStatement> ODBCConnection::createStatement() {
  std::shared_ptr<Statement> spiStatement = m_spiConnection->CreateStatement();
  std::shared_ptr<ODBCStatement> statement =
      std::make_shared<ODBCStatement>(*this, spiStatement);
  m_statements.push_back(statement);
  statement->CopyAttributesFromConnection(*this);
  return statement;
}

void ODBCConnection::dropStatement(ODBCStatement* stmt) {
  auto it = std::find_if(m_statements.begin(), m_statements.end(),
                         [&stmt](const std::shared_ptr<ODBCStatement>& statement) {
                           return statement.get() == stmt;
                         });
  if (m_statements.end() != it) {
    m_statements.erase(it);
  }
}

std::shared_ptr<ODBCDescriptor> ODBCConnection::createDescriptor() {
  std::shared_ptr<ODBCDescriptor> desc = std::make_shared<ODBCDescriptor>(
      m_spiConnection->GetDiagnostics(), this, nullptr, true, true, false);
  m_descriptors.push_back(desc);
  return desc;
}

void ODBCConnection::dropDescriptor(ODBCDescriptor* desc) {
  auto it = std::find_if(m_descriptors.begin(), m_descriptors.end(),
                         [&desc](const std::shared_ptr<ODBCDescriptor>& descriptor) {
                           return descriptor.get() == desc;
                         });
  if (m_descriptors.end() != it) {
    m_descriptors.erase(it);
  }
}

// Public Static
// ===================================================================================
std::string ODBCConnection::getPropertiesFromConnString(
    const std::string& connStr, Connection::ConnPropertyMap& properties) {
  const int groups[] = {1, 2};  // CONNECTION_STR_REGEX has two groups. key: 1, value: 2
  boost::xpressive::sregex_token_iterator regexIter(connStr.begin(), connStr.end(),
                                                    CONNECTION_STR_REGEX, groups),
      end;

  bool isDsnFirst = false;
  bool isDriverFirst = false;
  std::string dsn;
  for (auto it = regexIter; end != regexIter; ++regexIter) {
    std::string key = *regexIter;
    std::string value = *++regexIter;

    // If the DSN shows up before driver key, load settings from the DSN.
    // Only load values from the DSN once regardless of how many times the DSN
    // key shows up.
    if (boost::iequals(key, "DSN")) {
      if (!isDriverFirst) {
        if (!isDsnFirst) {
          isDsnFirst = true;
          loadPropertiesFromDSN(value, properties);
          dsn.swap(value);
        }
      }
      continue;
    } else if (boost::iequals(key, "Driver")) {
      if (!isDsnFirst) {
        isDriverFirst = true;
      }
      continue;
    }

    // Strip wrapping curly braces.
    if (value.size() >= 2 && value[0] == '{' && value[value.size() - 1] == '}') {
      value = value.substr(1, value.size() - 2);
    }

    // Overwrite the existing value. Later copies of the key take precedence,
    // including over entries in the DSN.
    properties[key] = std::move(value);
  }
  return dsn;
}
