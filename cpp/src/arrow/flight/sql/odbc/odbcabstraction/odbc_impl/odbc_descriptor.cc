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

#include "arrow/flight/sql/odbc/odbcabstraction/include/odbcabstraction/odbc_impl/odbc_descriptor.h"

#include <sql.h>
#include <sqlext.h>
#include <algorithm>
#include "arrow/flight/sql/odbc/odbcabstraction/include/odbcabstraction/exceptions.h"
#include "arrow/flight/sql/odbc/odbcabstraction/include/odbcabstraction/odbc_impl/attribute_utils.h"
#include "arrow/flight/sql/odbc/odbcabstraction/include/odbcabstraction/odbc_impl/odbc_connection.h"
#include "arrow/flight/sql/odbc/odbcabstraction/include/odbcabstraction/odbc_impl/odbc_statement.h"
#include "arrow/flight/sql/odbc/odbcabstraction/include/odbcabstraction/odbc_impl/type_utilities.h"
#include "arrow/flight/sql/odbc/odbcabstraction/include/odbcabstraction/spi/result_set_metadata.h"
#include "arrow/flight/sql/odbc/odbcabstraction/include/odbcabstraction/spi/statement.h"

using ODBC::DescriptorRecord;
using ODBC::ODBCConnection;
using ODBC::ODBCDescriptor;
using ODBC::ODBCStatement;

using driver::odbcabstraction::Diagnostics;
using driver::odbcabstraction::DriverException;
using driver::odbcabstraction::ResultSetMetadata;

namespace {
SQLSMALLINT CalculateHighestBoundRecord(const std::vector<DescriptorRecord>& records) {
  // Most applications will bind every column, so optimistically assume that we'll
  // find the next bound record fastest by counting backwards.
  for (size_t i = records.size(); i > 0; --i) {
    if (records[i - 1].m_isBound) {
      return i;
    }
  }
  return 0;
}
}  // namespace

// Public
// =========================================================================================
ODBCDescriptor::ODBCDescriptor(Diagnostics& baseDiagnostics, ODBCConnection* conn,
                               ODBCStatement* stmt, bool isAppDescriptor, bool isWritable,
                               bool is2xConnection)
    : m_diagnostics(baseDiagnostics.GetVendor(), baseDiagnostics.GetDataSourceComponent(),
                    driver::odbcabstraction::V_3),
      m_owningConnection(conn),
      m_parentStatement(stmt),
      m_arrayStatusPtr(nullptr),
      m_bindOffsetPtr(nullptr),
      m_rowsProccessedPtr(nullptr),
      m_arraySize(1),
      m_bindType(SQL_BIND_BY_COLUMN),
      m_highestOneBasedBoundRecord(0),
      m_is2xConnection(is2xConnection),
      m_isAppDescriptor(isAppDescriptor),
      m_isWritable(isWritable),
      m_hasBindingsChanged(true) {}

Diagnostics& ODBCDescriptor::GetDiagnostics_Impl() { return m_diagnostics; }

ODBCConnection& ODBCDescriptor::GetConnection() {
  if (m_owningConnection) {
    return *m_owningConnection;
  }
  assert(m_parentStatement);
  return m_parentStatement->GetConnection();
}

void ODBCDescriptor::SetHeaderField(SQLSMALLINT fieldIdentifier, SQLPOINTER value,
                                    SQLINTEGER bufferLength) {
  // Only these two fields can be set on the IRD.
  if (!m_isWritable && fieldIdentifier != SQL_DESC_ARRAY_STATUS_PTR &&
      fieldIdentifier != SQL_DESC_ROWS_PROCESSED_PTR) {
    throw DriverException("Cannot modify read-only descriptor", "HY016");
  }

  switch (fieldIdentifier) {
    case SQL_DESC_ALLOC_TYPE:
      throw DriverException("Invalid descriptor field", "HY091");
    case SQL_DESC_ARRAY_SIZE:
      SetAttribute(value, m_arraySize);
      m_hasBindingsChanged = true;
      break;
    case SQL_DESC_ARRAY_STATUS_PTR:
      SetPointerAttribute(value, m_arrayStatusPtr);
      m_hasBindingsChanged = true;
      break;
    case SQL_DESC_BIND_OFFSET_PTR:
      SetPointerAttribute(value, m_bindOffsetPtr);
      m_hasBindingsChanged = true;
      break;
    case SQL_DESC_BIND_TYPE:
      SetAttribute(value, m_bindType);
      m_hasBindingsChanged = true;
      break;
    case SQL_DESC_ROWS_PROCESSED_PTR:
      SetPointerAttribute(value, m_rowsProccessedPtr);
      m_hasBindingsChanged = true;
      break;
    case SQL_DESC_COUNT: {
      SQLSMALLINT newCount;
      SetAttribute(value, newCount);
      m_records.resize(newCount);

      if (m_isAppDescriptor && newCount <= m_highestOneBasedBoundRecord) {
        m_highestOneBasedBoundRecord = CalculateHighestBoundRecord(m_records);
      } else {
        m_highestOneBasedBoundRecord = newCount;
      }
      m_hasBindingsChanged = true;
      break;
    }
    default:
      throw DriverException("Invalid descriptor field", "HY091");
  }
}

void ODBCDescriptor::SetField(SQLSMALLINT recordNumber, SQLSMALLINT fieldIdentifier,
                              SQLPOINTER value, SQLINTEGER bufferLength) {
  if (!m_isWritable) {
    throw DriverException("Cannot modify read-only descriptor", "HY016");
  }

  // Handle header fields before validating the record number.
  switch (fieldIdentifier) {
    case SQL_DESC_ALLOC_TYPE:
    case SQL_DESC_ARRAY_SIZE:
    case SQL_DESC_ARRAY_STATUS_PTR:
    case SQL_DESC_BIND_OFFSET_PTR:
    case SQL_DESC_BIND_TYPE:
    case SQL_DESC_ROWS_PROCESSED_PTR:
    case SQL_DESC_COUNT:
      SetHeaderField(fieldIdentifier, value, bufferLength);
      return;
    default:
      break;
  }

  if (recordNumber == 0) {
    throw DriverException("Bookmarks are unsupported.", "07009");
  }

  if (recordNumber > m_records.size()) {
    throw DriverException("Invalid descriptor index", "HY009");
  }

  SQLSMALLINT zeroBasedRecord = recordNumber - 1;
  DescriptorRecord& record = m_records[zeroBasedRecord];
  switch (fieldIdentifier) {
    case SQL_DESC_AUTO_UNIQUE_VALUE:
    case SQL_DESC_BASE_COLUMN_NAME:
    case SQL_DESC_BASE_TABLE_NAME:
    case SQL_DESC_CASE_SENSITIVE:
    case SQL_DESC_CATALOG_NAME:
    case SQL_DESC_DISPLAY_SIZE:
    case SQL_DESC_FIXED_PREC_SCALE:
    case SQL_DESC_LABEL:
    case SQL_DESC_LITERAL_PREFIX:
    case SQL_DESC_LITERAL_SUFFIX:
    case SQL_DESC_LOCAL_TYPE_NAME:
    case SQL_DESC_NULLABLE:
    case SQL_DESC_NUM_PREC_RADIX:
    case SQL_DESC_ROWVER:
    case SQL_DESC_SCHEMA_NAME:
    case SQL_DESC_SEARCHABLE:
    case SQL_DESC_TABLE_NAME:
    case SQL_DESC_TYPE_NAME:
    case SQL_DESC_UNNAMED:
    case SQL_DESC_UNSIGNED:
    case SQL_DESC_UPDATABLE:
      throw DriverException("Cannot modify read-only field.", "HY092");
    case SQL_DESC_CONCISE_TYPE:
      SetAttribute(value, record.m_conciseType);
      record.m_isBound = false;
      m_hasBindingsChanged = true;
      break;
    case SQL_DESC_DATA_PTR:
      SetDataPtrOnRecord(value, recordNumber);
      break;
    case SQL_DESC_DATETIME_INTERVAL_CODE:
      SetAttribute(value, record.m_datetimeIntervalCode);
      record.m_isBound = false;
      m_hasBindingsChanged = true;
      break;
    case SQL_DESC_DATETIME_INTERVAL_PRECISION:
      SetAttribute(value, record.m_datetimeIntervalPrecision);
      record.m_isBound = false;
      m_hasBindingsChanged = true;
      break;
    case SQL_DESC_INDICATOR_PTR:
    case SQL_DESC_OCTET_LENGTH_PTR:
      SetPointerAttribute(value, record.m_indicatorPtr);
      m_hasBindingsChanged = true;
      break;
    case SQL_DESC_LENGTH:
      SetAttribute(value, record.m_length);
      record.m_isBound = false;
      m_hasBindingsChanged = true;
      break;
    case SQL_DESC_NAME:
      SetAttributeUTF8(value, bufferLength, record.m_name);
      m_hasBindingsChanged = true;
      break;
    case SQL_DESC_OCTET_LENGTH:
      SetAttribute(value, record.m_octetLength);
      record.m_isBound = false;
      m_hasBindingsChanged = true;
      break;
    case SQL_DESC_PARAMETER_TYPE:
      SetAttribute(value, record.m_paramType);
      record.m_isBound = false;
      m_hasBindingsChanged = true;
      break;
    case SQL_DESC_PRECISION:
      SetAttribute(value, record.m_precision);
      record.m_isBound = false;
      m_hasBindingsChanged = true;
      break;
    case SQL_DESC_SCALE:
      SetAttribute(value, record.m_scale);
      record.m_isBound = false;
      m_hasBindingsChanged = true;
      break;
    case SQL_DESC_TYPE:
      SetAttribute(value, record.m_type);
      record.m_isBound = false;
      m_hasBindingsChanged = true;
      break;
    default:
      throw DriverException("Invalid descriptor field", "HY091");
  }
}

void ODBCDescriptor::GetHeaderField(SQLSMALLINT fieldIdentifier, SQLPOINTER value,
                                    SQLINTEGER bufferLength,
                                    SQLINTEGER* outputLength) const {
  switch (fieldIdentifier) {
    case SQL_DESC_ALLOC_TYPE: {
      SQLSMALLINT result;
      if (m_owningConnection) {
        result = SQL_DESC_ALLOC_USER;
      } else {
        result = SQL_DESC_ALLOC_AUTO;
      }
      GetAttribute(result, value, bufferLength, outputLength);
      break;
    }
    case SQL_DESC_ARRAY_SIZE:
      GetAttribute(m_arraySize, value, bufferLength, outputLength);
      break;
    case SQL_DESC_ARRAY_STATUS_PTR:
      GetAttribute(m_arrayStatusPtr, value, bufferLength, outputLength);
      break;
    case SQL_DESC_BIND_OFFSET_PTR:
      GetAttribute(m_bindOffsetPtr, value, bufferLength, outputLength);
      break;
    case SQL_DESC_BIND_TYPE:
      GetAttribute(m_bindType, value, bufferLength, outputLength);
      break;
    case SQL_DESC_ROWS_PROCESSED_PTR:
      GetAttribute(m_rowsProccessedPtr, value, bufferLength, outputLength);
      break;
    case SQL_DESC_COUNT: {
      GetAttribute(m_highestOneBasedBoundRecord, value, bufferLength, outputLength);
      break;
    }
    default:
      throw DriverException("Invalid descriptor field", "HY091");
  }
}

void ODBCDescriptor::GetField(SQLSMALLINT recordNumber, SQLSMALLINT fieldIdentifier,
                              SQLPOINTER value, SQLINTEGER bufferLength,
                              SQLINTEGER* outputLength) {
  // Handle header fields before validating the record number.
  switch (fieldIdentifier) {
    case SQL_DESC_ALLOC_TYPE:
    case SQL_DESC_ARRAY_SIZE:
    case SQL_DESC_ARRAY_STATUS_PTR:
    case SQL_DESC_BIND_OFFSET_PTR:
    case SQL_DESC_BIND_TYPE:
    case SQL_DESC_ROWS_PROCESSED_PTR:
    case SQL_DESC_COUNT:
      GetHeaderField(fieldIdentifier, value, bufferLength, outputLength);
      return;
    default:
      break;
  }

  if (recordNumber == 0) {
    throw DriverException("Bookmarks are unsupported.", "07009");
  }

  if (recordNumber > m_records.size()) {
    throw DriverException("Invalid descriptor index", "07009");
  }

  // TODO: Restrict fields based on AppDescriptor IPD, and IRD.

  SQLSMALLINT zeroBasedRecord = recordNumber - 1;
  const DescriptorRecord& record = m_records[zeroBasedRecord];
  switch (fieldIdentifier) {
    case SQL_DESC_BASE_COLUMN_NAME:
      GetAttributeUTF8(record.m_baseColumnName, value, bufferLength, outputLength,
                       GetDiagnostics());
      break;
    case SQL_DESC_BASE_TABLE_NAME:
      GetAttributeUTF8(record.m_baseTableName, value, bufferLength, outputLength,
                       GetDiagnostics());
      break;
    case SQL_DESC_CATALOG_NAME:
      GetAttributeUTF8(record.m_catalogName, value, bufferLength, outputLength,
                       GetDiagnostics());
      break;
    case SQL_DESC_LABEL:
      GetAttributeUTF8(record.m_label, value, bufferLength, outputLength,
                       GetDiagnostics());
      break;
    case SQL_DESC_LITERAL_PREFIX:
      GetAttributeUTF8(record.m_literalPrefix, value, bufferLength, outputLength,
                       GetDiagnostics());
      break;
    case SQL_DESC_LITERAL_SUFFIX:
      GetAttributeUTF8(record.m_literalSuffix, value, bufferLength, outputLength,
                       GetDiagnostics());
      break;
    case SQL_DESC_LOCAL_TYPE_NAME:
      GetAttributeUTF8(record.m_localTypeName, value, bufferLength, outputLength,
                       GetDiagnostics());
      break;
    case SQL_DESC_NAME:
      GetAttributeUTF8(record.m_name, value, bufferLength, outputLength,
                       GetDiagnostics());
      break;
    case SQL_DESC_SCHEMA_NAME:
      GetAttributeUTF8(record.m_schemaName, value, bufferLength, outputLength,
                       GetDiagnostics());
      break;
    case SQL_DESC_TABLE_NAME:
      GetAttributeUTF8(record.m_tableName, value, bufferLength, outputLength,
                       GetDiagnostics());
      break;
    case SQL_DESC_TYPE_NAME:
      GetAttributeUTF8(record.m_typeName, value, bufferLength, outputLength,
                       GetDiagnostics());
      break;

    case SQL_DESC_DATA_PTR:
      GetAttribute(record.m_dataPtr, value, bufferLength, outputLength);
      break;
    case SQL_DESC_INDICATOR_PTR:
    case SQL_DESC_OCTET_LENGTH_PTR:
      GetAttribute(record.m_indicatorPtr, value, bufferLength, outputLength);
      break;

    case SQL_DESC_LENGTH:
      GetAttribute(record.m_length, value, bufferLength, outputLength);
      break;
    case SQL_DESC_OCTET_LENGTH:
      GetAttribute(record.m_octetLength, value, bufferLength, outputLength);
      break;

    case SQL_DESC_AUTO_UNIQUE_VALUE:
      GetAttribute(record.m_autoUniqueValue, value, bufferLength, outputLength);
      break;
    case SQL_DESC_CASE_SENSITIVE:
      GetAttribute(record.m_caseSensitive, value, bufferLength, outputLength);
      break;
    case SQL_DESC_DATETIME_INTERVAL_PRECISION:
      GetAttribute(record.m_datetimeIntervalPrecision, value, bufferLength, outputLength);
      break;
    case SQL_DESC_NUM_PREC_RADIX:
      GetAttribute(record.m_numPrecRadix, value, bufferLength, outputLength);
      break;

    case SQL_DESC_CONCISE_TYPE:
      GetAttribute(record.m_conciseType, value, bufferLength, outputLength);
      break;
    case SQL_DESC_DATETIME_INTERVAL_CODE:
      GetAttribute(record.m_datetimeIntervalCode, value, bufferLength, outputLength);
      break;
    case SQL_DESC_DISPLAY_SIZE:
      GetAttribute(record.m_displaySize, value, bufferLength, outputLength);
      break;
    case SQL_DESC_FIXED_PREC_SCALE:
      GetAttribute(record.m_fixedPrecScale, value, bufferLength, outputLength);
      break;
    case SQL_DESC_NULLABLE:
      GetAttribute(record.m_nullable, value, bufferLength, outputLength);
      break;
    case SQL_DESC_PARAMETER_TYPE:
      GetAttribute(record.m_paramType, value, bufferLength, outputLength);
      break;
    case SQL_DESC_PRECISION:
      GetAttribute(record.m_precision, value, bufferLength, outputLength);
      break;
    case SQL_DESC_ROWVER:
      GetAttribute(record.m_rowVer, value, bufferLength, outputLength);
      break;
    case SQL_DESC_SCALE:
      GetAttribute(record.m_scale, value, bufferLength, outputLength);
      break;
    case SQL_DESC_SEARCHABLE:
      GetAttribute(record.m_searchable, value, bufferLength, outputLength);
      break;
    case SQL_DESC_TYPE:
      GetAttribute(record.m_type, value, bufferLength, outputLength);
      break;
    case SQL_DESC_UNNAMED:
      GetAttribute(record.m_unnamed, value, bufferLength, outputLength);
      break;
    case SQL_DESC_UNSIGNED:
      GetAttribute(record.m_unsigned, value, bufferLength, outputLength);
      break;
    case SQL_DESC_UPDATABLE:
      GetAttribute(record.m_updatable, value, bufferLength, outputLength);
      break;
    default:
      throw DriverException("Invalid descriptor field", "HY091");
  }
}

SQLSMALLINT ODBCDescriptor::getAllocType() const {
  return m_owningConnection != nullptr ? SQL_DESC_ALLOC_USER : SQL_DESC_ALLOC_AUTO;
}

bool ODBCDescriptor::IsAppDescriptor() const { return m_isAppDescriptor; }

void ODBCDescriptor::RegisterToStatement(ODBCStatement* statement, bool isApd) {
  if (isApd) {
    m_registeredOnStatementsAsApd.push_back(statement);
  } else {
    m_registeredOnStatementsAsArd.push_back(statement);
  }
}

void ODBCDescriptor::DetachFromStatement(ODBCStatement* statement, bool isApd) {
  auto& vectorToUpdate =
      isApd ? m_registeredOnStatementsAsApd : m_registeredOnStatementsAsArd;
  auto it = std::find(vectorToUpdate.begin(), vectorToUpdate.end(), statement);
  if (it != vectorToUpdate.end()) {
    vectorToUpdate.erase(it);
  }
}

void ODBCDescriptor::ReleaseDescriptor() {
  for (ODBCStatement* stmt : m_registeredOnStatementsAsApd) {
    stmt->RevertAppDescriptor(true);
  }

  for (ODBCStatement* stmt : m_registeredOnStatementsAsArd) {
    stmt->RevertAppDescriptor(false);
  }

  if (m_owningConnection) {
    m_owningConnection->dropDescriptor(this);
  }
}

void ODBCDescriptor::PopulateFromResultSetMetadata(ResultSetMetadata* rsmd) {
  m_records.assign(rsmd->GetColumnCount(), DescriptorRecord());
  m_highestOneBasedBoundRecord = m_records.size() + 1;

  for (size_t i = 0; i < m_records.size(); ++i) {
    size_t oneBasedIndex = i + 1;
    m_records[i].m_baseColumnName = rsmd->GetBaseColumnName(oneBasedIndex);
    m_records[i].m_baseTableName = rsmd->GetBaseTableName(oneBasedIndex);
    m_records[i].m_catalogName = rsmd->GetCatalogName(oneBasedIndex);
    m_records[i].m_label = rsmd->GetColumnLabel(oneBasedIndex);
    m_records[i].m_literalPrefix = rsmd->GetLiteralPrefix(oneBasedIndex);
    m_records[i].m_literalSuffix = rsmd->GetLiteralSuffix(oneBasedIndex);
    m_records[i].m_localTypeName = rsmd->GetLocalTypeName(oneBasedIndex);
    m_records[i].m_name = rsmd->GetName(oneBasedIndex);
    m_records[i].m_schemaName = rsmd->GetSchemaName(oneBasedIndex);
    m_records[i].m_tableName = rsmd->GetTableName(oneBasedIndex);
    m_records[i].m_typeName = rsmd->GetTypeName(oneBasedIndex);
    m_records[i].m_conciseType =
        GetSqlTypeForODBCVersion(rsmd->GetConciseType(oneBasedIndex), m_is2xConnection);
    m_records[i].m_dataPtr = nullptr;
    m_records[i].m_indicatorPtr = nullptr;
    m_records[i].m_displaySize = rsmd->GetColumnDisplaySize(oneBasedIndex);
    m_records[i].m_octetLength = rsmd->GetOctetLength(oneBasedIndex);
    m_records[i].m_length = rsmd->GetLength(oneBasedIndex);
    m_records[i].m_autoUniqueValue =
        rsmd->IsAutoUnique(oneBasedIndex) ? SQL_TRUE : SQL_FALSE;
    m_records[i].m_caseSensitive =
        rsmd->IsCaseSensitive(oneBasedIndex) ? SQL_TRUE : SQL_FALSE;
    m_records[i].m_datetimeIntervalPrecision;  // TODO - update when rsmd adds this
    m_records[i].m_numPrecRadix = rsmd->GetNumPrecRadix(oneBasedIndex);
    m_records[i].m_datetimeIntervalCode;  // TODO
    m_records[i].m_fixedPrecScale =
        rsmd->IsFixedPrecScale(oneBasedIndex) ? SQL_TRUE : SQL_FALSE;
    m_records[i].m_nullable = rsmd->IsNullable(oneBasedIndex);
    m_records[i].m_paramType = SQL_PARAM_INPUT;
    m_records[i].m_precision = rsmd->GetPrecision(oneBasedIndex);
    m_records[i].m_rowVer = SQL_FALSE;
    m_records[i].m_scale = rsmd->GetScale(oneBasedIndex);
    m_records[i].m_searchable = rsmd->IsSearchable(oneBasedIndex);
    m_records[i].m_type =
        GetSqlTypeForODBCVersion(rsmd->GetDataType(oneBasedIndex), m_is2xConnection);
    m_records[i].m_unnamed = m_records[i].m_name.empty() ? SQL_TRUE : SQL_FALSE;
    m_records[i].m_unsigned = rsmd->IsUnsigned(oneBasedIndex) ? SQL_TRUE : SQL_FALSE;
    m_records[i].m_updatable = rsmd->GetUpdatable(oneBasedIndex);
  }
}

const std::vector<DescriptorRecord>& ODBCDescriptor::GetRecords() const {
  return m_records;
}

std::vector<DescriptorRecord>& ODBCDescriptor::GetRecords() { return m_records; }

void ODBCDescriptor::BindCol(SQLSMALLINT recordNumber, SQLSMALLINT cType,
                             SQLPOINTER dataPtr, SQLLEN bufferLength,
                             SQLLEN* indicatorPtr) {
  assert(m_isAppDescriptor);
  assert(m_isWritable);

  // The set of records auto-expands to the supplied record number.
  if (m_records.size() < recordNumber) {
    m_records.resize(recordNumber);
  }

  SQLSMALLINT zeroBasedRecordIndex = recordNumber - 1;
  DescriptorRecord& record = m_records[zeroBasedRecordIndex];

  record.m_type = cType;
  record.m_indicatorPtr = indicatorPtr;
  record.m_length = bufferLength;

  // Initialize default precision and scale for SQL_C_NUMERIC.
  if (record.m_type == SQL_C_NUMERIC) {
    record.m_precision = 38;
    record.m_scale = 0;
  }
  SetDataPtrOnRecord(dataPtr, recordNumber);
}

void ODBCDescriptor::SetDataPtrOnRecord(SQLPOINTER dataPtr, SQLSMALLINT recordNumber) {
  assert(recordNumber <= m_records.size());
  DescriptorRecord& record = m_records[recordNumber - 1];
  if (dataPtr) {
    record.CheckConsistency();
    record.m_isBound = true;
  } else {
    record.m_isBound = false;
  }
  record.m_dataPtr = dataPtr;

  // Bookkeeping on the highest bound record (used for returning SQL_DESC_COUNT)
  if (m_highestOneBasedBoundRecord < recordNumber && dataPtr) {
    m_highestOneBasedBoundRecord = recordNumber;
  } else if (m_highestOneBasedBoundRecord == recordNumber && !dataPtr) {
    m_highestOneBasedBoundRecord = CalculateHighestBoundRecord(m_records);
  }
  m_hasBindingsChanged = true;
}

void DescriptorRecord::CheckConsistency() {
  // TODO
}
