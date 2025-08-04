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

#include <arrow/flight/sql/odbc/odbcabstraction/include/odbcabstraction/odbc_impl/odbc_handle.h>

#include <sql.h>
#include <sqlext.h>
#include <sqltypes.h>
#include <memory>
#include <string>
#include <vector>

namespace driver {
namespace odbcabstraction {
class ResultSetMetadata;
}
}  // namespace driver
namespace ODBC {
class ODBCConnection;
class ODBCStatement;
}  // namespace ODBC

namespace ODBC {
struct DescriptorRecord {
  std::string m_baseColumnName;
  std::string m_baseTableName;
  std::string m_catalogName;
  std::string m_label;
  std::string m_literalPrefix;
  std::string m_literalSuffix;
  std::string m_localTypeName;
  std::string m_name;
  std::string m_schemaName;
  std::string m_tableName;
  std::string m_typeName;
  SQLPOINTER m_dataPtr = NULL;
  SQLLEN* m_indicatorPtr = NULL;
  SQLLEN m_displaySize = 0;
  SQLLEN m_octetLength = 0;
  SQLULEN m_length = 0;
  SQLINTEGER m_autoUniqueValue;
  SQLINTEGER m_caseSensitive = SQL_TRUE;
  SQLINTEGER m_datetimeIntervalPrecision = 0;
  SQLINTEGER m_numPrecRadix = 0;
  SQLSMALLINT m_conciseType = SQL_C_DEFAULT;
  SQLSMALLINT m_datetimeIntervalCode = 0;
  SQLSMALLINT m_fixedPrecScale = 0;
  SQLSMALLINT m_nullable = SQL_NULLABLE_UNKNOWN;
  SQLSMALLINT m_paramType = SQL_PARAM_INPUT;
  SQLSMALLINT m_precision = 0;
  SQLSMALLINT m_rowVer = 0;
  SQLSMALLINT m_scale = 0;
  SQLSMALLINT m_searchable = SQL_SEARCHABLE;
  SQLSMALLINT m_type = SQL_C_DEFAULT;
  SQLSMALLINT m_unnamed = SQL_TRUE;
  SQLSMALLINT m_unsigned = SQL_FALSE;
  SQLSMALLINT m_updatable = SQL_FALSE;
  bool m_isBound = false;

  void CheckConsistency();
};

class ODBCDescriptor : public ODBCHandle<ODBCDescriptor> {
 public:
  /// \brief Construct a new ODBCDescriptor object. Link the descriptor to a connection,
  /// if applicable. A nullptr should be supplied for conn if the descriptor should not be
  /// linked.
  ODBCDescriptor(driver::odbcabstraction::Diagnostics& baseDiagnostics,
                 ODBCConnection* conn, ODBCStatement* stmt, bool isAppDescriptor,
                 bool isWritable, bool is2xConnection);

  driver::odbcabstraction::Diagnostics& GetDiagnostics_Impl();

  ODBCConnection& GetConnection();

  void SetHeaderField(SQLSMALLINT fieldIdentifier, SQLPOINTER value,
                      SQLINTEGER bufferLength);
  void SetField(SQLSMALLINT recordNumber, SQLSMALLINT fieldIdentifier, SQLPOINTER value,
                SQLINTEGER bufferLength);
  void GetHeaderField(SQLSMALLINT fieldIdentifier, SQLPOINTER value,
                      SQLINTEGER bufferLength, SQLINTEGER* outputLength) const;
  void GetField(SQLSMALLINT recordNumber, SQLSMALLINT fieldIdentifier, SQLPOINTER value,
                SQLINTEGER bufferLength, SQLINTEGER* outputLength);
  SQLSMALLINT getAllocType() const;
  bool IsAppDescriptor() const;

  inline bool HaveBindingsChanged() const { return m_hasBindingsChanged; }

  void RegisterToStatement(ODBCStatement* statement, bool isApd);
  void DetachFromStatement(ODBCStatement* statement, bool isApd);
  void ReleaseDescriptor();

  void PopulateFromResultSetMetadata(driver::odbcabstraction::ResultSetMetadata* rsmd);

  const std::vector<DescriptorRecord>& GetRecords() const;
  std::vector<DescriptorRecord>& GetRecords();

  void BindCol(SQLSMALLINT recordNumber, SQLSMALLINT cType, SQLPOINTER dataPtr,
               SQLLEN bufferLength, SQLLEN* indicatorPtr);
  void SetDataPtrOnRecord(SQLPOINTER dataPtr, SQLSMALLINT recNumber);

  inline SQLULEN GetBindOffset() { return m_bindOffsetPtr ? *m_bindOffsetPtr : 0UL; }

  inline SQLULEN GetBoundStructOffset() {
    // If this is SQL_BIND_BY_COLUMN, m_bindType is zero which indicates no offset due to
    // use of a bound struct. If this is non-zero, row-wise binding is being used so the
    // app should set this to sizeof(their struct).
    return m_bindType;
  }

  inline SQLULEN GetArraySize() { return m_arraySize; }

  inline SQLUSMALLINT* GetArrayStatusPtr() { return m_arrayStatusPtr; }

  inline void SetRowsProcessed(SQLULEN rows) {
    if (m_rowsProccessedPtr) {
      *m_rowsProccessedPtr = rows;
    }
  }

  inline void NotifyBindingsHavePropagated() { m_hasBindingsChanged = false; }

  inline void NotifyBindingsHaveChanged() { m_hasBindingsChanged = true; }

 private:
  driver::odbcabstraction::Diagnostics m_diagnostics;
  std::vector<ODBCStatement*> m_registeredOnStatementsAsApd;
  std::vector<ODBCStatement*> m_registeredOnStatementsAsArd;
  std::vector<DescriptorRecord> m_records;
  ODBCConnection* m_owningConnection;
  ODBCStatement* m_parentStatement;
  SQLUSMALLINT* m_arrayStatusPtr;
  SQLULEN* m_bindOffsetPtr;
  SQLULEN* m_rowsProccessedPtr;
  SQLULEN m_arraySize;
  SQLINTEGER m_bindType;
  SQLSMALLINT m_highestOneBasedBoundRecord;
  const bool m_is2xConnection;
  bool m_isAppDescriptor;
  bool m_isWritable;
  bool m_hasBindingsChanged;
};
}  // namespace ODBC
