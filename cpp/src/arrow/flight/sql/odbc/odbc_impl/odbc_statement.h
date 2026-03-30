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

// platform.h platform.h includes windows.h so it needs to be included first
#include "arrow/flight/sql/odbc/odbc_impl/platform.h"

#include "arrow/flight/sql/odbc/odbc_impl/odbc_handle.h"
#include "arrow/flight/sql/odbc/odbc_impl/type_fwd.h"

#include <sql.h>
#include <memory>
#include <string>

namespace ODBC {

/**
 * @brief An abstraction over an ODBC connection handle. This also wraps an SPI
 * Connection.
 */
class ODBCStatement : public ODBCHandle<ODBCStatement> {
 public:
  ODBCStatement(const ODBCStatement&) = delete;
  ODBCStatement& operator=(const ODBCStatement&) = delete;

  ODBCStatement(ODBCConnection& connection,
                std::shared_ptr<arrow::flight::sql::odbc::Statement> spi_statement);

  ~ODBCStatement() = default;

  inline arrow::flight::sql::odbc::Diagnostics& GetDiagnosticsImpl() {
    return *diagnostics_;
  }

  ODBCConnection& GetConnection();

  void CopyAttributesFromConnection(ODBCConnection& connection);
  void Prepare(const std::string& query);
  void ExecutePrepared();
  void ExecuteDirect(const std::string& query);

  /// \brief Return true if the number of rows fetch was greater than zero.
  ///
  /// row_count_ptr and row_status_array are optional arguments, they are only needed for
  /// SQLExtendedFetch
  bool Fetch(size_t rows, SQLULEN* row_count_ptr = 0, SQLUSMALLINT* row_status_array = 0);
  bool IsPrepared() const;

  void GetStmtAttr(SQLINTEGER statement_attribute, SQLPOINTER output,
                   SQLINTEGER buffer_size, SQLINTEGER* str_len_ptr, bool is_unicode);
  void SetStmtAttr(SQLINTEGER statement_attribute, SQLPOINTER value,
                   SQLINTEGER buffer_size, bool is_unicode);

  /// \brief Revert back to implicitly allocated internal descriptors.
  /// isApd as True indicates APD descritor is to be reverted.
  /// isApd as False indicates ARD descritor is to be reverted.
  void RevertAppDescriptor(bool is_apd);

  inline ODBCDescriptor* GetIRD() { return ird_.get(); }

  inline ODBCDescriptor* GetARD() { return current_ard_; }

  inline SQLULEN GetRowsetSize() { return rowset_size_; }

  SQLRETURN GetData(SQLSMALLINT record_number, SQLSMALLINT c_type, SQLPOINTER data_ptr,
                    SQLLEN buffer_length, SQLLEN* indicator_ptr);

  SQLRETURN GetMoreResults();

  /// \brief Return number of columns from data set
  void GetColumnCount(SQLSMALLINT* column_count_ptr);

  /// \brief Return number of rows affected by an UPDATE, INSERT, or DELETE statement
  ///
  ///  -1 is returned as driver only supports SELECT statement
  void GetRowCount(SQLLEN* row_count_ptr);

  /// \brief Closes the cursor. This does _not_ un-prepare the statement or change
  /// bindings.
  void CloseCursor(bool suppress_errors);

  /// \brief Releases this statement from memory.
  void ReleaseStatement();

  void GetTables(const std::string* catalog, const std::string* schema,
                 const std::string* table, const std::string* table_type);
  void GetColumns(const std::string* catalog, const std::string* schema,
                  const std::string* table, const std::string* column);
  void GetTypeInfo(SQLSMALLINT data_type);
  void Cancel();

 private:
  ODBCConnection& connection_;
  std::shared_ptr<arrow::flight::sql::odbc::Statement> spi_statement_;
  std::shared_ptr<arrow::flight::sql::odbc::ResultSet> current_result_;
  arrow::flight::sql::odbc::Diagnostics* diagnostics_;

  std::shared_ptr<ODBCDescriptor> built_in_ard_;
  std::shared_ptr<ODBCDescriptor> built_in_apd_;
  std::shared_ptr<ODBCDescriptor> ipd_;
  std::shared_ptr<ODBCDescriptor> ird_;
  ODBCDescriptor* current_ard_;
  ODBCDescriptor* current_apd_;
  SQLULEN row_number_;
  SQLULEN max_rows_;
  SQLULEN rowset_size_;  // Used by SQLExtendedFetch instead of the ARD array size.
  bool is_prepared_;
  bool has_reached_end_of_result_;
};
}  // namespace ODBC
