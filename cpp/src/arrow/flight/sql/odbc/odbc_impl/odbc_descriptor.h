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

#include "arrow/flight/sql/odbc/odbc_impl/odbc_handle.h"
#include "arrow/flight/sql/odbc/odbc_impl/type_fwd.h"

#include <sql.h>
#include <sqlext.h>
#include <sqltypes.h>
#include <memory>
#include <string>
#include <vector>

namespace ODBC {

struct DescriptorRecord {
  std::string base_column_name;
  std::string base_table_name;
  std::string catalog_name;
  std::string label;
  std::string literal_prefix;
  std::string literal_suffix;
  std::string local_type_name;
  std::string name;
  std::string schema_name;
  std::string table_name;
  std::string type_name;
  SQLPOINTER data_ptr = NULL;
  SQLLEN* indicator_ptr = NULL;
  SQLLEN display_size = 0;
  SQLLEN octet_length = 0;
  SQLULEN length = 0;
  SQLINTEGER auto_unique_value;
  SQLINTEGER case_sensitive = SQL_TRUE;
  SQLINTEGER datetime_interval_precision = 0;
  SQLINTEGER num_prec_radix = 0;
  SQLSMALLINT concise_type = SQL_C_DEFAULT;
  SQLSMALLINT datetime_interval_code = 0;
  SQLSMALLINT fixed_prec_scale = 0;
  SQLSMALLINT nullable = SQL_NULLABLE_UNKNOWN;
  SQLSMALLINT param_type = SQL_PARAM_INPUT;
  SQLSMALLINT precision = 0;
  SQLSMALLINT row_ver = 0;
  SQLSMALLINT scale = 0;
  SQLSMALLINT searchable = SQL_SEARCHABLE;
  SQLSMALLINT type = SQL_C_DEFAULT;
  SQLSMALLINT unnamed = SQL_TRUE;
  SQLSMALLINT is_unsigned = SQL_FALSE;
  SQLSMALLINT updatable = SQL_FALSE;
  bool is_bound = false;

  void CheckConsistency();
};

class ODBCDescriptor : public ODBCHandle<ODBCDescriptor> {
 public:
  /// \brief Construct a new ODBCDescriptor object. Link the descriptor to a connection,
  /// if applicable. A nullptr should be supplied for conn if the descriptor should not be
  /// linked.
  ODBCDescriptor(arrow::flight::sql::odbc::Diagnostics& base_diagnostics,
                 ODBCConnection* conn, ODBCStatement* stmt, bool is_app_descriptor,
                 bool is_writable, bool is_2x_connection);

  arrow::flight::sql::odbc::Diagnostics& GetDiagnosticsImpl();

  ODBCConnection& GetConnection();

  void SetHeaderField(SQLSMALLINT field_identifier, SQLPOINTER value,
                      SQLINTEGER buffer_length);
  void SetField(SQLSMALLINT record_number, SQLSMALLINT field_identifier, SQLPOINTER value,
                SQLINTEGER buffer_length);
  void GetHeaderField(SQLSMALLINT field_identifier, SQLPOINTER value,
                      SQLINTEGER buffer_length, SQLINTEGER* output_length) const;
  void GetField(SQLSMALLINT record_number, SQLSMALLINT field_identifier, SQLPOINTER value,
                SQLINTEGER buffer_length, SQLINTEGER* output_length);
  SQLSMALLINT GetAllocType() const;
  bool IsAppDescriptor() const;

  inline bool HaveBindingsChanged() const { return has_bindings_changed_; }

  void RegisterToStatement(ODBCStatement* statement, bool is_apd);
  void DetachFromStatement(ODBCStatement* statement, bool is_apd);
  void ReleaseDescriptor();

  void PopulateFromResultSetMetadata(arrow::flight::sql::odbc::ResultSetMetadata* rsmd);

  const std::vector<DescriptorRecord>& GetRecords() const;
  std::vector<DescriptorRecord>& GetRecords();

  void BindCol(SQLSMALLINT record_number, SQLSMALLINT c_type, SQLPOINTER data_ptr,
               SQLLEN buffer_length, SQLLEN* indicator_ptr);
  void SetDataPtrOnRecord(SQLPOINTER data_ptr, SQLSMALLINT rec_number);

  inline SQLULEN GetBindOffset() { return bind_offset_ptr_ ? *bind_offset_ptr_ : 0UL; }

  inline SQLULEN GetBoundStructOffset() {
    // If this is SQL_BIND_BY_COLUMN, bind_type_ is zero which indicates no offset due to
    // use of a bound struct. If this is non-zero, row-wise binding is being used so the
    // app should set this to sizeof(their struct).
    return bind_type_;
  }

  inline SQLULEN GetArraySize() { return array_size_; }

  inline SQLUSMALLINT* GetArrayStatusPtr() { return array_status_ptr_; }

  inline void SetRowsProcessed(SQLULEN rows) {
    if (rows_proccessed_ptr_) {
      *rows_proccessed_ptr_ = rows;
    }
  }

  inline void NotifyBindingsHavePropagated() { has_bindings_changed_ = false; }

  inline void NotifyBindingsHaveChanged() { has_bindings_changed_ = true; }

 private:
  arrow::flight::sql::odbc::Diagnostics diagnostics_;
  std::vector<ODBCStatement*> registered_on_statements_as_apd_;
  std::vector<ODBCStatement*> registered_on_statements_as_ard_;
  std::vector<DescriptorRecord> records_;
  ODBCConnection* owning_connection_;
  ODBCStatement* parent_statement_;
  SQLUSMALLINT* array_status_ptr_;
  SQLULEN* bind_offset_ptr_;
  SQLULEN* rows_proccessed_ptr_;
  SQLULEN array_size_;
  SQLINTEGER bind_type_;
  SQLSMALLINT highest_one_based_bound_record_;
  const bool is_2x_connection_;
  bool is_app_descriptor_;
  bool is_writable_;
  bool has_bindings_changed_;
};
}  // namespace ODBC
