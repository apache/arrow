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
  std::string m_base_column_name;
  std::string m_base_table_name;
  std::string m_catalog_name;
  std::string m_label;
  std::string m_literal_prefix;
  std::string m_literal_suffix;
  std::string m_local_type_name;
  std::string m_name;
  std::string m_schema_name;
  std::string m_table_name;
  std::string m_type_name;
  SQLPOINTER m_data_ptr = NULL;
  SQLLEN* m_indicator_ptr = NULL;
  SQLLEN m_display_size = 0;
  SQLLEN m_octet_length = 0;
  SQLULEN m_length = 0;
  SQLINTEGER m_auto_unique_value;
  SQLINTEGER m_case_sensitive = SQL_TRUE;
  SQLINTEGER m_datetime_interval_precision = 0;
  SQLINTEGER m_num_prec_radix = 0;
  SQLSMALLINT m_concise_type = SQL_C_DEFAULT;
  SQLSMALLINT m_datetime_interval_code = 0;
  SQLSMALLINT m_fixed_prec_scale = 0;
  SQLSMALLINT m_nullable = SQL_NULLABLE_UNKNOWN;
  SQLSMALLINT m_param_type = SQL_PARAM_INPUT;
  SQLSMALLINT m_precision = 0;
  SQLSMALLINT m_row_ver = 0;
  SQLSMALLINT m_scale = 0;
  SQLSMALLINT m_searchable = SQL_SEARCHABLE;
  SQLSMALLINT m_type = SQL_C_DEFAULT;
  SQLSMALLINT m_unnamed = SQL_TRUE;
  SQLSMALLINT m_unsigned = SQL_FALSE;
  SQLSMALLINT m_updatable = SQL_FALSE;
  bool m_is_bound = false;

  void CheckConsistency();
};

class ODBCDescriptor : public ODBCHandle<ODBCDescriptor> {
 public:
  /// \brief Construct a new ODBCDescriptor object. Link the descriptor to a connection,
  /// if applicable. A nullptr should be supplied for conn if the descriptor should not be
  /// linked.
  ODBCDescriptor(driver::odbcabstraction::Diagnostics& base_diagnostics,
                 ODBCConnection* conn, ODBCStatement* stmt, bool is_app_descriptor,
                 bool is_writable, bool is_2x_connection);

  driver::odbcabstraction::Diagnostics& GetDiagnosticsImpl();

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

  inline bool HaveBindingsChanged() const { return m_has_bindings_changed; }

  void RegisterToStatement(ODBCStatement* statement, bool is_apd);
  void DetachFromStatement(ODBCStatement* statement, bool is_apd);
  void ReleaseDescriptor();

  void PopulateFromResultSetMetadata(driver::odbcabstraction::ResultSetMetadata* rsmd);

  const std::vector<DescriptorRecord>& GetRecords() const;
  std::vector<DescriptorRecord>& GetRecords();

  void BindCol(SQLSMALLINT record_number, SQLSMALLINT c_type, SQLPOINTER data_ptr,
               SQLLEN buffer_length, SQLLEN* indicator_ptr);
  void SetDataPtrOnRecord(SQLPOINTER data_ptr, SQLSMALLINT rec_number);

  inline SQLULEN GetBindOffset() { return m_bind_offset_ptr ? *m_bind_offset_ptr : 0UL; }

  inline SQLULEN GetBoundStructOffset() {
    // If this is SQL_BIND_BY_COLUMN, m_bind_type is zero which indicates no offset due to
    // use of a bound struct. If this is non-zero, row-wise binding is being used so the
    // app should set this to sizeof(their struct).
    return m_bind_type;
  }

  inline SQLULEN GetArraySize() { return m_array_size; }

  inline SQLUSMALLINT* GetArrayStatusPtr() { return m_array_status_ptr; }

  inline void SetRowsProcessed(SQLULEN rows) {
    if (m_rows_proccessed_ptr) {
      *m_rows_proccessed_ptr = rows;
    }
  }

  inline void NotifyBindingsHavePropagated() { m_has_bindings_changed = false; }

  inline void NotifyBindingsHaveChanged() { m_has_bindings_changed = true; }

 private:
  driver::odbcabstraction::Diagnostics m_diagnostics;
  std::vector<ODBCStatement*> m_registered_on_statements_as_apd;
  std::vector<ODBCStatement*> m_registered_on_statements_as_ard;
  std::vector<DescriptorRecord> m_records;
  ODBCConnection* m_owning_connection;
  ODBCStatement* m_parent_statement;
  SQLUSMALLINT* m_array_status_ptr;
  SQLULEN* m_bind_offset_ptr;
  SQLULEN* m_rows_proccessed_ptr;
  SQLULEN m_array_size;
  SQLINTEGER m_bind_type;
  SQLSMALLINT m_highest_one_based_bound_record;
  const bool m_is_2x_connection;
  bool m_is_app_descriptor;
  bool m_is_writable;
  bool m_has_bindings_changed;
};
}  // namespace ODBC
