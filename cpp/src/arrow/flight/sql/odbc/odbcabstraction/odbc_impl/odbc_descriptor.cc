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
    if (records[i - 1].m_is_bound) {
      return i;
    }
  }
  return 0;
}
}  // namespace

// Public
// =========================================================================================
ODBCDescriptor::ODBCDescriptor(Diagnostics& base_diagnostics, ODBCConnection* conn,
                               ODBCStatement* stmt, bool is_app_descriptor,
                               bool is_writable, bool is_2x_connection)
    : m_diagnostics(base_diagnostics.GetVendor(),
                    base_diagnostics.GetDataSourceComponent(),
                    driver::odbcabstraction::V_3),
      m_owning_connection(conn),
      m_parent_statement(stmt),
      m_array_status_ptr(nullptr),
      m_bind_offset_ptr(nullptr),
      m_rows_proccessed_ptr(nullptr),
      m_array_size(1),
      m_bind_type(SQL_BIND_BY_COLUMN),
      m_highest_one_based_bound_record(0),
      m_is_2x_connection(is_2x_connection),
      m_is_app_descriptor(is_app_descriptor),
      m_is_writable(is_writable),
      m_has_bindings_changed(true) {}

Diagnostics& ODBCDescriptor::GetDiagnosticsImpl() { return m_diagnostics; }

ODBCConnection& ODBCDescriptor::GetConnection() {
  if (m_owning_connection) {
    return *m_owning_connection;
  }
  assert(m_parent_statement);
  return m_parent_statement->GetConnection();
}

void ODBCDescriptor::SetHeaderField(SQLSMALLINT field_identifier, SQLPOINTER value,
                                    SQLINTEGER buffer_length) {
  // Only these two fields can be set on the IRD.
  if (!m_is_writable && field_identifier != SQL_DESC_ARRAY_STATUS_PTR &&
      field_identifier != SQL_DESC_ROWS_PROCESSED_PTR) {
    throw DriverException("Cannot modify read-only descriptor", "HY016");
  }

  switch (field_identifier) {
    case SQL_DESC_ALLOC_TYPE:
      throw DriverException("Invalid descriptor field", "HY091");
    case SQL_DESC_ARRAY_SIZE:
      SetAttribute(value, m_array_size);
      m_has_bindings_changed = true;
      break;
    case SQL_DESC_ARRAY_STATUS_PTR:
      SetPointerAttribute(value, m_array_status_ptr);
      m_has_bindings_changed = true;
      break;
    case SQL_DESC_BIND_OFFSET_PTR:
      SetPointerAttribute(value, m_bind_offset_ptr);
      m_has_bindings_changed = true;
      break;
    case SQL_DESC_BIND_TYPE:
      SetAttribute(value, m_bind_type);
      m_has_bindings_changed = true;
      break;
    case SQL_DESC_ROWS_PROCESSED_PTR:
      SetPointerAttribute(value, m_rows_proccessed_ptr);
      m_has_bindings_changed = true;
      break;
    case SQL_DESC_COUNT: {
      SQLSMALLINT new_count;
      SetAttribute(value, new_count);
      m_records.resize(new_count);

      if (m_is_app_descriptor && new_count <= m_highest_one_based_bound_record) {
        m_highest_one_based_bound_record = CalculateHighestBoundRecord(m_records);
      } else {
        m_highest_one_based_bound_record = new_count;
      }
      m_has_bindings_changed = true;
      break;
    }
    default:
      throw DriverException("Invalid descriptor field", "HY091");
  }
}

void ODBCDescriptor::SetField(SQLSMALLINT record_number, SQLSMALLINT field_identifier,
                              SQLPOINTER value, SQLINTEGER buffer_length) {
  if (!m_is_writable) {
    throw DriverException("Cannot modify read-only descriptor", "HY016");
  }

  // Handle header fields before validating the record number.
  switch (field_identifier) {
    case SQL_DESC_ALLOC_TYPE:
    case SQL_DESC_ARRAY_SIZE:
    case SQL_DESC_ARRAY_STATUS_PTR:
    case SQL_DESC_BIND_OFFSET_PTR:
    case SQL_DESC_BIND_TYPE:
    case SQL_DESC_ROWS_PROCESSED_PTR:
    case SQL_DESC_COUNT:
      SetHeaderField(field_identifier, value, buffer_length);
      return;
    default:
      break;
  }

  if (record_number == 0) {
    throw DriverException("Bookmarks are unsupported.", "07009");
  }

  if (record_number > m_records.size()) {
    throw DriverException("Invalid descriptor index", "HY009");
  }

  SQLSMALLINT zero_based_record = record_number - 1;
  DescriptorRecord& record = m_records[zero_based_record];
  switch (field_identifier) {
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
      SetAttribute(value, record.m_concise_type);
      record.m_is_bound = false;
      m_has_bindings_changed = true;
      break;
    case SQL_DESC_DATA_PTR:
      SetDataPtrOnRecord(value, record_number);
      break;
    case SQL_DESC_DATETIME_INTERVAL_CODE:
      SetAttribute(value, record.m_datetime_interval_code);
      record.m_is_bound = false;
      m_has_bindings_changed = true;
      break;
    case SQL_DESC_DATETIME_INTERVAL_PRECISION:
      SetAttribute(value, record.m_datetime_interval_precision);
      record.m_is_bound = false;
      m_has_bindings_changed = true;
      break;
    case SQL_DESC_INDICATOR_PTR:
    case SQL_DESC_OCTET_LENGTH_PTR:
      SetPointerAttribute(value, record.m_indicator_ptr);
      m_has_bindings_changed = true;
      break;
    case SQL_DESC_LENGTH:
      SetAttribute(value, record.m_length);
      record.m_is_bound = false;
      m_has_bindings_changed = true;
      break;
    case SQL_DESC_NAME:
      SetAttributeUTF8(value, buffer_length, record.m_name);
      m_has_bindings_changed = true;
      break;
    case SQL_DESC_OCTET_LENGTH:
      SetAttribute(value, record.m_octet_length);
      record.m_is_bound = false;
      m_has_bindings_changed = true;
      break;
    case SQL_DESC_PARAMETER_TYPE:
      SetAttribute(value, record.m_param_type);
      record.m_is_bound = false;
      m_has_bindings_changed = true;
      break;
    case SQL_DESC_PRECISION:
      SetAttribute(value, record.m_precision);
      record.m_is_bound = false;
      m_has_bindings_changed = true;
      break;
    case SQL_DESC_SCALE:
      SetAttribute(value, record.m_scale);
      record.m_is_bound = false;
      m_has_bindings_changed = true;
      break;
    case SQL_DESC_TYPE:
      SetAttribute(value, record.m_type);
      record.m_is_bound = false;
      m_has_bindings_changed = true;
      break;
    default:
      throw DriverException("Invalid descriptor field", "HY091");
  }
}

void ODBCDescriptor::GetHeaderField(SQLSMALLINT field_identifier, SQLPOINTER value,
                                    SQLINTEGER buffer_length,
                                    SQLINTEGER* output_length) const {
  switch (field_identifier) {
    case SQL_DESC_ALLOC_TYPE: {
      SQLSMALLINT result;
      if (m_owning_connection) {
        result = SQL_DESC_ALLOC_USER;
      } else {
        result = SQL_DESC_ALLOC_AUTO;
      }
      GetAttribute(result, value, buffer_length, output_length);
      break;
    }
    case SQL_DESC_ARRAY_SIZE:
      GetAttribute(m_array_size, value, buffer_length, output_length);
      break;
    case SQL_DESC_ARRAY_STATUS_PTR:
      GetAttribute(m_array_status_ptr, value, buffer_length, output_length);
      break;
    case SQL_DESC_BIND_OFFSET_PTR:
      GetAttribute(m_bind_offset_ptr, value, buffer_length, output_length);
      break;
    case SQL_DESC_BIND_TYPE:
      GetAttribute(m_bind_type, value, buffer_length, output_length);
      break;
    case SQL_DESC_ROWS_PROCESSED_PTR:
      GetAttribute(m_rows_proccessed_ptr, value, buffer_length, output_length);
      break;
    case SQL_DESC_COUNT: {
      GetAttribute(m_highest_one_based_bound_record, value, buffer_length, output_length);
      break;
    }
    default:
      throw DriverException("Invalid descriptor field", "HY091");
  }
}

void ODBCDescriptor::GetField(SQLSMALLINT record_number, SQLSMALLINT field_identifier,
                              SQLPOINTER value, SQLINTEGER buffer_length,
                              SQLINTEGER* output_length) {
  // Handle header fields before validating the record number.
  switch (field_identifier) {
    case SQL_DESC_ALLOC_TYPE:
    case SQL_DESC_ARRAY_SIZE:
    case SQL_DESC_ARRAY_STATUS_PTR:
    case SQL_DESC_BIND_OFFSET_PTR:
    case SQL_DESC_BIND_TYPE:
    case SQL_DESC_ROWS_PROCESSED_PTR:
    case SQL_DESC_COUNT:
      GetHeaderField(field_identifier, value, buffer_length, output_length);
      return;
    default:
      break;
  }

  if (record_number == 0) {
    throw DriverException("Bookmarks are unsupported.", "07009");
  }

  if (record_number > m_records.size()) {
    throw DriverException("Invalid descriptor index", "07009");
  }

  // TODO: Restrict fields based on AppDescriptor IPD, and IRD.

  SQLSMALLINT zero_based_record = record_number - 1;
  const DescriptorRecord& record = m_records[zero_based_record];
  switch (field_identifier) {
    case SQL_DESC_BASE_COLUMN_NAME:
      GetAttributeUTF8(record.m_base_column_name, value, buffer_length, output_length,
                       GetDiagnostics());
      break;
    case SQL_DESC_BASE_TABLE_NAME:
      GetAttributeUTF8(record.m_base_table_name, value, buffer_length, output_length,
                       GetDiagnostics());
      break;
    case SQL_DESC_CATALOG_NAME:
      GetAttributeUTF8(record.m_catalog_name, value, buffer_length, output_length,
                       GetDiagnostics());
      break;
    case SQL_DESC_LABEL:
      GetAttributeUTF8(record.m_label, value, buffer_length, output_length,
                       GetDiagnostics());
      break;
    case SQL_DESC_LITERAL_PREFIX:
      GetAttributeUTF8(record.m_literal_prefix, value, buffer_length, output_length,
                       GetDiagnostics());
      break;
    case SQL_DESC_LITERAL_SUFFIX:
      GetAttributeUTF8(record.m_literal_suffix, value, buffer_length, output_length,
                       GetDiagnostics());
      break;
    case SQL_DESC_LOCAL_TYPE_NAME:
      GetAttributeUTF8(record.m_local_type_name, value, buffer_length, output_length,
                       GetDiagnostics());
      break;
    case SQL_DESC_NAME:
      GetAttributeUTF8(record.m_name, value, buffer_length, output_length,
                       GetDiagnostics());
      break;
    case SQL_DESC_SCHEMA_NAME:
      GetAttributeUTF8(record.m_schema_name, value, buffer_length, output_length,
                       GetDiagnostics());
      break;
    case SQL_DESC_TABLE_NAME:
      GetAttributeUTF8(record.m_table_name, value, buffer_length, output_length,
                       GetDiagnostics());
      break;
    case SQL_DESC_TYPE_NAME:
      GetAttributeUTF8(record.m_type_name, value, buffer_length, output_length,
                       GetDiagnostics());
      break;

    case SQL_DESC_DATA_PTR:
      GetAttribute(record.m_data_ptr, value, buffer_length, output_length);
      break;
    case SQL_DESC_INDICATOR_PTR:
    case SQL_DESC_OCTET_LENGTH_PTR:
      GetAttribute(record.m_indicator_ptr, value, buffer_length, output_length);
      break;

    case SQL_DESC_LENGTH:
      GetAttribute(record.m_length, value, buffer_length, output_length);
      break;
    case SQL_DESC_OCTET_LENGTH:
      GetAttribute(record.m_octet_length, value, buffer_length, output_length);
      break;

    case SQL_DESC_AUTO_UNIQUE_VALUE:
      GetAttribute(record.m_auto_unique_value, value, buffer_length, output_length);
      break;
    case SQL_DESC_CASE_SENSITIVE:
      GetAttribute(record.m_case_sensitive, value, buffer_length, output_length);
      break;
    case SQL_DESC_DATETIME_INTERVAL_PRECISION:
      GetAttribute(record.m_datetime_interval_precision, value, buffer_length,
                   output_length);
      break;
    case SQL_DESC_NUM_PREC_RADIX:
      GetAttribute(record.m_num_prec_radix, value, buffer_length, output_length);
      break;

    case SQL_DESC_CONCISE_TYPE:
      GetAttribute(record.m_concise_type, value, buffer_length, output_length);
      break;
    case SQL_DESC_DATETIME_INTERVAL_CODE:
      GetAttribute(record.m_datetime_interval_code, value, buffer_length, output_length);
      break;
    case SQL_DESC_DISPLAY_SIZE:
      GetAttribute(record.m_display_size, value, buffer_length, output_length);
      break;
    case SQL_DESC_FIXED_PREC_SCALE:
      GetAttribute(record.m_fixed_prec_scale, value, buffer_length, output_length);
      break;
    case SQL_DESC_NULLABLE:
      GetAttribute(record.m_nullable, value, buffer_length, output_length);
      break;
    case SQL_DESC_PARAMETER_TYPE:
      GetAttribute(record.m_param_type, value, buffer_length, output_length);
      break;
    case SQL_DESC_PRECISION:
      GetAttribute(record.m_precision, value, buffer_length, output_length);
      break;
    case SQL_DESC_ROWVER:
      GetAttribute(record.m_row_ver, value, buffer_length, output_length);
      break;
    case SQL_DESC_SCALE:
      GetAttribute(record.m_scale, value, buffer_length, output_length);
      break;
    case SQL_DESC_SEARCHABLE:
      GetAttribute(record.m_searchable, value, buffer_length, output_length);
      break;
    case SQL_DESC_TYPE:
      GetAttribute(record.m_type, value, buffer_length, output_length);
      break;
    case SQL_DESC_UNNAMED:
      GetAttribute(record.m_unnamed, value, buffer_length, output_length);
      break;
    case SQL_DESC_UNSIGNED:
      GetAttribute(record.m_unsigned, value, buffer_length, output_length);
      break;
    case SQL_DESC_UPDATABLE:
      GetAttribute(record.m_updatable, value, buffer_length, output_length);
      break;
    default:
      throw DriverException("Invalid descriptor field", "HY091");
  }
}

SQLSMALLINT ODBCDescriptor::GetAllocType() const {
  return m_owning_connection != nullptr ? SQL_DESC_ALLOC_USER : SQL_DESC_ALLOC_AUTO;
}

bool ODBCDescriptor::IsAppDescriptor() const { return m_is_app_descriptor; }

void ODBCDescriptor::RegisterToStatement(ODBCStatement* statement, bool is_apd) {
  if (is_apd) {
    m_registered_on_statements_as_apd.push_back(statement);
  } else {
    m_registered_on_statements_as_ard.push_back(statement);
  }
}

void ODBCDescriptor::DetachFromStatement(ODBCStatement* statement, bool is_apd) {
  auto& vector_to_update =
      is_apd ? m_registered_on_statements_as_apd : m_registered_on_statements_as_ard;
  auto it = std::find(vector_to_update.begin(), vector_to_update.end(), statement);
  if (it != vector_to_update.end()) {
    vector_to_update.erase(it);
  }
}

void ODBCDescriptor::ReleaseDescriptor() {
  for (ODBCStatement* stmt : m_registered_on_statements_as_apd) {
    stmt->RevertAppDescriptor(true);
  }

  for (ODBCStatement* stmt : m_registered_on_statements_as_ard) {
    stmt->RevertAppDescriptor(false);
  }

  if (m_owning_connection) {
    m_owning_connection->DropDescriptor(this);
  }
}

void ODBCDescriptor::PopulateFromResultSetMetadata(ResultSetMetadata* rsmd) {
  m_records.assign(rsmd->GetColumnCount(), DescriptorRecord());
  m_highest_one_based_bound_record = m_records.size() + 1;

  for (size_t i = 0; i < m_records.size(); ++i) {
    size_t one_based_index = i + 1;
    m_records[i].m_base_column_name = rsmd->GetBaseColumnName(one_based_index);
    m_records[i].m_base_table_name = rsmd->GetBaseTableName(one_based_index);
    m_records[i].m_catalog_name = rsmd->GetCatalogName(one_based_index);
    m_records[i].m_label = rsmd->GetColumnLabel(one_based_index);
    m_records[i].m_literal_prefix = rsmd->GetLiteralPrefix(one_based_index);
    m_records[i].m_literal_suffix = rsmd->GetLiteralSuffix(one_based_index);
    m_records[i].m_local_type_name = rsmd->GetLocalTypeName(one_based_index);
    m_records[i].m_name = rsmd->GetName(one_based_index);
    m_records[i].m_schema_name = rsmd->GetSchemaName(one_based_index);
    m_records[i].m_table_name = rsmd->GetTableName(one_based_index);
    m_records[i].m_type_name = rsmd->GetTypeName(one_based_index);
    m_records[i].m_concise_type = GetSqlTypeForODBCVersion(
        rsmd->GetConciseType(one_based_index), m_is_2x_connection);
    m_records[i].m_data_ptr = nullptr;
    m_records[i].m_indicator_ptr = nullptr;
    m_records[i].m_display_size = rsmd->GetColumnDisplaySize(one_based_index);
    m_records[i].m_octet_length = rsmd->GetOctetLength(one_based_index);
    m_records[i].m_length = rsmd->GetLength(one_based_index);
    m_records[i].m_auto_unique_value =
        rsmd->IsAutoUnique(one_based_index) ? SQL_TRUE : SQL_FALSE;
    m_records[i].m_case_sensitive =
        rsmd->IsCaseSensitive(one_based_index) ? SQL_TRUE : SQL_FALSE;
    m_records[i].m_datetime_interval_precision;  // TODO - update when rsmd adds this
    m_records[i].m_num_prec_radix = rsmd->GetNumPrecRadix(one_based_index);
    m_records[i].m_datetime_interval_code;  // TODO
    m_records[i].m_fixed_prec_scale =
        rsmd->IsFixedPrecScale(one_based_index) ? SQL_TRUE : SQL_FALSE;
    m_records[i].m_nullable = rsmd->IsNullable(one_based_index);
    m_records[i].m_param_type = SQL_PARAM_INPUT;
    m_records[i].m_precision = rsmd->GetPrecision(one_based_index);
    m_records[i].m_row_ver = SQL_FALSE;
    m_records[i].m_scale = rsmd->GetScale(one_based_index);
    m_records[i].m_searchable = rsmd->IsSearchable(one_based_index);
    m_records[i].m_type =
        GetSqlTypeForODBCVersion(rsmd->GetDataType(one_based_index), m_is_2x_connection);
    m_records[i].m_unnamed = m_records[i].m_name.empty() ? SQL_TRUE : SQL_FALSE;
    m_records[i].m_unsigned = rsmd->IsUnsigned(one_based_index) ? SQL_TRUE : SQL_FALSE;
    m_records[i].m_updatable = rsmd->GetUpdatable(one_based_index);
  }
}

const std::vector<DescriptorRecord>& ODBCDescriptor::GetRecords() const {
  return m_records;
}

std::vector<DescriptorRecord>& ODBCDescriptor::GetRecords() { return m_records; }

void ODBCDescriptor::BindCol(SQLSMALLINT record_number, SQLSMALLINT c_type,
                             SQLPOINTER data_ptr, SQLLEN buffer_length,
                             SQLLEN* indicator_ptr) {
  assert(m_is_app_descriptor);
  assert(m_is_writable);

  // The set of records auto-expands to the supplied record number.
  if (m_records.size() < record_number) {
    m_records.resize(record_number);
  }

  SQLSMALLINT zero_based_record_index = record_number - 1;
  DescriptorRecord& record = m_records[zero_based_record_index];

  record.m_type = c_type;
  record.m_indicator_ptr = indicator_ptr;
  record.m_length = buffer_length;

  // Initialize default precision and scale for SQL_C_NUMERIC.
  if (record.m_type == SQL_C_NUMERIC) {
    record.m_precision = 38;
    record.m_scale = 0;
  }
  SetDataPtrOnRecord(data_ptr, record_number);
}

void ODBCDescriptor::SetDataPtrOnRecord(SQLPOINTER data_ptr, SQLSMALLINT record_number) {
  assert(record_number <= m_records.size());
  DescriptorRecord& record = m_records[record_number - 1];
  if (data_ptr) {
    record.CheckConsistency();
    record.m_is_bound = true;
  } else {
    record.m_is_bound = false;
  }
  record.m_data_ptr = data_ptr;

  // Bookkeeping on the highest bound record (used for returning SQL_DESC_COUNT)
  if (m_highest_one_based_bound_record < record_number && data_ptr) {
    m_highest_one_based_bound_record = record_number;
  } else if (m_highest_one_based_bound_record == record_number && !data_ptr) {
    m_highest_one_based_bound_record = CalculateHighestBoundRecord(m_records);
  }
  m_has_bindings_changed = true;
}

void DescriptorRecord::CheckConsistency() {
  // TODO
}
