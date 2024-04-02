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

using arrow::flight::sql::odbc::Diagnostics;
using arrow::flight::sql::odbc::DriverException;
using arrow::flight::sql::odbc::ResultSetMetadata;

namespace {
SQLSMALLINT CalculateHighestBoundRecord(const std::vector<DescriptorRecord>& records) {
  // Most applications will bind every column, so optimistically assume that we'll
  // find the next bound record fastest by counting backwards.
  for (size_t i = records.size(); i > 0; --i) {
    if (records[i - 1].is_bound) {
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
    : diagnostics_(base_diagnostics.GetVendor(),
                   base_diagnostics.GetDataSourceComponent(),
                   arrow::flight::sql::odbc::V_3),
      owning_connection_(conn),
      parent_statement_(stmt),
      array_status_ptr_(nullptr),
      bind_offset_ptr_(nullptr),
      rows_proccessed_ptr_(nullptr),
      array_size_(1),
      bind_type_(SQL_BIND_BY_COLUMN),
      highest_one_based_bound_record_(0),
      is_2x_connection_(is_2x_connection),
      is_app_descriptor_(is_app_descriptor),
      is_writable_(is_writable),
      has_bindings_changed_(true) {}

Diagnostics& ODBCDescriptor::GetDiagnosticsImpl() { return diagnostics_; }

ODBCConnection& ODBCDescriptor::GetConnection() {
  if (owning_connection_) {
    return *owning_connection_;
  }
  assert(parent_statement_);
  return parent_statement_->GetConnection();
}

void ODBCDescriptor::SetHeaderField(SQLSMALLINT field_identifier, SQLPOINTER value,
                                    SQLINTEGER buffer_length) {
  // Only these two fields can be set on the IRD.
  if (!is_writable_ && field_identifier != SQL_DESC_ARRAY_STATUS_PTR &&
      field_identifier != SQL_DESC_ROWS_PROCESSED_PTR) {
    throw DriverException("Cannot modify read-only descriptor", "HY016");
  }

  switch (field_identifier) {
    case SQL_DESC_ALLOC_TYPE:
      throw DriverException("Invalid descriptor field", "HY091");
    case SQL_DESC_ARRAY_SIZE:
      SetAttribute(value, array_size_);
      has_bindings_changed_ = true;
      break;
    case SQL_DESC_ARRAY_STATUS_PTR:
      SetPointerAttribute(value, array_status_ptr_);
      has_bindings_changed_ = true;
      break;
    case SQL_DESC_BIND_OFFSET_PTR:
      SetPointerAttribute(value, bind_offset_ptr_);
      has_bindings_changed_ = true;
      break;
    case SQL_DESC_BIND_TYPE:
      SetAttribute(value, bind_type_);
      has_bindings_changed_ = true;
      break;
    case SQL_DESC_ROWS_PROCESSED_PTR:
      SetPointerAttribute(value, rows_proccessed_ptr_);
      has_bindings_changed_ = true;
      break;
    case SQL_DESC_COUNT: {
      SQLSMALLINT new_count;
      SetAttribute(value, new_count);
      records_.resize(new_count);

      if (is_app_descriptor_ && new_count <= highest_one_based_bound_record_) {
        highest_one_based_bound_record_ = CalculateHighestBoundRecord(records_);
      } else {
        highest_one_based_bound_record_ = new_count;
      }
      has_bindings_changed_ = true;
      break;
    }
    default:
      throw DriverException("Invalid descriptor field", "HY091");
  }
}

void ODBCDescriptor::SetField(SQLSMALLINT record_number, SQLSMALLINT field_identifier,
                              SQLPOINTER value, SQLINTEGER buffer_length) {
  if (!is_writable_) {
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

  if (record_number > records_.size()) {
    throw DriverException("Invalid descriptor index", "HY009");
  }

  SQLSMALLINT zero_based_record = record_number - 1;
  DescriptorRecord& record = records_[zero_based_record];
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
      SetAttribute(value, record.concise_type);
      record.is_bound = false;
      has_bindings_changed_ = true;
      break;
    case SQL_DESC_DATA_PTR:
      SetDataPtrOnRecord(value, record_number);
      break;
    case SQL_DESC_DATETIME_INTERVAL_CODE:
      SetAttribute(value, record.datetime_interval_code);
      record.is_bound = false;
      has_bindings_changed_ = true;
      break;
    case SQL_DESC_DATETIME_INTERVAL_PRECISION:
      SetAttribute(value, record.datetime_interval_precision);
      record.is_bound = false;
      has_bindings_changed_ = true;
      break;
    case SQL_DESC_INDICATOR_PTR:
    case SQL_DESC_OCTET_LENGTH_PTR:
      SetPointerAttribute(value, record.indicator_ptr);
      has_bindings_changed_ = true;
      break;
    case SQL_DESC_LENGTH:
      SetAttribute(value, record.length);
      record.is_bound = false;
      has_bindings_changed_ = true;
      break;
    case SQL_DESC_NAME:
      SetAttributeUTF8(value, buffer_length, record.name);
      has_bindings_changed_ = true;
      break;
    case SQL_DESC_OCTET_LENGTH:
      SetAttribute(value, record.octet_length);
      record.is_bound = false;
      has_bindings_changed_ = true;
      break;
    case SQL_DESC_PARAMETER_TYPE:
      SetAttribute(value, record.param_type);
      record.is_bound = false;
      has_bindings_changed_ = true;
      break;
    case SQL_DESC_PRECISION:
      SetAttribute(value, record.precision);
      record.is_bound = false;
      has_bindings_changed_ = true;
      break;
    case SQL_DESC_SCALE:
      SetAttribute(value, record.scale);
      record.is_bound = false;
      has_bindings_changed_ = true;
      break;
    case SQL_DESC_TYPE:
      SetAttribute(value, record.type);
      record.is_bound = false;
      has_bindings_changed_ = true;
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
      if (owning_connection_) {
        result = SQL_DESC_ALLOC_USER;
      } else {
        result = SQL_DESC_ALLOC_AUTO;
      }
      GetAttribute(result, value, buffer_length, output_length);
      break;
    }
    case SQL_DESC_ARRAY_SIZE:
      GetAttribute(array_size_, value, buffer_length, output_length);
      break;
    case SQL_DESC_ARRAY_STATUS_PTR:
      GetAttribute(array_status_ptr_, value, buffer_length, output_length);
      break;
    case SQL_DESC_BIND_OFFSET_PTR:
      GetAttribute(bind_offset_ptr_, value, buffer_length, output_length);
      break;
    case SQL_DESC_BIND_TYPE:
      GetAttribute(bind_type_, value, buffer_length, output_length);
      break;
    case SQL_DESC_ROWS_PROCESSED_PTR:
      GetAttribute(rows_proccessed_ptr_, value, buffer_length, output_length);
      break;
    case SQL_DESC_COUNT: {
      // highest_one_based_bound_record_ equals number of records + 1
      GetAttribute(static_cast<SQLSMALLINT>(highest_one_based_bound_record_ - 1), value,
                   buffer_length, output_length);
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

  if (record_number > records_.size()) {
    throw DriverException("Invalid descriptor index", "07009");
  }

  // TODO: Restrict fields based on AppDescriptor IPD, and IRD.

  bool length_in_bytes = true;
  SQLSMALLINT zero_based_record = record_number - 1;
  const DescriptorRecord& record = records_[zero_based_record];
  switch (field_identifier) {
    case SQL_DESC_BASE_COLUMN_NAME:
      GetAttributeSQLWCHAR(record.base_column_name, length_in_bytes, value, buffer_length,
                           output_length, GetDiagnostics());
      break;
    case SQL_DESC_BASE_TABLE_NAME:
      GetAttributeSQLWCHAR(record.base_table_name, length_in_bytes, value, buffer_length,
                           output_length, GetDiagnostics());
      break;
    case SQL_DESC_CATALOG_NAME:
      GetAttributeSQLWCHAR(record.catalog_name, length_in_bytes, value, buffer_length,
                           output_length, GetDiagnostics());
      break;
    case SQL_DESC_LABEL:
      GetAttributeSQLWCHAR(record.label, length_in_bytes, value, buffer_length,
                           output_length, GetDiagnostics());
      break;
    case SQL_DESC_LITERAL_PREFIX:
      GetAttributeSQLWCHAR(record.literal_prefix, length_in_bytes, value, buffer_length,
                           output_length, GetDiagnostics());
      break;
    case SQL_DESC_LITERAL_SUFFIX:
      GetAttributeSQLWCHAR(record.literal_suffix, length_in_bytes, value, buffer_length,
                           output_length, GetDiagnostics());
      break;
    case SQL_DESC_LOCAL_TYPE_NAME:
      GetAttributeSQLWCHAR(record.local_type_name, length_in_bytes, value, buffer_length,
                           output_length, GetDiagnostics());
      break;
    case SQL_DESC_NAME:
      GetAttributeSQLWCHAR(record.name, length_in_bytes, value, buffer_length,
                           output_length, GetDiagnostics());
      break;
    case SQL_DESC_SCHEMA_NAME:
      GetAttributeSQLWCHAR(record.schema_name, length_in_bytes, value, buffer_length,
                           output_length, GetDiagnostics());
      break;
    case SQL_DESC_TABLE_NAME:
      GetAttributeSQLWCHAR(record.table_name, length_in_bytes, value, buffer_length,
                           output_length, GetDiagnostics());
      break;
    case SQL_DESC_TYPE_NAME:
      GetAttributeSQLWCHAR(record.type_name, length_in_bytes, value, buffer_length,
                           output_length, GetDiagnostics());
      break;

    case SQL_DESC_DATA_PTR:
      GetAttribute(record.data_ptr, value, buffer_length, output_length);
      break;
    case SQL_DESC_INDICATOR_PTR:
    case SQL_DESC_OCTET_LENGTH_PTR:
      GetAttribute(record.indicator_ptr, value, buffer_length, output_length);
      break;
    case SQL_COLUMN_LENGTH:  // ODBC 2.0
    case SQL_DESC_LENGTH:
      GetAttribute(record.length, value, buffer_length, output_length);
      break;
    case SQL_DESC_OCTET_LENGTH:
      GetAttribute(record.octet_length, value, buffer_length, output_length);
      break;

    case SQL_DESC_AUTO_UNIQUE_VALUE:
      GetAttribute(record.auto_unique_value, value, buffer_length, output_length);
      break;
    case SQL_DESC_CASE_SENSITIVE:
      GetAttribute(record.case_sensitive, value, buffer_length, output_length);
      break;
    case SQL_DESC_DATETIME_INTERVAL_PRECISION:
      GetAttribute(record.datetime_interval_precision, value, buffer_length,
                   output_length);
      break;
    case SQL_DESC_NUM_PREC_RADIX:
      GetAttribute(record.num_prec_radix, value, buffer_length, output_length);
      break;

    case SQL_DESC_CONCISE_TYPE:
      GetAttribute(record.concise_type, value, buffer_length, output_length);
      break;
    case SQL_DESC_DATETIME_INTERVAL_CODE:
      GetAttribute(record.datetime_interval_code, value, buffer_length, output_length);
      break;
    case SQL_DESC_DISPLAY_SIZE:
      GetAttribute(record.display_size, value, buffer_length, output_length);
      break;
    case SQL_DESC_FIXED_PREC_SCALE:
      GetAttribute(record.fixed_prec_scale, value, buffer_length, output_length);
      break;
    case SQL_DESC_NULLABLE:
      GetAttribute(record.nullable, value, buffer_length, output_length);
      break;
    case SQL_DESC_PARAMETER_TYPE:
      GetAttribute(record.param_type, value, buffer_length, output_length);
      break;
    case SQL_COLUMN_PRECISION:  // ODBC 2.0
    case SQL_DESC_PRECISION:
      GetAttribute(record.precision, value, buffer_length, output_length);
      break;
    case SQL_DESC_ROWVER:
      GetAttribute(record.row_ver, value, buffer_length, output_length);
      break;
    case SQL_COLUMN_SCALE:  // ODBC 2.0
    case SQL_DESC_SCALE:
      GetAttribute(record.scale, value, buffer_length, output_length);
      break;
    case SQL_DESC_SEARCHABLE:
      GetAttribute(record.searchable, value, buffer_length, output_length);
      break;
    case SQL_DESC_TYPE:
      GetAttribute(record.type, value, buffer_length, output_length);
      break;
    case SQL_DESC_UNNAMED:
      GetAttribute(record.unnamed, value, buffer_length, output_length);
      break;
    case SQL_DESC_UNSIGNED:
      GetAttribute(record.is_unsigned, value, buffer_length, output_length);
      break;
    case SQL_DESC_UPDATABLE:
      GetAttribute(record.updatable, value, buffer_length, output_length);
      break;
    default:
      throw DriverException("Invalid descriptor field", "HY091");
  }
}

SQLSMALLINT ODBCDescriptor::GetAllocType() const {
  return owning_connection_ != nullptr ? SQL_DESC_ALLOC_USER : SQL_DESC_ALLOC_AUTO;
}

bool ODBCDescriptor::IsAppDescriptor() const { return is_app_descriptor_; }

void ODBCDescriptor::RegisterToStatement(ODBCStatement* statement, bool is_apd) {
  if (is_apd) {
    registered_on_statements_as_apd_.push_back(statement);
  } else {
    registered_on_statements_as_ard_.push_back(statement);
  }
}

void ODBCDescriptor::DetachFromStatement(ODBCStatement* statement, bool is_apd) {
  auto& vector_to_update =
      is_apd ? registered_on_statements_as_apd_ : registered_on_statements_as_ard_;
  auto it = std::find(vector_to_update.begin(), vector_to_update.end(), statement);
  if (it != vector_to_update.end()) {
    vector_to_update.erase(it);
  }
}

void ODBCDescriptor::ReleaseDescriptor() {
  for (ODBCStatement* stmt : registered_on_statements_as_apd_) {
    stmt->RevertAppDescriptor(true);
  }

  for (ODBCStatement* stmt : registered_on_statements_as_ard_) {
    stmt->RevertAppDescriptor(false);
  }

  if (owning_connection_) {
    owning_connection_->DropDescriptor(this);
  }
}

void ODBCDescriptor::PopulateFromResultSetMetadata(ResultSetMetadata* rsmd) {
  records_.assign(rsmd->GetColumnCount(), DescriptorRecord());
  highest_one_based_bound_record_ = records_.size() + 1;

  for (size_t i = 0; i < records_.size(); ++i) {
    size_t one_based_index = i + 1;
    int16_t concise_type = rsmd->GetConciseType(one_based_index);

    records_[i].base_column_name = rsmd->GetBaseColumnName(one_based_index);
    records_[i].base_table_name = rsmd->GetBaseTableName(one_based_index);
    records_[i].catalog_name = rsmd->GetCatalogName(one_based_index);
    records_[i].label = rsmd->GetColumnLabel(one_based_index);
    records_[i].literal_prefix = rsmd->GetLiteralPrefix(one_based_index);
    records_[i].literal_suffix = rsmd->GetLiteralSuffix(one_based_index);
    records_[i].local_type_name = rsmd->GetLocalTypeName(one_based_index);
    records_[i].name = rsmd->GetName(one_based_index);
    records_[i].schema_name = rsmd->GetSchemaName(one_based_index);
    records_[i].table_name = rsmd->GetTableName(one_based_index);
    records_[i].type_name = rsmd->GetTypeName(one_based_index, concise_type);
    records_[i].concise_type = GetSqlTypeForODBCVersion(concise_type, is_2x_connection_);
    records_[i].data_ptr = nullptr;
    records_[i].indicator_ptr = nullptr;
    records_[i].display_size = rsmd->GetColumnDisplaySize(one_based_index);
    records_[i].octet_length = rsmd->GetOctetLength(one_based_index);
    records_[i].length = rsmd->GetLength(one_based_index);
    records_[i].auto_unique_value =
        rsmd->IsAutoUnique(one_based_index) ? SQL_TRUE : SQL_FALSE;
    records_[i].case_sensitive =
        rsmd->IsCaseSensitive(one_based_index) ? SQL_TRUE : SQL_FALSE;
    records_[i].datetime_interval_precision;  // TODO - update when rsmd adds this
    SQLINTEGER num_prec_radix = rsmd->GetNumPrecRadix(one_based_index);
    records_[i].num_prec_radix = num_prec_radix > 0 ? num_prec_radix : 0;
    records_[i].datetime_interval_code;  // TODO
    records_[i].fixed_prec_scale =
        rsmd->IsFixedPrecScale(one_based_index) ? SQL_TRUE : SQL_FALSE;
    records_[i].nullable = rsmd->IsNullable(one_based_index);
    records_[i].param_type = SQL_PARAM_INPUT;
    records_[i].precision = rsmd->GetPrecision(one_based_index);
    records_[i].row_ver = SQL_FALSE;
    records_[i].scale = rsmd->GetScale(one_based_index);
    records_[i].searchable = rsmd->IsSearchable(one_based_index);
    records_[i].type = rsmd->GetDataType(one_based_index);
    records_[i].unnamed = records_[i].name.empty() ? SQL_TRUE : SQL_FALSE;
    records_[i].is_unsigned = rsmd->IsUnsigned(one_based_index) ? SQL_TRUE : SQL_FALSE;
    records_[i].updatable = rsmd->GetUpdatable(one_based_index);
  }
}

const std::vector<DescriptorRecord>& ODBCDescriptor::GetRecords() const {
  return records_;
}

std::vector<DescriptorRecord>& ODBCDescriptor::GetRecords() { return records_; }

void ODBCDescriptor::BindCol(SQLSMALLINT record_number, SQLSMALLINT c_type,
                             SQLPOINTER data_ptr, SQLLEN buffer_length,
                             SQLLEN* indicator_ptr) {
  assert(is_app_descriptor_);
  assert(is_writable_);

  // The set of records auto-expands to the supplied record number.
  if (records_.size() < record_number) {
    records_.resize(record_number);
  }

  SQLSMALLINT zero_based_record_index = record_number - 1;
  DescriptorRecord& record = records_[zero_based_record_index];

  record.type = c_type;
  record.indicator_ptr = indicator_ptr;
  record.length = buffer_length;

  // Initialize default precision and scale for SQL_C_NUMERIC.
  if (record.type == SQL_C_NUMERIC) {
    record.precision = 38;
    record.scale = 0;
  }
  SetDataPtrOnRecord(data_ptr, record_number);
}

void ODBCDescriptor::SetDataPtrOnRecord(SQLPOINTER data_ptr, SQLSMALLINT record_number) {
  assert(record_number <= records_.size());
  DescriptorRecord& record = records_[record_number - 1];
  if (data_ptr) {
    record.CheckConsistency();
    record.is_bound = true;
  } else {
    record.is_bound = false;
  }
  record.data_ptr = data_ptr;

  // Bookkeeping on the highest bound record (used for returning SQL_DESC_COUNT)
  if (highest_one_based_bound_record_ < record_number && data_ptr) {
    highest_one_based_bound_record_ = record_number;
  } else if (highest_one_based_bound_record_ == record_number && !data_ptr) {
    highest_one_based_bound_record_ = CalculateHighestBoundRecord(records_);
  }
  has_bindings_changed_ = true;
}

void DescriptorRecord::CheckConsistency() {
  // TODO
}
