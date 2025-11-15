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

#include "arrow/flight/sql/odbc/odbc_impl/odbc_statement.h"

#include "arrow/flight/sql/odbc/odbc_impl/attribute_utils.h"
#include "arrow/flight/sql/odbc/odbc_impl/exceptions.h"
#include "arrow/flight/sql/odbc/odbc_impl/odbc_connection.h"
#include "arrow/flight/sql/odbc/odbc_impl/odbc_descriptor.h"
#include "arrow/flight/sql/odbc/odbc_impl/spi/result_set.h"
#include "arrow/flight/sql/odbc/odbc_impl/spi/result_set_metadata.h"
#include "arrow/flight/sql/odbc/odbc_impl/spi/statement.h"
#include "arrow/flight/sql/odbc/odbc_impl/types.h"

#include <sql.h>
#include <sqlext.h>
#include <sqltypes.h>
#include <boost/optional.hpp>
#include <boost/variant.hpp>
#include <utility>

using ODBC::DescriptorRecord;
using ODBC::ODBCConnection;
using ODBC::ODBCDescriptor;
using ODBC::ODBCStatement;

using arrow::flight::sql::odbc::DriverException;
using arrow::flight::sql::odbc::ResultSetMetadata;
using arrow::flight::sql::odbc::Statement;

namespace {
void DescriptorToHandle(SQLPOINTER output, ODBCDescriptor* descriptor,
                        SQLINTEGER* len_ptr) {
  if (output) {
    SQLHANDLE* output_handle = static_cast<SQLHANDLE*>(output);
    *output_handle = reinterpret_cast<SQLHANDLE>(descriptor);
  }
  if (len_ptr) {
    *len_ptr = sizeof(SQLHANDLE);
  }
}

size_t GetLength(const DescriptorRecord& record) {
  switch (record.type) {
    case SQL_C_CHAR:
    case SQL_C_WCHAR:
    case SQL_C_BINARY:
      return record.length;

    case SQL_C_BIT:
    case SQL_C_TINYINT:
    case SQL_C_STINYINT:
    case SQL_C_UTINYINT:
      return sizeof(SQLSCHAR);

    case SQL_C_SHORT:
    case SQL_C_SSHORT:
    case SQL_C_USHORT:
      return sizeof(SQLSMALLINT);

    case SQL_C_LONG:
    case SQL_C_SLONG:
    case SQL_C_ULONG:
    case SQL_C_FLOAT:
      return sizeof(SQLINTEGER);

    case SQL_C_SBIGINT:
    case SQL_C_UBIGINT:
    case SQL_C_DOUBLE:
      return sizeof(SQLBIGINT);

    case SQL_C_NUMERIC:
      return sizeof(SQL_NUMERIC_STRUCT);

    case SQL_C_DATE:
    case SQL_C_TYPE_DATE:
      return sizeof(SQL_DATE_STRUCT);

    case SQL_C_TIME:
    case SQL_C_TYPE_TIME:
      return sizeof(SQL_TIME_STRUCT);

    case SQL_C_TIMESTAMP:
    case SQL_C_TYPE_TIMESTAMP:
      return sizeof(SQL_TIMESTAMP_STRUCT);

    case SQL_C_INTERVAL_DAY:
    case SQL_C_INTERVAL_DAY_TO_HOUR:
    case SQL_C_INTERVAL_DAY_TO_MINUTE:
    case SQL_C_INTERVAL_DAY_TO_SECOND:
    case SQL_C_INTERVAL_HOUR:
    case SQL_C_INTERVAL_HOUR_TO_MINUTE:
    case SQL_C_INTERVAL_HOUR_TO_SECOND:
    case SQL_C_INTERVAL_MINUTE:
    case SQL_C_INTERVAL_MINUTE_TO_SECOND:
    case SQL_C_INTERVAL_SECOND:
    case SQL_C_INTERVAL_YEAR:
    case SQL_C_INTERVAL_YEAR_TO_MONTH:
    case SQL_C_INTERVAL_MONTH:
      return sizeof(SQL_INTERVAL_STRUCT);
    default:
      return record.length;
  }
}

SQLSMALLINT getc_typeForSQLType(const DescriptorRecord& record) {
  switch (record.concise_type) {
    case SQL_CHAR:
    case SQL_VARCHAR:
    case SQL_LONGVARCHAR:
      return SQL_C_CHAR;

    case SQL_WCHAR:
    case SQL_WVARCHAR:
    case SQL_WLONGVARCHAR:
      return SQL_C_WCHAR;

    case SQL_BINARY:
    case SQL_VARBINARY:
    case SQL_LONGVARBINARY:
      return SQL_C_BINARY;

    case SQL_TINYINT:
      return record.is_unsigned ? SQL_C_UTINYINT : SQL_C_STINYINT;

    case SQL_SMALLINT:
      return record.is_unsigned ? SQL_C_USHORT : SQL_C_SSHORT;

    case SQL_INTEGER:
      return record.is_unsigned ? SQL_C_ULONG : SQL_C_SLONG;

    case SQL_BIGINT:
      return record.is_unsigned ? SQL_C_UBIGINT : SQL_C_SBIGINT;

    case SQL_REAL:
      return SQL_C_FLOAT;

    case SQL_FLOAT:
    case SQL_DOUBLE:
      return SQL_C_DOUBLE;

    case SQL_DATE:
    case SQL_TYPE_DATE:
      return SQL_C_TYPE_DATE;

    case SQL_TIME:
    case SQL_TYPE_TIME:
      return SQL_C_TYPE_TIME;

    case SQL_TIMESTAMP:
    case SQL_TYPE_TIMESTAMP:
      return SQL_C_TYPE_TIMESTAMP;

    case SQL_C_INTERVAL_DAY:
      return SQL_INTERVAL_DAY;
    case SQL_C_INTERVAL_DAY_TO_HOUR:
      return SQL_INTERVAL_DAY_TO_HOUR;
    case SQL_C_INTERVAL_DAY_TO_MINUTE:
      return SQL_INTERVAL_DAY_TO_MINUTE;
    case SQL_C_INTERVAL_DAY_TO_SECOND:
      return SQL_INTERVAL_DAY_TO_SECOND;
    case SQL_C_INTERVAL_HOUR:
      return SQL_INTERVAL_HOUR;
    case SQL_C_INTERVAL_HOUR_TO_MINUTE:
      return SQL_INTERVAL_HOUR_TO_MINUTE;
    case SQL_C_INTERVAL_HOUR_TO_SECOND:
      return SQL_INTERVAL_HOUR_TO_SECOND;
    case SQL_C_INTERVAL_MINUTE:
      return SQL_INTERVAL_MINUTE;
    case SQL_C_INTERVAL_MINUTE_TO_SECOND:
      return SQL_INTERVAL_MINUTE_TO_SECOND;
    case SQL_C_INTERVAL_SECOND:
      return SQL_INTERVAL_SECOND;
    case SQL_C_INTERVAL_YEAR:
      return SQL_INTERVAL_YEAR;
    case SQL_C_INTERVAL_YEAR_TO_MONTH:
      return SQL_INTERVAL_YEAR_TO_MONTH;
    case SQL_C_INTERVAL_MONTH:
      return SQL_INTERVAL_MONTH;

    default:
      throw DriverException("Unknown SQL type: " + std::to_string(record.concise_type),
                            "HY003");
  }
}

void CopyAttribute(Statement& source, Statement& target,
                   Statement::StatementAttributeId attribute_id) {
  auto optional_value = source.GetAttribute(attribute_id);
  if (optional_value) {
    target.SetAttribute(attribute_id, *optional_value);
  }
}
}  // namespace

// Public
// =========================================================================================
ODBCStatement::ODBCStatement(ODBCConnection& connection,
                             std::shared_ptr<Statement> spi_statement)
    : connection_(connection),
      spi_statement_(std::move(spi_statement)),
      diagnostics_(&spi_statement_->GetDiagnostics()),
      built_in_ard_(std::make_shared<ODBCDescriptor>(spi_statement_->GetDiagnostics(),
                                                     nullptr, this, true, true,
                                                     connection.IsOdbc2Connection())),
      built_in_apd_(std::make_shared<ODBCDescriptor>(spi_statement_->GetDiagnostics(),
                                                     nullptr, this, true, true,
                                                     connection.IsOdbc2Connection())),
      ipd_(std::make_shared<ODBCDescriptor>(spi_statement_->GetDiagnostics(), nullptr,
                                            this, false, true,
                                            connection.IsOdbc2Connection())),
      ird_(std::make_shared<ODBCDescriptor>(spi_statement_->GetDiagnostics(), nullptr,
                                            this, false, false,
                                            connection.IsOdbc2Connection())),
      current_ard_(built_in_apd_.get()),
      current_apd_(built_in_apd_.get()),
      row_number_(0),
      max_rows_(0),
      rowset_size_(1),
      is_prepared_(false),
      has_reached_end_of_result_(false) {}

ODBCConnection& ODBCStatement::GetConnection() { return connection_; }

void ODBCStatement::CopyAttributesFromConnection(ODBCConnection& connection) {
  ODBCStatement& tracking_statement = connection.GetTrackingStatement();

  // Get abstraction attributes and copy to this spi_statement_.
  // Possible ODBC attributes are below, but many of these are not supported by warpdrive
  // or ODBCAbstaction:
  // SQL_ATTR_ASYNC_ENABLE:
  // SQL_ATTR_METADATA_ID:
  // SQL_ATTR_CONCURRENCY:
  // SQL_ATTR_CURSOR_TYPE:
  // SQL_ATTR_KEYSET_SIZE:
  // SQL_ATTR_MAX_LENGTH:
  // SQL_ATTR_MAX_ROWS:
  // SQL_ATTR_NOSCAN:
  // SQL_ATTR_QUERY_TIMEOUT:
  // SQL_ATTR_RETRIEVE_DATA:
  // SQL_ATTR_SIMULATE_CURSOR:
  // SQL_ATTR_USE_BOOKMARKS:
  CopyAttribute(*tracking_statement.spi_statement_, *spi_statement_,
                Statement::METADATA_ID);
  CopyAttribute(*tracking_statement.spi_statement_, *spi_statement_,
                Statement::MAX_LENGTH);
  CopyAttribute(*tracking_statement.spi_statement_, *spi_statement_, Statement::NOSCAN);
  CopyAttribute(*tracking_statement.spi_statement_, *spi_statement_,
                Statement::QUERY_TIMEOUT);

  // SQL_ATTR_ROW_BIND_TYPE:
  current_ard_->SetHeaderField(
      SQL_DESC_BIND_TYPE,
      reinterpret_cast<SQLPOINTER>(
          static_cast<SQLLEN>(tracking_statement.current_ard_->GetBoundStructOffset())),
      0);
}

bool ODBCStatement::IsPrepared() const { return is_prepared_; }

void ODBCStatement::Prepare(const std::string& query) {
  boost::optional<std::shared_ptr<ResultSetMetadata> > metadata =
      spi_statement_->Prepare(query);

  if (metadata) {
    ird_->PopulateFromResultSetMetadata(metadata->get());
  }
  is_prepared_ = true;
}

void ODBCStatement::ExecutePrepared() {
  if (!is_prepared_) {
    throw DriverException("Function sequence error", "HY010");
  }

  if (spi_statement_->ExecutePrepared()) {
    current_result_ = spi_statement_->GetResultSet();
    ird_->PopulateFromResultSetMetadata(
        spi_statement_->GetResultSet()->GetMetadata().get());
    has_reached_end_of_result_ = false;
  }
}

void ODBCStatement::ExecuteDirect(const std::string& query) {
  if (spi_statement_->Execute(query)) {
    current_result_ = spi_statement_->GetResultSet();
    ird_->PopulateFromResultSetMetadata(current_result_->GetMetadata().get());
    has_reached_end_of_result_ = false;
  }

  // Direct execution wipes out the prepared state.
  is_prepared_ = false;
}

bool ODBCStatement::Fetch(size_t rows) {
  if (has_reached_end_of_result_) {
    ird_->SetRowsProcessed(0);
    return false;
  }

  if (max_rows_) {
    rows = std::min(rows, max_rows_ - row_number_);
  }

  if (current_ard_->HaveBindingsChanged()) {
    // TODO: Deal handle when offset != buffer_length.

    // Wipe out all bindings in the ResultSet.
    // Note that the number of ARD records can both be more or less
    // than the number of columns.
    for (size_t i = 0; i < ird_->GetRecords().size(); i++) {
      if (i < current_ard_->GetRecords().size() &&
          current_ard_->GetRecords()[i].is_bound) {
        const DescriptorRecord& ard_record = current_ard_->GetRecords()[i];
        current_result_->BindColumn(i + 1, ard_record.type, ard_record.precision,
                                    ard_record.scale, ard_record.data_ptr,
                                    GetLength(ard_record), ard_record.indicator_ptr);
      } else {
        current_result_->BindColumn(i + 1,
                                    arrow::flight::sql::odbc::CDataType_CHAR
                                    /* arbitrary type, not used */,
                                    0, 0, nullptr, 0, nullptr);
      }
    }
    current_ard_->NotifyBindingsHavePropagated();
  }

  size_t rows_fetched = current_result_->Move(rows, current_ard_->GetBindOffset(),
                                              current_ard_->GetBoundStructOffset(),
                                              ird_->GetArrayStatusPtr());
  ird_->SetRowsProcessed(static_cast<SQLULEN>(rows_fetched));

  row_number_ += rows_fetched;
  has_reached_end_of_result_ = rows_fetched != rows;
  return rows_fetched != 0;
}

void ODBCStatement::GetStmtAttr(SQLINTEGER statement_attribute, SQLPOINTER output,
                                SQLINTEGER buffer_size, SQLINTEGER* str_len_ptr,
                                bool is_unicode) {
  boost::optional<Statement::Attribute> spi_attribute;
  switch (statement_attribute) {
    // Descriptor accessor attributes
    case SQL_ATTR_APP_PARAM_DESC:
      DescriptorToHandle(output, current_apd_, str_len_ptr);
      return;
    case SQL_ATTR_APP_ROW_DESC:
      DescriptorToHandle(output, current_ard_, str_len_ptr);
      return;
    case SQL_ATTR_IMP_PARAM_DESC:
      DescriptorToHandle(output, ipd_.get(), str_len_ptr);
      return;
    case SQL_ATTR_IMP_ROW_DESC:
      DescriptorToHandle(output, ird_.get(), str_len_ptr);
      return;

    // Attributes that are descriptor fields
    case SQL_ATTR_PARAM_BIND_OFFSET_PTR:
      current_apd_->GetHeaderField(SQL_DESC_BIND_OFFSET_PTR, output, buffer_size,
                                   str_len_ptr);
      return;
    case SQL_ATTR_PARAM_BIND_TYPE:
      current_apd_->GetHeaderField(SQL_DESC_BIND_TYPE, output, buffer_size, str_len_ptr);
      return;
    case SQL_ATTR_PARAM_OPERATION_PTR:
      current_apd_->GetHeaderField(SQL_DESC_ARRAY_STATUS_PTR, output, buffer_size,
                                   str_len_ptr);
      return;
    case SQL_ATTR_PARAM_STATUS_PTR:
      ipd_->GetHeaderField(SQL_DESC_ARRAY_STATUS_PTR, output, buffer_size, str_len_ptr);
      return;
    case SQL_ATTR_PARAMS_PROCESSED_PTR:
      ipd_->GetHeaderField(SQL_DESC_ROWS_PROCESSED_PTR, output, buffer_size, str_len_ptr);
      return;
    case SQL_ATTR_PARAMSET_SIZE:
      current_apd_->GetHeaderField(SQL_DESC_ARRAY_SIZE, output, buffer_size, str_len_ptr);
      return;
    case SQL_ATTR_ROW_ARRAY_SIZE:
      current_ard_->GetHeaderField(SQL_DESC_ARRAY_SIZE, output, buffer_size, str_len_ptr);
      return;
    case SQL_ATTR_ROW_BIND_OFFSET_PTR:
      current_ard_->GetHeaderField(SQL_DESC_BIND_OFFSET_PTR, output, buffer_size,
                                   str_len_ptr);
      return;
    case SQL_ATTR_ROW_BIND_TYPE:
      current_ard_->GetHeaderField(SQL_DESC_BIND_TYPE, output, buffer_size, str_len_ptr);
      return;
    case SQL_ATTR_ROW_OPERATION_PTR:
      current_ard_->GetHeaderField(SQL_DESC_ARRAY_STATUS_PTR, output, buffer_size,
                                   str_len_ptr);
      return;
    case SQL_ATTR_ROW_STATUS_PTR:
      ird_->GetHeaderField(SQL_DESC_ARRAY_STATUS_PTR, output, buffer_size, str_len_ptr);
      return;
    case SQL_ATTR_ROWS_FETCHED_PTR:
      ird_->GetHeaderField(SQL_DESC_ROWS_PROCESSED_PTR, output, buffer_size, str_len_ptr);
      return;

    case SQL_ATTR_ASYNC_ENABLE:
      GetAttribute(static_cast<SQLULEN>(SQL_ASYNC_ENABLE_OFF), output, buffer_size,
                   str_len_ptr);
      return;

#ifdef SQL_ATTR_ASYNC_STMT_EVENT
    case SQL_ATTR_ASYNC_STMT_EVENT:
      throw DriverException("Unsupported attribute", "HYC00");
#endif
#ifdef SQL_ATTR_ASYNC_STMT_PCALLBACK
    case SQL_ATTR_ASYNC_STMT_PCALLBACK:
      throw DriverException("Unsupported attribute", "HYC00");
#endif
#ifdef SQL_ATTR_ASYNC_STMT_PCONTEXT
    case SQL_ATTR_ASYNC_STMT_PCONTEXT:
      throw DriverException("Unsupported attribute", "HYC00");
#endif
    case SQL_ATTR_CURSOR_SCROLLABLE:
      GetAttribute(static_cast<SQLULEN>(SQL_NONSCROLLABLE), output, buffer_size,
                   str_len_ptr);
      return;

    case SQL_ATTR_CURSOR_SENSITIVITY:
      GetAttribute(static_cast<SQLULEN>(SQL_UNSPECIFIED), output, buffer_size,
                   str_len_ptr);
      return;

    case SQL_ATTR_CURSOR_TYPE:
      GetAttribute(static_cast<SQLULEN>(SQL_CURSOR_FORWARD_ONLY), output, buffer_size,
                   str_len_ptr);
      return;

    case SQL_ATTR_ENABLE_AUTO_IPD:
      GetAttribute(static_cast<SQLULEN>(SQL_FALSE), output, buffer_size, str_len_ptr);
      return;

    case SQL_ATTR_FETCH_BOOKMARK_PTR:
      GetAttribute(static_cast<SQLPOINTER>(NULL), output, buffer_size, str_len_ptr);
      return;

    case SQL_ATTR_KEYSET_SIZE:
      GetAttribute(static_cast<SQLULEN>(0), output, buffer_size, str_len_ptr);
      return;

    case SQL_ATTR_ROW_NUMBER:
      GetAttribute(static_cast<SQLULEN>(row_number_), output, buffer_size, str_len_ptr);
      return;
    case SQL_ATTR_SIMULATE_CURSOR:
      GetAttribute(static_cast<SQLULEN>(SQL_SC_UNIQUE), output, buffer_size, str_len_ptr);
      return;
    case SQL_ATTR_USE_BOOKMARKS:
      GetAttribute(static_cast<SQLULEN>(SQL_UB_OFF), output, buffer_size, str_len_ptr);
      return;
    case SQL_ATTR_CONCURRENCY:
      GetAttribute(static_cast<SQLULEN>(SQL_CONCUR_READ_ONLY), output, buffer_size,
                   str_len_ptr);
      return;
    case SQL_ATTR_MAX_ROWS:
      GetAttribute(static_cast<SQLULEN>(max_rows_), output, buffer_size, str_len_ptr);
      return;
    case SQL_ATTR_RETRIEVE_DATA:
      GetAttribute(static_cast<SQLULEN>(SQL_RD_ON), output, buffer_size, str_len_ptr);
      return;
    case SQL_ROWSET_SIZE:
      GetAttribute(static_cast<SQLULEN>(rowset_size_), output, buffer_size, str_len_ptr);
      return;

    // Driver-level statement attributes. These are all SQLULEN attributes.
    case SQL_ATTR_MAX_LENGTH:
      spi_attribute = spi_statement_->GetAttribute(Statement::MAX_LENGTH);
      break;
    case SQL_ATTR_METADATA_ID:
      spi_attribute = spi_statement_->GetAttribute(Statement::METADATA_ID);
      break;
    case SQL_ATTR_NOSCAN:
      spi_attribute = spi_statement_->GetAttribute(Statement::NOSCAN);
      break;
    case SQL_ATTR_QUERY_TIMEOUT:
      spi_attribute = spi_statement_->GetAttribute(Statement::QUERY_TIMEOUT);
      break;
    default:
      throw DriverException(
          "Invalid statement attribute: " + std::to_string(statement_attribute), "HY092");
  }

  if (spi_attribute) {
    GetAttribute(static_cast<SQLULEN>(boost::get<size_t>(*spi_attribute)), output,
                 buffer_size, str_len_ptr);
    return;
  }

  throw DriverException(
      "Invalid statement attribute: " + std::to_string(statement_attribute), "HY092");
}

void ODBCStatement::SetStmtAttr(SQLINTEGER statement_attribute, SQLPOINTER value,
                                SQLINTEGER buffer_size, bool is_unicode) {
  size_t attribute_to_write = 0;
  bool successfully_written = false;

  switch (statement_attribute) {
    case SQL_ATTR_APP_PARAM_DESC: {
      ODBCDescriptor* desc = static_cast<ODBCDescriptor*>(value);
      if (current_apd_ != desc) {
        if (current_apd_ != built_in_apd_.get()) {
          current_apd_->DetachFromStatement(this, true);
        }
        current_apd_ = desc;
        if (current_apd_ != built_in_apd_.get()) {
          desc->RegisterToStatement(this, true);
        }
      }
      return;
    }
    case SQL_ATTR_APP_ROW_DESC: {
      ODBCDescriptor* desc = static_cast<ODBCDescriptor*>(value);
      if (current_ard_ != desc) {
        if (current_ard_ != built_in_ard_.get()) {
          current_ard_->DetachFromStatement(this, false);
        }
        current_ard_ = desc;
        if (current_ard_ != built_in_ard_.get()) {
          desc->RegisterToStatement(this, false);
        }
      }
      return;
    }
    case SQL_ATTR_IMP_PARAM_DESC:
      throw DriverException("Cannot assign implementation descriptor.", "HY017");
    case SQL_ATTR_IMP_ROW_DESC:
      throw DriverException("Cannot assign implementation descriptor.", "HY017");
      // Attributes that are descriptor fields
    case SQL_ATTR_PARAM_BIND_OFFSET_PTR:
      current_apd_->SetHeaderField(SQL_DESC_BIND_OFFSET_PTR, value, buffer_size);
      return;
    case SQL_ATTR_PARAM_BIND_TYPE:
      current_apd_->SetHeaderField(SQL_DESC_BIND_TYPE, value, buffer_size);
      return;
    case SQL_ATTR_PARAM_OPERATION_PTR:
      current_apd_->SetHeaderField(SQL_DESC_ARRAY_STATUS_PTR, value, buffer_size);
      return;
    case SQL_ATTR_PARAM_STATUS_PTR:
      ipd_->SetHeaderField(SQL_DESC_ARRAY_STATUS_PTR, value, buffer_size);
      return;
    case SQL_ATTR_PARAMS_PROCESSED_PTR:
      ipd_->SetHeaderField(SQL_DESC_ROWS_PROCESSED_PTR, value, buffer_size);
      return;
    case SQL_ATTR_PARAMSET_SIZE:
      current_apd_->SetHeaderField(SQL_DESC_ARRAY_SIZE, value, buffer_size);
      return;
    case SQL_ATTR_ROW_ARRAY_SIZE:
      current_ard_->SetHeaderField(SQL_DESC_ARRAY_SIZE, value, buffer_size);
      return;
    case SQL_ATTR_ROW_BIND_OFFSET_PTR:
      current_ard_->SetHeaderField(SQL_DESC_BIND_OFFSET_PTR, value, buffer_size);
      return;
    case SQL_ATTR_ROW_BIND_TYPE:
      current_ard_->SetHeaderField(SQL_DESC_BIND_TYPE, value, buffer_size);
      return;
    case SQL_ATTR_ROW_OPERATION_PTR:
      current_ard_->SetHeaderField(SQL_DESC_ARRAY_STATUS_PTR, value, buffer_size);
      return;
    case SQL_ATTR_ROW_STATUS_PTR:
      ird_->SetHeaderField(SQL_DESC_ARRAY_STATUS_PTR, value, buffer_size);
      return;
    case SQL_ATTR_ROWS_FETCHED_PTR:
      ird_->SetHeaderField(SQL_DESC_ROWS_PROCESSED_PTR, value, buffer_size);
      return;

    case SQL_ATTR_ASYNC_ENABLE:
#ifdef SQL_ATTR_ASYNC_STMT_EVENT
    case SQL_ATTR_ASYNC_STMT_EVENT:
      throw DriverException("Unsupported attribute", "HYC00");
#endif
#ifdef SQL_ATTR_ASYNC_STMT_PCALLBACK
    case SQL_ATTR_ASYNC_STMT_PCALLBACK:
      throw DriverException("Unsupported attribute", "HYC00");
#endif
#ifdef SQL_ATTR_ASYNC_STMT_PCONTEXT
    case SQL_ATTR_ASYNC_STMT_PCONTEXT:
      throw DriverException("Unsupported attribute", "HYC00");
#endif
    case SQL_ATTR_CONCURRENCY:
      CheckIfAttributeIsSetToOnlyValidValue(value,
                                            static_cast<SQLULEN>(SQL_CONCUR_READ_ONLY));
      return;
    case SQL_ATTR_CURSOR_SCROLLABLE:
      CheckIfAttributeIsSetToOnlyValidValue(value,
                                            static_cast<SQLULEN>(SQL_NONSCROLLABLE));
      return;
    case SQL_ATTR_CURSOR_SENSITIVITY:
      CheckIfAttributeIsSetToOnlyValidValue(value, static_cast<SQLULEN>(SQL_UNSPECIFIED));
      return;
    case SQL_ATTR_CURSOR_TYPE:
      CheckIfAttributeIsSetToOnlyValidValue(
          value, static_cast<SQLULEN>(SQL_CURSOR_FORWARD_ONLY));
      return;
    case SQL_ATTR_ENABLE_AUTO_IPD:
      CheckIfAttributeIsSetToOnlyValidValue(value, static_cast<SQLULEN>(SQL_FALSE));
      return;
    case SQL_ATTR_FETCH_BOOKMARK_PTR:
      if (value != NULL) {
        throw DriverException("Optional feature not implemented", "HYC00");
      }
      return;
    case SQL_ATTR_KEYSET_SIZE:
      CheckIfAttributeIsSetToOnlyValidValue(value, static_cast<SQLULEN>(0));
      return;
    case SQL_ATTR_ROW_NUMBER:
      throw DriverException("Cannot set read-only attribute", "HY092");
    case SQL_ATTR_SIMULATE_CURSOR:
      CheckIfAttributeIsSetToOnlyValidValue(value, static_cast<SQLULEN>(SQL_SC_UNIQUE));
      return;
    case SQL_ATTR_USE_BOOKMARKS:
      CheckIfAttributeIsSetToOnlyValidValue(value, static_cast<SQLULEN>(SQL_UB_OFF));
      return;
    case SQL_ATTR_RETRIEVE_DATA:
      CheckIfAttributeIsSetToOnlyValidValue(value, static_cast<SQLULEN>(SQL_TRUE));
      return;
    case SQL_ROWSET_SIZE:
      SetAttribute(value, rowset_size_);
      return;

    case SQL_ATTR_MAX_ROWS:
      throw DriverException("Cannot set read-only attribute", "HY092");

    // Driver-leve statement attributes. These are all size_t attributes
    case SQL_ATTR_MAX_LENGTH:
      SetAttribute(value, attribute_to_write);
      successfully_written =
          spi_statement_->SetAttribute(Statement::MAX_LENGTH, attribute_to_write);
      break;
    case SQL_ATTR_METADATA_ID:
      SetAttribute(value, attribute_to_write);
      successfully_written =
          spi_statement_->SetAttribute(Statement::METADATA_ID, attribute_to_write);
      break;
    case SQL_ATTR_NOSCAN:
      SetAttribute(value, attribute_to_write);
      successfully_written =
          spi_statement_->SetAttribute(Statement::NOSCAN, attribute_to_write);
      break;
    case SQL_ATTR_QUERY_TIMEOUT:
      SetAttribute(value, attribute_to_write);
      successfully_written =
          spi_statement_->SetAttribute(Statement::QUERY_TIMEOUT, attribute_to_write);
      break;
    default:
      throw DriverException("Invalid attribute: " + std::to_string(attribute_to_write),
                            "HY092");
  }
  if (!successfully_written) {
    GetDiagnostics().AddWarning("Optional value changed.", "01S02",
                                arrow::flight::sql::odbc::ODBCErrorCodes_GENERAL_WARNING);
  }
}

void ODBCStatement::RevertAppDescriptor(bool isApd) {
  if (isApd) {
    current_apd_ = built_in_apd_.get();
  } else {
    current_ard_ = built_in_ard_.get();
  }
}

void ODBCStatement::CloseCursor(bool suppress_errors) {
  if (!suppress_errors && !current_result_) {
    throw DriverException("Invalid cursor state", "24000");
  }

  if (current_result_) {
    current_result_->Close();
    current_result_ = nullptr;
  }

  // Reset the fetching state of this statement.
  current_ard_->NotifyBindingsHaveChanged();
  row_number_ = 0;
  has_reached_end_of_result_ = false;
}

bool ODBCStatement::GetData(SQLSMALLINT record_number, SQLSMALLINT c_type,
                            SQLPOINTER data_ptr, SQLLEN buffer_length,
                            SQLLEN* indicator_ptr) {
  if (record_number == 0) {
    throw DriverException("Bookmarks are not supported", "07009");
  } else if (record_number > ird_->GetRecords().size()) {
    throw DriverException("Invalid column index: " + std::to_string(record_number),
                          "07009");
  }

  SQLSMALLINT evaluated_c_type = c_type;

  // TODO: Get proper default precision and scale from abstraction.
  int precision = 38;  // arrow::Decimal128Type::kMaxPrecision;
  int scale = 0;

  if (c_type == SQL_ARD_TYPE) {
    if (record_number > current_ard_->GetRecords().size()) {
      throw DriverException("Invalid column index: " + std::to_string(record_number),
                            "07009");
    }
    const DescriptorRecord& record = current_ard_->GetRecords()[record_number - 1];
    evaluated_c_type = record.concise_type;
    precision = record.precision;
    scale = record.scale;
  }

  // Note: this is intentionally not an else if, since the type can be SQL_C_DEFAULT in
  // the ARD.
  if (evaluated_c_type == SQL_C_DEFAULT) {
    if (record_number <= current_ard_->GetRecords().size()) {
      const DescriptorRecord& ard_record = current_ard_->GetRecords()[record_number - 1];
      precision = ard_record.precision;
      scale = ard_record.scale;
    }

    const DescriptorRecord& ird_record = ird_->GetRecords()[record_number - 1];
    evaluated_c_type = getc_typeForSQLType(ird_record);
  }

  return current_result_->GetData(record_number, evaluated_c_type, precision, scale,
                                  data_ptr, buffer_length, indicator_ptr);
}

void ODBCStatement::ReleaseStatement() {
  CloseCursor(true);
  connection_.DropStatement(this);
}

void ODBCStatement::GetTables(const std::string* catalog, const std::string* schema,
                              const std::string* table, const std::string* tableType) {
  CloseCursor(true);
  if (connection_.IsOdbc2Connection()) {
    current_result_ = spi_statement_->GetTables_V2(catalog, schema, table, tableType);
  } else {
    current_result_ = spi_statement_->GetTables_V3(catalog, schema, table, tableType);
  }
  ird_->PopulateFromResultSetMetadata(current_result_->GetMetadata().get());
  has_reached_end_of_result_ = false;

  // Direct execution wipes out the prepared state.
  is_prepared_ = false;
}

void ODBCStatement::GetColumns(const std::string* catalog, const std::string* schema,
                               const std::string* table, const std::string* column) {
  CloseCursor(true);
  if (connection_.IsOdbc2Connection()) {
    current_result_ = spi_statement_->GetColumns_V2(catalog, schema, table, column);
  } else {
    current_result_ = spi_statement_->GetColumns_V3(catalog, schema, table, column);
  }
  ird_->PopulateFromResultSetMetadata(current_result_->GetMetadata().get());
  has_reached_end_of_result_ = false;

  // Direct execution wipes out the prepared state.
  is_prepared_ = false;
}

void ODBCStatement::GetTypeInfo(SQLSMALLINT data_type) {
  CloseCursor(true);
  if (connection_.IsOdbc2Connection()) {
    current_result_ = spi_statement_->GetTypeInfo_V2(data_type);
  } else {
    current_result_ = spi_statement_->GetTypeInfo_V3(data_type);
  }
  ird_->PopulateFromResultSetMetadata(current_result_->GetMetadata().get());
  has_reached_end_of_result_ = false;

  // Direct execution wipes out the prepared state.
  is_prepared_ = false;
}

void ODBCStatement::Cancel() { spi_statement_->Cancel(); }
