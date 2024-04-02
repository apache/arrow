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

#include "arrow/flight/sql/odbc/odbcabstraction/include/odbcabstraction/diagnostics.h"
#include "arrow/flight/sql/odbc/odbcabstraction/include/odbcabstraction/platform.h"
#include "arrow/flight/sql/odbc/odbcabstraction/include/odbcabstraction/types.h"

#include <utility>

namespace {
void RewriteSQLStateForODBC2(std::string& sql_state) {
  if (sql_state[0] == 'H' && sql_state[1] == 'Y') {
    sql_state[0] = 'S';
    sql_state[1] = '1';
  }
}
}  // namespace

namespace arrow::flight::sql::odbc {

Diagnostics::Diagnostics(std::string vendor, std::string data_source_component,
                         OdbcVersion version)
    : vendor_(std::move(vendor)),
      data_source_component_(std::move(data_source_component)),
      version_(version) {}

void Diagnostics::SetDataSourceComponent(std::string component) {
  data_source_component_ = std::move(component);
}

std::string Diagnostics::GetDataSourceComponent() const { return data_source_component_; }

std::string Diagnostics::GetVendor() const { return vendor_; }

void arrow::flight::sql::odbc::Diagnostics::AddError(
    const arrow::flight::sql::odbc::DriverException& exception) {
  auto record = std::unique_ptr<DiagnosticsRecord>(new DiagnosticsRecord{
      exception.GetMessageText(), exception.GetSqlState(), exception.GetNativeError()});
  if (version_ == OdbcVersion::V_2) {
    RewriteSQLStateForODBC2(record->sql_state);
  }
  TrackRecord(*record);
  owned_records_.push_back(std::move(record));
}

void arrow::flight::sql::odbc::Diagnostics::AddWarning(std::string message,
                                                       std::string sql_state,
                                                       int32_t native_error) {
  auto record = std::unique_ptr<DiagnosticsRecord>(
      new DiagnosticsRecord{std::move(message), std::move(sql_state), native_error});
  if (version_ == OdbcVersion::V_2) {
    RewriteSQLStateForODBC2(record->sql_state);
  }
  TrackRecord(*record);
  owned_records_.push_back(std::move(record));
}

std::string arrow::flight::sql::odbc::Diagnostics::GetMessageText(
    uint32_t record_index) const {
  std::string message;
  if (!vendor_.empty()) {
    message += std::string("[") + vendor_ + "]";
  }
  const DiagnosticsRecord* rec = GetRecordAtIndex(record_index);
  return message + "[" + data_source_component_ + "] (" +
         std::to_string(rec->native_error) + ") " + rec->msg_text;
}

OdbcVersion Diagnostics::GetOdbcVersion() const { return version_; }

}  // namespace arrow::flight::sql::odbc
