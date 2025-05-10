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

#include <memory>
#include <string>
#include <vector>

#include <arrow/flight/sql/odbc/odbcabstraction/include/odbcabstraction/exceptions.h>
#include <arrow/flight/sql/odbc/odbcabstraction/include/odbcabstraction/types.h>

namespace driver {
namespace odbcabstraction {
class Diagnostics {
 public:
  struct DiagnosticsRecord {
    std::string msg_text_;
    std::string sql_state_;
    int32_t native_error_;
  };

 private:
  std::vector<const DiagnosticsRecord*> error_records_;
  std::vector<const DiagnosticsRecord*> warning_records_;
  std::vector<std::unique_ptr<DiagnosticsRecord>> owned_records_;
  std::string vendor_;
  std::string data_source_component_;
  OdbcVersion version_;

 public:
  Diagnostics(std::string vendor, std::string data_source_component, OdbcVersion version);
  void AddError(const DriverException& exception);
  void AddWarning(std::string message, std::string sql_state, int32_t native_error);

  /// \brief Add a pre-existing truncation warning.
  inline void AddTruncationWarning() {
    static const std::unique_ptr<DiagnosticsRecord> TRUNCATION_WARNING(
        new DiagnosticsRecord{"String or binary data, right-truncated.", "01004",
                              ODBCErrorCodes_TRUNCATION_WARNING});
    warning_records_.push_back(TRUNCATION_WARNING.get());
  }

  inline void TrackRecord(const DiagnosticsRecord& record) {
    if (record.sql_state_[0] == '0' && record.sql_state_[1] == '1') {
      warning_records_.push_back(&record);
    } else {
      error_records_.push_back(&record);
    }
  }

  void SetDataSourceComponent(std::string component);
  std::string GetDataSourceComponent() const;

  std::string GetVendor() const;

  inline void Clear() {
    error_records_.clear();
    warning_records_.clear();
    owned_records_.clear();
  }

  std::string GetMessageText(uint32_t record_index) const;
  std::string GetSQLState(uint32_t record_index) const {
    return GetRecordAtIndex(record_index)->sql_state_;
  }

  int32_t GetNativeError(uint32_t record_index) const {
    return GetRecordAtIndex(record_index)->native_error_;
  }

  inline size_t GetRecordCount() const {
    return error_records_.size() + warning_records_.size();
  }

  inline bool HasRecord(uint32_t record_index) const {
    return error_records_.size() + warning_records_.size() > record_index;
  }

  inline bool HasWarning() const { return !warning_records_.empty(); }

  inline bool HasError() const { return !error_records_.empty(); }

  OdbcVersion GetOdbcVersion() const;

 private:
  inline const DiagnosticsRecord* GetRecordAtIndex(uint32_t record_index) const {
    if (record_index < error_records_.size()) {
      return error_records_[record_index];
    }
    return warning_records_[record_index - error_records_.size()];
  }
};
}  // namespace odbcabstraction
}  // namespace driver
