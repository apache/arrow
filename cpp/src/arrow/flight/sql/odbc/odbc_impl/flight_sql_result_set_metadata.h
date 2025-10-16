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

#include "arrow/flight/sql/odbc/odbc_impl/spi/result_set_metadata.h"
#include "arrow/flight/sql/odbc/odbc_impl/types.h"
#include "arrow/flight/types.h"
#include "arrow/type.h"

namespace arrow::flight::sql::odbc {
class FlightSqlResultSetMetadata : public ResultSetMetadata {
 private:
  const MetadataSettings& metadata_settings_;
  std::shared_ptr<Schema> schema_;

 public:
  FlightSqlResultSetMetadata(const std::shared_ptr<FlightInfo>& flight_info,
                             const MetadataSettings& metadata_settings);

  FlightSqlResultSetMetadata(std::shared_ptr<Schema> schema,
                             const MetadataSettings& metadata_settings);

  size_t GetColumnCount() override;

  std::string GetColumnName(int column_position) override;

  size_t GetPrecision(int column_position) override;

  size_t GetScale(int column_position) override;

  uint16_t GetDataType(int column_position) override;

  Nullability IsNullable(int column_position) override;

  std::string GetSchemaName(int column_position) override;

  std::string GetCatalogName(int column_position) override;

  std::string GetTableName(int column_position) override;

  std::string GetColumnLabel(int column_position) override;

  size_t GetColumnDisplaySize(int column_position) override;

  std::string GetBaseColumnName(int column_position) override;

  std::string GetBaseTableName(int column_position) override;

  uint16_t GetConciseType(int column_position) override;

  size_t GetLength(int column_position) override;

  std::string GetLiteralPrefix(int column_position) override;

  std::string GetLiteralSuffix(int column_position) override;

  std::string GetLocalTypeName(int column_position) override;

  std::string GetName(int column_position) override;

  size_t GetNumPrecRadix(int column_position) override;

  size_t GetOctetLength(int column_position) override;

  std::string GetTypeName(int column_position, int data_type) override;

  Updatability GetUpdatable(int column_position) override;

  bool IsAutoUnique(int column_position) override;

  bool IsCaseSensitive(int column_position) override;

  Searchability IsSearchable(int column_position) override;

  /// \brief Returns true if the column is unsigned (not numeric)
  bool IsUnsigned(int column_position) override;

  bool IsFixedPrecScale(int column_position) override;
};
}  // namespace arrow::flight::sql::odbc
