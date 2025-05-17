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

#include "arrow/flight/sql/client.h"
#include "arrow/flight/sql/odbc/flight_sql/flight_sql_stream_chunk_buffer.h"
#include "arrow/flight/sql/odbc/flight_sql/record_batch_transformer.h"
#include "arrow/flight/sql/odbc/flight_sql/utils.h"
#include "arrow/flight/sql/odbc/odbcabstraction/include/odbcabstraction/diagnostics.h"
#include "arrow/flight/sql/odbc/odbcabstraction/include/odbcabstraction/exceptions.h"
#include "arrow/flight/sql/odbc/odbcabstraction/include/odbcabstraction/platform.h"
#include "arrow/flight/sql/odbc/odbcabstraction/include/odbcabstraction/spi/result_set.h"
#include "arrow/flight/sql/odbc/odbcabstraction/include/odbcabstraction/types.h"
#include "arrow/flight/types.h"

namespace driver {
namespace flight_sql {

using arrow::Schema;
using arrow::flight::FlightEndpoint;
using arrow::flight::FlightInfo;
using arrow::flight::FlightStreamChunk;
using arrow::flight::FlightStreamReader;
using arrow::flight::sql::FlightSqlClient;
using odbcabstraction::CDataType;
using odbcabstraction::DriverException;
using odbcabstraction::ResultSet;
using odbcabstraction::ResultSetMetadata;

class FlightSqlResultSetColumn;

class FlightSqlResultSet : public ResultSet {
 private:
  const odbcabstraction::MetadataSettings& metadata_settings_;
  FlightStreamChunkBuffer chunk_buffer_;
  FlightStreamChunk current_chunk_;
  std::shared_ptr<Schema> schema_;
  std::shared_ptr<RecordBatchTransformer> transformer_;
  std::shared_ptr<ResultSetMetadata> metadata_;
  std::vector<FlightSqlResultSetColumn> columns_;
  std::vector<int64_t> get_data_offsets_;
  odbcabstraction::Diagnostics& diagnostics_;
  int64_t current_row_;
  int num_binding_;
  bool reset_get_data_;

 public:
  ~FlightSqlResultSet() override;

  FlightSqlResultSet(FlightSqlClient& flight_sql_client,
                     const arrow::flight::FlightCallOptions& call_options,
                     const std::shared_ptr<FlightInfo>& flight_info,
                     const std::shared_ptr<RecordBatchTransformer>& transformer,
                     odbcabstraction::Diagnostics& diagnostics,
                     const odbcabstraction::MetadataSettings& metadata_settings);

  void Close() override;

  void Cancel() override;

  bool GetData(int column_n, int16_t target_type, int precision, int scale, void* buffer,
               size_t buffer_length, ssize_t* strlen_buffer) override;

  size_t Move(size_t rows, size_t bind_offset, size_t bind_type,
              uint16_t* row_status_array) override;

  std::shared_ptr<ResultSetMetadata> GetMetadata() override;

  void BindColumn(int column_n, int16_t target_type, int precision, int scale,
                  void* buffer, size_t buffer_length, ssize_t* strlen_buffer) override;
};

}  // namespace flight_sql
}  // namespace driver
