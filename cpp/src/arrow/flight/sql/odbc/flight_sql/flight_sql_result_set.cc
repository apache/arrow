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

#include "arrow/flight/sql/odbc/flight_sql/flight_sql_result_set.h"
#include "arrow/flight/sql/odbc/odbcabstraction/include/odbcabstraction/platform.h"

#include <utility>
#include "arrow/flight/types.h"
#include "arrow/scalar.h"

#include "arrow/flight/sql/odbc/flight_sql/flight_sql_result_set_column.h"
#include "arrow/flight/sql/odbc/flight_sql/flight_sql_result_set_metadata.h"
#include "arrow/flight/sql/odbc/flight_sql/utils.h"
#include "arrow/flight/sql/odbc/odbcabstraction/include/odbcabstraction/types.h"

namespace driver {
namespace flight_sql {

using arrow::Array;
using arrow::RecordBatch;
using arrow::Scalar;
using arrow::Status;
using arrow::flight::FlightEndpoint;
using arrow::flight::FlightStreamChunk;
using arrow::flight::FlightStreamReader;
using odbcabstraction::CDataType;
using odbcabstraction::DriverException;

FlightSqlResultSet::FlightSqlResultSet(
    FlightSqlClient& flight_sql_client,
    const arrow::flight::FlightCallOptions& call_options,
    const std::shared_ptr<FlightInfo>& flight_info,
    const std::shared_ptr<RecordBatchTransformer>& transformer,
    odbcabstraction::Diagnostics& diagnostics,
    const odbcabstraction::MetadataSettings& metadata_settings)
    : metadata_settings_(metadata_settings),
      chunk_buffer_(flight_sql_client, call_options, flight_info,
                    metadata_settings_.chunk_buffer_capacity_),
      transformer_(transformer),
      metadata_(transformer
                    ? new FlightSqlResultSetMetadata(transformer->GetTransformedSchema(),
                                                     metadata_settings_)
                    : new FlightSqlResultSetMetadata(flight_info, metadata_settings_)),
      columns_(metadata_->GetColumnCount()),
      get_data_offsets_(metadata_->GetColumnCount(), 0),
      diagnostics_(diagnostics),
      current_row_(0),
      num_binding_(0),
      reset_get_data_(false) {
  current_chunk_.data = nullptr;
  if (transformer_) {
    schema_ = transformer_->GetTransformedSchema();
  } else {
    ThrowIfNotOK(flight_info->GetSchema(nullptr).Value(&schema_));
  }

  for (size_t i = 0; i < columns_.size(); ++i) {
    columns_[i] = FlightSqlResultSetColumn(metadata_settings.use_wide_char_);
  }
}

size_t FlightSqlResultSet::Move(size_t rows, size_t bind_offset, size_t bind_type,
                                uint16_t* row_status_array) {
  // Consider it might be the first call to Move() and current_chunk is not
  // populated yet
  assert(rows > 0);
  if (current_chunk_.data == nullptr) {
    if (!chunk_buffer_.GetNext(&current_chunk_)) {
      return 0;
    }

    if (transformer_) {
      current_chunk_.data = transformer_->Transform(current_chunk_.data);
    }

    for (size_t column_num = 0; column_num < columns_.size(); ++column_num) {
      columns_[column_num].ResetAccessor(current_chunk_.data->column(column_num));
    }
  }

  // Reset GetData value offsets.
  if (num_binding_ != get_data_offsets_.size() && reset_get_data_) {
    std::fill(get_data_offsets_.begin(), get_data_offsets_.end(), 0);
  }

  size_t fetched_rows = 0;
  while (fetched_rows < rows) {
    size_t batch_rows = current_chunk_.data->num_rows();
    size_t rows_to_fetch = std::min(static_cast<size_t>(rows - fetched_rows),
                                    static_cast<size_t>(batch_rows - current_row_));

    if (rows_to_fetch == 0) {
      if (!chunk_buffer_.GetNext(&current_chunk_)) {
        break;
      }

      if (transformer_) {
        current_chunk_.data = transformer_->Transform(current_chunk_.data);
      }

      for (size_t column_num = 0; column_num < columns_.size(); ++column_num) {
        columns_[column_num].ResetAccessor(current_chunk_.data->column(column_num));
      }
      current_row_ = 0;
      continue;
    }

    for (auto& column : columns_) {
      // There can be unbound columns.
      if (!column.is_bound_) continue;

      auto* accessor = column.GetAccessorForBinding();
      ColumnBinding shifted_binding = column.binding_;
      uint16_t* shifted_row_status_array =
          row_status_array ? &row_status_array[fetched_rows] : nullptr;

      if (shifted_row_status_array) {
        std::fill(shifted_row_status_array, &shifted_row_status_array[rows_to_fetch],
                  odbcabstraction::RowStatus_SUCCESS);
      }

      size_t accessor_rows = 0;
      try {
        if (!bind_type) {
          // Columnar binding. Have the accessor convert multiple rows.
          if (shifted_binding.buffer) {
            shifted_binding.buffer =
                static_cast<uint8_t*>(shifted_binding.buffer) +
                accessor->GetCellLength(&shifted_binding) * fetched_rows + bind_offset;
          }

          if (shifted_binding.strlen_buffer) {
            shifted_binding.strlen_buffer = reinterpret_cast<ssize_t*>(
                reinterpret_cast<uint8_t*>(&shifted_binding.strlen_buffer[fetched_rows]) +
                bind_offset);
          }

          int64_t value_offset = 0;
          accessor_rows = accessor->GetColumnarData(
              &shifted_binding, current_row_, rows_to_fetch, value_offset, false,
              diagnostics_, shifted_row_status_array);
        } else {
          // Row-wise binding. Identify the base position of the buffer and indicator
          // based on the bind offset, the number of already-fetched rows, and the
          // bind_type holding the size of an application-side row.
          if (shifted_binding.buffer) {
            shifted_binding.buffer = static_cast<uint8_t*>(shifted_binding.buffer) +
                                     bind_offset + bind_type * fetched_rows;
          }

          if (shifted_binding.strlen_buffer) {
            shifted_binding.strlen_buffer = reinterpret_cast<ssize_t*>(
                reinterpret_cast<uint8_t*>(shifted_binding.strlen_buffer) + bind_offset +
                bind_type * fetched_rows);
          }

          // Loop and run the accessor one-row-at-a-time.
          for (size_t i = 0; i < rows_to_fetch; ++i) {
            int64_t value_offset = 0;

            // Adjust offsets passed to the accessor as we fetch rows.
            // Note that current_row_ is updated outside of this loop.
            accessor_rows += accessor->GetColumnarData(
                &shifted_binding, current_row_ + i, 1, value_offset, false, diagnostics_,
                shifted_row_status_array);
            if (shifted_binding.buffer) {
              shifted_binding.buffer =
                  static_cast<uint8_t*>(shifted_binding.buffer) + bind_type;
            }

            if (shifted_binding.strlen_buffer) {
              shifted_binding.strlen_buffer = reinterpret_cast<ssize_t*>(
                  reinterpret_cast<uint8_t*>(shifted_binding.strlen_buffer) + bind_type);
            }

            if (shifted_row_status_array) {
              shifted_row_status_array++;
            }
          }
        }
      } catch (...) {
        if (shifted_row_status_array) {
          std::fill(shifted_row_status_array, &shifted_row_status_array[rows_to_fetch],
                    odbcabstraction::RowStatus_ERROR);
        }
        throw;
      }

      if (rows_to_fetch != accessor_rows) {
        throw DriverException("Expected the same number of rows for all columns");
      }
    }

    current_row_ += static_cast<int64_t>(rows_to_fetch);
    fetched_rows += rows_to_fetch;
  }

  if (rows > fetched_rows && row_status_array) {
    std::fill(&row_status_array[fetched_rows], &row_status_array[rows],
              odbcabstraction::RowStatus_NOROW);
  }
  return fetched_rows;
}

void FlightSqlResultSet::Close() {
  chunk_buffer_.Close();
  current_chunk_.data = nullptr;
}

void FlightSqlResultSet::Cancel() {
  chunk_buffer_.Close();
  current_chunk_.data = nullptr;
}

bool FlightSqlResultSet::GetData(int column_n, int16_t target_type, int precision,
                                 int scale, void* buffer, size_t buffer_length,
                                 ssize_t* strlen_buffer) {
  reset_get_data_ = true;
  // Check if the offset is already at the end.
  int64_t& value_offset = get_data_offsets_[column_n - 1];
  if (value_offset == -1) {
    return false;
  }

  ColumnBinding binding(ConvertCDataTypeFromV2ToV3(target_type), precision, scale, buffer,
                        buffer_length, strlen_buffer);

  auto& column = columns_[column_n - 1];
  Accessor* accessor = column.GetAccessorForGetData(binding.target_type);

  // Note: current_row_ is always positioned at the index _after_ the one we are
  // on after calling Move(). So if we want to get data from the _last_ row
  // fetched, we need to subtract one from the current row.
  accessor->GetColumnarData(&binding, current_row_ - 1, 1, value_offset, true,
                            diagnostics_, nullptr);

  // If there was truncation, the converter would have reported it to the diagnostics.
  return diagnostics_.HasWarning();
}

std::shared_ptr<ResultSetMetadata> FlightSqlResultSet::GetMetadata() { return metadata_; }

void FlightSqlResultSet::BindColumn(int column_n, int16_t target_type, int precision,
                                    int scale, void* buffer, size_t buffer_length,
                                    ssize_t* strlen_buffer) {
  auto& column = columns_[column_n - 1];
  if (buffer == nullptr) {
    if (column.is_bound_) {
      num_binding_--;
    }
    column.ResetBinding();
    return;
  }

  if (!column.is_bound_) {
    num_binding_++;
  }

  ColumnBinding binding(ConvertCDataTypeFromV2ToV3(target_type), precision, scale, buffer,
                        buffer_length, strlen_buffer);
  column.SetBinding(binding, schema_->field(column_n - 1)->type()->id());
}

FlightSqlResultSet::~FlightSqlResultSet() = default;
}  // namespace flight_sql
}  // namespace driver
