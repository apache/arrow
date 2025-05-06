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

#include "arrow/flight/sql/odbc/flight_sql/flight_sql_get_type_info_reader.h"
#include "arrow/array.h"
#include "arrow/flight/sql/odbc/flight_sql/utils.h"
#include "arrow/flight/sql/odbc/odbcabstraction/include/odbcabstraction/platform.h"
#include "arrow/io/memory.h"

#include <utility>

namespace driver {
namespace flight_sql {

using arrow::BooleanArray;
using arrow::Int32Array;
using arrow::ListArray;
using arrow::StringArray;

using arrow::internal::checked_pointer_cast;
using std::nullopt;

GetTypeInfoReader::GetTypeInfoReader(std::shared_ptr<RecordBatch> record_batch)
    : record_batch_(std::move(record_batch)), current_row_(-1) {}

bool GetTypeInfoReader::Next() { return ++current_row_ < record_batch_->num_rows(); }

std::string GetTypeInfoReader::GetTypeName() {
  const auto& array = checked_pointer_cast<StringArray>(record_batch_->column(0));

  return array->GetString(current_row_);
}

int32_t GetTypeInfoReader::GetDataType() {
  const auto& array = checked_pointer_cast<Int32Array>(record_batch_->column(1));

  return array->GetView(current_row_);
}

optional<int32_t> GetTypeInfoReader::GetColumnSize() {
  const auto& array = checked_pointer_cast<Int32Array>(record_batch_->column(2));

  if (array->IsNull(current_row_)) return nullopt;

  return array->GetView(current_row_);
}

optional<std::string> GetTypeInfoReader::GetLiteralPrefix() {
  const auto& array = checked_pointer_cast<StringArray>(record_batch_->column(3));

  if (array->IsNull(current_row_)) return nullopt;

  return array->GetString(current_row_);
}

optional<std::string> GetTypeInfoReader::GetLiteralSuffix() {
  const auto& array = checked_pointer_cast<StringArray>(record_batch_->column(4));

  if (array->IsNull(current_row_)) return nullopt;

  return array->GetString(current_row_);
}

optional<std::vector<std::string>> GetTypeInfoReader::GetCreateParams() {
  const auto& array = checked_pointer_cast<ListArray>(record_batch_->column(5));

  if (array->IsNull(current_row_)) return nullopt;

  int values_length = array->value_length(current_row_);
  int start_offset = array->value_offset(current_row_);
  const auto& values_array = checked_pointer_cast<StringArray>(array->values());

  std::vector<std::string> result(values_length);
  for (int i = 0; i < values_length; ++i) {
    result[i] = values_array->GetString(start_offset + i);
  }

  return result;
}

int32_t GetTypeInfoReader::GetNullable() {
  const auto& array = checked_pointer_cast<Int32Array>(record_batch_->column(6));

  return array->GetView(current_row_);
}

bool GetTypeInfoReader::GetCaseSensitive() {
  const auto& array = checked_pointer_cast<BooleanArray>(record_batch_->column(7));

  return array->GetView(current_row_);
}

int32_t GetTypeInfoReader::GetSearchable() {
  const auto& array = checked_pointer_cast<Int32Array>(record_batch_->column(8));

  return array->GetView(current_row_);
}

optional<bool> GetTypeInfoReader::GetUnsignedAttribute() {
  const auto& array = checked_pointer_cast<BooleanArray>(record_batch_->column(9));

  if (array->IsNull(current_row_)) return nullopt;

  return array->GetView(current_row_);
}

bool GetTypeInfoReader::GetFixedPrecScale() {
  const auto& array = checked_pointer_cast<BooleanArray>(record_batch_->column(10));

  return array->GetView(current_row_);
}

optional<bool> GetTypeInfoReader::GetAutoIncrement() {
  const auto& array = checked_pointer_cast<BooleanArray>(record_batch_->column(11));

  if (array->IsNull(current_row_)) return nullopt;

  return array->GetView(current_row_);
}

optional<std::string> GetTypeInfoReader::GetLocalTypeName() {
  const auto& array = checked_pointer_cast<StringArray>(record_batch_->column(12));

  if (array->IsNull(current_row_)) return nullopt;

  return array->GetString(current_row_);
}

optional<int32_t> GetTypeInfoReader::GetMinimumScale() {
  const auto& array = checked_pointer_cast<Int32Array>(record_batch_->column(13));

  if (array->IsNull(current_row_)) return nullopt;

  return array->GetView(current_row_);
}

optional<int32_t> GetTypeInfoReader::GetMaximumScale() {
  const auto& array = checked_pointer_cast<Int32Array>(record_batch_->column(14));

  if (array->IsNull(current_row_)) return nullopt;

  return array->GetView(current_row_);
}

int32_t GetTypeInfoReader::GetSqlDataType() {
  const auto& array = checked_pointer_cast<Int32Array>(record_batch_->column(15));

  return array->GetView(current_row_);
}

optional<int32_t> GetTypeInfoReader::GetDatetimeSubcode() {
  const auto& array = checked_pointer_cast<Int32Array>(record_batch_->column(16));

  if (array->IsNull(current_row_)) return nullopt;

  return array->GetView(current_row_);
}

optional<int32_t> GetTypeInfoReader::GetNumPrecRadix() {
  const auto& array = checked_pointer_cast<Int32Array>(record_batch_->column(17));

  if (array->IsNull(current_row_)) return nullopt;

  return array->GetView(current_row_);
}

optional<int32_t> GetTypeInfoReader::GetIntervalPrecision() {
  const auto& array = checked_pointer_cast<Int32Array>(record_batch_->column(18));

  if (array->IsNull(current_row_)) return nullopt;

  return array->GetView(current_row_);
}

}  // namespace flight_sql
}  // namespace driver
