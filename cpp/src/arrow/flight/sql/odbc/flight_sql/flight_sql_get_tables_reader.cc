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

#include "arrow/flight/sql/odbc/flight_sql/flight_sql_get_tables_reader.h"
#include "arrow/array/builder_binary.h"
#include "arrow/array/builder_primitive.h"
#include "arrow/flight/sql/odbc/flight_sql/utils.h"
#include "arrow/flight/sql/odbc/odbcabstraction/include/odbcabstraction/platform.h"
#include "arrow/io/memory.h"
#include "arrow/status.h"

#include <utility>

namespace driver {
namespace flight_sql {

using arrow::BinaryArray;
using arrow::StringArray;

using arrow::internal::checked_pointer_cast;
using std::nullopt;

GetTablesReader::GetTablesReader(std::shared_ptr<RecordBatch> record_batch)
    : record_batch_(std::move(record_batch)), current_row_(-1) {}

bool GetTablesReader::Next() { return ++current_row_ < record_batch_->num_rows(); }

optional<std::string> GetTablesReader::GetCatalogName() {
  const auto& array = checked_pointer_cast<StringArray>(record_batch_->column(0));

  if (array->IsNull(current_row_)) return nullopt;

  return array->GetString(current_row_);
}

optional<std::string> GetTablesReader::GetDbSchemaName() {
  const auto& array = checked_pointer_cast<StringArray>(record_batch_->column(1));

  if (array->IsNull(current_row_)) return nullopt;

  return array->GetString(current_row_);
}

std::string GetTablesReader::GetTableName() {
  const auto& array = checked_pointer_cast<StringArray>(record_batch_->column(2));

  return array->GetString(current_row_);
}

std::string GetTablesReader::GetTableType() {
  const auto& array = checked_pointer_cast<StringArray>(record_batch_->column(3));

  return array->GetString(current_row_);
}

std::shared_ptr<Schema> GetTablesReader::GetSchema() {
  const auto& array = checked_pointer_cast<BinaryArray>(record_batch_->column(4));
  if (array == nullptr) {
    return nullptr;
  }

  // Create a non-owned Buffer to avoid copying
  arrow::io::BufferReader dataset_schema_reader(
      std::make_shared<arrow::Buffer>(array->GetView(current_row_)));
  arrow::ipc::DictionaryMemo in_memo;
  const arrow::Result<std::shared_ptr<Schema>>& result =
      arrow::ipc::ReadSchema(&dataset_schema_reader, &in_memo);
  if (!result.ok()) {
    // TODO: Ignoring this error until we fix the problem on Dremio server
    // The problem is that complex types columns are being returned without the children
    // types.
    return nullptr;
  }

  return result.ValueOrDie();
}

}  // namespace flight_sql
}  // namespace driver
