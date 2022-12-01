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

#include "arrow/flight/sql/example/sqlite_statement.h"

#include <algorithm>

#include <sqlite3.h>

#include "arrow/array/array_base.h"
#include "arrow/array/array_binary.h"
#include "arrow/array/array_nested.h"
#include "arrow/array/array_primitive.h"
#include "arrow/flight/sql/column_metadata.h"
#include "arrow/flight/sql/example/sqlite_server.h"
#include "arrow/scalar.h"
#include "arrow/table.h"
#include "arrow/type.h"
#include "arrow/util/checked_cast.h"

namespace arrow {
namespace flight {
namespace sql {
namespace example {

using arrow::internal::checked_cast;

std::shared_ptr<DataType> GetDataTypeFromSqliteType(const int column_type) {
  switch (column_type) {
    case SQLITE_INTEGER:
      return int64();
    case SQLITE_FLOAT:
      return float64();
    case SQLITE_BLOB:
      return binary();
    case SQLITE_TEXT:
      return utf8();
    case SQLITE_NULL:
    default:
      return null();
  }
}

int32_t GetPrecisionFromColumn(int column_type) {
  switch (column_type) {
    case SQLITE_INTEGER:
      return 10;
    case SQLITE_FLOAT:
      return 15;
    case SQLITE_NULL:
    default:
      return 0;
  }
}

ColumnMetadata GetColumnMetadata(int column_type, const char* table) {
  ColumnMetadata::ColumnMetadataBuilder builder = ColumnMetadata::Builder();

  builder.Scale(15).IsAutoIncrement(false).IsReadOnly(false);
  if (table == NULLPTR) {
    return builder.Build();
  } else if (column_type == SQLITE_TEXT || column_type == SQLITE_BLOB) {
    std::string table_name(table);
    builder.TableName(table_name);
  } else {
    std::string table_name(table);
    builder.TableName(table_name).Precision(GetPrecisionFromColumn(column_type));
  }
  return builder.Build();
}

arrow::Result<std::shared_ptr<SqliteStatement>> SqliteStatement::Create(
    sqlite3* db, const std::string& sql) {
  sqlite3_stmt* stmt = nullptr;
  int rc =
      sqlite3_prepare_v2(db, sql.c_str(), static_cast<int>(sql.size()), &stmt, NULLPTR);

  if (rc != SQLITE_OK) {
    std::string err_msg = "Can't prepare statement: " + std::string(sqlite3_errmsg(db));
    if (stmt != nullptr) {
      rc = sqlite3_finalize(stmt);
      if (rc != SQLITE_OK) {
        err_msg += "; Failed to finalize SQLite statement: ";
        err_msg += std::string(sqlite3_errmsg(db));
      }
    }
    return Status::Invalid(err_msg);
  }

  std::shared_ptr<SqliteStatement> result(new SqliteStatement(db, stmt));
  return result;
}

arrow::Result<std::shared_ptr<Schema>> SqliteStatement::GetSchema() const {
  std::vector<std::shared_ptr<Field>> fields;
  int column_count = sqlite3_column_count(stmt_);
  for (int i = 0; i < column_count; i++) {
    const char* column_name = sqlite3_column_name(stmt_, i);

    // SQLite does not always provide column types, especially when the statement has not
    // been executed yet. Because of this behaviour this method tries to get the column
    // types in two attempts:
    // 1. Use sqlite3_column_type(), which return SQLITE_NULL if the statement has not
    //    been executed yet
    // 2. Use sqlite3_column_decltype(), which returns correctly if given column is
    //    declared in the table.
    // Because of this limitation, it is not possible to know the column types for some
    // prepared statements, in this case it returns a dense_union type covering any type
    // SQLite supports.
    const int column_type = sqlite3_column_type(stmt_, i);
    const char* table = sqlite3_column_table_name(stmt_, i);
    std::shared_ptr<DataType> data_type = GetDataTypeFromSqliteType(column_type);
    if (data_type->id() == Type::NA) {
      // Try to retrieve column type from sqlite3_column_decltype
      const char* column_decltype = sqlite3_column_decltype(stmt_, i);
      if (column_decltype != NULLPTR) {
        ARROW_ASSIGN_OR_RAISE(data_type, GetArrowType(column_decltype));
      } else {
        // If it can not determine the actual column type, return a dense_union type
        // covering any type SQLite supports.
        data_type = GetUnknownColumnDataType();
      }
    }
    ColumnMetadata column_metadata = GetColumnMetadata(column_type, table);

    fields.push_back(
        arrow::field(column_name, data_type, column_metadata.metadata_map()));
  }

  return arrow::schema(fields);
}

SqliteStatement::~SqliteStatement() { sqlite3_finalize(stmt_); }

arrow::Result<int> SqliteStatement::Step() {
  int rc = sqlite3_step(stmt_);
  if (rc == SQLITE_ERROR) {
    return Status::ExecutionError("A SQLite runtime error has occurred: ",
                                  sqlite3_errmsg(db_));
  }

  return rc;
}

arrow::Result<int> SqliteStatement::Reset() {
  int rc = sqlite3_reset(stmt_);
  if (rc == SQLITE_ERROR) {
    return Status::ExecutionError("A SQLite runtime error has occurred: ",
                                  sqlite3_errmsg(db_));
  }

  return rc;
}

sqlite3_stmt* SqliteStatement::GetSqlite3Stmt() const { return stmt_; }

arrow::Result<int64_t> SqliteStatement::ExecuteUpdate() {
  while (true) {
    ARROW_ASSIGN_OR_RAISE(int rc, Step());
    if (rc == SQLITE_DONE) break;
  }
  return sqlite3_changes(db_);
}

Status SqliteStatement::SetParameters(
    std::vector<std::shared_ptr<arrow::RecordBatch>> parameters) {
  const int num_params = sqlite3_bind_parameter_count(stmt_);
  for (const auto& batch : parameters) {
    if (batch->num_columns() != num_params) {
      return Status::Invalid("Expected ", num_params, " parameters, but got ",
                             batch->num_columns());
    }
  }
  parameters_ = std::move(parameters);
  auto end = std::remove_if(
      parameters_.begin(), parameters_.end(),
      [](const std::shared_ptr<RecordBatch>& batch) { return batch->num_rows() == 0; });
  parameters_.erase(end, parameters_.end());
  return Status::OK();
}

Status SqliteStatement::Bind(size_t batch_index, int64_t row_index) {
  if (batch_index >= parameters_.size()) {
    return Status::IndexError("Cannot bind to batch ", batch_index);
  }
  const RecordBatch& batch = *parameters_[batch_index];
  if (row_index < 0 || row_index >= batch.num_rows()) {
    return Status::IndexError("Cannot bind to row ", row_index, " in batch ",
                              batch_index);
  }

  if (sqlite3_clear_bindings(stmt_) != SQLITE_OK) {
    return Status::Invalid("Failed to reset bindings: ", sqlite3_errmsg(db_));
  }
  for (int c = 0; c < batch.num_columns(); ++c) {
    Array* column = batch.column(c).get();
    int64_t column_index = row_index;
    if (column->type_id() == Type::DENSE_UNION) {
      // Allow polymorphic bindings via union
      const auto& u = checked_cast<const DenseUnionArray&>(*column);
      column_index = u.value_offset(column_index);
      column = u.field(u.child_id(row_index)).get();
    }

    int rc = 0;
    if (column->IsNull(column_index)) {
      rc = sqlite3_bind_null(stmt_, c + 1);
      continue;
    }
    switch (column->type_id()) {
      case Type::INT32: {
        const int32_t value =
            checked_cast<const Int32Array&>(*column).Value(column_index);
        rc = sqlite3_bind_int64(stmt_, c + 1, value);
        break;
      }
      case Type::INT64: {
        const int64_t value =
            checked_cast<const Int64Array&>(*column).Value(column_index);
        rc = sqlite3_bind_int64(stmt_, c + 1, value);
        break;
      }
      case Type::FLOAT: {
        const float value = checked_cast<const FloatArray&>(*column).Value(column_index);
        rc = sqlite3_bind_double(stmt_, c + 1, value);
        break;
      }
      case Type::DOUBLE: {
        const double value =
            checked_cast<const DoubleArray&>(*column).Value(column_index);
        rc = sqlite3_bind_double(stmt_, c + 1, value);
        break;
      }
      case Type::STRING: {
        const std::string_view value =
            checked_cast<const StringArray&>(*column).Value(column_index);
        rc = sqlite3_bind_text(stmt_, c + 1, value.data(), static_cast<int>(value.size()),
                               SQLITE_TRANSIENT);
        break;
      }
      default:
        return Status::TypeError("Received unsupported data type: ", *column->type());
    }
    if (rc != SQLITE_OK) {
      return Status::UnknownError("Failed to bind parameter: ", sqlite3_errmsg(db_));
    }
  }

  return Status::OK();
}

}  // namespace example
}  // namespace sql
}  // namespace flight
}  // namespace arrow
