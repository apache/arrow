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

#include <sqlite3.h>

#define BOOST_NO_CXX98_FUNCTION_BASE  // ARROW-17805
#include <boost/algorithm/string.hpp>

#include "arrow/flight/sql/column_metadata.h"
#include "arrow/flight/sql/example/sqlite_server.h"

namespace arrow {
namespace flight {
namespace sql {
namespace example {

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
        data_type = GetArrowType(column_decltype);
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
  ARROW_RETURN_NOT_OK(Step());
  return sqlite3_changes(db_);
}

}  // namespace example
}  // namespace sql
}  // namespace flight
}  // namespace arrow
