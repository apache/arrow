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

#include <sqlite3.h>
#include <iostream>

#include "arrow/api.h"
#include "arrow/flight/flight-sql/example/sqlite_statement.h"

namespace arrow {
namespace flight {
namespace sql {
namespace example {

std::shared_ptr<DataType> GetDataTypeFromSqliteType(const int column_type) {
  switch (column_type) {
    case SQLITE_INTEGER:return int64();
    case SQLITE_FLOAT:return float64();
    case SQLITE_BLOB:return binary();
    case SQLITE_TEXT:return utf8();
    case SQLITE_NULL:
    default:return null();
  }
}

Status SqliteStatement::Create(sqlite3 *db, const std::string &sql,
                               std::shared_ptr<SqliteStatement> *result) {
  sqlite3_stmt *stmt;
  const char *z_sql = sql.c_str();
  int rc =
      sqlite3_prepare_v2(db, z_sql, static_cast<int>(strlen(z_sql)), &stmt, NULLPTR);

  if (rc != SQLITE_OK) {
    sqlite3_finalize(stmt);
    return Status::RError("A SQLite runtime error has occurred: ", sqlite3_errmsg(db));
  }

  result->reset(new SqliteStatement(db, stmt));
  return Status::OK();
}

Status SqliteStatement::GetSchema(std::shared_ptr<Schema> *schema) const {
  std::vector<std::shared_ptr<Field>> fields;
  int column_count = sqlite3_column_count(stmt_);
  for (int i = 0; i < column_count; i++) {
    const char *column_name = sqlite3_column_name(stmt_, i);
    const int column_type = sqlite3_column_type(stmt_, i);

    std::shared_ptr<DataType> data_type = GetDataTypeFromSqliteType(column_type);
    fields.push_back(arrow::field(column_name, data_type));
  }

  *schema = arrow::schema(fields);
  return Status::OK();
}

SqliteStatement::~SqliteStatement() {
  sqlite3_finalize(stmt_);
}

Status SqliteStatement::Step(int *rc) {
  *rc = sqlite3_step(stmt_);
  if (*rc == SQLITE_ERROR) {
    return Status::RError("A SQLite runtime error has occurred: ", sqlite3_errmsg(db_));
  }

  return Status::OK();
}

sqlite3_stmt *SqliteStatement::GetSqlite3Stmt() {
  return stmt_;
}

}  // namespace example
}  // namespace sql
}  // namespace flight
}  // namespace arrow
