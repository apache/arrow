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

#include "arrow/flight/flight-sql/example/sqlite_statement.h"
#include "arrow/flight/flight-sql/example/sqlite_statement_batch_reader.h"

#include <sqlite3.h>

#include "arrow/api.h"

#define STRING_BUILDER_CASE(TYPE_CLASS, STMT, COLUMN)                         \
  case TYPE_CLASS##Type::type_id: {                                           \
    int bytes = sqlite3_column_bytes(STMT, COLUMN);                           \
    const unsigned char* string = sqlite3_column_text(STMT, COLUMN);          \
    ARROW_RETURN_NOT_OK(                                                      \
        (dynamic_cast<TYPE_CLASS##Builder&>(builder)).Append(string, bytes)); \
    break;                                                                    \
  }

#define BINARY_BUILDER_CASE(TYPE_CLASS, STMT, COLUMN)                              \
  case TYPE_CLASS##Type::type_id: {                                                \
    int bytes = sqlite3_column_bytes(STMT, COLUMN);                                \
    const void* blob = sqlite3_column_blob(STMT, COLUMN);                          \
    ARROW_RETURN_NOT_OK(                                                           \
        (dynamic_cast<TYPE_CLASS##Builder&>(builder)).Append((char*)blob, bytes)); \
    break;                                                                         \
  }

#define INT_BUILDER_CASE(TYPE_CLASS, STMT, COLUMN)                                    \
  case TYPE_CLASS##Type::type_id: {                                                   \
    sqlite3_int64 value = sqlite3_column_int64(STMT, COLUMN);                         \
    ARROW_RETURN_NOT_OK((dynamic_cast<TYPE_CLASS##Builder&>(builder)).Append(value)); \
    break;                                                                            \
  }

#define FLOAT_BUILDER_CASE(TYPE_CLASS, STMT, COLUMN)                                  \
  case TYPE_CLASS##Type::type_id: {                                                   \
    double value = sqlite3_column_double(STMT, COLUMN);                               \
    ARROW_RETURN_NOT_OK((dynamic_cast<TYPE_CLASS##Builder&>(builder)).Append(value)); \
    break;                                                                            \
  }

#define MAX_BATCH_SIZE 1024

namespace arrow {
namespace flight {
namespace sql {
namespace example {

std::shared_ptr<Schema> SqliteStatementBatchReader::schema() const { return schema_; }

SqliteStatementBatchReader::SqliteStatementBatchReader(
    std::shared_ptr<SqliteStatement> statement, std::shared_ptr<Schema> schema, int rc)
    : statement_(std::move(statement)), schema_(std::move(schema)), rc_(rc) {}

Status SqliteStatementBatchReader::Create(
    const std::shared_ptr<SqliteStatement> &statement_,
    std::shared_ptr<SqliteStatementBatchReader> *result) {

  int rc;
  ARROW_RETURN_NOT_OK(statement_->Step(&rc));

  std::shared_ptr<Schema> schema;
  ARROW_RETURN_NOT_OK(statement_->GetSchema(&schema));

  result->reset(new SqliteStatementBatchReader(statement_, schema, rc));

  return Status::OK();
}

Status SqliteStatementBatchReader::Create(
    const std::shared_ptr<SqliteStatement> &statement_,
    const std::shared_ptr<Schema> &schema,
    std::shared_ptr<SqliteStatementBatchReader> *result) {
  int rc = SQLITE_OK;
  result->reset(new SqliteStatementBatchReader(statement_, schema, rc));

  return Status::OK();
}

Status SqliteStatementBatchReader::ReadNext(std::shared_ptr<RecordBatch> *out) {
  sqlite3_stmt *stmt_ = statement_->GetSqlite3Stmt();

  const int num_fields = schema_->num_fields();
  std::vector<std::unique_ptr<arrow::ArrayBuilder>> builders(num_fields);

  for (int i = 0; i < num_fields; i++) {
    const std::shared_ptr<Field> &field = schema_->field(i);
    const std::shared_ptr<DataType> &field_type = field->type();

    ARROW_RETURN_NOT_OK(MakeBuilder(default_memory_pool(), field_type, &builders[i]));
  }

  if (rc_ == SQLITE_OK) {
    ARROW_RETURN_NOT_OK(statement_->Step(&rc_));
  }

  int rows = 0;
  while (rows < MAX_BATCH_SIZE && rc_ == SQLITE_ROW) {
    rows++;
    for (int i = 0; i < num_fields; i++) {
      const std::shared_ptr<Field> &field = schema_->field(i);
      const std::shared_ptr<DataType> &field_type = field->type();
      ArrayBuilder &builder = *builders[i];

      switch (field_type->id()) {
        INT_BUILDER_CASE(Int64, stmt_, i)
        INT_BUILDER_CASE(UInt64, stmt_, i)
        INT_BUILDER_CASE(Int32, stmt_, i)
        INT_BUILDER_CASE(UInt32, stmt_, i)
        INT_BUILDER_CASE(Int16, stmt_, i)
        INT_BUILDER_CASE(UInt16, stmt_, i)
        INT_BUILDER_CASE(Int8, stmt_, i)
        INT_BUILDER_CASE(UInt8, stmt_, i)
        FLOAT_BUILDER_CASE(Double, stmt_, i)
        FLOAT_BUILDER_CASE(Float, stmt_, i)
        FLOAT_BUILDER_CASE(HalfFloat, stmt_, i)
        BINARY_BUILDER_CASE(Binary, stmt_, i)
        BINARY_BUILDER_CASE(LargeBinary, stmt_, i)
        STRING_BUILDER_CASE(String, stmt_, i)
        STRING_BUILDER_CASE(LargeString, stmt_, i)
        default:
          return Status::NotImplemented("Not implemented SQLite data conversion to ",
                                        field_type->name());
      }
    }

    ARROW_RETURN_NOT_OK(statement_->Step(&rc_));
  }

  if (rows > 0) {
    std::vector<std::shared_ptr<Array>> arrays(builders.size());
    for (int i = 0; i < num_fields; i++) {
      ARROW_RETURN_NOT_OK(builders[i]->Finish(&arrays[i]));
    }

    *out = RecordBatch::Make(schema_, rows, arrays);
  } else {
    *out = NULLPTR;
  }

  return Status::OK();
}

}  // namespace example
}  // namespace sql
}  // namespace flight
}  // namespace arrow
