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

#include "arrow/flight/sql/example/sqlite_statement_batch_reader.h"

#include <sqlite3.h>

#include "arrow/builder.h"
#include "arrow/flight/sql/example/sqlite_statement.h"

#define STRING_BUILDER_CASE(TYPE_CLASS, STMT, COLUMN)                        \
  case TYPE_CLASS##Type::type_id: {                                          \
    auto builder = reinterpret_cast<TYPE_CLASS##Builder*>(array_builder);    \
    const int bytes = sqlite3_column_bytes(STMT, COLUMN);                    \
    const uint8_t* string =                                                  \
        reinterpret_cast<const uint8_t*>(sqlite3_column_text(STMT, COLUMN)); \
    if (string == nullptr) {                                                 \
      ARROW_RETURN_NOT_OK(builder->AppendNull());                            \
      break;                                                                 \
    }                                                                        \
    ARROW_RETURN_NOT_OK(builder->Append(string, bytes));                     \
    break;                                                                   \
  }

#define BINARY_BUILDER_CASE(TYPE_CLASS, STMT, COLUMN)                        \
  case TYPE_CLASS##Type::type_id: {                                          \
    auto builder = reinterpret_cast<TYPE_CLASS##Builder*>(array_builder);    \
    const int bytes = sqlite3_column_bytes(STMT, COLUMN);                    \
    const uint8_t* blob =                                                    \
        reinterpret_cast<const uint8_t*>(sqlite3_column_blob(STMT, COLUMN)); \
    if (blob == nullptr) {                                                   \
      ARROW_RETURN_NOT_OK(builder->AppendNull());                            \
      break;                                                                 \
    }                                                                        \
    ARROW_RETURN_NOT_OK(builder->Append(blob, bytes));                       \
    break;                                                                   \
  }

#define INT_BUILDER_CASE(TYPE_CLASS, STMT, COLUMN)                        \
  case TYPE_CLASS##Type::type_id: {                                       \
    using c_type = typename TYPE_CLASS##Type::c_type;                     \
    auto builder = reinterpret_cast<TYPE_CLASS##Builder*>(array_builder); \
    const sqlite3_int64 value = sqlite3_column_int64(STMT, COLUMN);       \
    ARROW_RETURN_NOT_OK(builder->Append(static_cast<c_type>(value)));     \
    break;                                                                \
  }

#define FLOAT_BUILDER_CASE(TYPE_CLASS, STMT, COLUMN)                          \
  case TYPE_CLASS##Type::type_id: {                                           \
    auto builder = reinterpret_cast<TYPE_CLASS##Builder*>(array_builder);     \
    const double value = sqlite3_column_double(STMT, COLUMN);                 \
    ARROW_RETURN_NOT_OK(                                                      \
        builder->Append(static_cast<const TYPE_CLASS##Type::c_type>(value))); \
    break;                                                                    \
  }

namespace arrow {
namespace flight {
namespace sql {
namespace example {

// Batch size for SQLite statement results
static constexpr int32_t kMaxBatchSize = 16384;

std::shared_ptr<Schema> SqliteStatementBatchReader::schema() const { return schema_; }

SqliteStatementBatchReader::SqliteStatementBatchReader(
    std::shared_ptr<SqliteStatement> statement, std::shared_ptr<Schema> schema)
    : statement_(std::move(statement)),
      schema_(std::move(schema)),
      rc_(SQLITE_OK),
      already_executed_(false) {}

arrow::Result<std::shared_ptr<SqliteStatementBatchReader>>
SqliteStatementBatchReader::Create(const std::shared_ptr<SqliteStatement>& statement_) {
  ARROW_RETURN_NOT_OK(statement_->Reset());
  if (!statement_->parameters().empty()) {
    // If there are parameters, infer the schema after binding the first row
    ARROW_RETURN_NOT_OK(statement_->Bind(0, 0));
  }
  ARROW_RETURN_NOT_OK(statement_->Step());
  ARROW_ASSIGN_OR_RAISE(auto schema, statement_->GetSchema());

  std::shared_ptr<SqliteStatementBatchReader> result(
      new SqliteStatementBatchReader(statement_, schema));

  return result;
}

arrow::Result<std::shared_ptr<SqliteStatementBatchReader>>
SqliteStatementBatchReader::Create(const std::shared_ptr<SqliteStatement>& statement,
                                   const std::shared_ptr<Schema>& schema) {
  return std::shared_ptr<SqliteStatementBatchReader>(
      new SqliteStatementBatchReader(statement, schema));
}

Status SqliteStatementBatchReader::ReadNext(std::shared_ptr<RecordBatch>* out) {
  sqlite3_stmt* stmt_ = statement_->GetSqlite3Stmt();

  const int num_fields = schema_->num_fields();
  std::vector<std::unique_ptr<arrow::ArrayBuilder>> builders(num_fields);

  for (int i = 0; i < num_fields; i++) {
    const std::shared_ptr<Field>& field = schema_->field(i);
    const std::shared_ptr<DataType>& field_type = field->type();

    ARROW_RETURN_NOT_OK(MakeBuilder(default_memory_pool(), field_type, &builders[i]));
  }

  int64_t rows = 0;
  while (true) {
    if (!already_executed_) {
      ARROW_ASSIGN_OR_RAISE(rc_, statement_->Reset());
      if (!statement_->parameters().empty()) {
        if (batch_index_ >= statement_->parameters().size()) {
          *out = nullptr;
          break;
        }
        ARROW_RETURN_NOT_OK(statement_->Bind(batch_index_, row_index_));
      }
      ARROW_ASSIGN_OR_RAISE(rc_, statement_->Step());
      already_executed_ = true;
    }

    while (rows < kMaxBatchSize && rc_ == SQLITE_ROW) {
      rows++;
      for (int i = 0; i < num_fields; i++) {
        const std::shared_ptr<Field>& field = schema_->field(i);
        const std::shared_ptr<DataType>& field_type = field->type();
        ArrayBuilder* array_builder = builders[i].get();

        if (sqlite3_column_type(stmt_, i) == SQLITE_NULL) {
          ARROW_RETURN_NOT_OK(array_builder->AppendNull());
          continue;
        }

        switch (field_type->id()) {
          // XXX This doesn't handle overflows when converting to the target
          // integer type.
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

      ARROW_ASSIGN_OR_RAISE(rc_, statement_->Step());
    }

    // If we still have bind parameters, bind again and retry
    const std::vector<std::shared_ptr<RecordBatch>>& params = statement_->parameters();
    if (!params.empty() && rc_ == SQLITE_DONE && batch_index_ < params.size()) {
      row_index_++;
      if (row_index_ < params[batch_index_]->num_rows()) {
        already_executed_ = false;
      } else {
        batch_index_++;
        row_index_ = 0;
        if (batch_index_ < params.size()) {
          already_executed_ = false;
        }
      }

      if (!already_executed_ && rows < kMaxBatchSize) continue;
    }

    if (rows > 0) {
      std::vector<std::shared_ptr<Array>> arrays(builders.size());
      for (int i = 0; i < num_fields; i++) {
        ARROW_RETURN_NOT_OK(builders[i]->Finish(&arrays[i]));
      }

      *out = RecordBatch::Make(schema_, rows, arrays);
    } else {
      *out = nullptr;
    }
    break;
  }
  return Status::OK();
}

#undef STRING_BUILDER_CASE
#undef BINARY_BUILDER_CASE
#undef INT_BUILDER_CASE
#undef FLOAT_BUILDER_CASE

}  // namespace example
}  // namespace sql
}  // namespace flight
}  // namespace arrow
