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

#include <cstring>
#include <memory>
#include <string>

#include "adbc/adbc.h"
#include "arrow/builder.h"
#include "arrow/c/bridge.h"
#include "arrow/record_batch.h"
#include "arrow/status.h"
#include "arrow/util/logging.h"
#include "arrow/util/string_builder.h"

namespace {

using arrow::Status;

void SetError(sqlite3* db, const std::string& source, struct AdbcError* error) {
  if (!error) return;
  std::string message =
      arrow::util::StringBuilder("[SQLite3] ", source, ": ", sqlite3_errmsg(db));
  if (error->message) {
    message.reserve(message.size() + 1 + std::strlen(error->message));
    message.append(1, '\n');
    message.append(error->message);
    delete[] error->message;
  }
  error->message = new char[message.size() + 1];
  message.copy(error->message, message.size());
  error->message[message.size()] = '\0';
}

class SqliteStatementImpl : public arrow::RecordBatchReader {
 public:
  explicit SqliteStatementImpl(sqlite3* db, sqlite3_stmt* stmt,
                               std::shared_ptr<arrow::Schema> schema, int sqlite_rc)
      : db_(db),
        stmt_(stmt),
        schema_(std::move(schema)),
        done_(sqlite_rc != SQLITE_ROW) {}

  // arrow::RecordBatchReader methods

  std::shared_ptr<arrow::Schema> schema() const override { return schema_; }
  Status ReadNext(std::shared_ptr<arrow::RecordBatch>* batch) override {
    constexpr int64_t kBatchSize = 1024;

    if (done_) {
      *batch = nullptr;
      return Status::OK();
    }

    std::vector<std::unique_ptr<arrow::ArrayBuilder>> builders(schema_->num_fields());
    for (int i = 0; static_cast<size_t>(i) < builders.size(); i++) {
      // TODO: allow overriding memory pool
      ARROW_RETURN_NOT_OK(arrow::MakeBuilder(arrow::default_memory_pool(),
                                             schema_->field(i)->type(), &builders[i]));
    }

    // The statement was stepped once at the start, so step at the end of the loop
    int64_t num_rows = 0;
    for (int64_t row = 0; row < kBatchSize; row++) {
      for (int64_t col = 0; col < schema_->num_fields(); col++) {
        const auto& field = schema_->field(col);
        switch (field->type()->id()) {
          case arrow::Type::INT64: {
            const sqlite3_int64 value = sqlite3_column_int64(stmt_, col);
            ARROW_RETURN_NOT_OK(
                dynamic_cast<arrow::Int64Builder*>(builders[col].get())->Append(value));
            break;
          }
          default:
            return Status::NotImplemented(field->ToString());
        }
      }
      num_rows++;

      int status = sqlite3_step(stmt_);
      if (status == SQLITE_ROW) {
        continue;
      } else if (status == SQLITE_DONE) {
        done_ = true;
        break;
      }
      ARROW_LOG(WARNING) << "[SQLite3] sqlite3_step: " << sqlite3_errmsg(db_);
      return Status::IOError("");
    }

    arrow::ArrayVector arrays(builders.size());
    for (size_t i = 0; i < builders.size(); i++) {
      ARROW_RETURN_NOT_OK(builders[i]->Finish(&arrays[i]));
    }
    *batch = arrow::RecordBatch::Make(schema_, num_rows, std::move(arrays));
    return Status::OK();
  }

  //----------------------------------------------------------
  // Common Functions
  //----------------------------------------------------------

  enum AdbcStatusCode Close(struct AdbcError* error) {
    auto status = sqlite3_finalize(stmt_);
    if (status != SQLITE_OK) {
      // TODO: record error
      return ADBC_STATUS_UNKNOWN;
    }
    return ADBC_STATUS_OK;
  }

  //----------------------------------------------------------
  // Statement Functions
  //----------------------------------------------------------

  enum AdbcStatusCode GetStream(const std::shared_ptr<SqliteStatementImpl>& self,
                                struct ArrowArrayStream* out, struct AdbcError* error) {
    auto status = arrow::ExportRecordBatchReader(self, out);
    if (!status.ok()) {
      ARROW_LOG(WARNING) << "[ADBC-SQLite3] Could not initialize result reader: "
                         << status.ToString();
      return ADBC_STATUS_UNKNOWN;
    }
    return ADBC_STATUS_OK;
  }

 private:
  sqlite3* db_;
  sqlite3_stmt* stmt_;
  std::shared_ptr<arrow::Schema> schema_;
  bool done_;
};

std::shared_ptr<arrow::Schema> StatementToSchema(sqlite3_stmt* stmt) {
  const int num_columns = sqlite3_column_count(stmt);
  arrow::FieldVector fields(num_columns);
  for (int i = 0; i < num_columns; i++) {
    const char* column_name = sqlite3_column_name(stmt, i);
    const int column_type = sqlite3_column_type(stmt, i);
    std::shared_ptr<arrow::DataType> arrow_type = nullptr;
    switch (column_type) {
      case SQLITE_INTEGER:
        arrow_type = arrow::int64();
        break;
      case SQLITE_FLOAT:
        arrow_type = arrow::float64();
        break;
      case SQLITE_BLOB:
        arrow_type = arrow::binary();
        break;
      case SQLITE_TEXT:
        arrow_type = arrow::utf8();
        break;
      case SQLITE_NULL:
      default:
        arrow_type = arrow::null();
        break;
    }
    fields[i] = arrow::field(column_name, std::move(arrow_type));
  }
  return arrow::schema(std::move(fields));
}

class AdbcSqliteImpl {
 public:
  explicit AdbcSqliteImpl(sqlite3* db) : db_(db) {}

  //----------------------------------------------------------
  // Common Functions
  //----------------------------------------------------------

  enum AdbcStatusCode Release(struct AdbcError* error) {
    auto status = sqlite3_close(db_);
    if (status != SQLITE_OK) {
      // TODO:
      return ADBC_STATUS_UNKNOWN;
    }
    return ADBC_STATUS_OK;
  }

  //----------------------------------------------------------
  // SQL Semantics
  //----------------------------------------------------------

  enum AdbcStatusCode SqlExecute(const char* query, size_t query_length,
                                 struct AdbcStatement* out, struct AdbcError* error) {
    // TODO: This needs to get RAII-guarded to clean up error handling
    sqlite3_stmt* stmt = nullptr;
    auto rc = sqlite3_prepare_v2(db_, query, query_length, &stmt, /*pzTail=*/nullptr);
    if (rc != SQLITE_OK) {
      if (stmt) {
        rc = sqlite3_finalize(stmt);
        if (rc != SQLITE_OK) {
          SetError(db_, "sqlite3_finalize", error);
          return ADBC_STATUS_UNKNOWN;
        }
      }
      SetError(db_, "sqlite3_prepare_v2", error);
      return ADBC_STATUS_UNKNOWN;
    }

    // Step the statement and get the schema (SQLite doesn't
    // necessarily know the schema until it begins to execute it)
    rc = sqlite3_step(stmt);
    if (rc == SQLITE_ERROR) {
      SetError(db_, "sqlite3_step", error);
      rc = sqlite3_finalize(stmt);
      if (rc != SQLITE_OK) {
        SetError(db_, "sqlite3_finalize", error);
      }
      return ADBC_STATUS_UNKNOWN;
    }
    auto schema = StatementToSchema(stmt);

    std::memset(out, 0, sizeof(*out));
    auto impl = std::make_shared<SqliteStatementImpl>(db_, stmt, std::move(schema), rc);
    out->private_data = new std::shared_ptr<SqliteStatementImpl>(impl);
    return ADBC_STATUS_OK;
  }

  static enum AdbcStatusCode SqlExecuteMethod(struct AdbcConnection* connection,
                                              const char* query, size_t query_length,
                                              struct AdbcStatement* out,
                                              struct AdbcError* error) {
    if (!connection->private_data) return ADBC_STATUS_UNINITIALIZED;
    auto* ptr =
        reinterpret_cast<std::shared_ptr<AdbcSqliteImpl>*>(connection->private_data);
    return (*ptr)->SqlExecute(query, query_length, out, error);
  }

 private:
  sqlite3* db_;
};

}  // namespace

void AdbcErrorRelease(struct AdbcError* error) {
  delete[] error->message;
  error->message = nullptr;
}

enum AdbcStatusCode AdbcConnectionInit(const struct AdbcConnectionOptions* options,
                                       struct AdbcConnection* out,
                                       struct AdbcError* error) {
  sqlite3* db = nullptr;
  auto status = sqlite3_open_v2(
      ":memory:", &db, SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE, /*zVfs=*/nullptr);
  if (status != SQLITE_OK) {
    if (db) {
      SetError(db, "sqlite3_open_v2", error);
    }
    return ADBC_STATUS_UNKNOWN;
  }

  auto impl = std::make_shared<AdbcSqliteImpl>(db);

  out->sql_execute = &AdbcSqliteImpl::SqlExecuteMethod;
  out->private_data = new std::shared_ptr<AdbcSqliteImpl>(impl);
  return ADBC_STATUS_OK;
}

enum AdbcStatusCode AdbcConnectionRelease(struct AdbcConnection* connection,
                                          struct AdbcError* error) {
  if (!connection->private_data) return ADBC_STATUS_UNINITIALIZED;
  auto ptr = reinterpret_cast<std::shared_ptr<AdbcSqliteImpl>*>(connection->private_data);
  enum AdbcStatusCode status = (*ptr)->Release(error);
  delete ptr;
  connection->private_data = nullptr;
  return status;
}

enum AdbcStatusCode AdbcStatementGetPartitionDesc(struct AdbcStatement* statement,
                                                  uint8_t* partition_desc,
                                                  struct AdbcError* error) {
  return ADBC_STATUS_NOT_IMPLEMENTED;
}

enum AdbcStatusCode AdbcStatementGetPartitionDescSize(struct AdbcStatement* statement,
                                                      size_t* length,
                                                      struct AdbcError* error) {
  return ADBC_STATUS_NOT_IMPLEMENTED;
}

enum AdbcStatusCode AdbcStatementGetStream(struct AdbcStatement* statement,
                                           struct ArrowArrayStream* out,
                                           struct AdbcError* error) {
  if (!statement->private_data) return ADBC_STATUS_UNINITIALIZED;
  auto* ptr =
      reinterpret_cast<std::shared_ptr<SqliteStatementImpl>*>(statement->private_data);
  return (*ptr)->GetStream(*ptr, out, error);
}

enum AdbcStatusCode AdbcStatementRelease(struct AdbcStatement* statement,
                                         struct AdbcError* error) {
  if (!statement->private_data) return ADBC_STATUS_UNINITIALIZED;
  auto* ptr =
      reinterpret_cast<std::shared_ptr<SqliteStatementImpl>*>(statement->private_data);
  auto status = (*ptr)->Close(error);
  delete ptr;
  statement->private_data = nullptr;
  return status;
}
