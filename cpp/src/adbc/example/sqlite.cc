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

#include "adbc/c/driver.h"
#include "adbc/c/types.h"
#include "arrow/builder.h"
#include "arrow/c/bridge.h"
#include "arrow/record_batch.h"
#include "arrow/status.h"
#include "arrow/util/logging.h"

namespace {

using arrow::Status;

class SqliteStatementImpl : public arrow::RecordBatchReader {
 public:
  explicit SqliteStatementImpl(sqlite3* db, sqlite3_stmt* stmt,
                               std::shared_ptr<arrow::Schema> schema, int sqlite_rc)
      : db_(db),
        stmt_(stmt),
        schema_(std::move(schema)),
        done_(sqlite_rc != SQLITE_ROW) {}

  // arrow::RecordBatchReader methods

  std::shared_ptr<arrow::Schema> schema() const { return schema_; }
  Status ReadNext(std::shared_ptr<arrow::RecordBatch>* batch) {
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

  // ADBC methods

  enum AdbcStatusCode Close() {
    auto status = sqlite3_finalize(stmt_);
    if (status != SQLITE_OK) {
      return ADBC_STATUS_UNKNOWN;
    }
    return ADBC_STATUS_OK;
  }

  static enum AdbcStatusCode CloseMethod(struct AdbcStatement* statement) {
    if (!statement->private_data) return ADBC_STATUS_UNINITIALIZED;
    auto* ptr =
        reinterpret_cast<std::shared_ptr<SqliteStatementImpl>*>(statement->private_data);
    return (*ptr)->Close();
  }

  static void ReleaseMethod(struct AdbcStatement* statement) {
    if (!statement->private_data) return;
    delete reinterpret_cast<std::shared_ptr<SqliteStatementImpl>*>(
        statement->private_data);
    statement->private_data = nullptr;
  }

  enum AdbcStatusCode GetResults(const std::shared_ptr<SqliteStatementImpl>& self,
                                 struct ArrowArrayStream* out) {
    auto status = arrow::ExportRecordBatchReader(self, out);
    if (!status.ok()) {
      ARROW_LOG(WARNING) << "[ADBC-SQLite3] Could not initialize result reader: "
                         << status.ToString();
      return ADBC_STATUS_UNKNOWN;
    }
    return ADBC_STATUS_OK;
  }

  static enum AdbcStatusCode GetResultsMethod(struct AdbcStatement* statement,
                                              struct ArrowArrayStream* out) {
    if (!statement->private_data) return ADBC_STATUS_UNINITIALIZED;
    auto* ptr =
        reinterpret_cast<std::shared_ptr<SqliteStatementImpl>*>(statement->private_data);
    return (*ptr)->GetResults(*ptr, out);
  }

 private:
  sqlite3* db_;
  sqlite3_stmt* stmt_;
  std::shared_ptr<arrow::Schema> schema_;
  bool done_;
};

class AdbcSqliteImpl {
 public:
  explicit AdbcSqliteImpl(sqlite3* db) : db_(db) {}

  enum AdbcStatusCode Close() {
    auto status = sqlite3_close(db_);
    if (status != SQLITE_OK) {
      return ADBC_STATUS_UNKNOWN;
    }
    return ADBC_STATUS_OK;
  }

  static enum AdbcStatusCode CloseMethod(struct AdbcConnection* connection) {
    if (!connection->private_data) return ADBC_STATUS_UNINITIALIZED;
    auto* ptr =
        reinterpret_cast<std::shared_ptr<AdbcSqliteImpl>*>(connection->private_data);
    return (*ptr)->Close();
  }

  static void ReleaseMethod(struct AdbcConnection* connection) {
    if (!connection->private_data) return;
    delete reinterpret_cast<std::shared_ptr<AdbcSqliteImpl>*>(connection->private_data);
    connection->private_data = nullptr;
  }

  // SQL

  enum AdbcStatusCode SqlExecute(const char* query, struct AdbcStatement* out) {
    // TODO: we should take an optional length to avoid strlen

    // TODO: This needs to get RAII-guarded to clean up error handling
    sqlite3_stmt* stmt = nullptr;
    auto status = sqlite3_prepare_v2(db_, query, /*nByte*/ -1, &stmt, /*pzTail=*/nullptr);
    if (status != SQLITE_OK) {
      if (stmt) {
        status = sqlite3_finalize(stmt);
        if (status != SQLITE_OK) {
          // TODO: append to error log
          return ADBC_STATUS_UNKNOWN;
        }
      }
      // TODO: append to error log
      return ADBC_STATUS_UNKNOWN;
    }

    // Step the statement and get the schema (SQLite doesn't
    // necessarily know the schema until it begins to execute it)
    status = sqlite3_step(stmt);
    if (status == SQLITE_ERROR) {
      // TODO: append to error log, free statement
      ARROW_LOG(WARNING) << "[SQLite3] sqlite3_step: " << sqlite3_errmsg(db_);
      return ADBC_STATUS_UNKNOWN;
    }

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
    auto schema = arrow::schema(std::move(fields));

    std::memset(out, 0, sizeof(*out));
    auto impl = std::make_shared<SqliteStatementImpl>(db_, stmt, schema, status);

    out->close = &SqliteStatementImpl::CloseMethod;
    out->release = &SqliteStatementImpl::ReleaseMethod;

    out->get_results = &SqliteStatementImpl::GetResultsMethod;

    out->private_data = new std::shared_ptr<SqliteStatementImpl>(impl);
    return ADBC_STATUS_OK;
  }

  static enum AdbcStatusCode SqlExecuteMethod(struct AdbcConnection* connection,
                                              const char* query,
                                              struct AdbcStatement* out) {
    if (!connection->private_data) return ADBC_STATUS_UNINITIALIZED;
    auto* ptr =
        reinterpret_cast<std::shared_ptr<AdbcSqliteImpl>*>(connection->private_data);
    return (*ptr)->SqlExecute(query, out);
  }

 private:
  sqlite3* db_;
};

}  // namespace

enum AdbcStatusCode AdbcDriverConnectionInit(const struct AdbcConnectionOptions* options,
                                             struct AdbcConnection* out) {
  sqlite3* db = nullptr;
  auto status = sqlite3_open_v2(
      ":memory:", &db, SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE, /*zVfs=*/nullptr);
  if (status != SQLITE_OK) {
    return ADBC_STATUS_UNKNOWN;
  }

  auto impl = std::make_shared<AdbcSqliteImpl>(db);

  out->close = &AdbcSqliteImpl::CloseMethod;
  out->release = &AdbcSqliteImpl::ReleaseMethod;

  out->sql_execute = &AdbcSqliteImpl::SqlExecuteMethod;

  out->private_data = new std::shared_ptr<AdbcSqliteImpl>(impl);

  return ADBC_STATUS_OK;
}
