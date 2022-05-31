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
#include <mutex>
#include <string>

#include "adbc/adbc.h"
#include "adbc/driver/util.h"
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

template <typename... Args>
void SetError(struct AdbcError* error, Args&&... args) {
  if (!error) return;
  std::string message =
      arrow::util::StringBuilder("[SQLite3] ", std::forward<Args>(args)...);
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

class SqliteDatabaseImpl {
 public:
  explicit SqliteDatabaseImpl(sqlite3* db) : db_(db), connection_count_(0) {}

  sqlite3* Connect() {
    std::lock_guard<std::mutex> guard(mutex_);
    ++connection_count_;
    return db_;
  }

  AdbcStatusCode Disconnect(struct AdbcError* error) {
    std::lock_guard<std::mutex> guard(mutex_);
    if (--connection_count_ < 0) {
      SetError(error, "Connection count underflow");
      return ADBC_STATUS_INTERNAL;
    }
    return ADBC_STATUS_OK;
  }

  AdbcStatusCode Release(struct AdbcError* error) {
    std::lock_guard<std::mutex> guard(mutex_);

    if (connection_count_ > 0) {
      SetError(error, "Cannot release database with ", connection_count_,
               " open connections");
      return ADBC_STATUS_INTERNAL;
    }

    auto status = sqlite3_close(db_);
    if (status != SQLITE_OK) {
      // TODO:
      return ADBC_STATUS_UNKNOWN;
    }
    return ADBC_STATUS_OK;
  }

 private:
  sqlite3* db_;
  int connection_count_;
  std::mutex mutex_;
};

class SqliteStatementImpl : public arrow::RecordBatchReader {
 public:
  SqliteStatementImpl()
      : db_(nullptr), stmt_(nullptr), schema_(nullptr), bind_index_(0), done_(false) {}

  void Init(sqlite3* db, sqlite3_stmt* stmt) {
    db_ = db;
    stmt_ = stmt;
  }

  //----------------------------------------------------------
  // arrow::RecordBatchReader
  //----------------------------------------------------------

  std::shared_ptr<arrow::Schema> schema() const override {
    DCHECK(schema_);
    return schema_;
  }

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
      for (int col = 0; col < schema_->num_fields(); col++) {
        const auto& field = schema_->field(col);
        switch (field->type()->id()) {
          case arrow::Type::INT64: {
            const sqlite3_int64 value = sqlite3_column_int64(stmt_, col);
            ARROW_RETURN_NOT_OK(
                dynamic_cast<arrow::Int64Builder*>(builders[col].get())->Append(value));
            break;
          }
          case arrow::Type::STRING: {
            const char* value =
                reinterpret_cast<const char*>(sqlite3_column_text(stmt_, col));
            if (!value) {
              ARROW_RETURN_NOT_OK(
                  dynamic_cast<arrow::StringBuilder*>(builders[col].get())->AppendNull());
            } else {
              const arrow::util::string_view view(value, std::strlen(value));
              ARROW_RETURN_NOT_OK(dynamic_cast<arrow::StringBuilder*>(builders[col].get())
                                      ->Append(value));
            }
            break;
          }
          default:
            return Status::NotImplemented("[SQLite3] Cannot read field '", field->name(),
                                          "' of type ", field->type()->ToString());
        }
      }
      num_rows++;

      int status = sqlite3_step(stmt_);
      if (status == SQLITE_ROW) {
        continue;
      } else if (status == SQLITE_DONE) {
        if (bind_parameters_ && bind_index_ < bind_parameters_->num_rows()) {
          status = sqlite3_reset(stmt_);
          if (status != SQLITE_OK) {
            return Status::IOError("[SQLite3] sqlite3_reset: ", sqlite3_errmsg(db_));
          }
          RETURN_NOT_OK(BindNext());
          status = sqlite3_step(stmt_);
          if (status == SQLITE_ROW) continue;
        } else {
          done_ = true;
          bind_parameters_.reset();
        }
        break;
      }
      return Status::IOError("[SQLite3] sqlite3_step: ", sqlite3_errmsg(db_));
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

  AdbcStatusCode Close(struct AdbcError* error) {
    if (stmt_) {
      auto status = sqlite3_finalize(stmt_);
      if (status != SQLITE_OK) {
        SetError(db_, "sqlite3_finalize", error);
        return ADBC_STATUS_UNKNOWN;
      }
      stmt_ = nullptr;
      bind_parameters_.reset();
    }
    return ADBC_STATUS_OK;
  }

  //----------------------------------------------------------
  // Statement Functions
  //----------------------------------------------------------

  AdbcStatusCode Bind(const std::shared_ptr<SqliteStatementImpl>& self,
                      struct ArrowArray* values, struct ArrowSchema* schema,
                      struct AdbcError* error) {
    auto status = arrow::ImportRecordBatch(values, schema).Value(&bind_parameters_);
    if (!status.ok()) {
      SetError(error, status);
      return ADBC_STATUS_INVALID_ARGUMENT;
    }
    return ADBC_STATUS_OK;
  }

  AdbcStatusCode Execute(const std::shared_ptr<SqliteStatementImpl>& self,
                         struct AdbcError* error) {
    int rc = 0;
    if (schema_) {
      rc = sqlite3_clear_bindings(stmt_);
      if (rc != SQLITE_OK) {
        SetError(db_, "sqlite3_reset_bindings", error);
        rc = sqlite3_finalize(stmt_);
        if (rc != SQLITE_OK) {
          SetError(db_, "sqlite3_finalize", error);
        }
        return ADBC_STATUS_IO;
      }

      rc = sqlite3_reset(stmt_);
      if (rc != SQLITE_OK) {
        SetError(db_, "sqlite3_reset", error);
        rc = sqlite3_finalize(stmt_);
        if (rc != SQLITE_OK) {
          SetError(db_, "sqlite3_finalize", error);
        }
        return ADBC_STATUS_IO;
      }
    }
    // Step the statement and get the schema (SQLite doesn't
    // necessarily know the schema until it begins to execute it)
    auto status = BindNext().Value(&rc);
    // XXX: with parameters, inferring the schema from the first
    // argument is inaccurate (what if one is null?). Is there a way
    // to hint to SQLite the real type?
    if (!status.ok()) {
      // TODO: map Arrow codes to ADBC codes
      SetError(error, status);
      return ADBC_STATUS_IO;
    } else if (rc != SQLITE_OK) {
      SetError(db_, "sqlite3_bind", error);
      rc = sqlite3_finalize(stmt_);
      if (rc != SQLITE_OK) {
        SetError(db_, "sqlite3_finalize", error);
      }
      return ADBC_STATUS_IO;
    }
    rc = sqlite3_step(stmt_);
    if (rc == SQLITE_ERROR) {
      SetError(db_, "sqlite3_step", error);
      rc = sqlite3_finalize(stmt_);
      if (rc != SQLITE_OK) {
        SetError(db_, "sqlite3_finalize", error);
      }
      return ADBC_STATUS_IO;
    }
    schema_ = StatementToSchema(stmt_);
    done_ = rc != SQLITE_ROW;
    return ADBC_STATUS_OK;
  }

  AdbcStatusCode GetStream(const std::shared_ptr<SqliteStatementImpl>& self,
                           struct ArrowArrayStream* out, struct AdbcError* error) {
    if (!stmt_ || !schema_) {
      SetError(error, "Statement has not yet been executed");
      return ADBC_STATUS_UNINITIALIZED;
    }
    auto status = arrow::ExportRecordBatchReader(self, out);
    if (!status.ok()) {
      SetError(error, "Could not initialize result reader: ", status);
      return ADBC_STATUS_UNKNOWN;
    }
    return ADBC_STATUS_OK;
  }

 private:
  arrow::Result<int> BindNext() {
    if (!bind_parameters_ || bind_index_ >= bind_parameters_->num_rows()) {
      return SQLITE_OK;
    }

    int col_index = 1;
    // TODO: multiple output rows
    const int bind_index = bind_index_++;
    for (const auto& column : bind_parameters_->columns()) {
      if (column->IsNull(bind_index)) {
        const int rc = sqlite3_bind_null(stmt_, col_index);
        if (rc != SQLITE_OK) return rc;
      } else {
        switch (column->type()->id()) {
          case arrow::Type::INT64: {
            const int rc = sqlite3_bind_int64(
                stmt_, col_index,
                static_cast<const arrow::Int64Array&>(*column).Value(bind_index));
            if (rc != SQLITE_OK) return rc;
            break;
          }
          case arrow::Type::STRING: {
            const auto& strings = static_cast<const arrow::StringArray&>(*column);
            const int rc = sqlite3_bind_text64(
                stmt_, col_index, strings.Value(bind_index).data(),
                strings.value_length(bind_index), SQLITE_STATIC, SQLITE_UTF8);
            if (rc != SQLITE_OK) return rc;
            break;
          }
          default:
            return arrow::Status::NotImplemented("Binding parameter of type ",
                                                 *column->type());
        }
      }
      col_index++;
    }

    return SQLITE_OK;
  }

  sqlite3* db_;
  sqlite3_stmt* stmt_;
  std::shared_ptr<arrow::Schema> schema_;
  std::shared_ptr<arrow::RecordBatch> bind_parameters_;
  int64_t bind_index_;
  bool done_;
};

class SqliteConnectionImpl {
 public:
  explicit SqliteConnectionImpl(std::shared_ptr<SqliteDatabaseImpl> database)
      : database_(std::move(database)), db_(database_->Connect()) {}

  //----------------------------------------------------------
  // Common Functions
  //----------------------------------------------------------

  AdbcStatusCode Release(struct AdbcError* error) { return database_->Disconnect(error); }

  //----------------------------------------------------------
  // SQL Semantics
  //----------------------------------------------------------

  AdbcStatusCode SqlExecute(const char* query, struct AdbcStatement* out,
                            struct AdbcError* error) {
    auto status = SqlPrepare(query, out, error);
    if (status != ADBC_STATUS_OK) return status;

    return AdbcStatementExecute(out, error);
  }

  AdbcStatusCode SqlPrepare(const char* query, struct AdbcStatement* out,
                            struct AdbcError* error) {
    if (!out->private_data) {
      SetError(error, "Statement is uninitialized, use AdbcStatementInit");
      return ADBC_STATUS_UNINITIALIZED;
    }
    auto* ptr =
        reinterpret_cast<std::shared_ptr<SqliteStatementImpl>*>(out->private_data);
    auto* impl = ptr->get();

    sqlite3_stmt* stmt = nullptr;
    auto rc = sqlite3_prepare_v2(db_, query, static_cast<int>(std::strlen(query)), &stmt,
                                 /*pzTail=*/nullptr);
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

    impl->Init(db_, stmt);
    return ADBC_STATUS_OK;
  }

 private:
  std::shared_ptr<SqliteDatabaseImpl> database_;
  sqlite3* db_;
};

}  // namespace

ADBC_DRIVER_EXPORT
void AdbcErrorRelease(struct AdbcError* error) {
  delete[] error->message;
  error->message = nullptr;
}

ADBC_DRIVER_EXPORT
AdbcStatusCode AdbcDatabaseInit(const struct AdbcDatabaseOptions* options,
                                struct AdbcDatabase* out, struct AdbcError* error) {
  sqlite3* db = nullptr;
  auto status = sqlite3_open_v2(
      ":memory:", &db, SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE, /*zVfs=*/nullptr);
  if (status != SQLITE_OK) {
    if (db) {
      SetError(db, "sqlite3_open_v2", error);
    }
    return ADBC_STATUS_UNKNOWN;
  }

  auto impl = std::make_shared<SqliteDatabaseImpl>(db);
  out->private_data = new std::shared_ptr<SqliteDatabaseImpl>(impl);
  return ADBC_STATUS_OK;
}

ADBC_DRIVER_EXPORT
AdbcStatusCode AdbcDatabaseRelease(struct AdbcDatabase* database,
                                   struct AdbcError* error) {
  if (!database->private_data) return ADBC_STATUS_UNINITIALIZED;
  auto ptr =
      reinterpret_cast<std::shared_ptr<SqliteDatabaseImpl>*>(database->private_data);
  AdbcStatusCode status = (*ptr)->Release(error);
  delete ptr;
  database->private_data = nullptr;
  return status;
}

ADBC_DRIVER_EXPORT
AdbcStatusCode AdbcConnectionInit(const struct AdbcConnectionOptions* options,
                                  struct AdbcConnection* out, struct AdbcError* error) {
  if (!options->database || !options->database->private_data) {
    SetError(error, "Must provide database");
    return ADBC_STATUS_INVALID_ARGUMENT;
  }
  auto ptr = reinterpret_cast<std::shared_ptr<SqliteDatabaseImpl>*>(
      options->database->private_data);
  auto impl = std::make_shared<SqliteConnectionImpl>(*ptr);
  out->private_data = new std::shared_ptr<SqliteConnectionImpl>(impl);
  return ADBC_STATUS_OK;
}

ADBC_DRIVER_EXPORT
AdbcStatusCode AdbcConnectionRelease(struct AdbcConnection* connection,
                                     struct AdbcError* error) {
  if (!connection->private_data) return ADBC_STATUS_UNINITIALIZED;
  auto ptr =
      reinterpret_cast<std::shared_ptr<SqliteConnectionImpl>*>(connection->private_data);
  AdbcStatusCode status = (*ptr)->Release(error);
  delete ptr;
  connection->private_data = nullptr;
  return status;
}

ADBC_DRIVER_EXPORT
AdbcStatusCode AdbcConnectionSqlExecute(struct AdbcConnection* connection,
                                        const char* query, struct AdbcStatement* out,
                                        struct AdbcError* error) {
  if (!connection->private_data) return ADBC_STATUS_UNINITIALIZED;
  auto* ptr =
      reinterpret_cast<std::shared_ptr<SqliteConnectionImpl>*>(connection->private_data);
  return (*ptr)->SqlExecute(query, out, error);
}

ADBC_DRIVER_EXPORT
AdbcStatusCode AdbcConnectionSqlPrepare(struct AdbcConnection* connection,
                                        const char* query, struct AdbcStatement* out,
                                        struct AdbcError* error) {
  if (!connection->private_data) return ADBC_STATUS_UNINITIALIZED;
  auto* ptr =
      reinterpret_cast<std::shared_ptr<SqliteConnectionImpl>*>(connection->private_data);
  return (*ptr)->SqlPrepare(query, out, error);
}

ADBC_DRIVER_EXPORT
AdbcStatusCode AdbcStatementGetPartitionDesc(struct AdbcStatement* statement,
                                             uint8_t* partition_desc,
                                             struct AdbcError* error) {
  return ADBC_STATUS_NOT_IMPLEMENTED;
}

ADBC_DRIVER_EXPORT
AdbcStatusCode AdbcStatementGetPartitionDescSize(struct AdbcStatement* statement,
                                                 size_t* length,
                                                 struct AdbcError* error) {
  return ADBC_STATUS_NOT_IMPLEMENTED;
}

ADBC_DRIVER_EXPORT
AdbcStatusCode AdbcStatementBind(struct AdbcStatement* statement,
                                 struct ArrowArray* values, struct ArrowSchema* schema,
                                 struct AdbcError* error) {
  if (!statement->private_data) return ADBC_STATUS_UNINITIALIZED;
  auto* ptr =
      reinterpret_cast<std::shared_ptr<SqliteStatementImpl>*>(statement->private_data);
  return (*ptr)->Bind(*ptr, values, schema, error);
}

ADBC_DRIVER_EXPORT
AdbcStatusCode AdbcStatementExecute(struct AdbcStatement* statement,
                                    struct AdbcError* error) {
  if (!statement->private_data) return ADBC_STATUS_UNINITIALIZED;
  auto* ptr =
      reinterpret_cast<std::shared_ptr<SqliteStatementImpl>*>(statement->private_data);
  return (*ptr)->Execute(*ptr, error);
}

ADBC_DRIVER_EXPORT
AdbcStatusCode AdbcStatementGetStream(struct AdbcStatement* statement,
                                      struct ArrowArrayStream* out,
                                      struct AdbcError* error) {
  if (!statement->private_data) return ADBC_STATUS_UNINITIALIZED;
  auto* ptr =
      reinterpret_cast<std::shared_ptr<SqliteStatementImpl>*>(statement->private_data);
  return (*ptr)->GetStream(*ptr, out, error);
}

ADBC_DRIVER_EXPORT
AdbcStatusCode AdbcStatementInit(struct AdbcConnection* connection,
                                 struct AdbcStatement* statement,
                                 struct AdbcError* error) {
  auto impl = std::make_shared<SqliteStatementImpl>();
  statement->private_data = new std::shared_ptr<SqliteStatementImpl>(impl);
  return ADBC_STATUS_OK;
}

ADBC_DRIVER_EXPORT
AdbcStatusCode AdbcStatementRelease(struct AdbcStatement* statement,
                                    struct AdbcError* error) {
  if (!statement->private_data) return ADBC_STATUS_UNINITIALIZED;
  auto* ptr =
      reinterpret_cast<std::shared_ptr<SqliteStatementImpl>*>(statement->private_data);
  auto status = (*ptr)->Close(error);
  delete ptr;
  statement->private_data = nullptr;
  return status;
}

extern "C" {
ARROW_EXPORT
AdbcStatusCode AdbcSqliteDriverInit(size_t count, struct AdbcDriver* driver,
                                    size_t* initialized) {
  if (count < ADBC_VERSION_0_0_1) return ADBC_STATUS_NOT_IMPLEMENTED;

  std::memset(driver, 0, sizeof(*driver));
  driver->ErrorRelease = AdbcErrorRelease;
  driver->DatabaseInit = AdbcDatabaseInit;
  driver->DatabaseRelease = AdbcDatabaseRelease;
  driver->ConnectionInit = AdbcConnectionInit;
  driver->ConnectionRelease = AdbcConnectionRelease;
  driver->ConnectionSqlExecute = AdbcConnectionSqlExecute;
  driver->ConnectionSqlPrepare = AdbcConnectionSqlPrepare;
  driver->StatementBind = AdbcStatementBind;
  driver->StatementExecute = AdbcStatementExecute;
  driver->StatementGetPartitionDesc = AdbcStatementGetPartitionDesc;
  driver->StatementGetPartitionDescSize = AdbcStatementGetPartitionDescSize;
  driver->StatementGetStream = AdbcStatementGetStream;
  driver->StatementInit = AdbcStatementInit;
  driver->StatementRelease = AdbcStatementRelease;
  *initialized = ADBC_VERSION_0_0_1;
  return ADBC_STATUS_OK;
}
}
