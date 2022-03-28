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

#include "arrow/flight/sql/example/sqlite_server.h"

#include <sqlite3.h>

#include <boost/algorithm/string.hpp>
#include <map>
#include <random>
#include <sstream>

#include "arrow/api.h"
#include "arrow/flight/sql/example/sqlite_sql_info.h"
#include "arrow/flight/sql/example/sqlite_statement.h"
#include "arrow/flight/sql/example/sqlite_statement_batch_reader.h"
#include "arrow/flight/sql/example/sqlite_tables_schema_batch_reader.h"
#include "arrow/flight/sql/example/sqlite_type_info.h"
#include "arrow/flight/sql/server.h"

namespace arrow {
namespace flight {
namespace sql {
namespace example {

namespace {

/// \brief Gets a SqliteStatement by given handle
arrow::Result<std::shared_ptr<SqliteStatement>> GetStatementByHandle(
    const std::map<std::string, std::shared_ptr<SqliteStatement>>& prepared_statements,
    const std::string& handle) {
  auto search = prepared_statements.find(handle);
  if (search == prepared_statements.end()) {
    return Status::Invalid("Prepared statement not found");
  }

  return search->second;
}

std::string PrepareQueryForGetTables(const GetTables& command) {
  std::stringstream table_query;

  table_query << "SELECT null as catalog_name, null as schema_name, name as "
                 "table_name, type as table_type FROM sqlite_master where 1=1";

  if (command.catalog.has_value()) {
    table_query << " and catalog_name='" << command.catalog.value() << "'";
  }

  if (command.db_schema_filter_pattern.has_value()) {
    table_query << " and schema_name LIKE '" << command.db_schema_filter_pattern.value()
                << "'";
  }

  if (command.table_name_filter_pattern.has_value()) {
    table_query << " and table_name LIKE '" << command.table_name_filter_pattern.value()
                << "'";
  }

  if (!command.table_types.empty()) {
    table_query << " and table_type IN (";
    size_t size = command.table_types.size();
    for (size_t i = 0; i < size; i++) {
      table_query << "'" << command.table_types[i] << "'";
      if (size - 1 != i) {
        table_query << ",";
      }
    }

    table_query << ")";
  }

  table_query << " order by table_name";
  return table_query.str();
}

Status SetParametersOnSQLiteStatement(sqlite3_stmt* stmt, FlightMessageReader* reader) {
  FlightStreamChunk chunk;
  while (true) {
    RETURN_NOT_OK(reader->Next(&chunk));
    std::shared_ptr<RecordBatch>& record_batch = chunk.data;
    if (record_batch == nullptr) break;

    const int64_t num_rows = record_batch->num_rows();
    const int& num_columns = record_batch->num_columns();

    for (int i = 0; i < num_rows; ++i) {
      for (int c = 0; c < num_columns; ++c) {
        const std::shared_ptr<Array>& column = record_batch->column(c);
        ARROW_ASSIGN_OR_RAISE(std::shared_ptr<Scalar> scalar, column->GetScalar(i));

        auto& holder = static_cast<DenseUnionScalar&>(*scalar).value;

        switch (holder->type->id()) {
          case Type::INT64: {
            int64_t value = static_cast<Int64Scalar&>(*holder).value;
            sqlite3_bind_int64(stmt, c + 1, value);
            break;
          }
          case Type::FLOAT: {
            double value = static_cast<FloatScalar&>(*holder).value;
            sqlite3_bind_double(stmt, c + 1, value);
            break;
          }
          case Type::STRING: {
            std::shared_ptr<Buffer> buffer = static_cast<StringScalar&>(*holder).value;
            sqlite3_bind_text(stmt, c + 1, reinterpret_cast<const char*>(buffer->data()),
                              static_cast<int>(buffer->size()), SQLITE_TRANSIENT);
            break;
          }
          case Type::BINARY: {
            std::shared_ptr<Buffer> buffer = static_cast<BinaryScalar&>(*holder).value;
            sqlite3_bind_blob(stmt, c + 1, buffer->data(),
                              static_cast<int>(buffer->size()), SQLITE_TRANSIENT);
            break;
          }
          default:
            return Status::Invalid("Received unsupported data type: ",
                                   holder->type->ToString());
        }
      }
    }
  }

  return Status::OK();
}

arrow::Result<std::unique_ptr<FlightDataStream>> DoGetSQLiteQuery(
    sqlite3* db, const std::string& query, const std::shared_ptr<Schema>& schema) {
  std::shared_ptr<SqliteStatement> statement;

  ARROW_ASSIGN_OR_RAISE(statement, SqliteStatement::Create(db, query));

  std::shared_ptr<SqliteStatementBatchReader> reader;
  ARROW_ASSIGN_OR_RAISE(reader, SqliteStatementBatchReader::Create(statement, schema));

  return std::unique_ptr<FlightDataStream>(new RecordBatchStream(reader));
}

arrow::Result<std::unique_ptr<FlightInfo>> GetFlightInfoForCommand(
    const FlightDescriptor& descriptor, const std::shared_ptr<Schema>& schema) {
  std::vector<FlightEndpoint> endpoints{FlightEndpoint{{descriptor.cmd}, {}}};
  ARROW_ASSIGN_OR_RAISE(auto result,
                        FlightInfo::Make(*schema, descriptor, endpoints, -1, -1))

  return std::unique_ptr<FlightInfo>(new FlightInfo(result));
}

std::string PrepareQueryForGetImportedOrExportedKeys(const std::string& filter) {
  return R"(SELECT * FROM (SELECT NULL AS pk_catalog_name,
    NULL AS pk_schema_name,
    p."table" AS pk_table_name,
    p."to" AS pk_column_name,
    NULL AS fk_catalog_name,
    NULL AS fk_schema_name,
    m.name AS fk_table_name,
    p."from" AS fk_column_name,
    p.seq AS key_sequence,
    NULL AS pk_key_name,
    NULL AS fk_key_name,
    CASE
        WHEN p.on_update = 'CASCADE' THEN 0
        WHEN p.on_update = 'RESTRICT' THEN 1
        WHEN p.on_update = 'SET NULL' THEN 2
        WHEN p.on_update = 'NO ACTION' THEN 3
        WHEN p.on_update = 'SET DEFAULT' THEN 4
    END AS update_rule,
    CASE
        WHEN p.on_delete = 'CASCADE' THEN 0
        WHEN p.on_delete = 'RESTRICT' THEN 1
        WHEN p.on_delete = 'SET NULL' THEN 2
        WHEN p.on_delete = 'NO ACTION' THEN 3
        WHEN p.on_delete = 'SET DEFAULT' THEN 4
    END AS delete_rule
  FROM sqlite_master m
  JOIN pragma_foreign_key_list(m.name) p ON m.name != p."table"
  WHERE m.type = 'table') WHERE )" +
         filter + R"( ORDER BY
  pk_catalog_name, pk_schema_name, pk_table_name, pk_key_name, key_sequence)";
}

}  // namespace

std::shared_ptr<DataType> GetArrowType(const char* sqlite_type) {
  if (sqlite_type == NULLPTR) {
    // SQLite may not know the column type yet.
    return null();
  }

  if (boost::iequals(sqlite_type, "int") || boost::iequals(sqlite_type, "integer")) {
    return int64();
  } else if (boost::iequals(sqlite_type, "REAL")) {
    return float64();
  } else if (boost::iequals(sqlite_type, "BLOB")) {
    return binary();
  } else if (boost::iequals(sqlite_type, "TEXT") ||
             boost::istarts_with(sqlite_type, "char") ||
             boost::istarts_with(sqlite_type, "varchar")) {
    return utf8();
  } else {
    throw std::invalid_argument("Invalid SQLite type: " + std::string(sqlite_type));
  }
}

int32_t GetSqlTypeFromTypeName(const char* sqlite_type) {
  if (sqlite_type == NULLPTR) {
    // SQLite may not know the column type yet.
    return SQLITE_NULL;
  }

  if (boost::iequals(sqlite_type, "int") || boost::iequals(sqlite_type, "integer")) {
    return SQLITE_INTEGER;
  } else if (boost::iequals(sqlite_type, "REAL")) {
    return SQLITE_FLOAT;
  } else if (boost::iequals(sqlite_type, "BLOB")) {
    return SQLITE_BLOB;
  } else if (boost::iequals(sqlite_type, "TEXT") ||
             boost::istarts_with(sqlite_type, "char") ||
             boost::istarts_with(sqlite_type, "varchar")) {
    return SQLITE_TEXT;
  } else {
    return SQLITE_NULL;
  }
}

class SQLiteFlightSqlServer::Impl {
  sqlite3* db_;
  std::map<std::string, std::shared_ptr<SqliteStatement>> prepared_statements_;
  std::default_random_engine gen_;

 public:
  explicit Impl(sqlite3* db) : db_(db) {}

  ~Impl() { sqlite3_close(db_); }

  std::string GenerateRandomString() {
    uint32_t length = 16;

    std::uniform_int_distribution<char> dist('0', 'z');
    std::string ret(length, 0);
    auto get_random_char = [&]() { return dist(gen_); };
    std::generate_n(ret.begin(), length, get_random_char);
    return ret;
  }

  arrow::Result<std::unique_ptr<FlightInfo>> GetFlightInfoStatement(
      const ServerCallContext& context, const StatementQuery& command,
      const FlightDescriptor& descriptor) {
    const std::string& query = command.query;

    ARROW_ASSIGN_OR_RAISE(auto statement, SqliteStatement::Create(db_, query));

    ARROW_ASSIGN_OR_RAISE(auto schema, statement->GetSchema());

    ARROW_ASSIGN_OR_RAISE(auto ticket_string, CreateStatementQueryTicket(query));
    std::vector<FlightEndpoint> endpoints{FlightEndpoint{{ticket_string}, {}}};
    ARROW_ASSIGN_OR_RAISE(auto result,
                          FlightInfo::Make(*schema, descriptor, endpoints, -1, -1))

    return std::unique_ptr<FlightInfo>(new FlightInfo(result));
  }

  arrow::Result<std::unique_ptr<FlightDataStream>> DoGetStatement(
      const ServerCallContext& context, const StatementQueryTicket& command) {
    const std::string& sql = command.statement_handle;

    std::shared_ptr<SqliteStatement> statement;
    ARROW_ASSIGN_OR_RAISE(statement, SqliteStatement::Create(db_, sql));

    std::shared_ptr<SqliteStatementBatchReader> reader;
    ARROW_ASSIGN_OR_RAISE(reader, SqliteStatementBatchReader::Create(statement));

    return std::unique_ptr<FlightDataStream>(new RecordBatchStream(reader));
  }

  arrow::Result<std::unique_ptr<FlightInfo>> GetFlightInfoCatalogs(
      const ServerCallContext& context, const FlightDescriptor& descriptor) {
    return GetFlightInfoForCommand(descriptor, SqlSchema::GetCatalogsSchema());
  }

  arrow::Result<std::unique_ptr<FlightDataStream>> DoGetCatalogs(
      const ServerCallContext& context) {
    // As SQLite doesn't support catalogs, this will return an empty record batch.

    const std::shared_ptr<Schema>& schema = SqlSchema::GetCatalogsSchema();

    StringBuilder catalog_name_builder;
    ARROW_ASSIGN_OR_RAISE(auto catalog_name, catalog_name_builder.Finish());

    const std::shared_ptr<RecordBatch>& batch =
        RecordBatch::Make(schema, 0, {catalog_name});

    ARROW_ASSIGN_OR_RAISE(auto reader, RecordBatchReader::Make({batch}));

    return std::unique_ptr<FlightDataStream>(new RecordBatchStream(reader));
  }

  arrow::Result<std::unique_ptr<FlightInfo>> GetFlightInfoSchemas(
      const ServerCallContext& context, const GetDbSchemas& command,
      const FlightDescriptor& descriptor) {
    return GetFlightInfoForCommand(descriptor, SqlSchema::GetDbSchemasSchema());
  }

  arrow::Result<std::unique_ptr<FlightDataStream>> DoGetDbSchemas(
      const ServerCallContext& context, const GetDbSchemas& command) {
    // As SQLite doesn't support schemas, this will return an empty record batch.

    const std::shared_ptr<Schema>& schema = SqlSchema::GetDbSchemasSchema();

    StringBuilder catalog_name_builder;
    ARROW_ASSIGN_OR_RAISE(auto catalog_name, catalog_name_builder.Finish());
    StringBuilder schema_name_builder;
    ARROW_ASSIGN_OR_RAISE(auto schema_name, schema_name_builder.Finish());

    const std::shared_ptr<RecordBatch>& batch =
        RecordBatch::Make(schema, 0, {catalog_name, schema_name});

    ARROW_ASSIGN_OR_RAISE(auto reader, RecordBatchReader::Make({batch}));

    return std::unique_ptr<FlightDataStream>(new RecordBatchStream(reader));
  }

  arrow::Result<std::unique_ptr<FlightInfo>> GetFlightInfoTables(
      const ServerCallContext& context, const GetTables& command,
      const FlightDescriptor& descriptor) {
    std::vector<FlightEndpoint> endpoints{FlightEndpoint{{descriptor.cmd}, {}}};

    bool include_schema = command.include_schema;

    ARROW_ASSIGN_OR_RAISE(
        auto result,
        FlightInfo::Make(include_schema ? *SqlSchema::GetTablesSchemaWithIncludedSchema()
                                        : *SqlSchema::GetTablesSchema(),
                         descriptor, endpoints, -1, -1))

    return std::unique_ptr<FlightInfo>(new FlightInfo(result));
  }

  arrow::Result<std::unique_ptr<FlightDataStream>> DoGetTables(
      const ServerCallContext& context, const GetTables& command) {
    std::string query = PrepareQueryForGetTables(command);

    std::shared_ptr<SqliteStatement> statement;
    ARROW_ASSIGN_OR_RAISE(statement, SqliteStatement::Create(db_, query));

    std::shared_ptr<SqliteStatementBatchReader> reader;
    ARROW_ASSIGN_OR_RAISE(reader, SqliteStatementBatchReader::Create(
                                      statement, SqlSchema::GetTablesSchema()));

    if (command.include_schema) {
      std::shared_ptr<SqliteTablesWithSchemaBatchReader> table_schema_reader =
          std::make_shared<SqliteTablesWithSchemaBatchReader>(reader, query, db_);
      return std::unique_ptr<FlightDataStream>(
          new RecordBatchStream(table_schema_reader));
    } else {
      return std::unique_ptr<FlightDataStream>(new RecordBatchStream(reader));
    }
  }

  arrow::Result<int64_t> DoPutCommandStatementUpdate(const ServerCallContext& context,
                                                     const StatementUpdate& command) {
    const std::string& sql = command.query;

    ARROW_ASSIGN_OR_RAISE(auto statement, SqliteStatement::Create(db_, sql));

    return statement->ExecuteUpdate();
  }

  arrow::Result<ActionCreatePreparedStatementResult> CreatePreparedStatement(
      const ServerCallContext& context,
      const ActionCreatePreparedStatementRequest& request) {
    std::shared_ptr<SqliteStatement> statement;
    ARROW_ASSIGN_OR_RAISE(statement, SqliteStatement::Create(db_, request.query));
    const std::string handle = GenerateRandomString();
    prepared_statements_[handle] = statement;

    ARROW_ASSIGN_OR_RAISE(auto dataset_schema, statement->GetSchema());

    sqlite3_stmt* stmt = statement->GetSqlite3Stmt();
    const int parameter_count = sqlite3_bind_parameter_count(stmt);
    std::vector<std::shared_ptr<arrow::Field>> parameter_fields;
    parameter_fields.reserve(parameter_count);

    // As SQLite doesn't know the parameter types before executing the query, the
    // example server is accepting any SQLite supported type as input by using a dense
    // union.
    const std::shared_ptr<DataType>& dense_union_type = GetUnknownColumnDataType();

    for (int i = 0; i < parameter_count; i++) {
      const char* parameter_name_chars = sqlite3_bind_parameter_name(stmt, i + 1);
      std::string parameter_name;
      if (parameter_name_chars == NULLPTR) {
        parameter_name = std::string("parameter_") + std::to_string(i + 1);
      } else {
        parameter_name = parameter_name_chars;
      }
      parameter_fields.push_back(field(parameter_name, dense_union_type));
    }

    const std::shared_ptr<Schema>& parameter_schema = arrow::schema(parameter_fields);

    ActionCreatePreparedStatementResult result{.dataset_schema = dataset_schema,
                                               .parameter_schema = parameter_schema,
                                               .prepared_statement_handle = handle};

    return result;
  }

  Status ClosePreparedStatement(const ServerCallContext& context,
                                const ActionClosePreparedStatementRequest& request) {
    const std::string& prepared_statement_handle = request.prepared_statement_handle;

    auto search = prepared_statements_.find(prepared_statement_handle);
    if (search != prepared_statements_.end()) {
      prepared_statements_.erase(prepared_statement_handle);
    } else {
      return Status::Invalid("Prepared statement not found");
    }

    return Status::OK();
  }

  arrow::Result<std::unique_ptr<FlightInfo>> GetFlightInfoPreparedStatement(
      const ServerCallContext& context, const PreparedStatementQuery& command,
      const FlightDescriptor& descriptor) {
    const std::string& prepared_statement_handle = command.prepared_statement_handle;

    auto search = prepared_statements_.find(prepared_statement_handle);
    if (search == prepared_statements_.end()) {
      return Status::Invalid("Prepared statement not found");
    }

    std::shared_ptr<SqliteStatement> statement = search->second;

    ARROW_ASSIGN_OR_RAISE(auto schema, statement->GetSchema());

    return GetFlightInfoForCommand(descriptor, schema);
  }

  arrow::Result<std::unique_ptr<FlightDataStream>> DoGetPreparedStatement(
      const ServerCallContext& context, const PreparedStatementQuery& command) {
    const std::string& prepared_statement_handle = command.prepared_statement_handle;

    auto search = prepared_statements_.find(prepared_statement_handle);
    if (search == prepared_statements_.end()) {
      return Status::Invalid("Prepared statement not found");
    }

    std::shared_ptr<SqliteStatement> statement = search->second;

    std::shared_ptr<SqliteStatementBatchReader> reader;
    ARROW_ASSIGN_OR_RAISE(reader, SqliteStatementBatchReader::Create(statement));

    return std::unique_ptr<FlightDataStream>(new RecordBatchStream(reader));
  }

  Status DoPutPreparedStatementQuery(const ServerCallContext& context,
                                     const PreparedStatementQuery& command,
                                     FlightMessageReader* reader,
                                     FlightMetadataWriter* writer) {
    const std::string& prepared_statement_handle = command.prepared_statement_handle;
    ARROW_ASSIGN_OR_RAISE(
        auto statement,
        GetStatementByHandle(prepared_statements_, prepared_statement_handle));

    sqlite3_stmt* stmt = statement->GetSqlite3Stmt();
    ARROW_RETURN_NOT_OK(SetParametersOnSQLiteStatement(stmt, reader));

    return Status::OK();
  }

  arrow::Result<int64_t> DoPutPreparedStatementUpdate(
      const ServerCallContext& context, const PreparedStatementUpdate& command,
      FlightMessageReader* reader) {
    const std::string& prepared_statement_handle = command.prepared_statement_handle;
    ARROW_ASSIGN_OR_RAISE(
        auto statement,
        GetStatementByHandle(prepared_statements_, prepared_statement_handle));

    sqlite3_stmt* stmt = statement->GetSqlite3Stmt();
    ARROW_RETURN_NOT_OK(SetParametersOnSQLiteStatement(stmt, reader));

    return statement->ExecuteUpdate();
  }

  arrow::Result<std::unique_ptr<FlightInfo>> GetFlightInfoTableTypes(
      const ServerCallContext& context, const FlightDescriptor& descriptor) {
    return GetFlightInfoForCommand(descriptor, SqlSchema::GetTableTypesSchema());
  }

  arrow::Result<std::unique_ptr<FlightDataStream>> DoGetTableTypes(
      const ServerCallContext& context) {
    std::string query = "SELECT DISTINCT type as table_type FROM sqlite_master";

    return DoGetSQLiteQuery(db_, query, SqlSchema::GetTableTypesSchema());
  }

  arrow::Result<std::unique_ptr<FlightInfo>> GetFlightInfoTypeInfo(
      const ServerCallContext& context, const GetXdbcTypeInfo& command,
      const FlightDescriptor& descriptor) {
    return GetFlightInfoForCommand(descriptor, SqlSchema::GetXdbcTypeInfoSchema());
  }

  arrow::Result<std::unique_ptr<FlightDataStream>> DoGetTypeInfo(
      const ServerCallContext& context, const GetXdbcTypeInfo& command) {
    const std::shared_ptr<RecordBatch>& type_info_result =
        command.data_type.has_value() ? DoGetTypeInfoResult(command.data_type.value())
                                      : DoGetTypeInfoResult();

    ARROW_ASSIGN_OR_RAISE(auto reader, RecordBatchReader::Make({type_info_result}));
    return std::unique_ptr<FlightDataStream>(new RecordBatchStream(reader));
  }

  arrow::Result<std::unique_ptr<FlightInfo>> GetFlightInfoPrimaryKeys(
      const ServerCallContext& context, const GetPrimaryKeys& command,
      const FlightDescriptor& descriptor) {
    return GetFlightInfoForCommand(descriptor, SqlSchema::GetPrimaryKeysSchema());
  }

  arrow::Result<std::unique_ptr<FlightDataStream>> DoGetPrimaryKeys(
      const ServerCallContext& context, const GetPrimaryKeys& command) {
    std::stringstream table_query;

    // The field key_name can not be recovered by the sqlite, so it is being set
    // to null following the same pattern for catalog_name and schema_name.
    table_query << "SELECT null as catalog_name, null as schema_name, table_name, "
                   "name as column_name,  pk as key_sequence, null as key_name\n"
                   "FROM pragma_table_info(table_name)\n"
                   "    JOIN (SELECT null as catalog_name, null as schema_name, name as "
                   "table_name, type as table_type\n"
                   "FROM sqlite_master) where 1=1 and pk != 0";

    const TableRef& table_ref = command.table_ref;
    if (table_ref.catalog.has_value()) {
      table_query << " and catalog_name LIKE '" << table_ref.catalog.value() << "'";
    }

    if (table_ref.db_schema.has_value()) {
      table_query << " and schema_name LIKE '" << table_ref.db_schema.value() << "'";
    }

    table_query << " and table_name LIKE '" << table_ref.table << "'";

    return DoGetSQLiteQuery(db_, table_query.str(), SqlSchema::GetPrimaryKeysSchema());
  }

  arrow::Result<std::unique_ptr<FlightInfo>> GetFlightInfoImportedKeys(
      const ServerCallContext& context, const GetImportedKeys& command,
      const FlightDescriptor& descriptor) {
    return GetFlightInfoForCommand(descriptor, SqlSchema::GetImportedKeysSchema());
  }

  arrow::Result<std::unique_ptr<FlightDataStream>> DoGetImportedKeys(
      const ServerCallContext& context, const GetImportedKeys& command) {
    const TableRef& table_ref = command.table_ref;
    std::string filter = "fk_table_name = '" + table_ref.table + "'";
    if (table_ref.catalog.has_value()) {
      filter += " AND fk_catalog_name = '" + table_ref.catalog.value() + "'";
    }
    if (table_ref.db_schema.has_value()) {
      filter += " AND fk_schema_name = '" + table_ref.db_schema.value() + "'";
    }
    std::string query = PrepareQueryForGetImportedOrExportedKeys(filter);

    return DoGetSQLiteQuery(db_, query, SqlSchema::GetImportedKeysSchema());
  }

  arrow::Result<std::unique_ptr<FlightInfo>> GetFlightInfoExportedKeys(
      const ServerCallContext& context, const GetExportedKeys& command,
      const FlightDescriptor& descriptor) {
    return GetFlightInfoForCommand(descriptor, SqlSchema::GetExportedKeysSchema());
  }

  arrow::Result<std::unique_ptr<FlightDataStream>> DoGetExportedKeys(
      const ServerCallContext& context, const GetExportedKeys& command) {
    const TableRef& table_ref = command.table_ref;
    std::string filter = "pk_table_name = '" + table_ref.table + "'";
    if (table_ref.catalog.has_value()) {
      filter += " AND pk_catalog_name = '" + table_ref.catalog.value() + "'";
    }
    if (table_ref.db_schema.has_value()) {
      filter += " AND pk_schema_name = '" + table_ref.db_schema.value() + "'";
    }
    std::string query = PrepareQueryForGetImportedOrExportedKeys(filter);

    return DoGetSQLiteQuery(db_, query, SqlSchema::GetExportedKeysSchema());
  }

  arrow::Result<std::unique_ptr<FlightInfo>> GetFlightInfoCrossReference(
      const ServerCallContext& context, const GetCrossReference& command,
      const FlightDescriptor& descriptor) {
    return GetFlightInfoForCommand(descriptor, SqlSchema::GetCrossReferenceSchema());
  }

  arrow::Result<std::unique_ptr<FlightDataStream>> DoGetCrossReference(
      const ServerCallContext& context, const GetCrossReference& command) {
    const TableRef& pk_table_ref = command.pk_table_ref;
    std::string filter = "pk_table_name = '" + pk_table_ref.table + "'";
    if (pk_table_ref.catalog.has_value()) {
      filter += " AND pk_catalog_name = '" + pk_table_ref.catalog.value() + "'";
    }
    if (pk_table_ref.db_schema.has_value()) {
      filter += " AND pk_schema_name = '" + pk_table_ref.db_schema.value() + "'";
    }

    const TableRef& fk_table_ref = command.fk_table_ref;
    filter += " AND fk_table_name = '" + fk_table_ref.table + "'";
    if (fk_table_ref.catalog.has_value()) {
      filter += " AND fk_catalog_name = '" + fk_table_ref.catalog.value() + "'";
    }
    if (fk_table_ref.db_schema.has_value()) {
      filter += " AND fk_schema_name = '" + fk_table_ref.db_schema.value() + "'";
    }
    std::string query = PrepareQueryForGetImportedOrExportedKeys(filter);

    return DoGetSQLiteQuery(db_, query, SqlSchema::GetCrossReferenceSchema());
  }

  Status ExecuteSql(const std::string& sql) {
    char* err_msg = nullptr;
    int rc = sqlite3_exec(db_, sql.c_str(), nullptr, nullptr, &err_msg);
    if (rc != SQLITE_OK) {
      std::string error_msg;
      if (err_msg != nullptr) {
        error_msg = err_msg;
      }
      sqlite3_free(err_msg);
      return Status::ExecutionError(error_msg);
    }
    return Status::OK();
  }
};

SQLiteFlightSqlServer::SQLiteFlightSqlServer(std::shared_ptr<Impl> impl)
    : impl_(std::move(impl)) {}

arrow::Result<std::shared_ptr<SQLiteFlightSqlServer>> SQLiteFlightSqlServer::Create() {
  sqlite3* db = nullptr;

  if (sqlite3_open(":memory:", &db)) {
    std::string err_msg = "Can't open database: ";
    if (db != nullptr) {
      err_msg += sqlite3_errmsg(db);
      sqlite3_close(db);
    } else {
      err_msg += "Unable to start SQLite. Insufficient memory";
    }

    return Status::Invalid(err_msg);
  }

  std::shared_ptr<Impl> impl = std::make_shared<Impl>(db);

  std::shared_ptr<SQLiteFlightSqlServer> result(new SQLiteFlightSqlServer(impl));
  for (const auto& id_to_result : GetSqlInfoResultMap()) {
    result->RegisterSqlInfo(id_to_result.first, id_to_result.second);
  }

  ARROW_RETURN_NOT_OK(result->ExecuteSql(R"(
    CREATE TABLE foreignTable (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    foreignName varchar(100),
    value int);

    CREATE TABLE intTable (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    keyName varchar(100),
    value int,
    foreignId int references foreignTable(id));

    INSERT INTO foreignTable (foreignName, value) VALUES ('keyOne', 1);
    INSERT INTO foreignTable (foreignName, value) VALUES ('keyTwo', 0);
    INSERT INTO foreignTable (foreignName, value) VALUES ('keyThree', -1);
    INSERT INTO intTable (keyName, value, foreignId) VALUES ('one', 1, 1);
    INSERT INTO intTable (keyName, value, foreignId) VALUES ('zero', 0, 1);
    INSERT INTO intTable (keyName, value, foreignId) VALUES ('negative one', -1, 1);
    INSERT INTO intTable (keyName, value, foreignId) VALUES (NULL, NULL, NULL);
  )"));

  return result;
}

SQLiteFlightSqlServer::~SQLiteFlightSqlServer() = default;

Status SQLiteFlightSqlServer::ExecuteSql(const std::string& sql) {
  return impl_->ExecuteSql(sql);
}

arrow::Result<std::unique_ptr<FlightInfo>> SQLiteFlightSqlServer::GetFlightInfoStatement(
    const ServerCallContext& context, const StatementQuery& command,
    const FlightDescriptor& descriptor) {
  return impl_->GetFlightInfoStatement(context, command, descriptor);
}

arrow::Result<std::unique_ptr<FlightDataStream>> SQLiteFlightSqlServer::DoGetStatement(
    const ServerCallContext& context, const StatementQueryTicket& command) {
  return impl_->DoGetStatement(context, command);
}

arrow::Result<std::unique_ptr<FlightInfo>> SQLiteFlightSqlServer::GetFlightInfoCatalogs(
    const ServerCallContext& context, const FlightDescriptor& descriptor) {
  return impl_->GetFlightInfoCatalogs(context, descriptor);
}

arrow::Result<std::unique_ptr<FlightDataStream>> SQLiteFlightSqlServer::DoGetCatalogs(
    const ServerCallContext& context) {
  return impl_->DoGetCatalogs(context);
}

arrow::Result<std::unique_ptr<FlightInfo>> SQLiteFlightSqlServer::GetFlightInfoSchemas(
    const ServerCallContext& context, const GetDbSchemas& command,
    const FlightDescriptor& descriptor) {
  return impl_->GetFlightInfoSchemas(context, command, descriptor);
}

arrow::Result<std::unique_ptr<FlightDataStream>> SQLiteFlightSqlServer::DoGetDbSchemas(
    const ServerCallContext& context, const GetDbSchemas& command) {
  return impl_->DoGetDbSchemas(context, command);
}

arrow::Result<std::unique_ptr<FlightInfo>> SQLiteFlightSqlServer::GetFlightInfoTables(
    const ServerCallContext& context, const GetTables& command,
    const FlightDescriptor& descriptor) {
  return impl_->GetFlightInfoTables(context, command, descriptor);
}

arrow::Result<std::unique_ptr<FlightDataStream>> SQLiteFlightSqlServer::DoGetTables(
    const ServerCallContext& context, const GetTables& command) {
  return impl_->DoGetTables(context, command);
}

arrow::Result<int64_t> SQLiteFlightSqlServer::DoPutCommandStatementUpdate(
    const ServerCallContext& context, const StatementUpdate& command) {
  return impl_->DoPutCommandStatementUpdate(context, command);
}

arrow::Result<ActionCreatePreparedStatementResult>
SQLiteFlightSqlServer::CreatePreparedStatement(
    const ServerCallContext& context,
    const ActionCreatePreparedStatementRequest& request) {
  return impl_->CreatePreparedStatement(context, request);
}

Status SQLiteFlightSqlServer::ClosePreparedStatement(
    const ServerCallContext& context,
    const ActionClosePreparedStatementRequest& request) {
  return impl_->ClosePreparedStatement(context, request);
}

arrow::Result<std::unique_ptr<FlightInfo>>
SQLiteFlightSqlServer::GetFlightInfoPreparedStatement(
    const ServerCallContext& context, const PreparedStatementQuery& command,
    const FlightDescriptor& descriptor) {
  return impl_->GetFlightInfoPreparedStatement(context, command, descriptor);
}

arrow::Result<std::unique_ptr<FlightDataStream>>
SQLiteFlightSqlServer::DoGetPreparedStatement(const ServerCallContext& context,
                                              const PreparedStatementQuery& command) {
  return impl_->DoGetPreparedStatement(context, command);
}

Status SQLiteFlightSqlServer::DoPutPreparedStatementQuery(
    const ServerCallContext& context, const PreparedStatementQuery& command,
    FlightMessageReader* reader, FlightMetadataWriter* writer) {
  return impl_->DoPutPreparedStatementQuery(context, command, reader, writer);
}

arrow::Result<int64_t> SQLiteFlightSqlServer::DoPutPreparedStatementUpdate(
    const ServerCallContext& context, const PreparedStatementUpdate& command,
    FlightMessageReader* reader) {
  return impl_->DoPutPreparedStatementUpdate(context, command, reader);
}

arrow::Result<std::unique_ptr<FlightInfo>> SQLiteFlightSqlServer::GetFlightInfoTableTypes(
    const ServerCallContext& context, const FlightDescriptor& descriptor) {
  return impl_->GetFlightInfoTableTypes(context, descriptor);
}

arrow::Result<std::unique_ptr<FlightDataStream>> SQLiteFlightSqlServer::DoGetTableTypes(
    const ServerCallContext& context) {
  return impl_->DoGetTableTypes(context);
}

arrow::Result<std::unique_ptr<FlightInfo>>
SQLiteFlightSqlServer::GetFlightInfoXdbcTypeInfo(
    const ServerCallContext& context, const arrow::flight::sql::GetXdbcTypeInfo& command,
    const FlightDescriptor& descriptor) {
  return impl_->GetFlightInfoTypeInfo(context, command, descriptor);
}

arrow::Result<std::unique_ptr<FlightDataStream>> SQLiteFlightSqlServer::DoGetXdbcTypeInfo(
    const ServerCallContext& context,
    const arrow::flight::sql::GetXdbcTypeInfo& command) {
  return impl_->DoGetTypeInfo(context, command);
}

arrow::Result<std::unique_ptr<FlightInfo>>
SQLiteFlightSqlServer::GetFlightInfoPrimaryKeys(const ServerCallContext& context,
                                                const GetPrimaryKeys& command,
                                                const FlightDescriptor& descriptor) {
  return impl_->GetFlightInfoPrimaryKeys(context, command, descriptor);
}

arrow::Result<std::unique_ptr<FlightDataStream>> SQLiteFlightSqlServer::DoGetPrimaryKeys(
    const ServerCallContext& context, const GetPrimaryKeys& command) {
  return impl_->DoGetPrimaryKeys(context, command);
}

arrow::Result<std::unique_ptr<FlightInfo>>
SQLiteFlightSqlServer::GetFlightInfoImportedKeys(const ServerCallContext& context,
                                                 const GetImportedKeys& command,
                                                 const FlightDescriptor& descriptor) {
  return impl_->GetFlightInfoImportedKeys(context, command, descriptor);
}

arrow::Result<std::unique_ptr<FlightDataStream>> SQLiteFlightSqlServer::DoGetImportedKeys(
    const ServerCallContext& context, const GetImportedKeys& command) {
  return impl_->DoGetImportedKeys(context, command);
}

arrow::Result<std::unique_ptr<FlightInfo>>
SQLiteFlightSqlServer::GetFlightInfoExportedKeys(const ServerCallContext& context,
                                                 const GetExportedKeys& command,
                                                 const FlightDescriptor& descriptor) {
  return impl_->GetFlightInfoExportedKeys(context, command, descriptor);
}

arrow::Result<std::unique_ptr<FlightDataStream>> SQLiteFlightSqlServer::DoGetExportedKeys(
    const ServerCallContext& context, const GetExportedKeys& command) {
  return impl_->DoGetExportedKeys(context, command);
}

arrow::Result<std::unique_ptr<FlightInfo>>
SQLiteFlightSqlServer::GetFlightInfoCrossReference(const ServerCallContext& context,
                                                   const GetCrossReference& command,
                                                   const FlightDescriptor& descriptor) {
  return impl_->GetFlightInfoCrossReference(context, command, descriptor);
}

arrow::Result<std::unique_ptr<FlightDataStream>>
SQLiteFlightSqlServer::DoGetCrossReference(const ServerCallContext& context,
                                           const GetCrossReference& command) {
  return impl_->DoGetCrossReference(context, command);
}

}  // namespace example
}  // namespace sql
}  // namespace flight
}  // namespace arrow
