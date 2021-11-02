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

#include <arrow/api.h>
#include <arrow/flight/flight_sql/example/sqlite_server.h>
#include <arrow/flight/flight_sql/example/sqlite_statement.h>
#include <arrow/flight/flight_sql/example/sqlite_statement_batch_reader.h>
#include <arrow/flight/flight_sql/example/sqlite_tables_schema_batch_reader.h>
#include <arrow/flight/flight_sql/server.h>
#include <sqlite3.h>

#include <boost/algorithm/string.hpp>
#include <boost/lexical_cast.hpp>
#include <boost/uuid/uuid.hpp>
#include <boost/uuid/uuid_io.hpp>
#include <sstream>

namespace arrow {
namespace flight {
namespace sql {
namespace example {

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
    return null();
  }
}

std::string PrepareQueryForGetTables(const GetTables& command) {
  std::stringstream table_query;

  table_query << "SELECT null as catalog_name, null as schema_name, name as "
                 "table_name, type as table_type FROM sqlite_master where 1=1";

  if (command.catalog.has_value()) {
    table_query << " and catalog_name='" << command.catalog.value() << "'";
  }

  if (command.schema_filter_pattern.has_value()) {
    table_query << " and schema_name LIKE '" << command.schema_filter_pattern.value()
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

SQLiteFlightSqlServer::SQLiteFlightSqlServer(sqlite3* db) : db_(db), uuid_generator_({}) {
  assert(db_);
}

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

    return Status::RError(err_msg);
  }

  std::shared_ptr<SQLiteFlightSqlServer> result(new SQLiteFlightSqlServer(db));

  ARROW_UNUSED(result->ExecuteSql(R"(
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
  )"));

  return result;
}

SQLiteFlightSqlServer::~SQLiteFlightSqlServer() { sqlite3_close(db_); }

Status SQLiteFlightSqlServer::ExecuteSql(const std::string& sql) {
  char* err_msg = nullptr;
  int rc = sqlite3_exec(db_, sql.c_str(), nullptr, nullptr, &err_msg);
  if (rc != SQLITE_OK) {
    std::string error_msg;
    if (err_msg != nullptr) {
      error_msg = err_msg;
    }
    sqlite3_free(err_msg);
    return Status::RError(error_msg);
  }
  return Status::OK();
}

Status DoGetSQLiteQuery(sqlite3* db, const std::string& query,
                        const std::shared_ptr<Schema>& schema,
                        std::unique_ptr<FlightDataStream>* result) {
  std::shared_ptr<SqliteStatement> statement;

  ARROW_ASSIGN_OR_RAISE(statement, SqliteStatement::Create(db, query));

  std::shared_ptr<SqliteStatementBatchReader> reader;
  ARROW_ASSIGN_OR_RAISE(reader, SqliteStatementBatchReader::Create(statement, schema));

  *result = std::unique_ptr<FlightDataStream>(new RecordBatchStream(reader));

  return Status::OK();
}

Status GetFlightInfoForCommand(const FlightDescriptor& descriptor,
                               std::unique_ptr<FlightInfo>* info,
                               const std::shared_ptr<Schema>& schema) {
  std::vector<FlightEndpoint> endpoints{FlightEndpoint{{descriptor.cmd}, {}}};
  ARROW_ASSIGN_OR_RAISE(auto result,
                        FlightInfo::Make(*schema, descriptor, endpoints, -1, -1))

  *info = std::unique_ptr<FlightInfo>(new FlightInfo(result));

  return Status::OK();
}

Status SQLiteFlightSqlServer::GetFlightInfoStatement(const StatementQuery& command,
                                                     const ServerCallContext& context,
                                                     const FlightDescriptor& descriptor,
                                                     std::unique_ptr<FlightInfo>* info) {
  const std::string& query = command.query;

  std::shared_ptr<SqliteStatement> statement;
  ARROW_ASSIGN_OR_RAISE(statement, SqliteStatement::Create(db_, query));

  std::shared_ptr<Schema> schema;
  ARROW_RETURN_NOT_OK(statement->GetSchema(&schema));

  std::string ticket_string = CreateStatementQueryTicket(query);
  std::vector<FlightEndpoint> endpoints{FlightEndpoint{{ticket_string}, {}}};
  ARROW_ASSIGN_OR_RAISE(auto result,
                        FlightInfo::Make(*schema, descriptor, endpoints, -1, -1))

  *info = std::unique_ptr<FlightInfo>(new FlightInfo(result));

  return Status::OK();
}

Status SQLiteFlightSqlServer::DoGetStatement(const StatementQueryTicket& command,
                                             const ServerCallContext& context,
                                             std::unique_ptr<FlightDataStream>* result) {
  const std::string& sql = command.statement_handle;

  std::shared_ptr<SqliteStatement> statement;
  ARROW_ASSIGN_OR_RAISE(statement, SqliteStatement::Create(db_, sql));

  std::shared_ptr<SqliteStatementBatchReader> reader;
  ARROW_ASSIGN_OR_RAISE(reader, SqliteStatementBatchReader::Create(statement));

  *result = std::unique_ptr<FlightDataStream>(new RecordBatchStream(reader));

  return Status::OK();
}

Status SQLiteFlightSqlServer::GetFlightInfoCatalogs(const ServerCallContext& context,
                                                    const FlightDescriptor& descriptor,
                                                    std::unique_ptr<FlightInfo>* info) {
  return GetFlightInfoForCommand(descriptor, info, SqlSchema::GetCatalogsSchema());
}

Status SQLiteFlightSqlServer::DoGetCatalogs(const ServerCallContext& context,
                                            std::unique_ptr<FlightDataStream>* result) {
  // As SQLite doesn't support catalogs, this will return an empty record batch.

  const std::shared_ptr<Schema>& schema = SqlSchema::GetCatalogsSchema();

  StringBuilder catalog_name_builder;
  ARROW_ASSIGN_OR_RAISE(auto catalog_name, catalog_name_builder.Finish());

  const std::shared_ptr<RecordBatch>& batch =
      RecordBatch::Make(schema, 0, {catalog_name});

  ARROW_ASSIGN_OR_RAISE(auto reader, RecordBatchReader::Make({batch}));
  *result = std::unique_ptr<FlightDataStream>(new RecordBatchStream(reader));
  return Status::OK();
}

Status SQLiteFlightSqlServer::GetFlightInfoSchemas(const GetSchemas& command,
                                                   const ServerCallContext& context,
                                                   const FlightDescriptor& descriptor,
                                                   std::unique_ptr<FlightInfo>* info) {
  return GetFlightInfoForCommand(descriptor, info, SqlSchema::GetSchemasSchema());
}

Status SQLiteFlightSqlServer::DoGetSchemas(const GetSchemas& command,
                                           const ServerCallContext& context,
                                           std::unique_ptr<FlightDataStream>* result) {
  // As SQLite doesn't support schemas, this will return an empty record batch.

  const std::shared_ptr<Schema>& schema = SqlSchema::GetSchemasSchema();

  StringBuilder catalog_name_builder;
  ARROW_ASSIGN_OR_RAISE(auto catalog_name, catalog_name_builder.Finish());
  StringBuilder schema_name_builder;
  ARROW_ASSIGN_OR_RAISE(auto schema_name, schema_name_builder.Finish());

  const std::shared_ptr<RecordBatch>& batch =
      RecordBatch::Make(schema, 0, {catalog_name, schema_name});

  ARROW_ASSIGN_OR_RAISE(auto reader, RecordBatchReader::Make({batch}));
  *result = std::unique_ptr<FlightDataStream>(new RecordBatchStream(reader));
  return Status::OK();
}

Status SQLiteFlightSqlServer::GetFlightInfoTables(const GetTables& command,
                                                  const ServerCallContext& context,
                                                  const FlightDescriptor& descriptor,
                                                  std::unique_ptr<FlightInfo>* info) {
  std::vector<FlightEndpoint> endpoints{FlightEndpoint{{descriptor.cmd}, {}}};

  bool include_schema = command.include_schema;

  ARROW_ASSIGN_OR_RAISE(
      auto result,
      FlightInfo::Make(include_schema ? *SqlSchema::GetTablesSchemaWithIncludedSchema()
                                      : *SqlSchema::GetTablesSchema(),
                       descriptor, endpoints, -1, -1))
  *info = std::unique_ptr<FlightInfo>(new FlightInfo(result));

  return Status::OK();
}

Status SQLiteFlightSqlServer::DoGetTables(const GetTables& command,
                                          const ServerCallContext& context,
                                          std::unique_ptr<FlightDataStream>* result) {
  std::string query = PrepareQueryForGetTables(command);

  std::shared_ptr<SqliteStatement> statement;
  ARROW_ASSIGN_OR_RAISE(statement, SqliteStatement::Create(db_, query));

  std::shared_ptr<SqliteStatementBatchReader> reader;
  ARROW_ASSIGN_OR_RAISE(reader, SqliteStatementBatchReader::Create(
                                    statement, SqlSchema::GetTablesSchema()));

  if (command.include_schema) {
    std::shared_ptr<SqliteTablesWithSchemaBatchReader> table_schema_reader =
        std::make_shared<SqliteTablesWithSchemaBatchReader>(reader, query, db_);
    *result =
        std::unique_ptr<FlightDataStream>(new RecordBatchStream(table_schema_reader));
  } else {
    *result = std::unique_ptr<FlightDataStream>(new RecordBatchStream(reader));
  }

  return Status::OK();
}

arrow::Result<int64_t> SQLiteFlightSqlServer::DoPutCommandStatementUpdate(
    const StatementUpdate& command, const ServerCallContext& context,
    std::unique_ptr<FlightMessageReader>& reader) {
  const std::string& sql = command.query;

  std::shared_ptr<SqliteStatement> statement;
  ARROW_ASSIGN_OR_RAISE(statement, SqliteStatement::Create(db_, sql));

  int64_t record_count;
  ARROW_RETURN_NOT_OK(statement->ExecuteUpdate(&record_count));

  return record_count;
}

arrow::Result<ActionCreatePreparedStatementResult>
SQLiteFlightSqlServer::CreatePreparedStatement(
    const ActionCreatePreparedStatementRequest& request,
    const ServerCallContext& context) {
  std::shared_ptr<SqliteStatement> statement;
  ARROW_ASSIGN_OR_RAISE(statement, SqliteStatement::Create(db_, request.query));
  boost::uuids::uuid uuid = uuid_generator_();
  prepared_statements_[uuid] = statement;

  std::shared_ptr<Schema> dataset_schema;
  ARROW_RETURN_NOT_OK(statement->GetSchema(&dataset_schema));

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

  ActionCreatePreparedStatementResult result{
      .dataset_schema = dataset_schema,
      .parameter_schema = parameter_schema,
      .prepared_statement_handle = boost::uuids::to_string(uuid)};

  return result;
}

Status SQLiteFlightSqlServer::ClosePreparedStatement(
    const ActionClosePreparedStatementRequest& request, const ServerCallContext& context,
    std::unique_ptr<ResultStream>* result) {
  const std::string& prepared_statement_handle = request.prepared_statement_handle;
  const auto& uuid = boost::lexical_cast<boost::uuids::uuid>(prepared_statement_handle);

  auto search = prepared_statements_.find(uuid);
  if (search != prepared_statements_.end()) {
    prepared_statements_.erase(uuid);
  } else {
    return Status::Invalid("Prepared statement not found");
  }

  // Need to instantiate a ResultStream, otherwise clients can not wait for completion.
  *result = std::unique_ptr<ResultStream>(new SimpleResultStream({}));
  return Status::OK();
}

Status SQLiteFlightSqlServer::GetFlightInfoPreparedStatement(
    const PreparedStatementQuery& command, const ServerCallContext& context,
    const FlightDescriptor& descriptor, std::unique_ptr<FlightInfo>* info) {
  const std::string& prepared_statement_handle = command.prepared_statement_handle;
  const auto& uuid = boost::lexical_cast<boost::uuids::uuid>(prepared_statement_handle);

  auto search = prepared_statements_.find(uuid);
  if (search == prepared_statements_.end()) {
    return Status::Invalid("Prepared statement not found");
  }

  std::shared_ptr<SqliteStatement> statement = search->second;

  std::shared_ptr<Schema> schema;
  ARROW_RETURN_NOT_OK(statement->GetSchema(&schema));

  return GetFlightInfoForCommand(descriptor, info, schema);
}

Status SQLiteFlightSqlServer::DoGetPreparedStatement(
    const PreparedStatementQuery& command, const ServerCallContext& context,
    std::unique_ptr<FlightDataStream>* result) {
  const std::string& prepared_statement_handle = command.prepared_statement_handle;
  const auto& uuid = boost::lexical_cast<boost::uuids::uuid>(prepared_statement_handle);

  auto search = prepared_statements_.find(uuid);
  if (search == prepared_statements_.end()) {
    return Status::Invalid("Prepared statement not found");
  }

  std::shared_ptr<SqliteStatement> statement = search->second;

  std::shared_ptr<SqliteStatementBatchReader> reader;
  ARROW_ASSIGN_OR_RAISE(reader, SqliteStatementBatchReader::Create(statement));

  *result = std::unique_ptr<FlightDataStream>(new RecordBatchStream(reader));

  return Status::OK();
}

Status SQLiteFlightSqlServer::GetStatementByHandle(
    const std::string& prepared_statement_handle,
    std::shared_ptr<SqliteStatement>* result) {
  const auto& uuid = boost::lexical_cast<boost::uuids::uuid>(prepared_statement_handle);

  auto search = prepared_statements_.find(uuid);
  if (search == prepared_statements_.end()) {
    return Status::Invalid("Prepared statement not found");
  }

  *result = search->second;
  return Status::OK();
}

Status SQLiteFlightSqlServer::DoPutPreparedStatementQuery(
    const PreparedStatementQuery& command, const ServerCallContext& context,
    FlightMessageReader* reader, FlightMetadataWriter* writer) {
  const std::string& prepared_statement_handle = command.prepared_statement_handle;
  std::shared_ptr<SqliteStatement> statement;
  ARROW_RETURN_NOT_OK(GetStatementByHandle(prepared_statement_handle, &statement));

  sqlite3_stmt* stmt = statement->GetSqlite3Stmt();
  ARROW_RETURN_NOT_OK(SetParametersOnSQLiteStatement(stmt, reader));

  return Status::OK();
}

arrow::Result<int64_t> SQLiteFlightSqlServer::DoPutPreparedStatementUpdate(
    const PreparedStatementUpdate& command, const ServerCallContext& context,
    FlightMessageReader* reader) {
  const std::string& prepared_statement_handle = command.prepared_statement_handle;

  std::shared_ptr<SqliteStatement> statement;
  ARROW_RETURN_NOT_OK(GetStatementByHandle(prepared_statement_handle, &statement));

  sqlite3_stmt* stmt = statement->GetSqlite3Stmt();
  ARROW_RETURN_NOT_OK(SetParametersOnSQLiteStatement(stmt, reader));

  int64_t record_count;
  ARROW_RETURN_NOT_OK(statement->ExecuteUpdate(&record_count));

  return record_count;
}

Status SQLiteFlightSqlServer::GetFlightInfoTableTypes(const ServerCallContext& context,
                                                      const FlightDescriptor& descriptor,
                                                      std::unique_ptr<FlightInfo>* info) {
  return GetFlightInfoForCommand(descriptor, info, SqlSchema::GetTableTypesSchema());
}

Status SQLiteFlightSqlServer::DoGetTableTypes(const ServerCallContext& context,
                                              std::unique_ptr<FlightDataStream>* result) {
  std::string query = "SELECT DISTINCT type as table_type FROM sqlite_master";

  return DoGetSQLiteQuery(db_, query, SqlSchema::GetTableTypesSchema(), result);
}

Status SQLiteFlightSqlServer::GetFlightInfoPrimaryKeys(
    const GetPrimaryKeys& command, const ServerCallContext& context,
    const FlightDescriptor& descriptor, std::unique_ptr<FlightInfo>* info) {
  return GetFlightInfoForCommand(descriptor, info, SqlSchema::GetPrimaryKeysSchema());
}

Status SQLiteFlightSqlServer::DoGetPrimaryKeys(
    const GetPrimaryKeys& command, const ServerCallContext& context,
    std::unique_ptr<FlightDataStream>* result) {
  std::stringstream table_query;

  // The field key_name can not be recovered by the sqlite, so it is being set
  // to null following the same pattern for catalog_name and schema_name.
  table_query << "SELECT null as catalog_name, null as schema_name, table_name, "
                 "name as column_name,  pk as key_sequence, null as key_name\n"
                 "FROM pragma_table_info(table_name)\n"
                 "    JOIN (SELECT null as catalog_name, null as schema_name, name as "
                 "table_name, type as table_type\n"
                 "FROM sqlite_master) where 1=1 and pk != 0";

  if (command.catalog.has_value()) {
    table_query << " and catalog_name LIKE '" << command.catalog.value() << "'";
  }

  if (command.schema.has_value()) {
    table_query << " and schema_name LIKE '" << command.schema.value() << "'";
  }

  table_query << " and table_name LIKE '" << command.table << "'";

  return DoGetSQLiteQuery(db_, table_query.str(), SqlSchema::GetPrimaryKeysSchema(),
                          result);
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

Status SQLiteFlightSqlServer::GetFlightInfoImportedKeys(
    const GetImportedKeys& command, const ServerCallContext& context,
    const FlightDescriptor& descriptor, std::unique_ptr<FlightInfo>* info) {
  return GetFlightInfoForCommand(descriptor, info, SqlSchema::GetImportedKeysSchema());
}

Status SQLiteFlightSqlServer::DoGetImportedKeys(
    const GetImportedKeys& command, const ServerCallContext& context,
    std::unique_ptr<FlightDataStream>* result) {
  std::string filter = "fk_table_name = '" + command.table + "'";
  if (command.catalog.has_value()) {
    filter += " AND fk_catalog_name = '" + command.catalog.value() + "'";
  }
  if (command.schema.has_value()) {
    filter += " AND fk_schema_name = '" + command.schema.value() + "'";
  }
  std::string query = PrepareQueryForGetImportedOrExportedKeys(filter);

  return DoGetSQLiteQuery(db_, query, SqlSchema::GetImportedKeysSchema(), result);
}

Status SQLiteFlightSqlServer::GetFlightInfoExportedKeys(
    const GetExportedKeys& command, const ServerCallContext& context,
    const FlightDescriptor& descriptor, std::unique_ptr<FlightInfo>* info) {
  return GetFlightInfoForCommand(descriptor, info, SqlSchema::GetExportedKeysSchema());
}

Status SQLiteFlightSqlServer::DoGetExportedKeys(
    const GetExportedKeys& command, const ServerCallContext& context,
    std::unique_ptr<FlightDataStream>* result) {
  std::string filter = "pk_table_name = '" + command.table + "'";
  if (command.catalog.has_value()) {
    filter += " AND pk_catalog_name = '" + command.catalog.value() + "'";
  }
  if (command.schema.has_value()) {
    filter += " AND pk_schema_name = '" + command.schema.value() + "'";
  }
  std::string query = PrepareQueryForGetImportedOrExportedKeys(filter);

  return DoGetSQLiteQuery(db_, query, SqlSchema::GetExportedKeysSchema(), result);
}

Status SQLiteFlightSqlServer::GetFlightInfoCrossReference(
    const GetCrossReference& command, const ServerCallContext& context,
    const FlightDescriptor& descriptor, std::unique_ptr<FlightInfo>* info) {
  return GetFlightInfoForCommand(descriptor, info, SqlSchema::GetCrossReferenceSchema());
}

Status SQLiteFlightSqlServer::DoGetCrossReference(
    const GetCrossReference& command, const ServerCallContext& context,
    std::unique_ptr<FlightDataStream>* result) {
  std::string filter = "pk_table_name = '" + command.pk_table + "'";
  if (command.pk_catalog.has_value()) {
    filter += " AND pk_catalog_name = '" + command.pk_catalog.value() + "'";
  }
  if (command.pk_schema.has_value()) {
    filter += " AND pk_schema_name = '" + command.pk_schema.value() + "'";
  }

  filter += " AND fk_table_name = '" + command.fk_table + "'";
  if (command.fk_catalog.has_value()) {
    filter += " AND fk_catalog_name = '" + command.fk_catalog.value() + "'";
  }
  if (command.fk_schema.has_value()) {
    filter += " AND fk_schema_name = '" + command.fk_schema.value() + "'";
  }
  std::string query = PrepareQueryForGetImportedOrExportedKeys(filter);

  return DoGetSQLiteQuery(db_, query, SqlSchema::GetCrossReferenceSchema(), result);
}

}  // namespace example
}  // namespace sql
}  // namespace flight
}  // namespace arrow
