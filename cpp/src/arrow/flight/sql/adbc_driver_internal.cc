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

#include "arrow/flight/sql/adbc_driver_internal.h"

#include "arrow/c/bridge.h"

namespace arrow::flight::sql {

AdbcStatusCode ArrowToAdbcStatusCode(const Status& status) {
  if (auto detail = FlightStatusDetail::UnwrapStatus(status)) {
    switch (detail->code()) {
      case FlightStatusCode::Internal:
        return ADBC_STATUS_INTERNAL;
      case FlightStatusCode::TimedOut:
        return ADBC_STATUS_TIMEOUT;
      case FlightStatusCode::Cancelled:
        return ADBC_STATUS_CANCELLED;
      case FlightStatusCode::Unauthenticated:
        return ADBC_STATUS_UNAUTHENTICATED;
      case FlightStatusCode::Unauthorized:
        return ADBC_STATUS_UNAUTHORIZED;
      case FlightStatusCode::Unavailable:
        return ADBC_STATUS_IO;
      case FlightStatusCode::Failed:
        return ADBC_STATUS_INTERNAL;
      default:
        break;
    }
  }
  switch (status.code()) {
    case StatusCode::OK:
      return ADBC_STATUS_OK;
    case StatusCode::OutOfMemory:
      return ADBC_STATUS_INTERNAL;
    case StatusCode::KeyError:
      return ADBC_STATUS_NOT_FOUND;
    case StatusCode::TypeError:
      return ADBC_STATUS_INVALID_ARGUMENT;
    case StatusCode::Invalid:
      return ADBC_STATUS_INVALID_ARGUMENT;
    case StatusCode::IOError:
      return ADBC_STATUS_IO;
    case StatusCode::CapacityError:
      return ADBC_STATUS_INTERNAL;
    case StatusCode::IndexError:
      return ADBC_STATUS_INVALID_ARGUMENT;
    case StatusCode::Cancelled:
      return ADBC_STATUS_CANCELLED;
    case StatusCode::UnknownError:
      return ADBC_STATUS_UNKNOWN;
    case StatusCode::NotImplemented:
      return ADBC_STATUS_NOT_IMPLEMENTED;
    case StatusCode::SerializationError:
    case StatusCode::RError:
    case StatusCode::CodeGenError:
    case StatusCode::ExpressionValidationError:
    case StatusCode::ExecutionError:
      return ADBC_STATUS_INTERNAL;
    case StatusCode::AlreadyExists:
      return ADBC_STATUS_INVALID_STATE;
    default:
      break;
  }
  return ADBC_STATUS_UNKNOWN;
}

void ReleaseError(struct AdbcError* error) {
  if (error->message) {
    delete[] error->message;
    error->message = nullptr;
  }
}

FlightClientOptions DefaultClientOptions() {
  FlightClientOptions client_options = FlightClientOptions::Defaults();
  client_options.middleware.push_back(MakeTracingClientMiddlewareFactory());
  client_options.generic_options.emplace_back("grpc.primary_user_agent",
                                              "ADBC Flight SQL " ARROW_VERSION_STRING);
  return client_options;
}

Status IndexDbSchemas(RecordBatchReader* reader, TableIndex* table_index) {
  // TODO: unit test
  ReaderIterator db_schemas(*SqlSchema::GetDbSchemasSchema(), reader);
  ARROW_RETURN_NOT_OK(db_schemas.Init());
  while (true) {
    ARROW_ASSIGN_OR_RAISE(bool have_data, db_schemas.Next());
    if (!have_data) break;
    std::optional<std::string> catalog(db_schemas.GetNullable<StringType>(0));
    std::optional<std::string> schema(db_schemas.GetNullable<StringType>(1));

    auto it = table_index->insert({catalog, {}});
    it.first->second.insert({schema, {0, 0}});
  }
  return Status::OK();
}

Status IndexTables(RecordBatchReader* reader, TableIndex* table_index,
                   std::shared_ptr<RecordBatch>* table_data) {
  ReaderIterator tables(*SqlSchema::GetTablesSchemaWithIncludedSchema(), reader);
  ARROW_RETURN_NOT_OK(tables.Init());

  int64_t start_index = 0;
  int64_t index = 0;
  RecordBatchVector batches;

  std::optional<std::string> catalog_name;
  std::optional<std::string> db_schema_name;
  bool first = true;

  while (true) {
    ARROW_ASSIGN_OR_RAISE(bool have_data, tables.Next());
    if (!have_data) break;

    if (std::shared_ptr<RecordBatch> batch = tables.NewBatch(); batch != nullptr) {
      batches.push_back(std::move(batch));
    }

    std::optional<std::string> catalog(tables.GetNullable<StringType>(0));
    std::optional<std::string> schema(tables.GetNullable<StringType>(1));

    if (first || catalog != catalog_name || schema != db_schema_name) {
      if (!first) {
        auto it = table_index->find(catalog_name);
        if (it != table_index->end()) {
          it->second.insert_or_assign(db_schema_name, std::make_pair(start_index, index));
        }
      }
      catalog_name = std::move(catalog);
      db_schema_name = std::move(schema);
      start_index = index;
      first = false;
    }

    index++;
  }
  if (!first) {
    auto it = table_index->find(catalog_name);
    if (it != table_index->end()) {
      it->second.insert_or_assign(db_schema_name, std::make_pair(start_index, index));
    }
  }

  ARROW_ASSIGN_OR_RAISE(auto all_data, Table::FromRecordBatches(
                                           SqlSchema::GetTablesSchemaWithIncludedSchema(),
                                           std::move(batches)));
  ARROW_ASSIGN_OR_RAISE(*table_data, all_data->CombineChunksToBatch());
  return Status::OK();
}

Status ExportRecordBatches(std::shared_ptr<Schema> schema, RecordBatchVector batches,
                           struct ArrowArrayStream* stream) {
  std::shared_ptr<RecordBatchReader> reader;
  ARROW_ASSIGN_OR_RAISE(reader,
                        RecordBatchReader::Make(std::move(batches), std::move(schema)));
  return arrow::ExportRecordBatchReader(std::move(reader), stream);
}

bool IsApproximatelyValidIdentifier(std::string_view name) {
  if (name.empty()) return false;
  // First character must be a letter or underscore
  if (!(name[0] == '_' || (name[0] >= 'a' && name[0] <= 'z') ||
        (name[0] >= 'A' && name[0] <= 'Z'))) {
    return false;
  }
  // Subsequent characters must be underscores, letters, or digits
  for (size_t i = 1; i < name.size(); i++) {
    if (!(name[i] == '_' || (name[i] >= 'a' && name[i] <= 'z') ||
          (name[i] >= 'A' && name[i] <= 'Z') || (name[i] >= '0' && name[i] <= '9'))) {
      return false;
    }
  }
  return true;
}

}  // namespace arrow::flight::sql
