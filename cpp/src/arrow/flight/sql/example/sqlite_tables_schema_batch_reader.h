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

#pragma once

#include <sqlite3.h>

#include <memory>
#include <string>

#include "arrow/flight/sql/example/sqlite_statement.h"
#include "arrow/flight/sql/example/sqlite_statement_batch_reader.h"
#include "arrow/record_batch.h"

namespace arrow {
namespace flight {
namespace sql {
namespace example {

class SqliteTablesWithSchemaBatchReader : public RecordBatchReader {
 private:
  std::shared_ptr<example::SqliteStatementBatchReader> reader_;
  std::string main_query_;
  sqlite3* db_;

 public:
  /// Constructor for SqliteTablesWithSchemaBatchReader class
  /// \param reader an shared_ptr from a SqliteStatementBatchReader.
  /// \param main_query  SQL query that originated reader's data.
  /// \param db     a pointer to the sqlite3 db.
  SqliteTablesWithSchemaBatchReader(
      std::shared_ptr<example::SqliteStatementBatchReader> reader, std::string main_query,
      sqlite3* db)
      : reader_(std::move(reader)), main_query_(std::move(main_query)), db_(db) {}

  std::shared_ptr<Schema> schema() const override;

  Status ReadNext(std::shared_ptr<RecordBatch>* batch) override;
};

}  // namespace example
}  // namespace sql
}  // namespace flight
}  // namespace arrow
